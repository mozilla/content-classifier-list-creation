#!/usr/bin/env python3

import argparse
import json
import os
import sys
import tempfile

import yaml

from .abp_parser import parse_rules
from .config import load_config
from .diff import diff_rules
from .rs import (
    approve_changes,
    batch_create_records,
    batch_delete_records,
    create_client,
    delete_all_records,
    find_record_by_name,
    get_records,
    request_review,
    upload_attachment,
)
from .transform import get_transform
from .utils import download_file


def main():
    parser = argparse.ArgumentParser(
        description="Upload files to a Remote Settings collection as attachments."
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "-t",
        "--auth-token",
        default=os.environ.get("REMOTE_SETTINGS_TOKEN"),
        help="Bearer auth token (default: REMOTE_SETTINGS_TOKEN env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making any requests",
    )
    parser.add_argument(
        "--clear",
        nargs="?",
        const=0,
        type=int,
        metavar="N",
        help="Delete records from the collection and exit. Use --clear for all, --clear N for at most N records.",
    )
    args = parser.parse_args()

    if not args.auth_token and not args.dry_run and args.clear is None:
        parser.error(
            "Auth token required. Use --auth-token or set REMOTE_SETTINGS_TOKEN env var."
        )
    if args.clear is not None and not args.auth_token:
        parser.error(
            "Auth token required for --clear. Use --auth-token or set REMOTE_SETTINGS_TOKEN env var."
        )

    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as e:
        print(f"Error loading config: {e}", file=sys.stderr)
        sys.exit(1)

    rs = config["remote_settings"]
    server_url = rs["server_url"].rstrip("/")
    bucket = rs["bucket"]
    default_collection = rs.get("collection")
    auto_approve = rs.get("auto_approve", False)
    list_entries = config["lists"]

    # Group list entries by collection
    collections = {}
    for entry in list_entries:
        col = entry.get("collection", default_collection)
        collections.setdefault(col, []).append(entry)

    # Build clients and fetch existing records per collection
    clients = {}
    existing_records = {}
    if not args.dry_run:
        for col in collections:
            clients[col] = create_client(server_url, args.auth_token, bucket, col)

    if args.clear is not None:
        clear_limit = args.clear if args.clear > 0 else None
        for col, client in clients.items():
            print(f"\nClearing collection: {col}")
            count = delete_all_records(client, limit=clear_limit)
            if count > 0:
                try:
                    if auto_approve:
                        approve_changes(client)
                    else:
                        request_review(client)
                except Exception as e:
                    print(f"  ERROR during review/approve for '{col}': {e}", file=sys.stderr)
        return

    if not args.dry_run:
        for col, client in clients.items():
            print(f"Fetching existing records from '{col}' ...")
            existing_records[col] = get_records(client)
            print(f"Found {len(existing_records[col])} existing record(s) in '{col}'")

    successes = []
    failures = []
    changed_collections = set()

    with tempfile.TemporaryDirectory() as tmp_dir:
        for col, entries in collections.items():
            client = clients.get(col)
            col_records = existing_records.get(col, [])

            for entry in entries:
                name = entry["name"]
                list_type = entry["type"]

                if list_type == "abp_records":
                    print(f"\nProcessing: {name} -> {col} (per-rule records)")

                    if args.dry_run:
                        print(f"  [dry-run] Would download: {entry['url']}")
                        print(f"  [dry-run] Would parse rules and diff against existing records")
                        print(f"  [dry-run] Would create/delete records as needed for '{name}'")
                        successes.append(name)
                        continue

                    try:
                        filepath = download_file(entry["url"], tmp_dir)
                        new_rules = parse_rules(filepath)
                        max_rules = entry.get("max_rules")
                        if max_rules and len(new_rules) > max_rules:
                            print(f"  Parsed {len(new_rules)} rules, limiting to {max_rules}")
                            new_rules = new_rules[:max_rules]
                        else:
                            print(f"  Parsed {len(new_rules)} rules from {name}")

                        to_create, to_delete = diff_rules(
                            new_rules, col_records, name
                        )
                        print(
                            f"  Diff: {len(to_create)} to create, {len(to_delete)} to delete"
                        )

                        if not to_create and not to_delete:
                            print(f"  No changes needed for '{name}'")
                        else:
                            batch_create_records(client, to_create, name)
                            batch_delete_records(client, to_delete)
                            changed_collections.add(col)

                        successes.append(name)
                    except Exception as e:
                        print(f"  ERROR: {e}", file=sys.stderr)
                        failures.append((name, str(e)))

                else:
                    is_transform = list_type != "abp"
                    record_data = {"Name": name}

                    existing = find_record_by_name(col_records, name)
                    action = "update" if existing else "create"

                    print(f"\nProcessing: {name} -> {col} ({action})")

                    if args.dry_run:
                        if is_transform:
                            for src in entry["sources"]:
                                print(f"  [dry-run] Would download: {src['url']}")
                            print(f"  [dry-run] Would run transform: {entry['transform']}")
                        else:
                            print(f"  [dry-run] Would download: {entry['url']}")
                        print(
                            f"  [dry-run] Would {action} record with data: {json.dumps(record_data)}"
                        )
                        successes.append(name)
                        continue

                    try:
                        record_id = existing["id"] if existing else None

                        if is_transform:
                            source_paths = {}
                            for src in entry["sources"]:
                                path = download_file(src["url"], tmp_dir)
                                source_paths[src["key"]] = path
                            transform_fn = get_transform(entry["transform"])
                            options = entry.get("transform_options", {})
                            filepath = transform_fn(source_paths, tmp_dir, options)
                        else:
                            filepath = download_file(entry["url"], tmp_dir)

                        upload_attachment(client, filepath, record_data, record_id)
                        changed_collections.add(col)
                        successes.append(name)
                    except Exception as e:
                        print(f"  ERROR: {e}", file=sys.stderr)
                        failures.append((name, str(e)))

    # Request review or approve changes per collection
    if not args.dry_run:
        for col in changed_collections:
            try:
                if auto_approve:
                    approve_changes(clients[col])
                else:
                    request_review(clients[col])
            except Exception as e:
                print(f"  ERROR during review/approve for '{col}': {e}", file=sys.stderr)
                failures.append((f"review/approve:{col}", str(e)))

    print("\n--- Summary ---")
    print(f"Succeeded: {len(successes)}")
    if successes:
        for name in successes:
            print(f"  - {name}")
    print(f"Failed: {len(failures)}")
    if failures:
        for name, err in failures:
            print(f"  - {name}: {err}")

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
