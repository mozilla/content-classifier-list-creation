#!/usr/bin/env python3

import argparse
import os
import sys
import tempfile

import yaml

from .config import load_config
from .processors import get_processor
from .rs import (
    approve_changes,
    create_client,
    delete_all_records,
    get_records,
    request_review,
)


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

    # Build clients per collection
    clients = {}
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
                    print(
                        f"  ERROR during review/approve for '{col}': {e}",
                        file=sys.stderr,
                    )
        return

    # Fetch existing records per collection
    existing_records = {}
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

                label, process_fn = get_processor(list_type)
                print(f"\nProcessing: {name} -> {col} ({label or 'attachment'})")

                try:
                    changed = process_fn(
                        entry, client, col_records, tmp_dir, args.dry_run
                    )
                    if changed:
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
                print(
                    f"  ERROR during review/approve for '{col}': {e}", file=sys.stderr
                )
                failures.append((f"review/approve:{col}", str(e)))

    print_summary(successes, failures)

    if failures:
        sys.exit(1)


def print_summary(successes, failures):
    print("\n--- Summary ---")
    print(f"Succeeded: {len(successes)}")
    if successes:
        for name in successes:
            print(f"  - {name}")
    print(f"Failed: {len(failures)}")
    if failures:
        for name, err in failures:
            print(f"  - {name}: {err}")


if __name__ == "__main__":
    main()
