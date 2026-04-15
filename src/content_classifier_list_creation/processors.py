"""Processors for each list type."""

import json

from .abp_parser import parse_rules
from .diff import diff_rules
from .rs import (
    batch_create_records,
    batch_delete_records,
    find_record_by_name,
    upload_attachment,
)
from .transform import get_transform
from .utils import download_file

def process_abp_records(entry, client, col_records, tmp_dir, dry_run):
    """Process an abp_records entry: download, parse, diff, batch create/delete.

    Returns True if records were changed on the server.
    """
    name = entry["name"]

    if dry_run:
        print(f"  [dry-run] Would download: {entry['url']}")
        print(f"  [dry-run] Would parse rules and diff against existing records")
        print(f"  [dry-run] Would create/delete records as needed for '{name}'")
        return False

    filepath = download_file(entry["url"], tmp_dir)
    new_rules = parse_rules(filepath)
    max_rules = entry.get("max_rules")
    if max_rules and len(new_rules) > max_rules:
        print(f"  Parsed {len(new_rules)} rules, limiting to {max_rules}")
        new_rules = new_rules[:max_rules]
    else:
        print(f"  Parsed {len(new_rules)} rules from {name}")

    to_create, to_delete = diff_rules(new_rules, col_records, name)
    print(f"  Diff: {len(to_create)} to create, {len(to_delete)} to delete")

    if not to_create and not to_delete:
        print(f"  No changes needed for '{name}'")
        return False

    batch_create_records(client, to_create, name)
    batch_delete_records(client, to_delete)
    return True


def process_disconnect_records(entry, client, col_records, tmp_dir, dry_run):
    """Process a disconnect_records entry: download sources, transform, parse, diff, batch create/delete.

    Returns True if records were changed on the server.
    """
    name = entry["name"]

    if dry_run:
        for src in entry["sources"]:
            print(f"  [dry-run] Would download: {src['url']}")
        print(f"  [dry-run] Would run transform: {entry['transform']}")
        print(f"  [dry-run] Would parse rules and diff against existing records")
        print(f"  [dry-run] Would create/delete records as needed for '{name}'")
        return False

    source_paths = {}
    for src in entry["sources"]:
        path = download_file(src["url"], tmp_dir)
        source_paths[src["key"]] = path
    transform_fn = get_transform(entry["transform"])
    options = entry.get("transform_options", {})
    filepath = transform_fn(source_paths, tmp_dir, options)

    new_rules = parse_rules(filepath)
    max_rules = entry.get("max_rules")
    if max_rules and len(new_rules) > max_rules:
        print(f"  Parsed {len(new_rules)} rules, limiting to {max_rules}")
        new_rules = new_rules[:max_rules]
    else:
        print(f"  Parsed {len(new_rules)} rules from {name}")

    to_create, to_delete = diff_rules(new_rules, col_records, name)
    print(f"  Diff: {len(to_create)} to create, {len(to_delete)} to delete")

    if not to_create and not to_delete:
        print(f"  No changes needed for '{name}'")
        return False

    batch_create_records(client, to_create, name)
    batch_delete_records(client, to_delete)
    return True


def process_attachment(entry, client, col_records, tmp_dir, dry_run):
    """Process an abp or transform entry: download, optionally transform, upload as attachment.

    Returns True if a record was uploaded.
    """
    name = entry["name"]
    list_type = entry["type"]
    is_transform = list_type != "abp"
    record_data = {"Name": name}

    existing = find_record_by_name(col_records, name)
    action = "update" if existing else "create"

    if dry_run:
        if is_transform:
            for src in entry["sources"]:
                print(f"  [dry-run] Would download: {src['url']}")
            print(f"  [dry-run] Would run transform: {entry['transform']}")
        else:
            print(f"  [dry-run] Would download: {entry['url']}")
        print(f"  [dry-run] Would {action} record with data: {json.dumps(record_data)}")
        return False

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
    return True


# Registry mapping list type -> (display label, processor function).
# Types not listed here fall back to process_attachment.
_PROCESSORS = {
    "abp_records": ("per-rule records", process_abp_records),
    "disconnect_records": (
        "per-rule records from transform",
        process_disconnect_records,
    ),
}


def get_processor(list_type):
    """Return (label, process_fn) for a list type, or (None, process_attachment) for fallback."""
    entry = _PROCESSORS.get(list_type)
    if entry:
        label, fn = entry
        return label, fn
    return None, process_attachment
