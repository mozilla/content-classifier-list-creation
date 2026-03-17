"""Diff logic for comparing new ABP rules against existing Remote Settings records."""

from .abp_parser import rule_id


def diff_rules(new_rules, existing_records, list_name):
    """Compare new rules against existing records using deterministic IDs.

    Args:
        new_rules: list of rule strings from the parsed ABP file.
        existing_records: list of record dicts from Remote Settings.
        list_name: the Name value to filter existing records by.

    Returns:
        A tuple (to_create, to_delete) where:
          - to_create is a list of (id, rule_text) tuples for new records.
          - to_delete is a list of record ID strings to remove.
    """
    # Build mapping of expected ID -> rule text for all new rules
    new_ids = {}
    for rule in new_rules:
        rid = rule_id(list_name, rule)
        new_ids[rid] = rule

    # Collect IDs of existing records belonging to this list
    existing_ids = {
        r["id"] for r in existing_records if r.get("Name") == list_name
    }

    ids_to_create = set(new_ids.keys()) - existing_ids
    ids_to_delete = existing_ids - set(new_ids.keys())

    to_create = [(rid, new_ids[rid]) for rid in ids_to_create]
    to_delete = list(ids_to_delete)

    return to_create, to_delete
