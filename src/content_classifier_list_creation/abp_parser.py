"""Parse ABP filter list files into individual rules."""

import uuid

NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "content-classifier-lists")


def rule_id(list_name, rule_text):
    """Generate a deterministic UUID for a rule based on list name and content."""
    return str(uuid.uuid5(NAMESPACE, f"{list_name}:{rule_text}"))


def parse_rules(filepath):
    """Read an ABP filter list file and return a list of rule strings.

    Skips empty lines, comment lines (starting with '!'),
    and ABP header lines (starting with '[Adblock Plus').
    """
    rules = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("!"):
                continue
            if stripped.startswith("[Adblock Plus"):
                continue
            rules.append(stripped)
    return rules
