"""Transform functions for converting list formats before upload."""

import json
import os
from datetime import datetime, timezone


def get_transform(name):
    """Look up a transform function by name. Raises ValueError if not found."""
    if name not in TRANSFORMS:
        raise ValueError(
            f"Unknown transform '{name}'. Available: {list(TRANSFORMS.keys())}"
        )
    return TRANSFORMS[name]


def disconnect_to_abp(sources, output_dir, options=None):
    """Convert Disconnect blocklist + entitylist JSON to AdBlock Plus format.

    Args:
        sources: dict mapping key -> local file path.
            "blocklist": path to block list file
            "entitylist": path to entity list file
        output_dir: directory to write the output file.
        options: dict with optional keys:
            "categories": list of category names to include (e.g. ["Advertising", "Analytics"]).
                If not specified, all categories are included.

    Returns:
        Path to the produced ABP-format text file.
    """
    options = options or {}

    for key in ("blocklist", "entitylist"):
        if key not in sources:
            raise ValueError(f"disconnect_to_abp: missing required source '{key}'")

    with open(sources["blocklist"]) as f:
        blacklist = json.load(f)
    with open(sources["entitylist"]) as f:
        entitylist = json.load(f)

    # Extract blocking domains from selected categories
    allowed_categories = options.get("categories")
    blocking_domains = set()
    categories = blacklist.get("categories", {})
    for cat_name, entries in categories.items():
        if allowed_categories and cat_name not in allowed_categories:
            continue
        for company_obj in entries:
            for company_name, url_map in company_obj.items():
                if not isinstance(url_map, dict):
                    continue
                for base_url, trackers in url_map.items():
                    if isinstance(trackers, list):
                        blocking_domains.update(trackers)

    # Build a mapping from resource domain -> set of same-entity property
    # domains, so we can fold entity exceptions into domain-restricted
    # blocking rules instead of emitting separate exception rules.
    resource_to_properties = {}
    entities = entitylist.get("entities", {})
    for entity_name, entity_data in entities.items():
        properties = entity_data.get("properties", [])
        resources = entity_data.get("resources", [])
        if properties and resources:
            for resource in resources:
                if resource not in resource_to_properties:
                    resource_to_properties[resource] = set()
                resource_to_properties[resource].update(properties)

    # Generate blocking rules. For domains that have entity data, use
    # $domain=~prop1|~prop2 to exclude same-entity pages directly,
    # avoiding the need for separate exception rules.
    blocking_rules = []
    for domain in sorted(blocking_domains):
        props = resource_to_properties.get(domain)
        if props:
            exclusions = "|".join(f"~{p}" for p in sorted(props))
            blocking_rules.append(f"||{domain}^$domain={exclusions}")
        else:
            blocking_rules.append(f"||{domain}^")

    # Write ABP-format output
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output_path = os.path.join(output_dir, "disconnect-tracking-protection.txt")

    with open(output_path, "w") as f:
        f.write("[Adblock Plus 2.0]\n")
        f.write("! Title: Disconnect Tracking Protection\n")
        f.write(f"! Last modified: {now}\n")
        f.write("\n")
        f.write("! --- Blocking rules ---\n")
        for rule in blocking_rules:
            f.write(rule + "\n")

    print(f"  Transform produced {len(blocking_rules)} blocking rules")
    return output_path


TRANSFORMS = {
    "disconnect_to_abp": disconnect_to_abp,
}
