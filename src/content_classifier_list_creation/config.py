"""YAML config loading and validation."""

import yaml

from .transform import get_transform


def load_config(path):
    """Load and validate YAML config file."""
    with open(path) as f:
        config = yaml.safe_load(f)

    rs = config.get("remote_settings")
    if not rs:
        raise ValueError("Config missing 'remote_settings' section")
    for key in ("server_url", "bucket"):
        if not rs.get(key):
            raise ValueError(f"Config missing 'remote_settings.{key}'")

    default_collection = rs.get("collection")

    lists = config.get("lists")
    if not lists:
        raise ValueError("Config missing 'lists' section or it is empty")
    for i, entry in enumerate(lists):
        name = entry.get("name")
        if not name:
            raise ValueError(f"List entry {i} missing 'name'")

        if not entry.get("collection") and not default_collection:
            raise ValueError(
                f"List entry {i} ('{name}'): no 'collection' specified and no default in remote_settings"
            )

        list_type = entry.get("type")
        if not list_type:
            raise ValueError(f"List entry {i} ('{name}') missing 'type'")

        if list_type in ("abp", "abp_records"):
            if not entry.get("url"):
                raise ValueError(f"List entry {i} ('{name}'): type '{list_type}' requires 'url'")
            if "sources" in entry or "transform" in entry:
                raise ValueError(
                    f"List entry {i} ('{name}'): type '{list_type}' cannot have 'sources'/'transform'"
                )
            max_rules = entry.get("max_rules")
            if max_rules is not None:
                if not isinstance(max_rules, int) or max_rules <= 0:
                    raise ValueError(
                        f"List entry {i} ('{name}'): 'max_rules' must be a positive integer"
                    )
        else:
            if "url" in entry:
                raise ValueError(
                    f"List entry {i} ('{name}'): type '{list_type}' should use 'sources'/'transform', not 'url'"
                )
            if "sources" not in entry or "transform" not in entry:
                raise ValueError(
                    f"List entry {i} ('{name}'): type '{list_type}' requires both 'sources' and 'transform'"
                )
            sources = entry["sources"]
            if not isinstance(sources, list) or len(sources) == 0:
                raise ValueError(
                    f"List entry {i} ('{name}'): 'sources' must be a non-empty list"
                )
            for j, src in enumerate(sources):
                if not isinstance(src, dict) or not src.get("key") or not src.get("url"):
                    raise ValueError(
                        f"List entry {i} ('{name}'), source {j}: must have 'key' and 'url'"
                    )
            get_transform(entry["transform"])

    return config
