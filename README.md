# content-classifier-list-creation

A CLI tool to deploy filter lists to Mozilla Remote Settings for the Content Classifier.

## Usage

```bash
cc-list-creation -c config/dev.yaml -t <auth-token>
```

### Options

| Flag                 | Description                                                |
| -------------------- | ---------------------------------------------------------- |
| `-c`, `--config`     | Path to YAML config file (default: `config.yaml`)          |
| `-t`, `--auth-token` | Bearer auth token (or set `REMOTE_SETTINGS_TOKEN` env var) |
| `--dry-run`          | Print what would be done without making any requests       |
| `--clear`            | Delete all records from the collection(s) and exit         |
| `--clear N`          | Delete at most N records from the collection(s) and exit   |

## Configuration

Config files live in `config/` (dev, stage, prod). Each defines a Remote Settings target and a list of entries to deploy.

```yaml
remote_settings:
  server_url: "https://remote-settings-dev.allizom.org/v1"
  bucket: "main-workspace"
  collection: "content-classifier-lists" # default collection (optional if per-list)
  auto_approve: true # auto-approve changes (default: false)

lists:
  - name: "easylist"
    type: abp_records
    url: "https://easylist.to/easylist/easylist.txt"
    collection: "easylist-rules" # optional per-list collection override
    max_rules: 10000 # optional limit on number of rules
```

### List types

| Type                 | Description                                                                                                       |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `abp`                | Downloads an ABP filter list and uploads it as a single attachment to one record                                  |
| `abp_records`        | Downloads an ABP filter list, parses individual rules, and creates one record per rule with diff-based updates    |
| `disconnect`         | Downloads source JSON files, transforms to ABP format via `disconnect_to_abp`, and uploads as a single attachment |
| `disconnect_records` | Same as `disconnect` but creates one record per rule with diff-based updates                                      |

### Per-rule record types (`abp_records`, `disconnect_records`)

These types use deterministic UUID v5 record IDs derived from the rule content. On each run, the tool diffs the current rules against existing records and only creates new rules or deletes removed ones. This avoids re-uploading unchanged rules.

Optional fields:

- `collection`: override the default collection (supports one-list-one-collection model)
- `max_rules`: limit the maximum number of rules uploaded

## Project structure

```
config/
  dev.yaml            # Dev environment config
  stage.yaml          # Stage environment config
  prod.yaml           # Production environment config
src/content_classifier_list_creation/
  __main__.py          # CLI entry point
  config.py            # YAML config loading and validation
  processors.py        # Processors for each list type
  rs.py                # Remote Settings API wrapper (kinto_http)
  abp_parser.py        # ABP filter list parser and deterministic ID generation
  diff.py              # Diff logic for per-rule records
  transform.py         # Transform functions (disconnect_to_abp)
  utils.py             # Utilities (file download)
```
