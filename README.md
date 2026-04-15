# openalex-snapshot

Standalone CLI for OpenAlex snapshot download, conversion, verification, schema inspection, and indexing.

## Requirements

- `duckdb` available in `PATH` (or pass `--duckdb-bin` where supported)
- `aws` CLI available in `PATH` for `download` / `verify_download`

Version:

- `openalex-snapshot --version`

Argument precedence (highest wins):

1. CLI arguments
2. Config subcommand section values
3. Config `defaults` section values
4. Built-in defaults

## Command Set

- `config`
- `all`
- `check`
- `download`
- `verify_download`
- `convert`
- `verify_convert`
- `schema`
- `verify_schema`
- `index`
- `verify_index`
- `repair_convert`
- `report`
- `prune-reports`
- `progress`
- `skills`

## Quick examples

```bash
# preflight
openalex-snapshot check --root-dir /Volumes/openalex --dataset all

# convert one dataset
openalex-snapshot convert \
  --root-dir /Volumes/openalex \
  --dataset works \
  --profile safe \
  --workers 1

# verify conversion
openalex-snapshot verify_convert \
  --root-dir /Volumes/openalex \
  --dataset works \
  --scope dataset \
  --metadata-level both

# repair from verify report
openalex-snapshot repair_convert \
  --root-dir /Volumes/openalex \
  --from-verify-report /Volumes/openalex/.openalex-snapshot_metadata/reports/verify_convert-123456.json

# bootstrap AI skills folder
openalex-snapshot skills --root-dir /Volumes/openalex
```

## Metadata layout

Under `<root>/.openalex-snapshot_metadata`:

- `reports/` (global)
- `datasets/<dataset>/{schemata,verify,logs,reports}`
- `download/{manifests,logs,reports}`

## Documentation

- `docs/README.md`
- `docs/quickstart.md`
- `docs/commands/`
- `docs/operations/`
- `NEWS.md`
- `ARCHITECTURE_AND_DECISIONS.md`
- `AI_SKILLS_USAGE.md`
