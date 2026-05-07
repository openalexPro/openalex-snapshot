# Troubleshooting

## Out of memory in convert

Use:

- `--profile safe`
- `--workers 1`
- explicit `--max-memory-mb`

If needed, convert datasets and files in smaller units.

## Verify appears to slow down over time

Expected when remaining files are larger and `id-hash` is enabled.
Progress is item-based, not byte-based.

## Mixed old/new metadata folders

Current canonical location is:

- `openalex-snapshot_metadata/`

Expected structure:

- `openalex-snapshot_metadata/reports/` — latest report per command
- `openalex-snapshot_metadata/archived/<timestamp>/` — previous runs
- `openalex-snapshot_metadata/download/download.log`
- `openalex-snapshot_metadata/<dataset>/schemata/`
- `openalex-snapshot_metadata/<dataset>/convert/`
- `openalex-snapshot_metadata/<dataset>/conversion-verify/`
- `openalex-snapshot_metadata/<dataset>/index/`
- `openalex-snapshot_metadata/<dataset>/index-verify/`

## Schema errors around nested types

Ensure canonical schema cache exists:

- `.<dataset>_metadata/schemata/unified_schema.csv`

If stale, re-run with schema refresh options.
