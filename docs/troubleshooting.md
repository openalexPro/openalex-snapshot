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

- `.openalex-snapshot_metadata/`

Expected structure:

- `.openalex-snapshot_metadata/reports`
- `.openalex-snapshot_metadata/download/{manifests,logs,reports}`
- `.openalex-snapshot_metadata/datasets/<dataset>/{schemata,verify,logs,reports}`

## Schema errors around nested types

Ensure canonical schema cache exists:

- `.<dataset>_metadata/schemata/unified_schema.csv`

If stale, re-run with schema refresh options.
