# Troubleshooting

## Out of memory in convert

`safe` is the default profile and should handle the largest files (≈1 GB compressed) on
any host via DuckDB spill-to-disk. If you still observe OOM:

- Confirm you're on the default profile: drop `--profile <name>` to inherit `safe`.
- Lower the memory cap so DuckDB spills earlier: `--max-memory-mb 2048`.
- Pre-split very large gz files: `--split-size 256mb` (produces numbered parquets per chunk).
- Isolate the suspect file with `--input-file <rel-path>` and retry.

See [`docs/operations/low-memory.md`](operations/low-memory.md) for the full runbook.

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
