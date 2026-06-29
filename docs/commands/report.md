# Command: report

Show run reports written under `openalex-snapshot_metadata/reports/`. A report is written only
when a command has failures.

## Usage

```bash
# show latest reports with per-dataset breakdown (default)
openalex-snapshot report --root-dir /data --latest

# show only aggregate totals (suppress per-dataset rows)
openalex-snapshot report --root-dir /data --latest --summary

# show full JSON details
openalex-snapshot report --root-dir /data --latest --full

# filter by command
openalex-snapshot report --root-dir /data --command verify_download --latest
```

## Default output

Each report is shown as a header line followed by a per-dataset table:

```
=== verify_download [2026-06-27 00:28:53  3s  ok]  verify_download-1782660000.json
  dataset                scanned       ok   failed  skipped
  ------------------------------------------------------
  authors                    546      546        0        0
  works                     2127     2127        0        0  !
```

Datasets with failures are marked with `!`.

## Notes

- `--config` is a **global** flag and must precede the subcommand:
  `openalex-snapshot --config ./openalex-snapshot.yaml report`
- `--summary` prints only aggregate totals; suppresses per-dataset rows
- `--command <name>` filters by command (e.g. `download`, `verify_download`, `index`)
- `--full` prints full JSON details
