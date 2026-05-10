# Command: report

Show run reports written under `openalex-snapshot_metadata`.

## Usage

```bash
# show latest reports with per-dataset breakdown (default)
openalex-snapshot --config ./openalex-snapshot.yaml report --latest

# show only aggregate totals (suppress per-dataset rows)
openalex-snapshot --config ./openalex-snapshot.yaml report --latest --summary

# show full JSON details
openalex-snapshot --config ./openalex-snapshot.yaml report --latest --full

# list available archived run timestamps
openalex-snapshot report --root-dir /data --list

# show a specific archived run
openalex-snapshot report --root-dir /data --archived 1715000000 --latest

# filter by command
openalex-snapshot report --root-dir /data --command verify_convert --latest
```

## Default output

Each report is shown as a header line followed by a per-dataset table:

```
=== convert [2026-05-09 00:28:53  40m18s  ok]  convert-1778260662.json
  dataset                scanned       ok   failed  skipped
  ------------------------------------------------------
  authors                    546      546        0        0
  works                     2127     2127        0        0  !
```

Datasets with failures are marked with `!`.

## Notes

- `--config` is a **global** flag and must precede the subcommand:
  `openalex-snapshot --config ./openalex-snapshot.yaml report`
- `--summary` prints only aggregate totals (old behavior); suppresses per-dataset rows
- `--list` shows timestamps available under `openalex-snapshot_metadata/archived/`
- `--archived <timestamp>` reads `openalex-snapshot_metadata/archived/<timestamp>/reports/`
- `--command <name>` filters by command (e.g. `verify_convert`, `repair_convert`)
- `--full` prints full JSON details
