# Command: report

Show run reports written under `openalex-snapshot_metadata`.

## Usage

```bash
# show current run reports
openalex-snapshot report --root-dir /data --latest

# show full JSON details
openalex-snapshot report --root-dir /data --latest --full

# list available archived run timestamps
openalex-snapshot report --root-dir /data --list

# show a specific archived run
openalex-snapshot report --root-dir /data --archived 1715000000 --latest
```

## Notes

- default view shows `openalex-snapshot_metadata/reports/` (current run)
- `--list` shows timestamps available under `openalex-snapshot_metadata/archived/`
- `--archived <timestamp>` reads `openalex-snapshot_metadata/archived/<timestamp>/reports/`
- `--command <name>` filters by command (e.g. `verify_convert`, `repair_convert`)
- `--full` prints full JSON details
