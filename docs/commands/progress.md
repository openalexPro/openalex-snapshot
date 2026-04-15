# Command: progress

Monitor active/recent runs from report/log files, including from another terminal session.

## Usage

```bash
openalex-snapshot progress --root-dir /data
openalex-snapshot progress --root-dir /data --once
openalex-snapshot progress --root-dir /data --command convert --dataset works
```

## Notes

- default mode is live watch
- `--once` prints one snapshot and exits
- `--json` emits machine-readable output
