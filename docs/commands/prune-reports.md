# Command: prune-reports

Remove older report files and keep the latest set.

## Usage

```bash
openalex-snapshot prune-reports --root-dir /data
```

## Notes

- keeps recent reports and deletes older ones according to command policy
- use before long-running batches to reduce metadata noise
