# Command: repair_convert

Re-convert files that failed in a prior `verify_convert` report.

## Usage

```bash
openalex-snapshot repair_convert \
  --root-dir /data \
  --dataset works \
  --from-verify-report /data/openalex-snapshot_metadata/reports/verify_convert-123456.json
```

## Behavior

- selects actionable `phase=verify_metrics` failures
- deletes failed parquet outputs (if present)
- re-converts selected sources
- re-verifies repaired files
