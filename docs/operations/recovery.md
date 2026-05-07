# Runbook: Recovery and Resume

## Resume convert

Re-run the same convert command; existing outputs are skipped.

## Repair failed files

```bash
openalex-snapshot repair_convert \
  --root-dir /data \
  --from-verify-report /data/openalex-snapshot_metadata/reports/verify_convert-<ts>.json
```

## Diagnose failures

- `openalex-snapshot report --root-dir /data --latest --full`
- `openalex-snapshot progress --root-dir /data --once`
