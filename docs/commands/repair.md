# Command: repair_convert

Re-convert files that failed in a prior `verify_convert` report.

## Usage

```bash
# safe profile (default) — single worker, max memory
openalex-snapshot repair_convert \
  --root-dir /data \
  --dataset works \
  --from-verify-report /data/openalex-snapshot_metadata/reports/verify_convert-123456.json

# stratified — applies the same per-size-bucket parallel passes as `convert`
openalex-snapshot repair_convert \
  --root-dir /data \
  --dataset works \
  --profile stratified-36
```

## Behavior

- Selects actionable `phase=verify_metrics` failures
- Deletes failed parquet outputs (if present)
- Re-converts selected sources via the same stratified-plan machinery as `convert` (see [`convert.md`](./convert.md#profile--tuning))
- Re-verifies repaired files inline

`--profile` accepts the same names as `convert`: `safe` (default), `stratified-36`, or any user-defined profile from `openalex-snapshot.profiles.yaml`.
