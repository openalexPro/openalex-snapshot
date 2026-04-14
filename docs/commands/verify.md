# Command: verify_convert

Verify snapshot/parquet parity for structure and file-level metrics.

## Usage

```bash
openalex-snapshot verify_convert \
  --root-dir /data \
  --dataset works \
  --scope dataset \
  --metadata-level both
```

## Scope

- `file`: sampled file-pair checks
- `dataset`: all files in one dataset
- `snapshot`: all selected datasets

## Metadata levels

- `row-count`
- `id-hash`
- `both`
