# Command: verify_index

Verify index integrity and coverage for one dataset or all datasets.

## Usage

```bash
openalex-snapshot verify_index \
  --root-dir /data \
  --dataset all
```

## Checks

- required index columns are present
- referenced parquet files exist
- row-number references are valid
