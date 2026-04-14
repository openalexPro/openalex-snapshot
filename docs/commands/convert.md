# Command: convert

Convert OpenAlex snapshot `.json.gz` files into parquet while preserving relative structure.

## Usage

```bash
openalex-snapshot convert \
  --root-dir /data \
  --dataset works \
  --profile safe \
  --workers 1
```

## Key behavior

- 1 input `.gz` maps to 1 output `.parquet`
- resume-safe output skipping
- optional post-conversion verification (enabled by default)
- supports selected-file conversion via repeated `--input-file`
