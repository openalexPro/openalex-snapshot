# Command: verify_schema

Verify schema parity across schema sources.

## Usage

```bash
openalex-snapshot verify_schema \
  --root-dir /data \
  --dataset works \
  --left source \
  --right parquet
```

## Notes

- exits non-zero when schema differences are found
- default comparison is source vs parquet
