# Command: schema

Inspect schema from source/cache/parquet and optionally diff schema sources.

## Usage

```bash
openalex-snapshot schema \
  --root-dir /data \
  --dataset works \
  --from auto \
  --format arrow-r
```

## Notes

- canonical cache: `openalex-snapshot_metadata/<dataset>/schemata/unified_schema.csv`
- `arrow-r` format is stable JSON for R Arrow comparisons
