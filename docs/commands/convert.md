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
- verification is separate via `verify_convert`
- supports selected-file conversion via repeated `--input-file`

## Profile / tuning

`--profile` controls the DuckDB memory budget per worker (derived from 80% of usable RAM,
clamped to a range). Only `safe` also caps the worker count.

| Profile    | Workers cap | Memory fraction    | Memory range  |
|------------|-------------|--------------------|---------------|
| `safe`     | max 2       | 15% of usable RAM  | 1 – 8 GiB    |
| `balanced` | (none)      | 35% of usable RAM  | 4 – 24 GiB   |
| `fast`     | (none)      | 55% of usable RAM  | 8 – 32 GiB   |

Fallback when RAM cannot be detected: `safe`=2 GiB, `balanced`=6 GiB, `fast`=12 GiB.

Use `--max-memory-mb` to override the profile memory calculation entirely.
Workers set via `--workers` or config are respected unless `safe` clamps them.
