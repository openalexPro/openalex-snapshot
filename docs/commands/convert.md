# Command: convert

Convert OpenAlex snapshot `.json.gz` files into parquet while preserving relative structure.

## Usage

```bash
openalex-snapshot convert \
  --root-dir /data \
  --dataset works \
  --profile balanced \
  --workers 4
```

## Key behavior

- 1 input `.gz` maps to 1 output `.parquet` (unless `--split-size` is set; see below)
- Resume-safe output skipping
- Verification is separate via `verify_convert`
- Supports selected-file conversion via repeated `--input-file`

## Profile / tuning

`--profile` controls the **global** DuckDB memory budget shared across all in-process worker
connections (derived from 80% of usable RAM × fraction, clamped to a min/max range).

| Profile    | Workers cap | Memory fraction    | Memory range   |
|------------|-------------|--------------------|----------------|
| `auto`     | (none)      | 65% of usable RAM  | 4 – 32 GiB    |
| `balanced` | (none)      | 65% of usable RAM  | 4 – 32 GiB    |
| `safe`     | max 2       | 15% of usable RAM  | 1 –  8 GiB   |
| `fast`     | (none)      | 80% of usable RAM  | 8 – 48 GiB   |

`auto` is the default and behaves identically to `balanced`.

Fallback when RAM cannot be detected: `safe`=2 GiB, `balanced`/`auto`=6 GiB, `fast`=12 GiB.

Use `--max-memory-mb` to override the profile memory calculation entirely.
Workers set via `--workers` or config are respected unless `safe` clamps them.

## Large-file handling

By default (`--split-size 0`) large files are processed directly by in-process DuckDB, which
streams and spills to disk as needed within the global memory budget.

Set `--split-size <SIZE>` (e.g. `256mb`, `512mb`) to pre-split gz files larger than that
threshold into chunks before conversion. Each chunk produces a numbered parquet file
(e.g. `part_0000_001.parquet`, `part_0000_002.parquet`). Use this only if you observe OOM
despite a generous profile, or when running on a machine with very limited RAM.
