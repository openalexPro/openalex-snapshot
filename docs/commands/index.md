# Command: index

Build a parquet lookup index equivalent to R `build_corpus_index()`.

Default dataset is `all` (build indexes for all datasets). Use `--dataset <name>` to limit to one dataset.
When running with `--dataset all`, existing per-dataset index files are skipped and missing ones are built. If `--index-file` is supplied, it is ignored in `all` mode.

## Usage

```bash
openalex-snapshot index \
  --root-dir /data \
  --dataset works \
  --profile balanced
```

## Output columns

- `id`
- `id_block`
- `parquet_file`
- `file_row_number`

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
