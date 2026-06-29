# Command: index

Build a parquet lookup index equivalent to R `build_corpus_index()`.

Default dataset is `all` (build indexes for all datasets). Use `--dataset <name>` to limit to one dataset.
When running with `--dataset all`, existing per-dataset index files are skipped and missing ones are built, and the raw `*_aws` staging dirs (e.g. `works_aws`) are skipped in favour of the canonical dataset (`works`). If `--index-file` is supplied, it is ignored in `all` mode.

## Usage

```bash
openalex-snapshot index \
  --root-dir /data \
  --dataset works
```

## Output columns

- `id`
- `id_block`
- `parquet_file`
- `file_row_number`

## Tuning

`index` reads the downloaded parquet corpus one file at a time, so its memory needs are modest — the default tuning is fine for any host with ≥4 GB RAM. Use `--max-memory-mb <N>` and `--workers <N>` only if you need to constrain resources.
