# Command: index

Build a parquet lookup index equivalent to R `build_corpus_index()`.

Default dataset is `all` (build indexes for all datasets). Use `--dataset <name>` to limit to one dataset.
When running with `--dataset all`, existing per-dataset index files are skipped and missing ones are built. If `--index-file` is supplied, it is ignored in `all` mode.

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

`index` reads parquet files (already-converted output of `convert`), so its memory needs are modest compared to `convert`. The default tuning is fine for any host with ≥4 GB RAM. Use `--max-memory-mb <N>` and `--workers <N>` only if you need to constrain resources.

> **Note**: the legacy `--profile safe|balanced|fast` flag is accepted on this command for backwards compatibility but doesn't drive stratified parallelism. The stratified profile system (`safe`, `stratified-36`, custom `stratified-N` from `openalex-snapshot.performance.yaml`) currently applies only to `convert` and `repair_convert`. See [`convert.md`](./convert.md#profile--tuning).
