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
