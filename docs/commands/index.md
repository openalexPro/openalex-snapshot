# Command: index

Build a parquet lookup index equivalent to R `build_corpus_index()`.

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
