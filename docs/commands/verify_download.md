# Command: verify_download

Validate the downloaded parquet corpus against the official
`s3://openalex/data/parquet/manifest.json`.

## Usage

```bash
openalex-snapshot verify_download --root-dir /data
# size-only (fastest):
openalex-snapshot verify_download --root-dir /data --quick
# full row scan (catches data-page corruption):
openalex-snapshot verify_download --root-dir /data --full
```

## Checks

For every file listed in the manifest:

- **presence** — the local file exists (works files are checked under `works_aws/`)
- **size** — local byte size equals the manifest `content_length`
- **row count** — parquet row count equals the manifest `record_count`
  - default (`meta`): footer metadata only (`parquet_file_metadata`) — cheap
  - `--full`: actual `COUNT(*)` scan — slower, catches corrupt data pages
  - `--quick`: skip the row-count check entirely
- **extra files** (`--check-extra`, on by default) — flag local `.parquet` files not in the manifest

The enriched `works/` directory is validated by `enrich`'s own row-parity self-check, not here.

## Output

A JSON report is written only when failures are detected; a clean run prints a one-line
summary and exits 0.
