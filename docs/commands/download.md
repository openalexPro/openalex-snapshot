# Command: download

Download the OpenAlex snapshot, which is now published **natively in parquet** at
`s3://openalex/data/parquet/`. No conversion is needed.

Each dataset is synced **per-dataset** so `aws s3 sync --delete` only ever manages its own
directory:

- `works` → `<root>/parquet/works_aws/` (raw, the stable sync target)
- every other dataset → `<root>/parquet/<dataset>/`

After a successful `works` sync, `download` **auto-runs `enrich`** to produce the canonical
enriched `<root>/parquet/works/` (raw columns + `abstract` + `citation`). Pass `--no-enrich`
to skip enrichment (raw `works_aws/` only).

## Usage

```bash
openalex-snapshot download --root-dir /data
# raw-only (skip enrichment):
openalex-snapshot download --root-dir /data --no-enrich
# one dataset:
openalex-snapshot download --root-dir /data --dataset authors
```

Per-dataset sync intent:

```bash
aws s3 sync --delete s3://openalex/data/parquet/<dataset>/ /data/parquet/<dataset>/ --no-sign-request
```

## Transfer tuning

These are applied via a temporary `AWS_CONFIG_FILE` for the sync only — your global
`~/.aws/config` is never modified:

| Flag | Default |
|---|---|
| `--max-concurrent-requests` | `10` |
| `--max-queue-size` | `50000` |
| `--multipart-chunksize` | `32MB` |

## Notes

- The dataset list and disk-space preflight come from the published
  `s3://openalex/data/parquet/manifest.json`.
- Validation is a separate step — run `verify_download`.
- Space: delete `parquet/works_aws/` to reclaim raw space (the next snapshot will re-download
  works in full, then re-enrich), or delete `parquet/works/` (re-enrich anytime from
  `works_aws/`).
