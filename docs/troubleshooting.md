# Troubleshooting

## Download is slow or stalls

- Tune S3 transfer concurrency: `--max-concurrent-requests`, `--max-queue-size`,
  `--multipart-chunksize` (applied via a temporary AWS config, so your global `~/.aws/config`
  is never modified).
- Re-running `download` resumes incrementally — only missing/changed files are transferred.

## `verify_download` reports a size or row-count mismatch

The local file no longer matches the published `manifest.json`. Re-run `download` to re-fetch
the affected file(s), then re-run `verify_download`. Use `--full` for a row-scan that also
catches corrupt data pages, or `--quick` to check presence + size only.

## `index` or `extract` can't find works

`extract` routes `W` IDs to `parquet/works/` (the enriched corpus). If you only downloaded the
raw snapshot with `--no-enrich`, run `enrich` first (or re-run `download` without `--no-enrich`).
`index --dataset all` deliberately skips the raw `*_aws` staging dirs.

## Out of memory

The pipeline processes one parquet file at a time, so memory use is modest. If you still hit
limits, constrain workers/memory: `--workers <N>` and `--max-memory-mb <N>`.

## Metadata layout

Current canonical location is `openalex-snapshot_metadata/`:

- `openalex-snapshot.lock` — present while a command runs
- `reports/` — a JSON report per command, written only when failures occur
- `download/manifest.json` — the fetched OpenAlex manifest (audit copy)
- `<dataset>/index/`, `<dataset>/index-verify/` — index logs
