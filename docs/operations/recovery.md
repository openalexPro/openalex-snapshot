# Runbook: Recovery and Resume

The parquet-native pipeline is resumable at every stage — just re-run the command.

## Resume a download

`aws s3 sync` is incremental: re-running `download` only transfers files that are missing or
changed. Auto-enrichment of works also resumes (see below).

```bash
openalex-snapshot download --root-dir /data
```

## Re-enrich works

`enrich` is incremental: it skips enriched files that are newer than their source and only
rebuilds changed partitions. Force a full rebuild with `--overwrite`.

```bash
openalex-snapshot enrich --root-dir /data
openalex-snapshot enrich --root-dir /data --overwrite   # rebuild everything
```

If `parquet/works/` is gone but `parquet/works_aws/` is intact, just re-run `enrich`. If
`parquet/works_aws/` was deleted to save space, re-run `download` (works is re-fetched, then
re-enriched).

## Rebuild an index

Per-file shard index building is resumable; `--overwrite` forces a clean rebuild.

```bash
openalex-snapshot index --root-dir /data --dataset all
openalex-snapshot index --root-dir /data --dataset works --overwrite
```

## Re-check integrity

```bash
openalex-snapshot verify_download --root-dir /data        # vs the published manifest
openalex-snapshot verify_index   --root-dir /data --dataset all
```

## Diagnose failures

A JSON report is written under `openalex-snapshot_metadata/reports/` only when a command has
failures:

- `openalex-snapshot report --root-dir /data --latest --full`
- `openalex-snapshot progress --root-dir /data --once`
