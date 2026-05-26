# Runbook: Recovery and Resume

## Resume convert

Re-run the same convert command; existing outputs are skipped.

## Repair failed files (built into convert)

If `verify_convert` reported failures, just re-run `convert`:

```bash
openalex-snapshot convert --root-dir /data
```

Convert reads the latest `verify_convert` report on startup and deletes any
output parquet flagged as bad, so the normal skip-if-exists loop re-builds it.

To skip auto-repair (process only files whose parquet is missing):

```bash
openalex-snapshot convert --root-dir /data --auto-repair=false
```

To force a single specific file regardless of the verify report:

```bash
openalex-snapshot convert --root-dir /data --input-file <rel/path/to/file.gz>
```

(`--input-file` always wins over auto-repair.)

## Diagnose failures

- `openalex-snapshot report --root-dir /data --latest --full`
- `openalex-snapshot progress --root-dir /data --once`
