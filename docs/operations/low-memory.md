# Runbook: Low-Memory Conversion

`safe` is the **default** profile and is the right answer for low-memory hosts.  No flags
needed beyond `--root-dir` and `--dataset`:

```bash
openalex-snapshot convert \
  --root-dir /data \
  --dataset works
```

If you need to constrain memory further (e.g. on an 8 GB box, or to leave headroom for
other processes):

```bash
openalex-snapshot convert \
  --root-dir /data \
  --dataset works \
  --max-memory-mb 4096
```

Notes:
- `safe` runs **one worker at a time** with a generous per-worker memory cap (45 % of
  usable RAM, clamped to 8–24 GiB on single-worker mode).  This is sufficient to convert
  the largest works files (~1 GB compressed → ~15 GB uncompressed JSON) on any host with
  spill-to-disk enabled (which is automatic via the DuckDB `temp_directory` setting).
- `--max-memory-mb N` overrides the profile's per-worker memory cap.
- Use repeated `--input-file <rel-path>` to isolate and retry specific problematic files.
- Spill-to-disk writes intermediate data to
  `<root>/openalex-snapshot_metadata/duckdb_tmp/`.  Ensure that filesystem has free
  space comparable to your largest decompressed input (10–20 GB is generous for works).

## When `safe` is not enough

If you observe OOM even under `safe`:

1. **Reduce concurrency**: `--workers 1` is already the safe default; nothing to tighten.
2. **Lower memory cap**: `--max-memory-mb 2048` forces a smaller DuckDB budget so it
   spills earlier (slower but safer).
3. **Pre-split large gz files**: `--split-size 256mb` decomposes large inputs into
   chunks before conversion, producing numbered parquets (`part_0000_001.parquet`,
   `part_0000_002.parquet`, …).
