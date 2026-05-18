# Command: convert

Convert OpenAlex snapshot `.json.gz` files into parquet while preserving relative structure.

## Usage

```bash
# default profile (safe) — single-worker, max-memory; works on any host
openalex-snapshot convert \
  --root-dir /data \
  --dataset works

# stratified profile (recommended for 32+ GB hosts) — partitions files by gz size
# and parallelises each size bucket with its own worker count / memory budget
openalex-snapshot convert \
  --root-dir /data \
  --dataset works \
  --profile stratified-36
```

## Key behavior

- 1 input `.gz` maps to 1 output `.parquet` (unless `--split-size` is set; see below)
- Resume-safe output skipping
- Verification is separate via `verify_convert`
- Supports selected-file conversion via repeated `--input-file`
- Per-stratum execution under stratified profiles — see "Profile / tuning" below

## Profile / tuning

`--profile` selects a built-in or user-defined performance profile.  Built-ins:

| Profile          | Kind        | Behaviour |
|------------------|-------------|-----------|
| `safe` (default) | Single-pass | 1 worker (clampable to 2), generous per-worker memory (45 % of usable RAM, clamped 8 – 24 GiB on single-worker mode).  Conservative and works on any host. |
| `stratified-36`  | Stratified  | Empirically tuned for ~36 GB RAM hosts.  Partitions the file list by gz size and runs one rayon parallel pass per non-empty stratum, largest-files-first. |

`stratified-36`'s strata (workers × per-worker memory):

| gz size           | Workers | Per-worker mem |
|-------------------|---------|----------------|
| <400 MB           | 4       | 4 800 MB       |
| 400–600 MB        | 3       | 6 400 MB       |
| 600–800 MB        | 2       | 9 600 MB       |
| 800+ MB           | 1       | 13 000 MB      |

For hosts with different RAM than 36 GB, write a custom `profiles.yaml` (see below) or use `safe`.

### Custom profiles via `profiles.yaml`

A sibling YAML file `openalex-snapshot.profiles.yaml` (in the working directory, or via the `--profiles-config <path>` global flag) defines additional named profiles.  User-defined names with the same name as a built-in override it.

```yaml
# openalex-snapshot.profiles.yaml
profiles:
  stratified-16:
    description: "Tuned for ~16 GB RAM"
    min_ram_gb: 12
    kind: stratified
    strata:
      - max_file_mb: 400
        workers: 2
        per_worker_mb: 3000
      - max_file_mb: 800
        workers: 1
        per_worker_mb: 5500
      - workers: 1            # max_file_mb omitted = catch-all (no upper bound)
        per_worker_mb: 6500
```

Then run `openalex-snapshot convert --profile stratified-16 …`.

### Overrides

- `--max-memory-mb N` overrides every stratum's per-worker memory cap.
- `--workers N` collapses a stratified profile into a single flat parallel pass with N workers and the largest stratum's memory.  Use only when you know all your files fit one bucket.

## Large-file handling

By default (`--split-size 0`) large files are processed directly by in-process DuckDB, which
streams and spills to disk as needed within the per-stratum memory budget.  The spill
directory is at `<root>/openalex-snapshot_metadata/duckdb_tmp/` (created automatically).

Set `--split-size <SIZE>` (e.g. `256mb`, `512mb`) to pre-split gz files larger than that
threshold into chunks before conversion.  Each chunk produces a numbered parquet file
(e.g. `part_0000_001.parquet`, `part_0000_002.parquet`).  Use this only if you observe OOM
despite a generous profile, or when running on a machine with very limited RAM.

## Reading the run log

Each dataset prints two key lines:

```
[convert] dataset=works profile=stratified-36 strata=4 flat=false
[convert] dataset=works stratum 1/4: files=54 workers=1 per_worker_mb=13000
[convert] dataset=works stratum 2/4: files=35 workers=2 per_worker_mb=9600
...
```

- `flat=false` ⇒ stratified mode is active (file list partitioned across strata).
- `flat=true` ⇒ a single parallel pass (Safe profile, or stratified collapsed by `--workers`).
- Strata are emitted **largest-files-first** so risky files surface failures early.
