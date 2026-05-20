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
- **Auto-repair from the latest `verify_convert` report** — see "Auto-repair" below.
- Verification is separate via `verify_convert`
- Supports selected-file conversion via repeated `--input-file`
- Per-stratum execution under stratified profiles — see "Profile / tuning" below

## Auto-repair from verify report

At startup, `convert` reads the most-recent `verify_convert` report under
`<root>/openalex-snapshot_metadata/reports/`.  For any parquet that report flagged
(phase `verify_metrics` or `convert_file`), the existing parquet is deleted so the
normal *skip-if-exists* filter re-includes that file in the convert pass.

**Net effect: running `convert` a second time fixes whatever `verify_convert` flagged.**
There is no separate `repair_convert` subcommand — convert is its own repair.

Behaviour:

- **Default:** enabled.  No flag needed.
- **Opt-out per-run:** `--auto-repair=false`.
- **Opt-out in config:** `convert.auto_repair: false` in `openalex-snapshot.yaml`.
- **Ignored when `--input-file` is given:** if you explicitly name files, only those
  are processed and the verify report is not consulted.  Most predictable for ad-hoc
  work.
- **No-op when there's no verify report** (or when the report has no `verify_metrics`
  / `convert_file` failures for the current datasets).

In a pipeline (`all`), the orchestrator loops `convert → verify_convert` up to
`--retry N` times.  Each retry's convert call auto-repairs whatever the prior
verify flagged.

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

For hosts with different RAM than 36 GB, write a custom `performance.yaml` (see below) or use `safe`.

### Custom profiles via `performance.yaml`

A sibling YAML file `openalex-snapshot.performance.yaml` (in the working directory, or via the `--performance-config <path>` global flag) defines additional named profiles.  User-defined names with the same name as a built-in override it.

**Fastest path** — scaffold a profile auto-derived from your host's RAM:

```bash
openalex-snapshot config --create-profiles
# writes ./openalex-snapshot.performance.yaml with a `stratified-<RAM_GB>` profile,
# plus commented examples for half- and double-RAM tiers.
```

To preview what would be written without creating the file:

```bash
openalex-snapshot config --create-profiles --stdout
```

To inspect what profiles are visible (built-ins + anything loaded from your `performance.yaml`):

```bash
openalex-snapshot config --list-profiles
```

The generated file's schema:

```yaml
# openalex-snapshot.performance.yaml
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
