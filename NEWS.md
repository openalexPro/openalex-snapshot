# NEWS

All notable changes to `openalex-snapshot` are documented in this file.

## [Unreleased]

### Added

- **Cargo workspace skeleton.**  The repository is now a Cargo workspace with two members:
  `openalex-core` (shared library crate) and `openalex-snapshot` (the CLI binary, now under
  `openalex-snapshot/`).  The binary name, `cargo install` target, release artifacts, and
  runtime behaviour are **unchanged**.  `openalex-core` starts with two pure SQL-string helpers
  (`works_abstract_expr`, `works_citation_expr`) extracted from the CLI — the foundation for
  future `extendr`-based R integration.

- **Works enrichment in `convert`.**  Two derived columns are now written to every works parquet:
  - `abstract` (VARCHAR) — plain-text abstract reconstructed from `abstract_inverted_index`
    (positions sorted ascending, words joined with single spaces).
  - `citation` (VARCHAR) — `"Author (year)"` / `"A & B (year)"` / `"A et al. (year)"`
    derived from `authorships` + `publication_year`.  Null `publication_year` renders as
    `"(n.d.)"`.  Null/empty `authorships` ⇒ null citation.
  - `abstract_inverted_index` is **kept** in the output for callers that want the original
    inverted form.
  - Both columns are added only when the underlying source columns are present in the
    inferred schema (so synthetic / minimal test fixtures convert cleanly).

### Fixed

- **Schema cache no longer uppercases STRUCT field identifiers.**  `normalize_duckdb_type`
  used to apply `to_uppercase()` to the entire type string, including struct field names.
  DuckDB's `read_json(columns = …)` matches JSON keys to struct field names
  case-sensitively, so a cached schema like `STRUCT(AUTHOR STRUCT(DISPLAY_NAME VARCHAR))`
  silently filled every inner struct field with NULL when reading JSON with lowercase
  keys (`{"author": {"display_name": ...}}`).  This affected every nested-struct column
  in works (and other datasets): `authorships[*].author`, `apc_list.value`, `biblio.*`,
  `best_oa_location.source`, …  The fix preserves identifier case while still uppercasing
  type keywords (`BIGINT`, `VARCHAR`, `STRUCT`, …).  **Migration:** run
  `convert --refresh-cache` once to regenerate the schema cache with corrected field-name
  case; existing parquets containing NULL nested-struct fields will need a re-convert
  (or rely on convert's auto-repair from a fresh `verify_convert` report).

- **`convert` auto-repair from the latest `verify_convert` report.**  On startup `convert`
  reads `<root>/openalex-snapshot_metadata/reports/verify_convert-*.json` (most recent),
  deletes any output parquet that report flagged (`phase ∈ { verify_metrics, convert_file }`),
  and lets the normal *skip-if-exists* filter re-include those files in the convert pass.
  Net effect: running `convert` a second time fixes whatever `verify_convert` flagged —
  no separate `repair_convert` subcommand needed.  Default on; opt out per-run with
  `--auto-repair=false`, in config with `convert.auto_repair: false`, or by passing
  `--input-file` (which always takes precedence over the verify report).
- **Stratified profiles** for `convert`.  A new `ProfileRegistry` resolves `--profile <name>` against built-ins plus an optional user `openalex-snapshot.performance.yaml`.  Stratified profiles partition the file list by gz size and run one rayon parallel pass per non-empty stratum, each with its own worker count and DuckDB memory budget (largest-files-first execution order).  Built-in `stratified-36` provides empirically-tuned strata for 32+ GB hosts (4×4800 MB / 3×6400 MB / 2×9600 MB / 1×13000 MB, by gz-size buckets <400 / 400–600 / 600–800 / 800+ MB).
- New global flag `--performance-config <path>` (auto-discovers `./openalex-snapshot.performance.yaml`).  Built-in profile names always work without this file.
- New `config --create-profiles` flag scaffolds a starter `performance.yaml` auto-derived from the host's detected RAM.  The emitted profile is named `stratified-<RAM_GB>`, with workers + per-worker memory linearly scaled from the 36 GB baseline and capped so total memory never exceeds 55 % of system RAM (parallel) or 40 % (single-worker catch-all).
- New `config --list-profiles` flag prints all built-in + user profiles with their strata as a table.
- `convert` logs per-stratum execution lines, e.g. `[convert] dataset=works stratum 2/4: files=35 workers=2 per_worker_mb=9600`.

### Changed

- `all` now loops `convert → verify_convert` (instead of `convert → verify_convert → repair_convert`) up to `--retry N` times.  Each retry uses convert's new auto-repair to delete and re-build the parquets the prior verify flagged.  `--retry`'s default is unchanged (1 extra attempt).
- **Default profile for `convert` is now `safe`** (was `auto`).  Safe runs single-worker with generous per-worker memory (45 % of usable RAM, clamped 8–24 GiB on workers=1) and reliably handles the largest works files via DuckDB spill-to-disk.
- `--workers N` on a stratified profile collapses the plan into a single flat pass with the largest stratum's memory.  Predictable: explicit flags always override.
- `ConvertArgs::profile` is now `String` (was the `Profile` enum).  Profile names are resolved at runtime against the registry; unknown names produce a clear error listing the available profiles.

### Removed

- **The `repair_convert` subcommand is gone.**  Its work folds into `convert`'s auto-repair path (see Added).  Configs containing a `repair_convert:` section or `all.enable_repair_convert: ...` will now fail `config --verify` with `unknown field`.  Migration: delete those entries.  Scripts calling `openalex-snapshot repair_convert ...` should be rewritten as `openalex-snapshot convert ...` (no extra flags needed — auto-repair runs by default).
- The `--from-verify-report <path>` flag is gone with the subcommand.  Auto-repair always uses the latest report under `reports/`.
- The `Profile` enum (`Auto` / `Balanced` / `Fast`) is gone.  `safe` remains as a named built-in profile (`ProfileKind::Safe`); the other names are no longer accepted.  Existing configs containing `profile: auto|balanced|fast` will fail `config --verify` and produce a clear "unknown profile" error at runtime — replace with `safe` or `stratified-36`.
- The `--profile` flag has been removed from all non-Convert subcommands (`verify_convert`, `schema`, `verify_schema`, `index`, `extract`, `verify_index`, `validate_download`, `check`).  These commands now use a fixed light tuning (workers = min(detected_cpus, 4), memory = 8 GiB) with `--workers` / `--max-memory-mb` overrides — they don't benefit from profile tuning the way `convert` does.
- The `config --create fast` template mode is removed (the `fast` profile no longer exists).  Only `complete` and `safe` template modes remain.
- Helper functions `resolve_tuning`, `resolve_tuning_with_total`, `auto_profile_memory_mb` are removed.  Two legacy unit tests covering them are gone.

### Fixed

- DuckDB `SET temp_directory` is now applied exactly once via a `OnceLock`, eliminating the warning `Cannot switch temporary directory after the current one has been used` that appeared on the second and subsequent datasets of any `all` run.  Spill-to-disk now works reliably across multi-dataset runs.

## [0.4.1] - 2026-05-11

### Fixed

- `extract`: the INNER JOIN between the index (which stores full-URL IDs like `https://openalex.org/W1234`) and the req_ids CSV (which was written with normalized short IDs like `W1234`) always returned empty results — `extract` had never returned matching records for standard short-form OpenAlex IDs. Fixed by adding `canonical_openalex_id()` which expands short IDs to full URLs before writing the req_ids CSV.
- `extract`: now acquires a lock file so runs are visible to `progress --watch`.
- Removed stale `ensure_duckdb()` calls from `convert`, `extract`, `verify`, `schema`, `verify_schema`, and `repair` (DuckDB is statically linked; there is no external binary to check). Deleted the now-dead wrapper function.
- `check`: correctly reports DuckDB as bundled/statically linked instead of looking for an external binary.

## [0.4.0] - 2026-05-10

### Added

- `report` default output now shows a **per-dataset breakdown** (scanned / ok / failed / skipped) under each report header, with `!` marking any dataset with failures.
- `report --summary` flag to suppress per-dataset rows and show only aggregate totals (previous default behavior).

### Changed

- DuckDB is now **in-process** via the `duckdb` Rust crate (statically linked; no external `duckdb` binary required at runtime). Replaced all subprocess invocations (`std::process::Command`) with an in-process connection shared across rayon worker threads via `OnceLock<Mutex<Connection>>` + per-thread `try_clone()`.
- Global DuckDB memory limit (`SET memory_limit`) is now set once before the parallel pass as `per_worker_mb × workers`, so all worker connections share a single correct budget. Previously the limit was set per-query inside each worker, which caused all workers to share an unintentionally small cap.
- Profile memory fractions updated for the in-process model (old values were calibrated for per-subprocess isolation):
  - `balanced` / `auto`: **65%** of usable RAM, 4–32 GiB (was 35%, 4–24 GiB)
  - `fast`: **80%** of usable RAM, 8–48 GiB (was 55%, 8–32 GiB)
  - `safe`: 15% of usable RAM, 1–8 GiB (unchanged)
- `Profile::Auto` is now an alias for `Profile::Balanced` (single parallel pass). The two-tier serial large-file pass has been removed; in-process DuckDB with a generous memory budget handles large files via streaming.

## [0.3.0] - 2026-05-08

### Added

- `Profile::Auto` (default): parallel convert mode with pre-split support for very large gz files (`--split-size`).
- `--split-size <SIZE>` config/CLI option: decompress and chunk gz files larger than the threshold before converting. `0` (default) disables splitting; in-process DuckDB handles large files via streaming.
- Per-step timing in convert: schema inference elapsed, pass done (ok/failed/elapsed).
- `(N of M)` file count and ETA in live `progress --watch` output.
- Parallel schema inference using a rayon thread pool (was serial).

### Changed

- Default workers: `cpus-2` via `available_parallelism()` (0 is the auto sentinel; explicit value overrides).
- Global memory budget divided across workers, preventing swap pressure from the old per-worker full-budget assignment.
- Both passes sort files largest-first to minimise tail-latency stragglers.
- `run_all` fixed: sub-command args were hardcoded to `workers=4` / `Profile::Balanced`; now inherit auto defaults.
- `schemata/` cache excluded from archive and log cleanup so it persists across runs.
- `repair_convert` falls back to convert report when no verify report exists; auto-resolves latest verify report.

## [0.2.0] - 2026-05-07

### Added

- `skills` subcommand to bootstrap a project-local `skills/` folder for AI agents.
- `check` subcommand for dependency/path/disk/memory preflight checks.
- `extract` subcommand to extract records by OpenAlex IDs using per-dataset parquet indexes.
- Top-level CLI version output via `openalex-snapshot --version`.
- CLI version embedded in run report payloads for traceability.

### Changed
- Metadata directory restructured for simplicity:
  - Renamed `.openalex-snapshot_metadata/` → `openalex-snapshot_metadata/` (no leading dot)
  - Flattened `datasets/<dataset>/` → `<dataset>/` directly under metadata root
  - Per-dataset step subdirs now separate: `convert/`, `conversion-verify/`, `index/`, `index-verify/`, `schemata/` (each created only when that step runs)
  - Single `archived/<timestamp>/` stores completed runs (mirroring active structure)
  - Download logs at `download/download.log` (no `logs/` subfolder)
  - Per-dataset report copies eliminated; single global `reports/` only
  - Automatic migration from old layout on first run
- Added lockfile `openalex-snapshot_metadata/openalex-snapshot.lock` (JSON with PID + command + start time); `progress --watch` continues while lockfile exists and PID is alive
- `report --list` shows available archived run timestamps; `report --archived <timestamp>` shows that run's reports
- `prune-reports` now prunes `archived/<timestamp>/` folders (keep N newest)
- Config templates now include a `check:` section.
- `index`/`verify_index` default dataset changed from `works` to `all`.
- `index --dataset all` now skips existing per-dataset index files and continues building missing ones (and ignores `--index-file` in `all` mode).
- Documentation + AI consistency sweep:
  - README now includes install guidance (source build, cargo install, release binaries)
  - MkDocs command coverage expanded to include `all`, `config`, `report`, `prune-reports`, `progress`, `verify_schema`, `verify_index`
  - stale migration reference removed from docs index
  - AI skills guidance/templates now include explicit `extract` runbook coverage
- Documentation consistency sweep across README/docs/man/skills references:
  - precedence clarified as `CLI > command section > defaults section > built-in defaults`
  - naming aligned to `openalex-snapshot` repo/binary usage
  - config template mode docs aligned to `complete|safe|fast`

## [0.1.0] - 2026-04-14

### Added
- Root-dir-first command model (`--root-dir`), with snapshot/parquet/metadata derivation.
- Pipeline subcommands: `download`, `verify_download`, `convert`, `verify_convert`, `schema`, `verify_schema`, `index`, `verify_index`, `repair_convert`.
- Reporting, pruning, and progress monitoring commands.
- Config management (`config --create`, `config --verify`) and pipeline orchestration (`all`).
- Continue-and-report execution model and metadata/report persistence.
