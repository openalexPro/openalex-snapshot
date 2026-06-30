# NEWS

All notable changes to `openalex-snapshot` are documented in this file.

## [0.6.0] - 2026-06-29

### Changed — pipeline logic moved into the `openalex-core` library

- **The parquet pipeline operations now live in `openalex-core`, not the CLI.** The download/
  verify/enrich/index/extract algorithms were extracted from `openalex-snapshot/src/main.rs` into
  library modules — `openalex_core::{parquetio, manifest, enrich, index, extract}` — so the CLI and
  the `openalexPro` R package (via `extendr`) call **one shared implementation** and produce
  identical results. The CLI is now a thin orchestration layer (clap parsing, config precedence,
  locking, rayon fan-out, progress bars, JSON reports) that delegates the per-file/row work to the
  library. No behaviour change: the CLI output and all commands are unchanged.
- **`arrow`/`parquet` are no longer direct dependencies of the CLI crate** — it reaches them through
  `openalex-core`. `main.rs` dropped ~540 lines of moved code. New `openalex-core` unit tests cover
  abstract reconstruction (ordering, duplicate words, escaped keys, empty/invalid input).

### Removed — DuckDB dependency dropped entirely

- **The `duckdb` crate is gone from the binary.** `enrich` is now pure Rust: the abstract is
  reconstructed from the JSON `abstract_inverted_index` with a duplicate-key-preserving parser, and
  the citation is built from the nested `authorships` struct — output verified **byte-identical** to
  the previous DuckDB SQL across a full 142,844-row works partition (and ~12× faster). With `index`,
  `extract`, and the verify commands already ported (below), no code path uses DuckDB, so the
  bundled-DuckDB dependency was removed. Result: a ~11 MB binary (down from ~100 MB+), much faster
  builds, and no C++ toolchain needed to build the CLI. `duckdb` has been removed from `openalex-core`
  as well — neither workspace crate depends on it.

### Removed — orphaned convert profile system from the CLI

- Dropped the CLI's stratified-profile feature, now unused since `convert` is gone:
  `config --create-profiles` / `--list-profiles`, the global `--performance-config` flag, the
  `performance.yaml` generator, and the now-no-op `all --retry` flag (hidden, still accepted).
  `openalex-core::profile` (the planner types) is retained for the R package; the CLI no longer
  references it beyond `detect_total_memory_mb` (used by `check`). Config back-compat preserved:
  legacy `profile:` / `convert:` keys still parse and are ignored.

### Removed — deprecated JSON commands; parquet/arrow port

- **Deleted the deprecated `convert` / `verify_convert` / `schema` / `verify_schema` subcommands**
  and ~3,600 lines of JSON-pipeline machinery (schema inference + cache, the convert profile
  planner integration, verify-convert metrics + auto-repair). Existing config files still load:
  the `convert`/`verify_convert`/`schema` sections and `all.enable_convert`/`enable_verify_convert`
  keys are parsed-and-ignored. The `openalex-core` profile/conversion modules are kept (R package).
- **`index`, `extract`, `verify_index`, and `verify_download` are now pure Rust** using the
  `arrow` / `parquet` crates instead of DuckDB:
  - row counts use parquet footer metadata (zero scan; `verify_download --full` decodes pages),
  - `index` reads only the `id` column and writes shards with `ArrowWriter` (id_block/file_row_number
    semantics byte-identical to the previous DuckDB formula),
  - `extract` resolves ids via a `HashSet` over the index and filters rows with
    `arrow::compute::filter`, preserving all columns including nested `authorships` structs.
  DuckDB is now used **only by `enrich`** (the abstract/citation derivation).

### Changed — parquet-native pipeline

OpenAlex now publishes the snapshot **natively in parquet** (`s3://openalex/data/parquet/`),
so the tool is now a parquet-native pipeline: **download → verify_download → enrich → index →
extract**. The JSON→parquet `convert` step is obsolete.

- **`download` rewritten for parquet.** Syncs `s3://openalex/data/parquet/<dataset>/`
  **per-dataset** into `<root>/parquet/`. Works lands in `parquet/works_aws/` (the stable
  `aws s3 sync` target) and is **auto-enriched** into the canonical `parquet/works/` (opt out
  with `--no-enrich`). The dataset list and disk preflight come from the published
  `manifest.json`. New transfer-tuning flags (`--max-concurrent-requests` [default 10],
  `--max-queue-size`, `--multipart-chunksize`) are applied via a temporary `AWS_CONFIG_FILE`
  so your global `~/.aws/config` is never touched.

- **`verify_download` rewritten to use `manifest.json`.** Validates per-file presence, size
  (`content_length`), and row count (`record_count`) — a stronger check than the old gzip
  integrity test. Default reads footer metadata (`parquet_file_metadata`); `--full` does a row
  scan; `--quick` skips row counts.

- **New `enrich` subcommand** (and `works_abstract_expr_from_json` in `openalex-core`). Builds
  `parquet/works/` from `parquet/works_aws/`, adding `abstract` (reconstructed from the JSON
  `abstract_inverted_index`) and `citation`. Incremental and row-parity checked; the raw
  `works_aws/` is never modified, so it stays manifest-verifiable and incremental sync is
  unaffected. `index --dataset all` skips `*_aws` staging dirs; `extract` routes `W` IDs to
  `works/`.

- **`all` no longer converts by default.** `all.enable_convert` / `all.enable_verify_convert`
  now default to `false`; the pipeline is `download → verify_download → index → verify_index`.

- **`convert` / `verify_convert` / `schema` / `verify_schema` are deprecated.** The code is kept
  (marked deprecated in `--help`) for legacy `snapshot/` JSON trees, but their docs and man pages
  have been removed.

- **Slimmer metadata/logging.** Per-step `.log` files, manifest JSONL snapshots, and schema
  caches are no longer written by the active pipeline; reports are written only when a command
  has failures.

## [0.5.0] - 2026-05-23

### Added

- **`openalex-core` Phase B: profile system + SQL helpers extracted.**  The following
  are now `pub` items in `openalex-core` rather than private CLI internals:
  - **`profile` module** — `Stratum`, `ProfileKind`, `ProfileDef`, `ProfilesYaml`,
    `ProfileRegistry`, `FilePair`, `StratumPlan`, `ConvertPlan`, `build_convert_plan`,
    `validate_profile_def`, `derive_stratified_profile_for_ram`, `detect_total_memory_mb`,
    and the two safe-mode memory-budget helpers.  The profile planner is now independently
    testable and reusable without importing DuckDB or the CLI.
  - **`sql` module** — `normalize_duckdb_type`, `sql_quote`, `parse_size_str`.
  - 10 new unit tests in `openalex-core` cover all extracted items.
  - `serde` and `serde_yaml` are now workspace-level dependency declarations.

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

- **`openalex-core` Phase D: JSON→Parquet conversion pipeline in the shared library.**
  A new `conversion` feature (deps: `duckdb` bundled + `rayon`) exposes
  `conversion::snapshot_to_parquet`, `build_corpus_index`, and `lookup_by_id` as library
  functions in `openalex-core`.  The R package (`openalexPro`) can call these directly via
  `extendr` instead of shelling out to the CLI, giving R and the CLI a single shared
  implementation.

### Fixed

- **`lookup_by_id`: output files now use zero-padded index names** (`part_00000.parquet`,
  `part_00001.parquet`, …) instead of basename-derived names.  Date-partitioned corpora
  contain many files with identical basenames (e.g. `updated_date=X/part_0000.parquet`);
  using the basename caused rayon threads to collide on the same output path, so only the
  first write succeeded and all others failed with "COPY failed".

- **`lookup_by_id`: output batched to ~10,000 rows per file.**  Date-partitioned corpora
  yield ~130 matching rows per source file, which previously produced ~30,000 tiny output
  parquets for large extracts.  Entries are now grouped into batches of 10,000 rows and
  written as a single `UNION ALL BY NAME` COPY, drastically reducing file count without
  increasing peak memory.

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

## [0.4.2] - 2026-05-11

### Fixed

- `convert` / `repair`: in-process DuckDB now correctly spills to disk when the memory limit is reached. Previously, `Connection::open_in_memory()` had no `temp_directory` and would OOM instead of spilling — producing the same failures as the old subprocess approach for large files. The spill directory (`<root>/openalex-snapshot_metadata/duckdb_tmp/`) is created automatically on the same filesystem as the parquet output.

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
