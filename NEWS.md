# NEWS

All notable changes to `openalex-snapshot` are documented in this file.

## [Unreleased]

## [0.3.0] - 2026-05-08

### Added

- `Profile::Auto` (default): two-tier convert mode — small files run in parallel (balanced), large files run serially with maximised memory.
- `large_file_threshold_mb` config/CLI option to override the auto large-file threshold.
- Per-step timing in convert: schema inference elapsed, small-pass done (ok/failed/elapsed), large-pass done (ok/failed/elapsed).
- `(N of M)` file count and ETA in live `progress --watch` output.
- Parallel schema inference using a rayon thread pool (was serial).

### Changed

- Default workers: `cpus-2` via `available_parallelism()` (0 is the auto sentinel; explicit value overrides).
- Per-worker memory = total balanced budget ÷ workers, preventing swap pressure from the old per-worker full-budget assignment.
- Worker count capped so aggregate memory stays within the balanced budget (1280 MiB floor).
- Non-works datasets skip the large-file threshold entirely (`threshold=all`) — every file goes through the parallel pass.
- Works threshold now uses `large_mem_mb` as reference (~589 MB on 36 GB) instead of `balanced_mem_mb` (~86 MB).
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
