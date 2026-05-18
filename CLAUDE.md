# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
cargo build --release          # production build
cargo build                    # dev build
cargo test --all-targets --locked   # run all tests (requires duckdb in PATH)
cargo test <test_name>         # run a single test by name
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

Tests in `tests/cli_smoke.rs` use the `duckdb` CLI binary for parquet verification steps and will skip gracefully if it is not in `PATH`. The main binary does **not** require an external `duckdb` binary — DuckDB is statically linked via the `duckdb` crate (`features = ["bundled", "json", "parquet"]`).

## Architecture

The entire application is a single binary implemented in `src/main.rs` (~10,400 lines). There are no library crates or submodules.

**Path model** — all runtime paths derive from a single `--root-dir`:
- `<root>/snapshot/` — downloaded snapshot (JSON.GZ files)
- `<root>/parquet/` — converted parquet output
- `<root>/openalex-snapshot_metadata/` — reports, logs, schema caches, verify state

**Argument precedence** (highest wins):
1. Explicit CLI flags
2. Config command-specific section
3. Config `defaults` section
4. Built-in defaults

**Execution model** — commands use continue-and-report rather than fail-fast: failures accumulate and are written to timestamped JSON reports; the process exits non-zero only if failure entries exist.

**Schema caching** — canonical schema is `unified_schema.csv` (`col_name,col_type`) under `openalex-snapshot_metadata/<dataset>/schemata/`. JSON schema caches are auxiliary and never canonical. CSV always takes precedence over JSON.

**Indexing** — two-stage: per-file shard indexes are built first (resumable), then combined into `<dataset>_id_idx.parquet` with columns `id, id_block, parquet_file, file_row_number`.

**Extract routing** — IDs are routed by OpenAlex entity prefix (`W`=works, `A`=authors, etc.) and taxonomy namespace prefixes, resolving to the correct dataset index.

**DuckDB** — all parquet reads and writes use the `duckdb` Rust crate in-process (statically linked; no external binary required). A global `Connection` is held in an `OnceLock<Mutex<Connection>>`; each rayon worker thread clones it via `try_clone()` and stores the clone in `thread_local!` storage. Spill-to-disk is enabled via `SET temp_directory` (OnceLock-guarded so it's only applied once per process). The global memory limit (`SET memory_limit`) is set fresh per stratum — see "Profile / stratified plan" below.

**Profile / stratified plan** — `convert` and `repair_convert` resolve `--profile <name>` against a `ProfileRegistry` (built-ins `safe`, `stratified-36`, plus optional user profiles from `profiles.yaml`). `build_convert_plan(...)` produces a `ConvertPlan { strata: Vec<StratumPlan>, flat }` where each `StratumPlan` carries its own worker count, per-worker memory cap, and the subset of files in that gz-size bucket. `run_convert` / `run_repair` iterate the strata, configuring DuckDB memory + rayon pool fresh per stratum. `--workers N` collapses a stratified plan into a single flat pass for compatibility. All other subcommands (`verify_convert`, `schema`, `verify_schema`, `index`, `extract`, `verify_index`, `validate_download`, `check`) have no `--profile` flag — they use `light_tuning_with_override(workers, max_memory_mb)` which returns workers = min(detected_cpus, 4) and memory = 8 GiB by default.

## Invariants (from ARCHITECTURE_AND_DECISIONS.md)

- `all` requires an explicit `--config` path; it will not run without one.
- `index --dataset all` skips existing shard files and ignores `--index-file`.
- `check` without `--strict` is warn-only (exits 0 even with warnings).
- Config templates: `complete` | `safe` — these are the only valid modes.

## When Adding a Subcommand

1. Add long help string and register in the top-level command listing in `CLI_LONG_ABOUT`.
2. Add a config section with precedence integration (CLI > section > defaults > built-ins).
3. Add report persistence if the command has operational outcomes.
4. Update `NEWS.md`, `docs/commands/<name>.md`, `mkdocs.yml` nav, and man pages in the same change set.
5. Add CLI parsing and behaviour tests in `tests/cli_smoke.rs`.

## Worktree and PR Conventions

- Changes go through a PR from a `claude/<name>` worktree branch — never commit directly to `main` except for trivial fixes.
- Do **not** delete `claude/*` branches after merging — they are kept for auditing AI contributions.
- Tag releases on `main` after the PR is merged; pushing a `v*` tag triggers the release workflow.
