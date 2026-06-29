# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
cargo build --release          # production build
cargo build                    # dev build
cargo test --all-targets --locked   # run all tests (some build parquet fixtures via a `duckdb` CLI if present)
cargo test <test_name>         # run a single test by name
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

Some `tests/cli_smoke.rs` cases use a `duckdb` CLI to build small parquet fixtures and skip gracefully if it is not in `PATH`. The binary itself no longer depends on DuckDB — all parquet I/O uses the pure-Rust `arrow` + `parquet` crates.

## Architecture

This repository is a **Cargo workspace** with two members:

- **`openalex-core/`** — shared library crate (`openalex-core`).  Home of the **parquet pipeline operations** — the single implementation called by both the CLI and the `openalexPro` R package (via `extendr`), so the two produce identical results.  Modules: `parquetio` (footer row counts, columns, distinct values, UTF-8 collection, SNAPPY writer props), `manifest` (the OpenAlex parquet `manifest.json` model + corpus path mapping), `enrich` (`reconstruct_abstract` + `build_citation_array` + `enrich_one`), `index` (`build_index_shard` + `concat_index_shards` + `id_block_of`), `extract` (`extract_index_lookup` + `extract_rows_to_parquet`).  Plus `profile` (convert-plan planner) and `sql` (SQL string helpers) used by the R package.  The crate depends on `arrow` + `parquet`.
- **`openalex-snapshot/`** — the CLI binary (`openalex-snapshot`).  Its source is `openalex-snapshot/src/main.rs` (~6,900 lines).  It is a **thin orchestration layer**: clap argument parsing, config precedence, locking, rayon per-file fan-out, progress bars, and JSON reporting — the per-file/row work is delegated to `openalex-core`.  The CLI no longer depends on `arrow`/`parquet` directly (it reaches them through `openalex-core`).  The binary name, install path, and behaviour are unchanged.

The binary is built with `cargo build --release -p openalex-snapshot`; the release workflow passes `-p openalex-snapshot`.  All workspace members are tested with `cargo test --workspace`.

The CLI is a single binary in `openalex-snapshot/src/main.rs`; all reusable pipeline logic lives in `openalex-core`.

**Parquet-native pipeline (current)** — OpenAlex now publishes the snapshot natively in parquet
(`s3://openalex/data/parquet/`), so the active pipeline is **download → verify_download → enrich →
index → extract** with no JSON→parquet conversion. `download` syncs the official parquet per-dataset
into `<root>/parquet/`, `verify_download` validates it against the published `manifest.json`, and
`enrich` adds `abstract`/`citation` to works. The legacy JSON `convert` / `verify_convert` /
`schema` / `verify_schema` commands and the DuckDB dependency have been **removed**; the pipeline
is pure Rust over the `arrow`/`parquet` crates.

**Path model** — all runtime paths derive from a single `--root-dir`:
- `<root>/parquet/` — the parquet corpus. `download` syncs each dataset into `parquet/<dataset>/`;
  the raw works lands in `parquet/works_aws/` (stable `aws s3 sync` target) and `enrich` writes the
  canonical enriched `parquet/works/`. `index --dataset all` skips `*_aws` staging dirs.
- `<root>/snapshot/` — **legacy** JSON.GZ snapshot (only used by the deprecated convert/schema path).
- `<root>/openalex-snapshot_metadata/` — lockfile, the fetched `download/manifest.json`, and a JSON
  report per command written only on failure (per-step logs / schema caches / archived runs are no
  longer produced by the active pipeline).

**Argument precedence** (highest wins):
1. Explicit CLI flags
2. Config command-specific section
3. Config `defaults` section
4. Built-in defaults

**Execution model** — commands use continue-and-report rather than fail-fast: failures accumulate and are written to timestamped JSON reports; the process exits non-zero only if failure entries exist.

**Schema caching** — canonical schema is `unified_schema.csv` (`col_name,col_type`) under `openalex-snapshot_metadata/<dataset>/schemata/`. JSON schema caches are auxiliary and never canonical. CSV always takes precedence over JSON.

**Indexing** — two-stage: per-file shard indexes are built first (resumable), then combined into `<dataset>_id_idx.parquet` with columns `id, id_block, parquet_file, file_row_number`.

**Extract routing** — IDs are routed by OpenAlex entity prefix (`W`=works, `A`=authors, etc.) and taxonomy namespace prefixes, resolving to the correct dataset index.

**No DuckDB** — the active pipeline (download/verify_download/enrich/index/extract/verify_index) is
pure Rust over the `arrow` + `parquet` crates, implemented in **`openalex-core`** (see the module
list above). Row counts come from parquet footer metadata (`openalex_core::parquetio`); `index`
reads the `id` column and writes shards with `ArrowWriter` (`openalex_core::index`); `extract`
filters with `arrow::compute::filter_record_batch` (`openalex_core::extract`); `enrich` derives
`abstract` (from the JSON `abstract_inverted_index`, via a duplicate-key-preserving parse) and
`citation` (from the nested `authorships` struct) in `openalex_core::enrich`. The `duckdb` crate has
been removed from **both** crates entirely (it was only the deprecated JSON-convert path). Some
`tests/cli_smoke.rs` cases shell out to a `duckdb` CLI to build parquet fixtures and skip if it is
absent.

**Tuning** — every command uses `light_tuning_with_override(workers, max_memory_mb)`: workers =
`min(detected_cpus, 4)` and an 8 GiB default cap unless overridden by `--workers` / `--max-memory-mb`.
`rayon` provides per-file parallelism; memory stays modest because parquet I/O streams one file at a
time. (The old convert stratified-profile system is gone; `openalex-core::profile` still ships the
planner types for the R package, but the CLI no longer uses them.)

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
