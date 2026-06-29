# Architecture and Decisions

This document captures project invariants and decision records so human and AI contributors can continue development safely.

## Decision: parquet-native pipeline (supersedes the JSON convert path)

OpenAlex now publishes the snapshot natively in parquet (`s3://openalex/data/parquet/`,
Hive-partitioned `<entity>/updated_date=YYYY-MM-DD/part_*.parquet`, with a top-level
`manifest.json`). Decisions:

- The corpus is the **downloaded official parquet**, byte-identical to OpenAlex and verifiable
  against `manifest.json`. Active pipeline: download → verify_download → enrich → index → extract.
- `download` syncs **per-dataset** into `<root>/parquet/<dataset>/`; raw works → `parquet/works_aws/`
  (the stable `aws s3 sync` target — never renamed, so an interrupted run can't trigger a full
  re-download), enriched works → canonical `parquet/works/`. Other datasets share `parquet/`.
- Enrichment (`abstract` + `citation`) is **on-the-fly per file** into a separate dir, never
  mutating the synced data; it auto-runs after a works download (opt out: `--no-enrich`).
  `index --dataset all` skips `*_aws`; `extract` routes `W` → `works/`.
- `verify_download` validates presence + size (`content_length`) + row count (`record_count`) from
  the manifest; default footer-metadata, `--full` row scan, `--quick` size-only.
- Transfer tuning is applied via a temporary `AWS_CONFIG_FILE` (no global `~/.aws/config` change).
- Metadata/logging slimmed: lockfile + fetched `manifest.json` + a report written only on failure.
- `convert` / `verify_convert` / `schema` / `verify_schema` are **deprecated** (code kept for legacy
  `snapshot/` JSON trees; docs/man removed). `all` defaults `enable_convert`/`enable_verify_convert`
  to `false`.

The sections below predate this and describe the deprecated JSON convert path.

## Core Model

- Binary name: `openalex-snapshot`.
- Root-first path model:
  - snapshot: `<root>/snapshot`
  - parquet: `<root>/parquet`
  - metadata: `<root>/openalex-snapshot_metadata`
- Dataset metadata layout:
  - `<root>/openalex-snapshot_metadata/<dataset>/schemata/` — schema cache
  - `<root>/openalex-snapshot_metadata/<dataset>/convert/` — convert logs
  - `<root>/openalex-snapshot_metadata/<dataset>/conversion-verify/` — verify_convert logs + metrics
  - `<root>/openalex-snapshot_metadata/<dataset>/index/` — index logs
  - `<root>/openalex-snapshot_metadata/<dataset>/index-verify/` — verify_index logs
- Download metadata layout:
  - `<root>/openalex-snapshot_metadata/download/download.log`
- Global reports (current run only):
  - `<root>/openalex-snapshot_metadata/reports/`
- Archived runs:
  - `<root>/openalex-snapshot_metadata/archived/<timestamp>/`
- Lockfile (present while a command runs):
  - `<root>/openalex-snapshot_metadata/openalex-snapshot.lock`

## Runtime Invariants

- Continue-and-report is preferred over fail-fast where safe.
- Commands exit non-zero when failure entries exist (unless explicitly warn-only behavior is defined, e.g. `check` without `--strict`).
- Reports are JSON, command-scoped, and timestamped.
- Reports include CLI version for binary traceability.

## Conversion and Schema Decisions

- Canonical schema cache is CSV:
  - `unified_schema.csv` with `col_name,col_type`.
- Source JSON schema cache is auxiliary and never canonical.
- Conversion column typing is built from canonical schema cache.
- Relative data path mapping is preserved from source snapshot to parquet.

## Validation Decisions

- `verify_convert` validates structure and per-file metrics parity.
- `repair_convert` is verify-report-driven and only targets actionable file-level failures.
- `verify_schema` is separate from `schema` inspection for consistent command semantics.
- `extract` uses `<dataset>_id_idx.parquet` indexes and supports entity-prefix plus taxonomy-namespace ID routing.

## Download Decisions

- Default strategy follows OpenAlex guidance using AWS CLI sync semantics.
- Strict download verification validates:
  - remote presence parity,
  - size parity,
  - gzip integrity for `.json.gz` files.

## Config Decisions

- Config precedence:
  1. explicit CLI flags
  2. command-specific section
  3. `defaults` section
  4. built-in defaults
- `all` requires explicit `--config`.
- Config templates support `complete` (default) and `safe` generation modes.

## Agent Contribution Rules

- Keep command naming consistent with action/verify pattern.
- Preserve root-dir path derivation and metadata conventions.
- Add tests for CLI parsing + behavior whenever new flags/commands are introduced.
- Update `NEWS.md`, docs, and man pages in the same change set as behavior changes.
- Keep README install pathways current (source build, cargo install, release binaries) and aligned with release artifact naming.

## Extension Checklist

When adding a subcommand:
1. Add long help and top-level command listing.
2. Add config section (if relevant) with precedence integration.
3. Add report persistence integration if command has operational outcomes.
4. Add README/docs/man coverage.
5. Add CLI and behavior tests.
