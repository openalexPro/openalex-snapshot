# Architecture and Decisions

This document captures project invariants and decision records so human and AI contributors can continue development safely.

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
