# NEWS

All notable changes to `openalex-snapshot` are documented in this file.

## [Unreleased]

## [0.1.0] - 2026-04-16

### Added
- Root-dir-first command model (`--root-dir`), with snapshot/parquet/metadata derivation.
- Pipeline subcommands: `download`, `verify_download`, `convert`, `verify_convert`, `schema`, `verify_schema`, `index`, `verify_index`, `repair_convert`.
- `skills` subcommand to bootstrap a project-local `skills/` folder for AI agents.
- `check` subcommand for dependency/path/disk/memory preflight checks.
- `extract` subcommand to extract records by OpenAlex IDs using per-dataset parquet indexes.
- Reporting, pruning, and progress monitoring commands (`report`, `prune-reports`, `progress`).
- Config management (`config --create`, `config --verify`) and pipeline orchestration (`all`).
- Continue-and-report execution model and metadata/report persistence.
- Top-level CLI version output via `openalex-snapshot --version`.
- CLI version embedded in run report payloads for traceability.

### Changed
- Config templates include a `check:` section.
- `index`/`verify_index` default dataset is `all`.
- `index --dataset all` skips existing per-dataset index files and continues building missing ones (ignores `--index-file` in `all` mode).
