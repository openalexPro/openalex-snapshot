# NEWS

All notable changes to `openalex-snapshot` are documented in this file.

## [Unreleased]

### Added
- `skills` subcommand to bootstrap a project-local `skills/` folder for AI agents.
- `check` subcommand for dependency/path/disk/memory preflight checks.
- Top-level CLI version output via `openalex-snapshot --version`.
- CLI version embedded in run report payloads for traceability.

### Changed
- Config templates now include a `check:` section.
- README and command docs updated for `skills` and `check` usage.

## [0.1.0] - 2026-04-14

### Added
- Root-dir-first command model (`--root-dir`), with snapshot/parquet/metadata derivation.
- Pipeline subcommands: `download`, `verify_download`, `convert`, `verify_convert`, `schema`, `verify_schema`, `index`, `verify_index`, `repair_convert`.
- Reporting, pruning, and progress monitoring commands.
- Config management (`config --create`, `config --verify`) and pipeline orchestration (`all`).
- Continue-and-report execution model and metadata/report persistence.
