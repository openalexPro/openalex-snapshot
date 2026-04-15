# NEWS

All notable changes to `openalex-snapshot` are documented in this file.

## [Unreleased]

### Added
- `skills` subcommand to bootstrap a project-local `skills/` folder for AI agents.
- `check` subcommand for dependency/path/disk/memory preflight checks.
- `extract` subcommand to extract records by OpenAlex IDs using per-dataset parquet indexes.
- Top-level CLI version output via `openalex-snapshot --version`.
- CLI version embedded in run report payloads for traceability.

### Changed
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
