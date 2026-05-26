<!-- badges section begin -->
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19596862.svg)](https://doi.org/10.5281/zenodo.19596862)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/rkrug/openalex-snapshot/actions/workflows/ci.yml/badge.svg)](https://github.com/rkrug/openalex-snapshot/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/rkrug/openalex-snapshot)](https://github.com/rkrug/openalex-snapshot/releases/latest)
<!-- badges section end -->

# openalex-snapshot

Standalone CLI for OpenAlex snapshot download, conversion, verification, schema inspection, and indexing.

## AI Contribution

This project was built with significant AI assistance:

- **[OpenAI Codex](https://openai.com/codex)** was used for the initial implementation, generating the core architecture, command structure, and the majority of the Rust source code.
- **[Claude Code](https://claude.ai/code)** (Anthropic) was used for ongoing development, refinement, bug fixes, and documentation.

All AI-generated code was directed and tested by the project author.

## What It Does

`openalex-snapshot` manages the full snapshot pipeline in one CLI:

- `download` + `verify_download` for snapshot sync and integrity checks
- `convert` + `verify_convert` + `repair_convert` for JSON.GZ -> parquet with validation/recovery
- `index` + `verify_index` for ID lookup indexes
- `extract` for targeted parquet extraction by OpenAlex IDs
- `schema` + `verify_schema` for schema inspection and parity checks
- `report` / `progress` for run-state visibility
- `all` for config-driven orchestration

Default root layout:
- snapshot: `<root>/snapshot`
- parquet: `<root>/parquet`
- metadata: `<root>/openalex-snapshot_metadata`

## Requirements

- `duckdb` available in `PATH` (or pass `--duckdb-bin` where supported)
- `aws` CLI available in `PATH` for `download` / `verify_download`

## Install

### 1. Build from source

```bash
git clone https://github.com/rkrug/openalex-snapshot.git
cd openalex-snapshot
cargo build --release
./target/release/openalex-snapshot --help
```

### 2. Install with Cargo

```bash
git clone https://github.com/rkrug/openalex-snapshot.git
cd openalex-snapshot
cargo install --path .
openalex-snapshot --help
```

### 3. Install from GitHub release binaries

Download the archive for your platform from GitHub Releases:
- Linux: `openalex-snapshot-<tag>-x86_64-unknown-linux-gnu.tar.gz`
- macOS Intel: `openalex-snapshot-<tag>-x86_64-apple-darwin.tar.gz`
- macOS Apple Silicon: `openalex-snapshot-<tag>-aarch64-apple-darwin.tar.gz`
- Windows: `openalex-snapshot-<tag>-x86_64-pc-windows-msvc.zip`

Then unpack and place `openalex-snapshot` (or `openalex-snapshot.exe`) on your `PATH`.

> **macOS note:** Because the binaries are not notarized with Apple, macOS Gatekeeper will block them on first run with a warning that the app "cannot be opened". To allow the binary, run this once after unpacking:
>
> ```bash
> xattr -dr com.apple.quarantine openalex-snapshot
> ```
>
> Alternatively, open System Settings → Privacy & Security and click "Open Anyway". This is a one-time step per binary. To avoid it entirely, install via Cargo (option 1 or 2 above), which compiles locally and bypasses Gatekeeper.

Version:

- `openalex-snapshot --version`

Argument precedence (highest wins):

1. CLI arguments
2. Config subcommand section values
3. Config `defaults` section values
4. Built-in defaults

## Command Set

- `config`
- `all`
- `check`
- `download`
- `verify_download`
- `convert`
- `verify_convert`
- `schema`
- `verify_schema`
- `index`
- `extract`
- `verify_index`
- `repair_convert`
- `report`
- `prune-reports`
- `progress`
- `skills`

## Quick examples

```bash
# preflight
openalex-snapshot check --root-dir /Volumes/openalex --dataset all

# download + verify snapshot
openalex-snapshot download --root-dir /Volumes/openalex
openalex-snapshot verify_download --root-dir /Volumes/openalex

# convert one dataset (uses safe profile by default — works on any host)
openalex-snapshot convert \
  --root-dir /Volumes/openalex \
  --dataset works

# …or, on a 32+ GB host, use the empirically tuned stratified profile for a faster run:
openalex-snapshot convert \
  --root-dir /Volumes/openalex \
  --dataset works \
  --profile stratified-36

# verify conversion
openalex-snapshot verify_convert \
  --root-dir /Volumes/openalex \
  --dataset works \
  --scope dataset \
  --metadata-level both

# extract by OpenAlex IDs (writes one parquet per resolved dataset)
openalex-snapshot extract \
  --root-dir /Volumes/openalex \
  --ids /Volumes/openalex/ids.csv \
  --output /Volumes/openalex/extract.parquet

# repair from verify report
openalex-snapshot repair_convert \
  --root-dir /Volumes/openalex \
  --from-verify-report /Volumes/openalex/openalex-snapshot_metadata/reports/verify_convert-123456.json

# bootstrap AI skills folder
openalex-snapshot skills --root-dir /Volumes/openalex
```

## Metadata layout

Under `<root>/openalex-snapshot_metadata`:

- `reports/` — latest report per command (current run)
- `archived/<timestamp>/` — previous completed runs
- `download/download.log`
- `<dataset>/schemata/` — schema cache
- `<dataset>/convert/` — convert logs (created when convert runs)
- `<dataset>/conversion-verify/` — verify_convert logs + metrics
- `<dataset>/index/` — index logs
- `<dataset>/index-verify/` — verify_index logs

## Documentation

- `docs/README.md`
- `docs/quickstart.md`
- `docs/commands/`
- `docs/operations/`
- `NEWS.md`
- `ARCHITECTURE_AND_DECISIONS.md`
- `AI_SKILLS_USAGE.md`
