<!-- badges section begin -->
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19596862.svg)](https://doi.org/10.5281/zenodo.19596862)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/openalexPro/openalex-snapshot/actions/workflows/ci.yml/badge.svg)](https://github.com/openalexPro/openalex-snapshot/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/openalexPro/openalex-snapshot)](https://github.com/openalexPro/openalex-snapshot/releases/latest)
<!-- badges section end -->

# openalex-snapshot

Standalone CLI for OpenAlex snapshot download, conversion, verification, schema inspection, and indexing.

## AI Contribution

This project was built with significant AI assistance:

- **[OpenAI Codex](https://openai.com/codex)** was used for the initial implementation, generating the core architecture, command structure, and the majority of the Rust source code.
- **[Claude Code](https://claude.ai/code)** (Anthropic) was used for ongoing development, refinement, bug fixes, and documentation.

All AI-generated code was directed and tested by the project author.

## What It Does

OpenAlex now publishes the snapshot **natively in parquet**, so `openalex-snapshot` is a
parquet-native pipeline — no JSON→parquet conversion:

- `download` + `verify_download` — sync the official parquet snapshot and validate it against
  the published `manifest.json` (presence, size, row count)
- `enrich` — add `abstract` + `citation` columns to works (auto-run by `download`)
- `index` + `verify_index` — ID lookup indexes
- `extract` — targeted parquet extraction by OpenAlex IDs
- `report` / `progress` — run-state visibility
- `all` — config-driven orchestration (`download → verify_download → index → verify_index`)

The `convert` / `verify_convert` / `schema` / `verify_schema` commands are **deprecated** —
they operated on the old JSON.GZ snapshot and remain only for legacy `snapshot/` trees.

Default root layout:
- parquet corpus: `<root>/parquet` (raw works in `<root>/parquet/works_aws`, enriched in
  `<root>/parquet/works`)
- metadata: `<root>/openalex-snapshot_metadata`

## Requirements

- `aws` CLI available in `PATH` for `download` / `verify_download`
- DuckDB is statically linked into the binary (no external `duckdb` needed at runtime; the test
  suite uses a `duckdb` CLI if present)

## Install

### 1. Build from source

```bash
git clone https://github.com/openalexPro/openalex-snapshot.git
cd openalex-snapshot
cargo build --release
./target/release/openalex-snapshot --help
```

### 2. Install with Cargo

```bash
git clone https://github.com/openalexPro/openalex-snapshot.git
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
- `enrich`
- `index`
- `extract`
- `verify_index`
- `report`
- `prune-reports`
- `progress`
- `skills`

Deprecated (legacy JSON snapshot): `convert`, `verify_convert`, `schema`, `verify_schema`.

## Quick examples

```bash
# preflight
openalex-snapshot check --root-dir /Volumes/openalex --dataset all

# download the parquet snapshot (auto-enriches works) + verify against the manifest
openalex-snapshot download --root-dir /Volumes/openalex
openalex-snapshot verify_download --root-dir /Volumes/openalex

# (re-)enrich works on demand: works_aws/ -> works/ (abstract + citation)
openalex-snapshot enrich --root-dir /Volumes/openalex

# build indexes for all datasets (skips the raw works_aws/ staging dir)
openalex-snapshot index --root-dir /Volumes/openalex --dataset all

# extract by OpenAlex IDs (writes one parquet per resolved dataset)
openalex-snapshot extract \
  --root-dir /Volumes/openalex \
  --ids /Volumes/openalex/ids.csv \
  --output /Volumes/openalex/extract.parquet

# or run the whole pipeline from config
openalex-snapshot all --config ./openalex-snapshot.yaml

# bootstrap AI skills folder
openalex-snapshot skills --root-dir /Volumes/openalex
```

## Metadata layout

Under `<root>/openalex-snapshot_metadata`:

- `openalex-snapshot.lock` — present while a command runs
- `reports/` — a JSON report per command, written only when failures occur
- `download/manifest.json` — the fetched OpenAlex manifest (audit copy)
- `<dataset>/index/`, `<dataset>/index-verify/` — index logs

## Documentation

- `docs/README.md`
- `docs/quickstart.md`
- `docs/commands/`
- `docs/operations/`
- `NEWS.md`
- `ARCHITECTURE_AND_DECISIONS.md`
- `AI_SKILLS_USAGE.md`
