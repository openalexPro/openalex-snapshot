# openalex-snapshot Documentation

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19596862.svg)](https://doi.org/10.5281/zenodo.19596862)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/rkrug/openalex-snapshot/blob/main/LICENSE)
[![CI](https://github.com/rkrug/openalex-snapshot/actions/workflows/ci.yml/badge.svg)](https://github.com/rkrug/openalex-snapshot/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/rkrug/openalex-snapshot)](https://github.com/rkrug/openalex-snapshot/releases/latest)

This folder contains internet-facing documentation for `openalex-snapshot`.

Argument precedence (highest wins): CLI arguments > config subcommand section > config `defaults` section > built-in defaults.

OpenAlex now publishes the snapshot natively in parquet, so `openalex-snapshot` is a
parquet-native pipeline: **download → verify_download → (auto) enrich → index → extract**.
The JSON→parquet `convert`/`schema` commands are deprecated (kept for legacy snapshots).

Recommended page order:

1. `quickstart.md`
2. `commands/check.md`
3. `commands/config.md`
4. `commands/all.md`
5. `commands/download.md`
6. `commands/verify_download.md`
7. `commands/enrich.md`
8. `commands/index.md`
9. `commands/verify_index.md`
10. `commands/extract.md`
11. `commands/report.md`
12. `commands/prune-reports.md`
13. `commands/progress.md`
14. `commands/skills.md`
15. `operations/low-memory.md`
16. `operations/cache-precompute.md`
17. `operations/recovery.md`
18. `troubleshooting.md`
