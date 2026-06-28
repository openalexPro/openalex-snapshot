# Quickstart

OpenAlex now publishes the snapshot natively in parquet, so the flow is:
**download → verify_download → (auto) enrich → index → extract**.

## Build

```bash
cd openalex-snapshot
cargo build --release
./target/release/openalex-snapshot --help
```

## Preflight

```bash
./target/release/openalex-snapshot check \
  --root-dir /Volumes/openalex \
  --dataset all
```

## Download the parquet snapshot

Syncs the official parquet per-dataset into `<root>/parquet/` and auto-enriches `works`
(`works_aws/` → `works/`, adding `abstract` + `citation`). Pass `--no-enrich` to skip.

```bash
./target/release/openalex-snapshot download --root-dir /Volumes/openalex
```

## Verify the download

Checks every file against the published `manifest.json` (presence + size + row count).

```bash
./target/release/openalex-snapshot verify_download --root-dir /Volumes/openalex
# fast size-only:  --quick      full row scan:  --full
```

## Enrich works (only if you used --no-enrich)

```bash
./target/release/openalex-snapshot enrich --root-dir /Volumes/openalex
```

## Build and verify indexes

```bash
./target/release/openalex-snapshot index \
  --root-dir /Volumes/openalex \
  --dataset all          # builds an index per dataset; skips the raw works_aws/ staging dir

./target/release/openalex-snapshot verify_index \
  --root-dir /Volumes/openalex \
  --dataset all
```

## Extract by IDs

```bash
./target/release/openalex-snapshot extract \
  --root-dir /Volumes/openalex \
  --ids /Volumes/openalex/ids.csv \
  --output /Volumes/openalex/extract.parquet
```

## Or run the whole pipeline from config

```bash
./target/release/openalex-snapshot all --config ./openalex-snapshot.yaml
```

> The `convert` / `verify_convert` / `schema` / `verify_schema` commands are deprecated — they
> operated on the old JSON.GZ snapshot and remain only for legacy `snapshot/` trees.
