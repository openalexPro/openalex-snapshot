# Quickstart

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

## Convert snapshot to parquet

```bash
./target/release/openalex-snapshot convert \
  --root-dir /Volumes/openalex \
  --dataset works \
  --profile balanced
```

## Verify output

```bash
./target/release/openalex-snapshot verify_convert \
  --root-dir /Volumes/openalex \
  --dataset works \
  --scope dataset \
  --metadata-level both
```

## Build and verify index

```bash
./target/release/openalex-snapshot index \
  --root-dir /Volumes/openalex \
  --dataset works

./target/release/openalex-snapshot verify_index \
  --root-dir /Volumes/openalex \
  --dataset works
```

## Extract by IDs

```bash
./target/release/openalex-snapshot extract \
  --root-dir /Volumes/openalex \
  --ids /Volumes/openalex/ids.csv \
  --output /Volumes/openalex/extract.parquet
```

## Download and verify snapshot

```bash
./target/release/openalex-snapshot download --root-dir /Volumes/openalex
./target/release/openalex-snapshot verify_download --root-dir /Volumes/openalex
```

## Repair failed files from verify report

```bash
./target/release/openalex-snapshot repair_convert \
  --root-dir /Volumes/openalex \
  --from-verify-report /Volumes/openalex/.openalex-snapshot_metadata/reports/verify_convert-123456.json
```
