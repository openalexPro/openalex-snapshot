# Command: all

Run the full parquet-native pipeline from config.

## Usage

```bash
openalex-snapshot all --config ./openalex-snapshot.yaml
```

## Flow

1. `download` (if enabled) — syncs the official parquet; auto-enriches `works` unless
   `download.no_enrich: true`
2. `verify_download` (if enabled) — manifest presence/size/row-count
3. `index` (if enabled) — builds `<dataset>_id_idx.parquet` for each dataset (skips `*_aws`)
4. `verify_index` (if enabled)

Each stage can be toggled in the `all:` config section
(`enable_download`, `enable_verify_download`, `enable_index`, `enable_verify_index`).
