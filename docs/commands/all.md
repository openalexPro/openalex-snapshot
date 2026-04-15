# Command: all

Run the full pipeline from config with a bounded `verify_convert`/`repair_convert` retry loop.

## Usage

```bash
openalex-snapshot all --config ./openalex-snapshot.yaml --retry 2
```

## Flow

1. `check` (if enabled)
2. `download` (if enabled)
3. `verify_download` (if enabled)
4. `convert`
5. `verify_convert`
6. `repair_convert` (only when verify fails, up to `--retry`)
7. `index`
8. `verify_index`
