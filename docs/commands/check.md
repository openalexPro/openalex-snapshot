# check

Run environment and capacity preflight checks before heavy operations.

## Usage

```bash
openalex-snapshot check --root-dir . --dataset all
```

## What it checks

- dependency binaries (`aws` — `duckdb` is bundled and reported as ok)
- path writability for root/parquet/metadata
- download disk requirement (parquet `manifest.json` size + safety margin)

## Options

- `--strict` fail on warnings as well as failures
- `--json` machine-readable output
- `--precise` enable precise source inventory estimate
- shared runtime args: `--dataset`, `--workers`, `--max-memory-mb`
