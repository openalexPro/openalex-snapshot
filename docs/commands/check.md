# check

Run environment and capacity preflight checks before heavy operations.

## Usage

```bash
openalex-snapshot check --root-dir . --dataset all
```

## What it checks

- dependency binaries (`duckdb`, `aws`)
- path writability for root/snapshot/parquet/metadata
- download disk requirement (remote manifest + safety margin)
- convert disk requirement (precise source inventory estimate)
- memory/tuning risk based on profile/workers/memory settings

## Options

- `--strict` fail on warnings as well as failures
- `--json` machine-readable output
- `--precise` enable precise source inventory estimate
- shared runtime args: `--dataset`, `--profile`, `--workers`, `--max-memory-mb`
