# Command: verify_download

Strictly validate downloaded snapshot integrity.

## Usage

```bash
openalex-snapshot verify_download --root-dir /data
```

## Checks

- remote manifest parity
- missing files
- size mismatches
- optional extra local files
- gzip integrity for `.json.gz`
