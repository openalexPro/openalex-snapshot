# Runbook: Low-Memory Conversion

```bash
openalex-snapshot convert \
  --root-dir /data \
  --dataset works \
  --profile safe \
  --workers 1 \
  --max-memory-mb 4096
```

Notes:
- keep workers low (`1`) for hard memory constraints
- use repeated `--input-file` to isolate problematic files
