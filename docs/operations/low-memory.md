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
- `safe` profile allocates 15% of usable RAM (1–8 GiB global), caps workers at 2
- `--workers 1` ensures the full budget goes to one file at a time
- `--max-memory-mb` overrides the profile calculation if you need a precise cap
- Use repeated `--input-file` to isolate and retry specific problematic files
