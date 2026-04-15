# Command: config

Create or verify `openalex-snapshot` YAML config files.

## Usage

```bash
openalex-snapshot config --create complete
openalex-snapshot config --create safe --config ./openalex-snapshot-safe.yaml
openalex-snapshot config --verify --config ./openalex-snapshot.yaml
```

## Notes

- `--create` templates: `complete` (default), `safe`, `fast`
- precedence: CLI > command section > defaults section > built-in defaults
- `--verify` fails on unknown keys or wrong types
