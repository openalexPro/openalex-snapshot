# skills

Create a project-local `skills/` starter pack for AI coding agents.

## Usage

```bash
openalex-snapshot skills --root-dir .
```

## Options

- `--root-dir <path>` root containing snapshot/parquet/metadata layout
- `--overwrite` rewrite generated template files
- `--stdout` print template manifest/content
- `--explain` print planned actions only

## Behavior

- Default mode is non-destructive: missing files are created, existing files are kept.
- Generated content focuses on command operation, pipeline runbook, recovery, and docs hygiene.
