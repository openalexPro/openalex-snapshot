# Migration Plan: Separate Repository

This project is a strong candidate for a dedicated repository.

## Why split out

- Independent release cadence for CLI
- Clear issue tracker for conversion/index/verify topics
- Easier binary distribution and CI focused on Rust toolchain
- Cleaner separation from R package internals

## Recommended target layout

```text
openalex-convert/
  Cargo.toml
  src/
  tests/
  man/
  docs/
  .github/workflows/
  README.md
  CHANGELOG.md
  LICENSE
```

## Suggested release workflow

1. Tag semantic versions (`vX.Y.Z`)
2. Build release binaries for macOS/Linux
3. Publish release notes with breaking-change section
4. Keep docs version notes per release

## Compatibility statement

Document explicit contracts:

- folder mapping parity (`.gz` -> `.parquet`)
- cache contract (`.<dataset>_metadata/schemata/unified_schema.csv`)
- verify metadata contract (`source_file_metrics.csv`)
- `schema --format arrow-r` format stability

## Transition checklist

1. Copy `openalex-convert/` into new repo.
2. Preserve git history if desired (`git subtree split` or `filter-repo`).
3. Add CI for `cargo test`, integration smoke tests, and docs checks.
4. Update R package docs to point to external CLI repo/releases.
5. Publish initial `v0.x` release with migration notes.
