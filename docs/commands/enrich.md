# Command: enrich

Enrich the raw works parquet with two derived columns, writing a separate enriched copy and
leaving the downloaded data untouched:

- `<root>/parquet/works_aws/` — raw official works (input; manifest-verifiable)
- `<root>/parquet/works/` — enriched output (raw columns **+ `abstract` + `citation`**)

`download` runs this automatically for `works` unless `--no-enrich` is given; the standalone
command is for re-runs and repair.

## Derived columns

- **`abstract`** — plain text reconstructed from the JSON `abstract_inverted_index`
  (positions sorted ascending, words joined with single spaces).
- **`citation`** — `"Author (year)"` / `"A & B (year)"` / `"A et al. (year)"` from
  `authorships` + `publication_year`; null year renders as `(n.d.)`; null/empty authorships ⇒ null.

Each column is only added when its source column exists in the parquet.

## Usage

```bash
openalex-snapshot enrich --root-dir /data
# rebuild everything (ignore the incremental skip):
openalex-snapshot enrich --root-dir /data --overwrite
```

## Behavior

- Mirrors the `updated_date=.../part_*.parquet` partition layout.
- **Incremental**: skips outputs newer than their source; re-running after an incremental
  download only rewrites changed partitions.
- **Row-parity self-check**: the enriched row count must equal the source row count, or the
  file is treated as a failure and removed.
- The raw `works_aws/` is never modified, so it stays byte-identical to OpenAlex and the next
  snapshot's incremental `aws s3 sync` is unaffected.
- Only the `works` dataset is supported (other datasets need no enrichment).

## Reading enriched works

`index --dataset all` skips `*_aws` staging dirs and indexes the canonical `works/`; `extract`
routes `W` IDs to `works/`, so extracted rows include `abstract` + `citation`.
