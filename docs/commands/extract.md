# Command: extract

Extract records by OpenAlex IDs from CSV using per-dataset parquet indexes.

The command auto-routes each ID to a dataset using:

- entity prefixes (`W`, `A`, `S`, `I`, `T`, `K`, `P`, `F`, `G`, `C`)
- taxonomy namespaces (`countries/`, `continents/`, `languages/`, `domains/`, `fields/`, `subfields/`, `sdgs/`, `work-types/`, `source-types/`, `licenses/`, `institution-types/`)

It writes one output file per resolved dataset as:

- `<output_base>_<dataset>.parquet`

Unknown/unmapped IDs are skipped and reported.

## Example

```bash
openalex-snapshot extract \
  --root-dir /Volumes/openalex \
  --ids /Volumes/openalex/ids.csv \
  --output /Volumes/openalex/extract.parquet \
  --profile balanced
```

## Notes

- Requires per-dataset index files (for example `parquet/works_id_idx.parquet`).
- Build indexes first with `openalex-snapshot index`.
- Use `--dataset <name>` to restrict extraction to one dataset.
