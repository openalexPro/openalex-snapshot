//! Extracting records by id using a built index.
//!
//! [`extract_index_lookup`] scans an `*_id_idx.parquet` to find which corpus
//! files hold the requested ids; [`extract_rows_to_parquet`] streams those files
//! and writes the matching rows (all columns, including nested struct/list ones)
//! to an output parquet. The CLI resolves the index and corpus paths; the
//! row-level filtering lives here so R extracts identically.

use crate::parquetio::{snappy_writer_props, string_col_values};
use anyhow::{anyhow, Context, Result};
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Scan an `*_id_idx.parquet`, returning the matched ids and the (relative) parquet_file
/// paths that contain them — the set of `requested` ids that appear in the index.
pub fn extract_index_lookup(
    index_file: &Path,
    requested: &BTreeSet<String>,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let f = fs::File::open(index_file).with_context(|| format!("open {}", index_file.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("open parquet {}", index_file.display()))?;
    let pq = builder.parquet_schema();
    let leaves: Vec<usize> = (0..pq.num_columns())
        .filter(|&i| matches!(pq.column(i).name(), "id" | "parquet_file"))
        .collect();
    let mask = ProjectionMask::leaves(pq, leaves);
    let reader = builder
        .with_projection(mask)
        .build()
        .with_context(|| format!("read parquet {}", index_file.display()))?;
    let mut matched: BTreeSet<String> = BTreeSet::new();
    let mut files: BTreeSet<String> = BTreeSet::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("decode {}", index_file.display()))?;
        let idcol = batch
            .column_by_name("id")
            .ok_or_else(|| anyhow!("index {} missing id column", index_file.display()))?;
        let pfcol = batch
            .column_by_name("parquet_file")
            .ok_or_else(|| anyhow!("index {} missing parquet_file column", index_file.display()))?;
        let ids = string_col_values(idcol, "id")?;
        let pfs = string_col_values(pfcol, "parquet_file")?;
        for (ido, pfo) in ids.iter().zip(pfs.iter()) {
            if let Some(id) = ido {
                if requested.contains(id) {
                    matched.insert(id.clone());
                    if let Some(pf) = pfo {
                        files.insert(pf.clone());
                    }
                }
            }
        }
    }
    Ok((matched, files))
}

/// Stream each corpus file, keep rows whose `id` is in `matched`, and write all columns
/// (including nested struct/list columns) to `out`. Returns the number of rows written.
pub fn extract_rows_to_parquet(
    files: &[PathBuf],
    matched: &BTreeSet<String>,
    out: &Path,
) -> Result<u64> {
    let first =
        fs::File::open(&files[0]).with_context(|| format!("open {}", files[0].display()))?;
    let schema = ParquetRecordBatchReaderBuilder::try_new(first)
        .with_context(|| format!("open parquet {}", files[0].display()))?
        .schema()
        .clone();
    let outf = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut writer = ArrowWriter::try_new(outf, schema, Some(snappy_writer_props()))
        .with_context(|| format!("init writer {}", out.display()))?;
    let mut written: u64 = 0;
    for file in files {
        let f = fs::File::open(file).with_context(|| format!("open {}", file.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| format!("open parquet {}", file.display()))?
            .build()
            .with_context(|| format!("read parquet {}", file.display()))?;
        for batch in reader {
            let batch = batch.with_context(|| format!("decode {}", file.display()))?;
            let idcol = batch
                .column_by_name("id")
                .ok_or_else(|| anyhow!("corpus file {} missing id column", file.display()))?;
            let ids = string_col_values(idcol, "id")?;
            let mask: BooleanArray = ids
                .iter()
                .map(|o| Some(o.as_deref().is_some_and(|s| matched.contains(s))))
                .collect();
            let keep = mask.true_count();
            if keep == 0 {
                continue;
            }
            let filtered = filter_record_batch(&batch, &mask)
                .with_context(|| format!("filter {}", file.display()))?;
            writer
                .write(&filtered)
                .with_context(|| format!("write {}", out.display()))?;
            written += keep as u64;
        }
    }
    writer
        .close()
        .with_context(|| format!("close {}", out.display()))?;
    Ok(written)
}
