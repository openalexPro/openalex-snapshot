//! Pure-Rust parquet I/O primitives shared by the pipeline operations.
//!
//! These wrap the `arrow` + `parquet` crates with the small, dependency-free
//! helpers the CLI and the `openalexPro` R package both need: footer row counts,
//! column inspection, projection-scanned distinct values, the canonical SNAPPY
//! writer settings, and UTF-8 column collection. They take plain `&Path` /
//! `ArrayRef` arguments and return `anyhow::Result`, so any client can call them.

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, ArrayRef, LargeStringArray, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs;
use std::path::Path;

/// Collect a Utf8/LargeUtf8 arrow column into owned `Option<String>` values.
pub fn string_col_values(arr: &ArrayRef, ctx: &str) -> Result<Vec<Option<String>>> {
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        Ok((0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
            .collect())
    } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        Ok((0..a.len())
            .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
            .collect())
    } else {
        bail!("expected a UTF8 string column for {ctx}")
    }
}

/// The canonical writer properties used by every parquet file this tool emits.
pub fn snappy_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

/// Total row count of a parquet file from its footer metadata (no data scan).
pub fn parquet_rowcount_meta(path: &Path) -> Result<u64> {
    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(f)
        .with_context(|| format!("read parquet footer {}", path.display()))?;
    Ok(reader.metadata().file_metadata().num_rows().max(0) as u64)
}

/// Total row count by decoding every row group — catches data-page corruption a footer
/// read would miss. Used by `verify_download --full`.
pub fn parquet_full_rowcount(path: &Path) -> Result<u64> {
    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("open parquet {}", path.display()))?
        .build()
        .with_context(|| format!("read parquet {}", path.display()))?;
    let mut n: u64 = 0;
    for batch in reader {
        let batch = batch.with_context(|| format!("decode parquet {}", path.display()))?;
        n += batch.num_rows() as u64;
    }
    Ok(n)
}

/// Top-level (root) column names of a parquet file, from footer metadata (no data scan).
pub fn parquet_top_level_columns(path: &Path) -> Result<Vec<String>> {
    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(f)
        .with_context(|| format!("read parquet footer {}", path.display()))?;
    Ok(reader
        .metadata()
        .file_metadata()
        .schema()
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect())
}

/// Distinct non-null values of a string column, reading only that column (projection).
pub fn parquet_distinct_strings(path: &Path, col: &str) -> Result<Vec<String>> {
    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("open parquet {}", path.display()))?;
    let pq = builder.parquet_schema();
    let leaf = (0..pq.num_columns())
        .find(|&i| pq.column(i).name() == col)
        .ok_or_else(|| anyhow!("column {col} not found in {}", path.display()))?;
    let mask = ProjectionMask::leaves(pq, std::iter::once(leaf));
    let reader = builder
        .with_projection(mask)
        .build()
        .with_context(|| format!("read parquet {}", path.display()))?;
    let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("decode parquet {}", path.display()))?;
        let arr = batch.column(0);
        if let Some(sa) = arr.as_any().downcast_ref::<StringArray>() {
            for i in 0..sa.len() {
                if !sa.is_null(i) {
                    set.insert(sa.value(i).to_string());
                }
            }
        } else if let Some(sa) = arr.as_any().downcast_ref::<LargeStringArray>() {
            for i in 0..sa.len() {
                if !sa.is_null(i) {
                    set.insert(sa.value(i).to_string());
                }
            }
        } else {
            bail!(
                "column {col} is not a UTF8 string column in {}",
                path.display()
            );
        }
    }
    Ok(set.into_iter().collect())
}
