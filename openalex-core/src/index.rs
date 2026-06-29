//! Building the per-dataset id index (`<dataset>_id_idx.parquet`).
//!
//! Two stages, both pure parquet I/O: [`build_index_shard`] reads the `id`
//! column of one corpus file and emits a shard with `(id, id_block,
//! parquet_file, file_row_number)`; [`concat_index_shards`] merges all shards
//! into the final index. The CLI drives the per-file parallelism and resume
//! logic; the row-level work lives here so the R package gets the same index.

use crate::parquetio::{snappy_writer_props, string_col_values};
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Block bucket for an OpenAlex id: the trailing integer divided by 10000.
/// `None` when the id has no trailing digits.
pub fn id_block_of(id: &str) -> Option<i32> {
    let rev_digits: String = id
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if rev_digits.is_empty() {
        return None;
    }
    let digits: String = rev_digits.chars().rev().collect();
    let v: i64 = digits.parse().ok()?;
    Some((v / 10000) as i32)
}

/// Arrow schema of an index shard / the combined index parquet.
pub fn index_shard_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("id_block", DataType::Int32, true),
        Field::new("parquet_file", DataType::Utf8, false),
        Field::new("file_row_number", DataType::Int64, false),
    ]))
}

/// Stage 1: read the `id` column of one corpus parquet, derive (id, id_block,
/// parquet_file=rel, file_row_number) and stream it into a shard parquet.
pub fn build_index_shard(src: &Path, rel: &str, out: &Path) -> Result<()> {
    let f = fs::File::open(src).with_context(|| format!("open {}", src.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("open parquet {}", src.display()))?;
    let pq = builder.parquet_schema();
    let id_leaf = (0..pq.num_columns())
        .find(|&i| pq.column(i).name() == "id")
        .ok_or_else(|| anyhow!("'id' column not found in {}", src.display()))?;
    let mask = ProjectionMask::leaves(pq, std::iter::once(id_leaf));
    let reader = builder
        .with_projection(mask)
        .build()
        .with_context(|| format!("read parquet {}", src.display()))?;

    let schema = index_shard_schema();
    let outf = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut writer = ArrowWriter::try_new(outf, schema.clone(), Some(snappy_writer_props()))
        .with_context(|| format!("init writer {}", out.display()))?;
    let mut row_no: i64 = 0;
    for batch in reader {
        let batch = batch.with_context(|| format!("decode {}", src.display()))?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        let ids = string_col_values(batch.column(0), "id")?;
        let id_block: Int32Array = ids
            .iter()
            .map(|o| o.as_deref().and_then(id_block_of))
            .collect();
        let id_arr: StringArray = ids.iter().map(|o| o.as_deref()).collect();
        let pfile: StringArray = (0..n).map(|_| Some(rel)).collect();
        let frn: Int64Array = (row_no..row_no + n as i64).map(Some).collect();
        row_no += n as i64;
        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_arr) as ArrayRef,
                Arc::new(id_block) as ArrayRef,
                Arc::new(pfile) as ArrayRef,
                Arc::new(frn) as ArrayRef,
            ],
        )?;
        writer
            .write(&rb)
            .with_context(|| format!("write {}", out.display()))?;
    }
    writer
        .close()
        .with_context(|| format!("close {}", out.display()))?;
    Ok(())
}

/// Stage 2: concatenate all shard parquets in `shard_dir` into a single `out` parquet.
pub fn concat_index_shards(shard_dir: &Path, out: &Path) -> Result<()> {
    let mut shards: Vec<PathBuf> = fs::read_dir(shard_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    shards.sort();
    if shards.is_empty() {
        bail!("no index shards found in {}", shard_dir.display());
    }
    let outf = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut writer = ArrowWriter::try_new(outf, index_shard_schema(), Some(snappy_writer_props()))
        .with_context(|| format!("init writer {}", out.display()))?;
    for sh in &shards {
        let f = fs::File::open(sh).with_context(|| format!("open {}", sh.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| format!("open parquet {}", sh.display()))?
            .build()
            .with_context(|| format!("read parquet {}", sh.display()))?;
        for batch in reader {
            let batch = batch.with_context(|| format!("decode {}", sh.display()))?;
            writer
                .write(&batch)
                .with_context(|| format!("write {}", out.display()))?;
        }
    }
    writer
        .close()
        .with_context(|| format!("close {}", out.display()))?;
    Ok(())
}
