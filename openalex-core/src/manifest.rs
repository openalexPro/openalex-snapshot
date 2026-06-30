//! The official OpenAlex parquet manifest model
//! (`s3://openalex/data/parquet/manifest.json`) and the corpus path mapping.
//!
//! The manifest gives per-file exact byte size and row count, which drives
//! `verify_download`. Parsing is pure (`from_json`); fetching it over S3 is left
//! to the caller (the CLI shells out to `aws s3 cp`) so this crate stays free of
//! any network/process dependency and is equally callable from R.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Local staging directory name for a dataset. The raw official `works` sync
/// lands in `works_aws/` (the stable `aws s3 sync` target, never overwritten by
/// enrich); every other dataset uses its own name.
pub fn local_dir_for(dataset: &str) -> &str {
    if dataset == "works" {
        "works_aws"
    } else {
        dataset
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetManifest {
    #[serde(default)]
    pub date: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    pub meta: ManifestMeta,
    pub entities: Vec<ManifestEntity>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestMeta {
    pub record_count: u64,
    pub content_length: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestEntity {
    pub entity: String,
    pub content_length: u64,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestFile {
    pub url: String,
    pub meta: ManifestFileMeta,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestFileMeta {
    pub content_length: u64,
    pub record_count: u64,
}

impl ParquetManifest {
    /// Parse the top-level manifest JSON text. Tolerates extra/unknown keys.
    pub fn from_json(json: &str) -> Result<ParquetManifest> {
        serde_json::from_str(json).context("failed to parse parquet manifest.json")
    }
}

/// Map a manifest file URL (`s3://<bucket>/data/parquet/<entity>/updated_date=.../part.parquet`)
/// to its local path under `<parquet_dir>/<local_dir_for(entity)>/updated_date=.../part.parquet`.
pub fn manifest_url_to_local(parquet_dir: &Path, url: &str) -> Option<PathBuf> {
    let marker = "/data/parquet/";
    let idx = url.find(marker)?;
    let rel = &url[idx + marker.len()..]; // "<entity>/updated_date=.../part.parquet"
    let mut parts = rel.splitn(2, '/');
    let entity = parts.next()?;
    let rest = parts.next().unwrap_or("");
    let mut p = parquet_dir.join(local_dir_for(entity));
    if !rest.is_empty() {
        p = p.join(rest);
    }
    Some(p)
}
