//! JSON→Parquet conversion pipeline.
//!
//! Provides four entry points used by the `openalexPro` R package via the
//! extendr bridge:
//!
//! - [`snapshot_to_parquet`] — full snapshot pipeline (schema inference +
//!   per-file parallel COPY).
//! - [`api_files_to_parquet`] — parallel COPY for API JSON responses (schema
//!   inference stays in R).
//! - [`build_corpus_index`] — two-stage ID-lookup index builder.
//! - [`lookup_by_id`] — ID-based record extraction using a pre-built index.
//!
//! All functions are gated behind the `conversion` feature flag.

use anyhow::{Context, Result};
use duckdb::Connection;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ── Schema inference helpers ─────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct ColumnDef {
    name: String,
    col_type: String,
}

/// Numeric types in widening order (TINYINT < … < DOUBLE).
const NUMERIC_ORDER: &[&str] = &[
    "TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE",
];

fn is_complex_type(t: &str) -> bool {
    let u = t.to_uppercase();
    u.starts_with("STRUCT") || u.starts_with("LIST") || u.starts_with("MAP")
}

/// Count the number of top-level fields in a `STRUCT(...)` type string.
fn count_struct_fields(t: &str) -> usize {
    let inner = match t
        .to_uppercase()
        .strip_prefix("STRUCT(")
        .and_then(|s| s.strip_suffix(')'))
    {
        Some(s) => &t[7..t.len() - 1 - (t.len() - 7 - s.len())],
        None => return 0,
    };
    let mut depth = 0usize;
    let mut count = 1usize;
    for ch in inner.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => count += 1,
            _ => {}
        }
    }
    count
}

fn widen_types(types: &[String]) -> String {
    let mut seen = HashSet::new();
    let unique: Vec<&str> = types
        .iter()
        .map(String::as_str)
        .filter(|&s| seen.insert(s))
        .collect();

    if unique.len() == 1 {
        return unique[0].to_string();
    }

    // Complex types (STRUCT/LIST/MAP) beat simple types.
    let complex: Vec<&str> = unique
        .iter()
        .copied()
        .filter(|t| is_complex_type(t))
        .collect();
    match complex.len() {
        0 => {}
        1 => return complex[0].to_string(),
        _ => {
            return complex
                .iter()
                .max_by_key(|t| count_struct_fields(t))
                .unwrap()
                .to_string();
        }
    }

    // Pure numeric conflicts → widest wins.
    let ranks: Vec<Option<usize>> = unique
        .iter()
        .map(|t| {
            NUMERIC_ORDER
                .iter()
                .position(|&n| n.eq_ignore_ascii_case(t))
        })
        .collect();
    if ranks.iter().any(Option::is_some) {
        let max_rank = ranks.iter().filter_map(|r| *r).max().unwrap();
        return NUMERIC_ORDER[max_rank].to_string();
    }

    // Fallback.
    "VARCHAR".to_string()
}

fn merge_schemas(schemas: Vec<Vec<ColumnDef>>) -> Vec<ColumnDef> {
    let mut ordered_names: Vec<String> = Vec::new();
    let mut col_type_map: HashMap<String, Vec<String>> = HashMap::new();

    for schema in &schemas {
        for col in schema {
            if !col_type_map.contains_key(&col.name) {
                ordered_names.push(col.name.clone());
                col_type_map.insert(col.name.clone(), Vec::new());
            }
            col_type_map
                .get_mut(&col.name)
                .unwrap()
                .push(col.col_type.clone());
        }
    }

    ordered_names
        .iter()
        .map(|name| {
            let types = col_type_map.get(name).unwrap();
            ColumnDef {
                name: name.clone(),
                col_type: widen_types(types),
            }
        })
        .collect()
}

/// Run `DESCRIBE SELECT * FROM read_json_auto(['<file>'], …)` and return the
/// inferred schema for that file.
fn infer_schema_one(conn: &Connection, file: &str, extra_opts: &str) -> Result<Vec<ColumnDef>> {
    let sql = format!(
        "DESCRIBE SELECT * FROM read_json_auto(['{}'], union_by_name = true, ignore_errors = true{})",
        file.replace('\'', "\\'"),
        extra_opts
    );
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("DESCRIBE failed for {file}"))?;
    let mut rows = stmt.query([]).context("query failed")?;
    let mut cols = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let col_type: String = row.get(1)?;
        cols.push(ColumnDef { name, col_type });
    }
    Ok(cols)
}

fn build_columns_clause(schema: &[ColumnDef]) -> String {
    let defs: Vec<String> = schema
        .iter()
        .map(|c| format!("'{}': '{}'", c.name, c.col_type))
        .collect();
    format!("{{{}}}", defs.join(", "))
}

/// Load a `unified_schema.csv` written by this function or the R
/// `infer_json_schema()`.  Returns `None` if the file is absent or malformed.
fn load_schema_cache(cache_path: &Path) -> Option<Vec<ColumnDef>> {
    let content = std::fs::read_to_string(cache_path).ok()?;
    let mut lines = content.lines();
    let header = lines.next()?;
    if !header
        .trim_start_matches('\u{feff}')
        .starts_with("col_name")
    {
        return None;
    }
    let cols: Vec<ColumnDef> = lines
        .filter_map(|line| {
            // Handle both quoted and unquoted CSV values.
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let (name, col_type) = if line.starts_with('"') {
                // Quoted CSV: "name","type"
                let mut parts = line.splitn(2, "\",\"");
                let name = parts.next()?.trim_matches('"').to_string();
                let col_type = parts.next()?.trim_matches('"').to_string();
                (name, col_type)
            } else {
                let mut parts = line.splitn(2, ',');
                (parts.next()?.to_string(), parts.next()?.to_string())
            };
            if name.is_empty() {
                None
            } else {
                Some(ColumnDef { name, col_type })
            }
        })
        .collect();
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

fn save_schema_cache(cache_path: &Path, schema: &[ColumnDef]) -> Result<()> {
    let mut content = String::from("col_name,col_type\n");
    for col in schema {
        content.push_str(&format!("{},{}\n", col.name, col.col_type));
    }
    std::fs::write(cache_path, content).context("write schema cache")?;
    Ok(())
}

// ── Path utilities ───────────────────────────────────────────────────────────

fn collect_files_recursive(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        let mut entries: Vec<_> = entries.flatten().collect();
        entries.sort_by_key(|e| e.path());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                files.extend(collect_files_recursive(&path, ext));
            } else if path.to_string_lossy().ends_with(ext) {
                files.push(path);
            }
        }
    }
    files
}

/// Convert a path to forward-slash form for DuckDB SQL.
fn dq(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

// ── snapshot_to_parquet ──────────────────────────────────────────────────────

/// Full snapshot→Parquet pipeline.
///
/// `snapshot_dir` must contain a `data/` subdirectory with per-dataset NDJSON
/// (`.json.gz`) files.  `parquet_dir` receives the converted Parquet files,
/// mirroring the relative layout of `snapshot_dir/data/`.
///
/// - `data_sets`: datasets to convert; empty = all (excluding `merged_ids`).
/// - `workers`: rayon thread count; 0 or 1 = sequential.
/// - `sample_size`: number of gz files to sample for schema inference; 0 = all.
/// - `memory_limit`: DuckDB memory limit string, e.g. `"8GB"`; empty = none.
/// - `temp_dir`: DuckDB temp directory; empty = system default.
pub fn snapshot_to_parquet(
    snapshot_dir: &str,
    parquet_dir: &str,
    data_sets: Vec<String>,
    workers: usize,
    sample_size: usize,
    memory_limit: &str,
    temp_dir: &str,
    verbose: bool,
) -> Result<()> {
    let snapshot_path = Path::new(snapshot_dir);
    let parquet_path = Path::new(parquet_dir);
    std::fs::create_dir_all(parquet_path).context("create parquet_dir")?;
    // Suppress macOS Spotlight indexing.
    let _ = std::fs::File::create(parquet_path.join(".metadata_never_index"));

    let data_dir = snapshot_path.join("data");

    // Resolve dataset list.
    let ds_list: Vec<String> = if data_sets.is_empty() {
        let mut dirs: Vec<String> = std::fs::read_dir(&data_dir)
            .with_context(|| format!("read_dir {}", data_dir.display()))?
            .flatten()
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                if name != "merged_ids" {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();
        dirs.sort();
        dirs
    } else {
        data_sets
    };

    let ml = if memory_limit.is_empty() {
        None
    } else {
        Some(memory_limit)
    };
    let td = if temp_dir.is_empty() {
        None
    } else {
        Some(temp_dir)
    };

    for ds in &ds_list {
        if verbose {
            eprintln!("[snapshot_to_parquet] Processing {} ...", ds);
        }
        let ds_start = std::time::Instant::now();

        let json_dir = data_dir.join(ds);
        let parquet_ds = parquet_path.join(ds);
        std::fs::create_dir_all(&parquet_ds).context("create parquet dataset dir")?;

        let gz_files = collect_files_recursive(&json_dir, ".gz");
        if gz_files.is_empty() {
            eprintln!(
                "[snapshot_to_parquet] Warning: no .gz files found for '{}', skipping.",
                ds
            );
            continue;
        }

        // Resume: skip already-converted files.
        let existing_parquets: HashSet<PathBuf> = collect_files_recursive(&parquet_ds, ".parquet")
            .into_iter()
            .map(|p| p.strip_prefix(&parquet_ds).unwrap_or(&p).to_path_buf())
            .collect();

        let todo: Vec<(PathBuf, PathBuf)> = gz_files
            .iter()
            .filter_map(|gf| {
                let rel = gf
                    .strip_prefix(&json_dir)
                    .unwrap_or(gf)
                    .with_extension("parquet");
                if existing_parquets.contains(&rel) {
                    None
                } else {
                    let out = parquet_ds.join(&rel);
                    Some((gf.clone(), out))
                }
            })
            .collect();

        let skipped = gz_files.len() - todo.len();
        if skipped > 0 {
            eprintln!(
                "[snapshot_to_parquet]   Skipping {} already converted file(s)",
                skipped
            );
        }
        if todo.is_empty() {
            eprintln!("[snapshot_to_parquet]   All files already converted.");
            continue;
        }
        if verbose {
            eprintln!(
                "[snapshot_to_parquet]   Converting {} file(s)...",
                todo.len()
            );
        }

        // Works need a larger max object size (long abstracts).
        let extra_opts = if ds == "works" {
            ", maximum_object_size=1000000000"
        } else {
            ""
        };

        // ── Schema inference ────────────────────────────────────────────────
        let schema_cache_dir = parquet_ds.join(".schema_cache");
        std::fs::create_dir_all(&schema_cache_dir).ok();
        let unified_cache = schema_cache_dir.join("unified_schema.csv");

        let schema: Option<Vec<ColumnDef>> = if unified_cache.exists() {
            if verbose {
                eprintln!("[snapshot_to_parquet]   Loading cached schema.");
            }
            load_schema_cache(&unified_cache)
        } else {
            let n_sample = if sample_size > 0 && todo.len() > sample_size {
                sample_size
            } else {
                todo.len()
            };
            let sample_files: Vec<&PathBuf> = todo[..n_sample].iter().map(|(gf, _)| gf).collect();

            if verbose {
                eprintln!(
                    "[snapshot_to_parquet]   Inferring schema from {} file(s)...",
                    sample_files.len()
                );
            }

            let conn = Connection::open_in_memory().context("open DuckDB for schema")?;
            conn.execute_batch("INSTALL json; LOAD json;").ok();
            if let Some(m) = ml {
                conn.execute_batch(&format!("SET memory_limit = '{}'", m))
                    .ok();
            }

            let schemas: Vec<Vec<ColumnDef>> = sample_files
                .iter()
                .filter_map(|f| {
                    infer_schema_one(&conn, &dq(f), extra_opts)
                        .ok()
                        .filter(|s| !s.is_empty())
                })
                .collect();

            if schemas.is_empty() {
                if verbose {
                    eprintln!(
                        "[snapshot_to_parquet]   Schema inference failed for all sampled files."
                    );
                }
                None
            } else {
                let mut merged = merge_schemas(schemas);
                // Apply DuckDB type normalisation.
                for col in &mut merged {
                    col.col_type = crate::normalize_duckdb_type(&col.col_type);
                }
                // Works: store abstract_inverted_index as raw VARCHAR to avoid
                // DuckDB's case-folding collision on duplicate JSON keys.
                if ds == "works" {
                    if let Some(col) = merged
                        .iter_mut()
                        .find(|c| c.name == "abstract_inverted_index")
                    {
                        col.col_type = "VARCHAR".to_string();
                    }
                }
                save_schema_cache(&unified_cache, &merged).ok();
                Some(merged)
            }
        };

        let columns_clause_arc: Arc<Option<String>> =
            Arc::new(schema.as_ref().map(|s| build_columns_clause(s)));
        let ml_arc = Arc::new(ml.map(str::to_string));
        let td_arc = Arc::new(td.map(str::to_string));
        let extra_opts_str = extra_opts.to_string();

        // ── Parallel file conversion ────────────────────────────────────────
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers.max(1))
            .build()
            .context("build rayon pool")?;

        pool.install(|| {
            todo.par_iter().for_each(|(input, output)| {
                if let Some(parent) = output.parent() {
                    std::fs::create_dir_all(parent).ok();
                }
                let result = convert_one_file(
                    &dq(input),
                    &dq(output),
                    columns_clause_arc.as_deref(),
                    &extra_opts_str,
                    ml_arc.as_deref(),
                    td_arc.as_deref(),
                );
                if let Err(e) = result {
                    eprintln!(
                        "[snapshot_to_parquet] Failed to convert {}: {}",
                        input.display(),
                        e
                    );
                }
            });
        });

        if verbose {
            eprintln!(
                "[snapshot_to_parquet]   done after {:.2}s",
                ds_start.elapsed().as_secs_f64()
            );
        }
    }

    Ok(())
}

/// Open a per-worker DuckDB connection and run one COPY statement.
fn convert_one_file(
    input: &str,
    output: &str,
    columns_clause: Option<&str>,
    extra_opts: &str,
    memory_limit: Option<&str>,
    temp_dir: Option<&str>,
) -> Result<()> {
    let conn = Connection::open_in_memory().context("open worker DuckDB")?;
    conn.execute_batch("INSTALL json; LOAD json;").ok();
    if let Some(m) = memory_limit {
        conn.execute_batch(&format!("SET memory_limit = '{}'", m))
            .ok();
    }
    if let Some(t) = temp_dir {
        conn.execute_batch(&format!("SET temp_directory = '{}'", t))
            .ok();
    }

    let read_fn = if let Some(cols) = columns_clause {
        format!("read_json('{}', columns = {}{})", input, cols, extra_opts)
    } else {
        format!("read_json_auto('{}'{}", input, extra_opts)
    };

    let sql = format!(
        "COPY (SELECT * FROM {}) TO '{}' \
         (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)",
        read_fn, output
    );
    conn.execute_batch(&sql)
        .with_context(|| format!("COPY failed for {}", input))?;
    Ok(())
}

// ── api_files_to_parquet ─────────────────────────────────────────────────────

/// Parallel per-file COPY for OpenAlex API JSON responses.
///
/// Schema inference remains in R; this function receives pre-computed SQL
/// fragments and executes them in parallel via rayon.
///
/// - `array_field`: key containing the records array (`"results"`,
///   `"group_by"`); empty = single-record file (no unnesting).
/// - `list_type`: DuckDB STRUCT type string for the array items (e.g.
///   `"STRUCT(id VARCHAR, …)[]"`); empty = use `read_json_auto`.
/// - `extra_select`: SQL fragment appended after `SELECT *`, e.g.
///   `", abstract_expr AS abstract, 'p1' AS page"`.
pub fn api_files_to_parquet(
    input_files: &[String],
    output_files: &[String],
    array_field: &str,
    list_type: &str,
    extra_select: &str,
    workers: usize,
    verbose: bool,
) -> Result<()> {
    if input_files.len() != output_files.len() {
        anyhow::bail!("input_files and output_files must have the same length");
    }

    let af = if array_field.is_empty() {
        None
    } else {
        Some(array_field)
    };
    let lt = if list_type.is_empty() {
        None
    } else {
        Some(list_type)
    };

    let array_field_arc = Arc::new(af.map(str::to_string));
    let list_type_arc = Arc::new(lt.map(str::to_string));
    let extra_select_arc = Arc::new(extra_select.to_string());

    let pairs: Vec<(String, String)> = input_files
        .iter()
        .zip(output_files.iter())
        .map(|(i, o)| (i.clone(), o.clone()))
        .collect();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()
        .context("build rayon pool")?;

    pool.install(|| {
        pairs.par_iter().for_each(|(fn_in, fn_out)| {
            if let Some(parent) = Path::new(fn_out).parent() {
                std::fs::create_dir_all(parent).ok();
            }

            let result = (|| -> Result<()> {
                let conn = Connection::open_in_memory().context("open DuckDB")?;
                conn.execute_batch("INSTALL json; LOAD json;").ok();

                let sql = match array_field_arc.as_deref() {
                    None => {
                        // Single-record file: bare JSON object.
                        format!(
                            "COPY (\n  SELECT *{}\n  FROM read_json_auto('{}')\n) \
                             TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)",
                            extra_select_arc,
                            fn_in.replace('\'', "\\'"),
                            fn_out.replace('\'', "\\'")
                        )
                    }
                    Some(af) => {
                        // Paginated file: unnest the named array.
                        let read_spec = match list_type_arc.as_deref() {
                            Some(lt) => format!(
                                "read_json('{}', columns = {{'{}': '{}', 'meta': 'JSON'}})",
                                fn_in.replace('\'', "\\'"),
                                af,
                                lt
                            ),
                            None => format!("read_json_auto('{}')", fn_in.replace('\'', "\\'")),
                        };
                        format!(
                            "COPY (\n  SELECT *{}\n  FROM (\n    SELECT r.*\n    \
                             FROM (SELECT unnest({}) AS r FROM {})\n  )\n) \
                             TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)",
                            extra_select_arc,
                            af,
                            read_spec,
                            fn_out.replace('\'', "\\'")
                        )
                    }
                };

                conn.execute_batch(&sql)
                    .with_context(|| format!("COPY failed for {}", fn_in))?;
                Ok(())
            })();

            if let Err(e) = result {
                if verbose {
                    eprintln!("[api_files_to_parquet] Failed to convert {}: {}", fn_in, e);
                }
            }
        });
    });

    Ok(())
}

// ── build_corpus_index ───────────────────────────────────────────────────────

/// Build a two-stage ID-lookup index for a single Parquet corpus directory.
///
/// Stage 1: per-file shard indexes (parallel via rayon).
/// Stage 2: combine shards into `<corpus_name>_id_idx.parquet`.
///
/// Returns the path to the created index file.
pub fn build_corpus_index(
    corpus_dir: &str,
    workers: usize,
    memory_limit: &str,
    overwrite: bool,
    verbose: bool,
) -> Result<String> {
    let corpus_path = Path::new(corpus_dir)
        .canonicalize()
        .with_context(|| format!("canonicalize corpus_dir: {}", corpus_dir))?;

    if !corpus_path.is_dir() {
        anyhow::bail!("corpus_dir is not a directory: {}", corpus_dir);
    }

    let parent_dir = corpus_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("corpus_dir has no parent: {}", corpus_dir))?;
    let corpus_name = corpus_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("corpus_dir has no basename"))?
        .to_string_lossy();

    let index_file = parent_dir.join(format!("{}_id_idx.parquet", corpus_name));

    if index_file.exists() {
        if !overwrite {
            eprintln!(
                "[build_corpus_index] index_file exists — creation skipped. \
                 Delete manually or set overwrite = TRUE: {}",
                index_file.display()
            );
            return Ok(dq(&index_file));
        }
        std::fs::remove_file(&index_file).context("remove existing index")?;
    }

    let parquet_files = collect_files_recursive(&corpus_path, ".parquet");
    if parquet_files.is_empty() {
        anyhow::bail!("No .parquet files found in {}", corpus_dir);
    }

    let ml = if memory_limit.is_empty() {
        None
    } else {
        Some(memory_limit)
    };

    if verbose {
        eprintln!(
            "[build_corpus_index] Building index from: {}",
            corpus_path.display()
        );
        eprintln!(
            "[build_corpus_index]     Writing to: {}",
            index_file.display()
        );
    }

    let total_start = std::time::Instant::now();
    let temp_dir_path = PathBuf::from(format!("{}_tmp", index_file.display()));
    std::fs::create_dir_all(&temp_dir_path).context("create temp dir")?;
    let _ = std::fs::File::create(temp_dir_path.join(".metadata_never_index"));

    // Compute the depth of parent_dir (number of path components) so that we
    // can derive relative paths from absolute ones by stripping that prefix.
    let parent_str = dq(parent_dir);
    let parent_depth = parent_str.split('/').count();

    if verbose {
        eprintln!(
            "[build_corpus_index] Stage 1: Indexing {} parquet file(s){}",
            parquet_files.len(),
            if workers > 1 {
                format!(" with {} workers...", workers)
            } else {
                " sequentially...".to_string()
            }
        );
    }

    let ml_arc = Arc::new(ml.map(str::to_string));
    let temp_dir_path_arc = Arc::new(temp_dir_path.clone());

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()
        .context("build rayon pool")?;

    pool.install(|| {
        parquet_files
            .par_iter()
            .enumerate()
            .for_each(|(i, pf)| {
                let out_file = temp_dir_path_arc.join(format!("idx_{:05}.parquet", i + 1));

                // Resume support.
                if out_file.exists() {
                    return;
                }

                let result = (|| -> Result<()> {
                    let conn = Connection::open_in_memory().context("open DuckDB")?;
                    conn.execute_batch(
                        "SET threads = 1; SET preserve_insertion_order = false;",
                    )
                    .ok();
                    if let Some(m) = ml_arc.as_deref() {
                        conn.execute_batch(&format!("SET memory_limit = '{}'", m)).ok();
                    }

                    // Derive relative path from parent_dir.
                    let pf_str = dq(pf);
                    let pf_parts: Vec<&str> = pf_str.split('/').collect();
                    let rel_path = if pf_parts.len() > parent_depth {
                        pf_parts[parent_depth..].join("/")
                    } else {
                        pf_str.clone()
                    };

                    let sql = format!(
                        "COPY (\
                            SELECT \
                                id, \
                                CAST(FLOOR(CAST(regexp_extract(id, '([0-9]+)$', 1) AS BIGINT) / 10000) AS INTEGER) AS id_block, \
                                '{}' AS parquet_file, \
                                file_row_number \
                            FROM read_parquet('{}', file_row_number = true)\
                        ) TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                        rel_path,
                        pf_str,
                        dq(&out_file)
                    );

                    conn.execute_batch(&sql)
                        .with_context(|| format!("Stage 1 failed for {}", pf_str))?;
                    Ok(())
                })();

                if let Err(e) = result {
                    eprintln!(
                        "[build_corpus_index] Stage 1 failed for {}: {}",
                        pf.display(),
                        e
                    );
                }
            });
    });

    if verbose {
        eprintln!("[build_corpus_index]     Stage 1 complete.");
    }

    // Stage 2: combine shards into a single index file.
    if verbose {
        eprintln!(
            "[build_corpus_index] Stage 2: Combining into single index file {}",
            index_file.display()
        );
    }

    let conn = Connection::open_in_memory().context("open DuckDB for stage 2")?;
    conn.execute_batch("SET preserve_insertion_order = false;")
        .ok();
    if let Some(m) = ml {
        conn.execute_batch(&format!("SET memory_limit = '{}'", m))
            .ok();
    }

    // Use the glob pattern to read all shard files.
    let shard_glob = format!("{}/*.parquet", dq(&temp_dir_path));
    let copy_sql = format!(
        "COPY (SELECT * FROM read_parquet('{}')) TO '{}' \
         (FORMAT PARQUET, COMPRESSION SNAPPY)",
        shard_glob,
        dq(&index_file)
    );
    conn.execute_batch(&copy_sql)
        .context("Stage 2 combine failed")?;

    std::fs::remove_dir_all(&temp_dir_path).ok();

    if verbose {
        let file_size = std::fs::metadata(&index_file).map(|m| m.len()).unwrap_or(0);
        eprintln!(
            "[build_corpus_index] Done! Index size: {:.2} GB",
            file_size as f64 / 1_073_741_824.0
        );
        eprintln!(
            "[build_corpus_index] Total time: {:.2} minutes",
            total_start.elapsed().as_secs_f64() / 60.0
        );
    }

    Ok(dq(&index_file))
}

// ── lookup_by_id ─────────────────────────────────────────────────────────────

/// Look up records by OpenAlex ID using a pre-built index.
///
/// Reads the index, filters to the requested IDs, and extracts matching rows
/// from the Parquet corpus files into `output` (which must not already exist).
///
/// Returns the number of records written to `output`.
pub fn lookup_by_id(
    index_file: &str,
    ids: &[String],
    output: &str,
    workers: usize,
    verbose: bool,
) -> Result<u64> {
    if !Path::new(index_file).exists() {
        anyhow::bail!("Index file not found: {}", index_file);
    }

    let output_path = Path::new(output);
    if output_path.exists() {
        anyhow::bail!("Output directory already exists: {}", output);
    }
    std::fs::create_dir_all(output_path).context("create output dir")?;

    let snapshot_path = Path::new(index_file)
        .parent()
        .ok_or_else(|| anyhow::anyhow!("index_file has no parent"))?;

    // Normalise IDs to long form.
    let normalised: Vec<String> = ids
        .iter()
        .map(|id| {
            if id.starts_with("https://openalex.org/") {
                id.clone()
            } else {
                format!("https://openalex.org/{}", id)
            }
        })
        .collect();

    if verbose {
        eprintln!("[lookup_by_id] Looking up {} ID(s)...", normalised.len());
    }

    // Query the index.  For large ID sets we write to a temp table to avoid
    // hitting SQL string-length limits.
    let conn = Connection::open_in_memory().context("open DuckDB for index query")?;

    // Build the VALUES list for the IN clause.  DuckDB handles thousands of
    // values; for millions a temp table would be better but is not needed for
    // typical use cases.
    let ids_sql: Vec<String> = normalised
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "\\'")))
        .collect();
    let ids_list = ids_sql.join(", ");

    let query = format!(
        "SELECT id, parquet_file, file_row_number \
         FROM read_parquet('{}') \
         WHERE id IN ({})",
        index_file.replace('\'', "\\'"),
        ids_list
    );

    let mut stmt = conn.prepare(&query).context("prepare index query")?;
    let mut rows = stmt.query([]).context("query index")?;

    let mut file_map: HashMap<String, Vec<i64>> = HashMap::new();
    let mut match_count = 0u64;

    while let Some(row) = rows.next()? {
        let _id: String = row.get(0)?;
        let parquet_file: String = row.get(1)?;
        let file_row_number: i64 = row.get(2)?;
        file_map
            .entry(parquet_file)
            .or_default()
            .push(file_row_number);
        match_count += 1;
    }
    drop(stmt);
    drop(conn);

    if match_count == 0 {
        eprintln!("[lookup_by_id] No matching records found in index.");
        return Ok(0);
    }

    if verbose {
        eprintln!(
            "[lookup_by_id] Found {} matching records across {} file(s).",
            match_count,
            file_map.len()
        );
    }

    // Resolve relative paths → absolute paths under snapshot_path.
    let entries: Vec<(String, Vec<i64>)> = file_map
        .into_iter()
        .map(|(rel_path, row_numbers)| {
            let full_path = snapshot_path.join(&rel_path);
            (dq(&full_path), row_numbers)
        })
        .collect();

    // Group entries into batches so each output parquet file holds ~10,000 rows.
    // This prevents the 1-file-per-source-partition explosion that occurs with
    // date-partitioned corpora (e.g. updated_date=X/part_0000.parquet each
    // containing only ~130 matching rows).
    const TARGET_ROWS_PER_OUTPUT: usize = 10_000;
    let mut batches: Vec<Vec<(String, Vec<i64>)>> = Vec::new();
    let mut current_batch: Vec<(String, Vec<i64>)> = Vec::new();
    let mut current_count: usize = 0;
    for (pq_file, row_numbers) in entries {
        let n = row_numbers.len();
        if current_count + n > TARGET_ROWS_PER_OUTPUT && !current_batch.is_empty() {
            batches.push(std::mem::take(&mut current_batch));
            current_count = 0;
        }
        current_count += n;
        current_batch.push((pq_file, row_numbers));
    }
    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    let output_str = dq(output_path);

    let written = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let written_clone = Arc::clone(&written);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()
        .context("build rayon pool")?;

    pool.install(|| {
        batches.par_iter().enumerate().for_each(|(idx, batch)| {
            let out_file = format!("{}/part_{:05}.parquet", output_str, idx);

            // Build a UNION ALL across all source files in this batch.
            let selects: Vec<String> = batch
                .iter()
                .map(|(pq_file, row_numbers)| {
                    let row_filter = row_numbers
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "SELECT * FROM read_parquet('{}', file_row_number = true) \
                         WHERE file_row_number IN ({})",
                        pq_file, row_filter
                    )
                })
                .collect();

            let batch_rows: u64 = batch.iter().map(|(_, r)| r.len() as u64).sum();
            let copy_sql = format!(
                "COPY ({}) TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                selects.join(" UNION ALL BY NAME "),
                out_file
            );

            let result = (|| -> Result<()> {
                let conn = Connection::open_in_memory().context("open DuckDB")?;
                conn.execute_batch(&copy_sql)
                    .with_context(|| format!("COPY failed for batch {}", idx))?;
                Ok(())
            })();

            match result {
                Ok(()) => {
                    written_clone.fetch_add(batch_rows, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("[lookup_by_id] Failed to write batch {}: {}", idx, e);
                }
            }
        });
    });

    let total = written.load(std::sync::atomic::Ordering::Relaxed);
    if verbose {
        eprintln!("[lookup_by_id] Written {} records to {}", total, output);
    }

    Ok(total)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widen_identical() {
        assert_eq!(
            widen_types(&["VARCHAR".to_string(), "VARCHAR".to_string()]),
            "VARCHAR"
        );
    }

    #[test]
    fn widen_complex_wins() {
        assert_eq!(
            widen_types(&["VARCHAR".to_string(), "STRUCT(id VARCHAR)".to_string()]),
            "STRUCT(id VARCHAR)"
        );
    }

    #[test]
    fn widen_numeric_widest() {
        assert_eq!(
            widen_types(&["INTEGER".to_string(), "BIGINT".to_string()]),
            "BIGINT"
        );
    }

    #[test]
    fn widen_fallback_varchar() {
        assert_eq!(
            widen_types(&["BOOLEAN".to_string(), "VARCHAR".to_string()]),
            "VARCHAR"
        );
    }

    #[test]
    fn count_struct_fields_simple() {
        assert_eq!(count_struct_fields("STRUCT(id VARCHAR, name VARCHAR)"), 2);
    }

    #[test]
    fn merge_schemas_basic() {
        let s1 = vec![
            ColumnDef {
                name: "id".into(),
                col_type: "VARCHAR".into(),
            },
            ColumnDef {
                name: "year".into(),
                col_type: "INTEGER".into(),
            },
        ];
        let s2 = vec![
            ColumnDef {
                name: "id".into(),
                col_type: "VARCHAR".into(),
            },
            ColumnDef {
                name: "year".into(),
                col_type: "BIGINT".into(),
            },
            ColumnDef {
                name: "title".into(),
                col_type: "VARCHAR".into(),
            },
        ];
        let merged = merge_schemas(vec![s1, s2]);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].name, "id");
        assert_eq!(merged[1].col_type, "BIGINT"); // widened
        assert_eq!(merged[2].name, "title");
    }
}
