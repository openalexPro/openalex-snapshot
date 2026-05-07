use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

fn has_duckdb() -> bool {
    Command::new("duckdb")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn write_gz_ndjson(path: &std::path::Path, lines: &[&str]) {
    let mut enc = flate2::write::GzEncoder::new(
        fs::File::create(path).unwrap(),
        flate2::Compression::default(),
    );
    for l in lines {
        writeln!(enc, "{}", l).unwrap();
    }
    enc.finish().unwrap();
}

#[cfg(unix)]
fn make_mock_aws(path: &std::path::Path) {
    let script = r#"#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  echo "aws-cli/2.0.0"
  exit 0
fi
if [ "${1:-}" = "s3" ] && [ "${2:-}" = "sync" ]; then
  src=""
  dst=""
  for a in "$@"; do
    case "$a" in
      s3|sync|--delete|--no-sign-request|--endpoint-url|--region|--profile) ;;
      s3://*) src="$a" ;;
      *) if [ -z "$dst" ] && [ "$a" != "s3" ] && [ "$a" != "sync" ]; then dst="$a"; fi ;;
    esac
  done
  root="${MOCK_S3_ROOT:-}"
  key="${src#s3://mock-openalex/}"
  if [ "$src" = "s3://mock-openalex" ]; then key=""; fi
  from="$root/$key"
  mkdir -p "$dst"
  if [ -d "$from" ]; then
    cp -R "$from"/. "$dst"/
  fi
  exit 0
fi
if [ "${1:-}" = "s3api" ] && [ "${2:-}" = "list-objects-v2" ]; then
  root="${MOCK_S3_ROOT:-}"
  prefix=""
  next=""
  prev=""
  for a in "$@"; do
    if [ "$prev" = "--prefix" ]; then prefix="$a"; fi
    if [ "$prev" = "--continuation-token" ]; then next="$a"; fi
    prev="$a"
  done
  if [ -n "$next" ]; then
    echo '{"IsTruncated":false,"Contents":[]}'
    exit 0
  fi
  p="$root/$prefix"
  if [ ! -d "$p" ]; then
    echo '{"IsTruncated":false,"Contents":[]}'
    exit 0
  fi
  tmp="$(mktemp)"
  (cd "$root" && find "$prefix" -type f | sort) > "$tmp"
  printf '{"IsTruncated":false,"Contents":['
  first=1
  while IFS= read -r f; do
    sz=$(wc -c < "$root/$f" | tr -d ' ')
    [ $first -eq 0 ] && printf ','
    first=0
    printf '{"Key":"%s","Size":%s,"ETag":"\\"mock\\"","LastModified":"1970-01-01T00:00:00Z"}' "$f" "$sz"
  done < "$tmp"
  printf ']}'
  rm -f "$tmp"
  exit 0
fi
echo "unsupported aws mock call" >&2
exit 2
"#;
    fs::write(path, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).unwrap();
}

fn write_report_json(path: &std::path::Path, command: &str, ts: i64, failed: u64) {
    let doc = json!({
        "command": command,
        "started_at_unix": ts,
        "finished_at_unix": ts + 1,
        "duration_seconds": 1.0,
        "args": {},
        "totals_items_scanned": 10,
        "totals_succeeded": 10 - failed,
        "totals_failed": failed,
        "totals_skipped": 0,
        "datasets": [],
        "failures": [],
    });
    fs::write(path, serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
}

#[test]
fn convert_and_verify_structure() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");

    let ds = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[
            r#"{"id":"https://openalex.org/A1","display_name":"A"}"#,
            r#"{"id":"https://openalex.org/A2","display_name":"B"}"#,
        ],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let status = Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    assert!(parquet.join("authors/part_000/part1.parquet").exists());

    let status = Command::new(&exe)
        .args([
            "verify_convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--scope",
            "dataset",
        ])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn schema_arrow_r_output() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let snapshot = root.join("snapshot");
    let _parquet = root.join("parquet");

    let ds = snapshot.join("data/works/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[
            r#"{"id":"https://openalex.org/W1","title":"T1"}"#,
            r#"{"id":"https://openalex.org/W2","title":"T2"}"#,
        ],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let status = Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let out = Command::new(&exe)
        .args([
            "schema",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
            "--from",
            "auto",
            "--format",
            "arrow-r",
        ])
        .output()
        .unwrap();

    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("\"fields\""));
    assert!(text.contains("\"dataset\": \"works\""));
}

#[test]
fn index_builds_expected_columns() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");
    let ds = snapshot.join("data/works/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[
            r#"{"id":"https://openalex.org/W1000000001","title":"T1"}"#,
            r#"{"id":"https://openalex.org/domains/2","title":"T2"}"#,
        ],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let status = Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let status = Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
            "--profile",
            "balanced",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let idx = parquet.join("works_id_idx.parquet");
    assert!(idx.exists());

    let out = Command::new("duckdb")
        .args([
            "-csv",
            "-c",
            &format!(
                "SELECT COUNT(*) AS n, MIN(id_block) AS min_b, MAX(id_block) AS max_b FROM read_parquet('{}');",
                idx.to_string_lossy().replace('\'', "''")
            ),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("n"));
}

#[test]
fn index_skip_and_overwrite() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");
    let ds = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/A123456789","display_name":"A"}"#],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    assert!(Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
        ])
        .status()
        .unwrap()
        .success());

    assert!(Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
        ])
        .status()
        .unwrap()
        .success());

    let idx = parquet.join("authors_id_idx.parquet");
    let t1 = fs::metadata(&idx).unwrap().modified().unwrap();
    let s2 = Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
        ])
        .status()
        .unwrap();
    assert!(s2.success());
    let t2 = fs::metadata(&idx).unwrap().modified().unwrap();
    assert_eq!(t1, t2);

    let s3 = Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--overwrite",
        ])
        .status()
        .unwrap();
    assert!(s3.success());
    let t3 = fs::metadata(&idx).unwrap().modified().unwrap();
    assert!(t3 >= t2);
}

#[test]
fn index_all_builds_multiple_datasets() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");

    let works = snapshot.join("data/works/part_000");
    let authors = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&works).unwrap();
    fs::create_dir_all(&authors).unwrap();
    write_gz_ndjson(
        &works.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/W1000000001","title":"T1"}"#],
    );
    write_gz_ndjson(
        &authors.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/A1000000001","display_name":"A"}"#],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    assert!(Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "all",
        ])
        .status()
        .unwrap()
        .success());

    assert!(Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "all"
        ])
        .status()
        .unwrap()
        .success());

    assert!(parquet.join("works_id_idx.parquet").exists());
    assert!(parquet.join("authors_id_idx.parquet").exists());
}

#[test]
fn index_cli_dataset_not_overridden_by_config() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");

    let works = snapshot.join("data/works/part_000");
    let authors = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&works).unwrap();
    fs::create_dir_all(&authors).unwrap();
    write_gz_ndjson(
        &works.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/W1000000001","title":"T1"}"#],
    );
    write_gz_ndjson(
        &authors.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/A1000000001","display_name":"A"}"#],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    assert!(Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "all",
        ])
        .status()
        .unwrap()
        .success());

    let cfg = root.join("openalex-snapshot.yaml");
    fs::write(
        &cfg,
        format!(
            "defaults:\n  root_dir: {}\n  dataset: all\nindex:\n  dataset: all\n",
            root.to_string_lossy()
        ),
    )
    .unwrap();

    assert!(Command::new(&exe)
        .args([
            "--config",
            cfg.to_str().unwrap(),
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
        ])
        .status()
        .unwrap()
        .success());

    assert!(parquet.join("works_id_idx.parquet").exists());
    assert!(!parquet.join("authors_id_idx.parquet").exists());
}

#[test]
fn precedence_config_over_default_when_cli_unset() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let cfg = root.join("openalex-snapshot.yaml");
    fs::write(&cfg, "index:\n  dataset: authors\n").unwrap();

    let out = Command::new(&exe)
        .args([
            "--config",
            cfg.to_str().unwrap(),
            "--print-effective-config",
            "index",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("corpus_dir: ./parquet/authors")
            || s.contains("corpus_dir: .\\parquet\\authors"),
        "{}",
        s
    );
}

#[test]
fn precedence_default_when_cli_and_config_unset() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let out = Command::new(&exe)
        .current_dir(root)
        .args(["--print-effective-config", "index"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("corpus_dir: ./parquet/all") || s.contains("corpus_dir: .\\parquet\\all"),
        "{}",
        s
    );
}

#[test]
fn canonical_unified_schema_csv_written() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let _parquet = root.join("parquet");
    let ds = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/A1","display_name":"A"}"#],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    assert!(Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
        ])
        .status()
        .unwrap()
        .success());

    let csv = root.join(".openalex-snapshot_metadata/datasets/authors/schemata/unified_schema.csv");
    assert!(csv.exists());
    let txt = fs::read_to_string(csv).unwrap();
    let first = txt.lines().next().unwrap_or("");
    assert_eq!(first, "col_name,col_type");
}

#[test]
fn csv_cache_precedence_over_json_cache() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let _parquet = root.join("parquet");
    let ds = snapshot.join("data/works/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/W1","title":"T1"}"#],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    assert!(Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
        ])
        .status()
        .unwrap()
        .success());

    // Corrupt JSON cache on purpose; canonical CSV must still drive schema loading.
    let json_cache =
        root.join(".openalex-snapshot_metadata/datasets/works/schemata/source_schema.json");
    fs::write(&json_cache, "{ not valid json").unwrap();

    let out = Command::new(&exe)
        .args([
            "schema",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
            "--from",
            "source",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
}

#[test]
fn convert_only_selected_input_file() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");
    let ds = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&ds).unwrap();
    let f1 = ds.join("part1.gz");
    let f2 = ds.join("part2.gz");
    write_gz_ndjson(
        &f1,
        &[r#"{"id":"https://openalex.org/A1","display_name":"A"}"#],
    );
    write_gz_ndjson(
        &f2,
        &[r#"{"id":"https://openalex.org/A2","display_name":"B"}"#],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let status = Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--input-file",
            "part_000/part2.gz",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    assert!(!parquet.join("authors/part_000/part1.parquet").exists());
    assert!(parquet.join("authors/part_000/part2.parquet").exists());
}

#[test]
fn legacy_schema_cache_is_auto_migrated() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");
    fs::create_dir_all(snapshot.join("data/authors/part_000")).unwrap();

    let legacy = parquet.join("authors/.schema_cache");
    fs::create_dir_all(&legacy).unwrap();
    fs::write(
        legacy.join("unified_schema.csv"),
        "col_name,col_type\nid,VARCHAR\ndisplay_name,VARCHAR\n",
    )
    .unwrap();

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let out = Command::new(&exe)
        .args([
            "schema",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--from",
            "cache",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());

    let new_csv =
        root.join(".openalex-snapshot_metadata/datasets/authors/schemata/unified_schema.csv");
    assert!(new_csv.exists());
    assert!(!legacy.exists());
}

#[test]
fn repair_reconverts_failed_verify_files() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let parquet = root.join("parquet");
    let ds = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[
            r#"{"id":"https://openalex.org/A1","display_name":"A"}"#,
            r#"{"id":"https://openalex.org/A2","display_name":"B"}"#,
        ],
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    assert!(Command::new(&exe)
        .args([
            "convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
        ])
        .status()
        .unwrap()
        .success());

    let out_file = parquet.join("authors/part_000/part1.parquet");
    fs::write(&out_file, b"bad").unwrap();

    let failed_verify = Command::new(&exe)
        .args([
            "verify_convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--scope",
            "dataset",
            "--metadata-level",
            "both",
        ])
        .status()
        .unwrap();
    assert!(!failed_verify.success());

    let reports_dir = root.join(".openalex-snapshot_metadata/datasets/authors/reports");
    let mut verify_reports: Vec<PathBuf> = fs::read_dir(&reports_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("verify_convert-") && n.ends_with(".json"))
                .unwrap_or(false)
        })
        .collect();
    verify_reports.sort();
    let verify_report = verify_reports.last().unwrap().clone();

    let repair_status = Command::new(&exe)
        .args([
            "repair_convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--from-verify-report",
            verify_report.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(repair_status.success());

    let ok_verify = Command::new(&exe)
        .args([
            "verify_convert",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "authors",
            "--scope",
            "dataset",
            "--metadata-level",
            "both",
        ])
        .status()
        .unwrap();
    assert!(ok_verify.success());
}

#[test]
#[cfg(unix)]
fn download_and_validate_download_with_mock_aws() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let s3 = root.join("mock-s3");
    let snapshot = root.join("snapshot");
    fs::create_dir_all(s3.join("data/authors/part_000")).unwrap();
    write_gz_ndjson(
        &s3.join("data/authors/part_000/part1.gz"),
        &[r#"{"id":"https://openalex.org/A1","display_name":"A"}"#],
    );

    let aws_bin = root.join("aws-mock.sh");
    make_mock_aws(&aws_bin);
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let status = Command::new(&exe)
        .args([
            "download",
            "--root-dir",
            root.to_str().unwrap(),
            "--s3-uri",
            "s3://mock-openalex",
            "--dataset",
            "authors",
            "--aws-bin",
            aws_bin.to_str().unwrap(),
        ])
        .env("MOCK_S3_ROOT", s3.to_str().unwrap())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(snapshot.join("data/authors/part_000/part1.gz").exists());
    assert!(root
        .join(".openalex-snapshot_metadata/download/reports")
        .exists());

    // Corrupt local gzip and verify strict validation fails.
    fs::write(snapshot.join("data/authors/part_000/part1.gz"), b"bad").unwrap();
    let status = Command::new(&exe)
        .args([
            "verify_download",
            "--root-dir",
            root.to_str().unwrap(),
            "--s3-uri",
            "s3://mock-openalex",
            "--dataset",
            "authors",
            "--aws-bin",
            aws_bin.to_str().unwrap(),
        ])
        .env("MOCK_S3_ROOT", s3.to_str().unwrap())
        .status()
        .unwrap();
    assert!(!status.success());
}

#[test]
fn report_lists_latest_and_prune_keeps_latest() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let global_reports = root.join(".openalex-snapshot_metadata/reports");
    fs::create_dir_all(&global_reports).unwrap();
    write_report_json(&global_reports.join("verify-100.json"), "verify", 100, 1);
    write_report_json(&global_reports.join("verify-200.json"), "verify", 200, 0);
    write_report_json(&global_reports.join("convert-150.json"), "convert", 150, 0);

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let out = Command::new(&exe)
        .args([
            "report",
            "--root-dir",
            root.to_str().unwrap(),
            "--source",
            "parquet",
            "--latest",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("verify"));
    assert!(txt.contains("convert"));
    assert!(!txt.contains("started_at=100"));
    assert!(txt.contains("200"));

    let status = Command::new(&exe)
        .args([
            "prune-reports",
            "--root-dir",
            root.to_str().unwrap(),
            "--source",
            "parquet",
            "--keep-per-command",
            "1",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(global_reports.join("verify-200.json").exists());
    assert!(!global_reports.join("verify-100.json").exists());
    assert!(global_reports.join("convert-150.json").exists());
}

#[test]
fn config_create_and_verify_roundtrip() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let cfg = root.join("openalex-snapshot.yaml");
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let status = Command::new(&exe)
        .args([
            "config",
            "--create",
            "complete",
            "--config",
            cfg.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(cfg.exists());

    let out = Command::new(&exe)
        .args(["config", "--verify", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("status=ok"));
}

#[test]
fn config_create_without_template_mode_defaults_to_complete() {
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let td = tempfile::tempdir().unwrap();
    let cfg = td.path().join("openalex-snapshot.yaml");
    let out = Command::new(&exe)
        .args(["config", "--create", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let txt = fs::read_to_string(cfg).unwrap();
    assert!(txt.contains("# openalex-snapshot.yaml"));
}

#[test]
fn config_create_mode_contracts() {
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let complete = Command::new(&exe)
        .args(["config", "--create", "complete", "--stdout"])
        .output()
        .unwrap();
    assert!(complete.status.success());
    let c = String::from_utf8_lossy(&complete.stdout);
    assert!(c.contains("schema:"));
    assert!(c.contains("repair_convert:"));
    assert!(c.contains("progress:"));
    assert!(!c.contains("\ncorpus_dir:"));

    let safe = Command::new(&exe)
        .args(["config", "--create", "safe", "--stdout"])
        .output()
        .unwrap();
    assert!(safe.status.success());
    let sf = String::from_utf8_lossy(&safe.stdout);
    assert!(sf.contains("# openalex-snapshot.yaml (safe)"));
    assert!(sf.contains("profile: safe"));
    assert!(sf.contains("workers: 1"));

    let fast = Command::new(&exe)
        .args(["config", "--create", "fast", "--stdout"])
        .output()
        .unwrap();
    assert!(fast.status.success());
    let f = String::from_utf8_lossy(&fast.stdout);
    assert!(f.contains("# openalex-snapshot.yaml (fast)"));
    assert!(f.contains("profile: fast"));
    assert!(f.contains("workers: 8"));
}

#[test]
fn config_verify_fails_for_unknown_key() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let cfg = root.join("bad.yaml");
    fs::write(&cfg, "defaults:\n  nope: 1\n").unwrap();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let status = Command::new(&exe)
        .args(["config", "--verify", "--config", cfg.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(!status.success());
}

#[test]
fn progress_once_reads_latest_active_report() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let reports = root.join(".openalex-snapshot_metadata/reports");
    fs::create_dir_all(&reports).unwrap();
    let active = serde_json::json!({
        "command":"convert",
        "started_at_unix": 1000,
        "finished_at_unix": serde_json::Value::Null,
        "duration_seconds": serde_json::Value::Null,
        "args": {},
        "totals_items_scanned": 10,
        "totals_succeeded": 4,
        "totals_failed": 1,
        "totals_skipped": 5,
        "datasets": [{"dataset":"works","items_scanned":10,"succeeded":4,"failed":1,"skipped":5}],
        "failures": []
    });
    fs::write(
        reports.join("convert-1000.json"),
        serde_json::to_vec_pretty(&active).unwrap(),
    )
    .unwrap();

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let out = Command::new(&exe)
        .args(["progress", "--root-dir", root.to_str().unwrap(), "--once"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("[progress] command=convert"));
}

#[test]
fn version_flag_works() {
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let out = Command::new(&exe).arg("--version").output().unwrap();
    assert!(out.status.success());
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("openalex-snapshot"));
}

#[test]
fn skills_creates_expected_files() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let status = Command::new(&exe)
        .args(["skills", "--root-dir", root.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(root.join("skills/README.md").exists());
    assert!(root.join("skills/cli-operations/SKILL.md").exists());
    assert!(root.join("skills/pipeline-runbook/SKILL.md").exists());
}

#[test]
fn check_explain_works() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let out = Command::new(&exe)
        .args([
            "check",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "all",
            "--explain",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("--explain: check"));
}

#[test]
fn all_requires_explicit_config() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let out = Command::new(&exe)
        .args(["all", "--root-dir", root.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--config"));
}

#[test]
fn all_explain_shows_resolved_plan() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let cfg = root.join("openalex-snapshot.yaml");
    fs::write(
        &cfg,
        r#"
all:
  retry: 3
  enable_download: false
  enable_verify_download: false
"#,
    )
    .unwrap();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let out = Command::new(&exe)
        .args([
            "all",
            "--config",
            cfg.to_str().unwrap(),
            "--root-dir",
            root.to_str().unwrap(),
            "--explain",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let txt = String::from_utf8_lossy(&out.stdout);
    assert!(txt.contains("--explain: all"));
    assert!(txt.contains("retry: 3"));
}

#[test]
fn all_pipeline_runs_without_download_when_disabled() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let snapshot = root.join("snapshot");
    let ds = snapshot.join("data/authors/part_000");
    fs::create_dir_all(&ds).unwrap();
    write_gz_ndjson(
        &ds.join("part1.gz"),
        &[r#"{"id":"https://openalex.org/A1","display_name":"A"}"#],
    );
    let cfg = root.join("openalex-snapshot.yaml");
    fs::write(
        &cfg,
        r#"
all:
  retry: 1
  enable_download: false
  enable_verify_download: false
  enable_convert: true
  enable_verify_convert: true
  enable_repair_convert: true
  enable_index: true
  enable_verify_index: true
index:
  dataset: authors
verify_index:
  dataset: authors
"#,
    )
    .unwrap();
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let status = Command::new(&exe)
        .args([
            "all",
            "--config",
            cfg.to_str().unwrap(),
            "--root-dir",
            root.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let reports = root.join(".openalex-snapshot_metadata/reports");
    let has_all = fs::read_dir(&reports)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .any(|n| n.starts_with("all-") && n.ends_with(".json"));
    assert!(has_all);
}
