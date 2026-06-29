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

/// Write a small parquet corpus file via the duckdb CLI (callers gate on has_duckdb()).
/// `select_body` is a SELECT producing the rows, e.g.
/// `SELECT * FROM (VALUES ('https://openalex.org/W1','T1')) AS t(id, title)`.
#[allow(dead_code)]
fn make_parquet(path: &std::path::Path, select_body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let sql = format!(
        "COPY ({select_body}) TO '{}' (FORMAT PARQUET);",
        path.to_string_lossy().replace('\'', "''")
    );
    let st = Command::new("duckdb").args(["-c", &sql]).status().unwrap();
    assert!(
        st.success(),
        "duckdb make_parquet failed for {}",
        path.display()
    );
}

#[allow(dead_code)]
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
    // Mock aws for the parquet-native flow: supports `s3 cp <uri> -` (cat a file
    // from MOCK_S3_ROOT to stdout, used for manifest.json) and `s3 sync` (cp -R).
    let script = r#"#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  echo "aws-cli/2.0.0"
  exit 0
fi
if [ "${1:-}" = "s3" ] && [ "${2:-}" = "cp" ]; then
  src="${3:-}"
  root="${MOCK_S3_ROOT:-}"
  key="${src#s3://mock-openalex/}"
  cat "$root/$key"
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
fn index_builds_expected_columns() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    let parquet = root.join("parquet");
    make_parquet(
        &parquet.join("works/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/W1000000001','T1'), ('https://openalex.org/domains/2','T2')) AS t(id, title)",
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
    let status = Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works",
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

    let parquet = root.join("parquet");
    make_parquet(
        &parquet.join("authors/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/A123456789','A')) AS t(id, display_name)",
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));
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
    let parquet = root.join("parquet");
    make_parquet(
        &parquet.join("works/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/W1000000001','T1')) AS t(id, title)",
    );
    make_parquet(
        &parquet.join("authors/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/A1000000001','A')) AS t(id, display_name)",
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

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
    let parquet = root.join("parquet");
    make_parquet(
        &parquet.join("works/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/W1000000001','T1')) AS t(id, title)",
    );
    make_parquet(
        &parquet.join("authors/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/A1000000001','A')) AS t(id, display_name)",
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

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
#[cfg(unix)]
fn download_and_validate_download_parquet_with_mock_aws() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let s3 = root.join("mock-s3");
    // Remote parquet layout: data/parquet/authors/updated_date=.../part_0000.parquet
    let remote_ds = s3.join("data/parquet/authors/updated_date=2020-01-01");
    fs::create_dir_all(&remote_ds).unwrap();
    let remote_file = remote_ds.join("part_0000.parquet");
    let status = Command::new("duckdb")
        .args([
            "-c",
            &format!(
                "COPY (SELECT * FROM (VALUES ('https://openalex.org/A1','Alice'), ('https://openalex.org/A2','Bob')) AS t(id, display_name)) TO '{}' (FORMAT PARQUET);",
                remote_file.to_string_lossy().replace('\'', "''")
            ),
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let size = fs::metadata(&remote_file).unwrap().len();

    // Top-level manifest.json describing that one file.
    let manifest = json!({
        "date": "2020-01-01",
        "format": "parquet",
        "meta": { "record_count": 2, "content_length": size },
        "entities": [ {
            "entity": "authors",
            "content_length": size,
            "files": [ {
                "url": "s3://mock-openalex/data/parquet/authors/updated_date=2020-01-01/part_0000.parquet",
                "meta": { "content_length": size, "record_count": 2 }
            } ]
        } ]
    });
    fs::write(
        s3.join("data/parquet/manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let aws_bin = root.join("aws-mock.sh");
    make_mock_aws(&aws_bin);
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    let local_file = root.join("parquet/authors/updated_date=2020-01-01/part_0000.parquet");

    // download
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
    assert!(local_file.exists(), "downloaded parquet should exist");

    // verify_download passes (presence + size + footer rowcount).
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
    assert!(
        status.success(),
        "verify_download should pass on a clean copy"
    );
    // A successful command writes no report file (slim logging).
    let reports = root.join("openalex-snapshot_metadata/reports");
    let has_vd_report = reports.exists()
        && fs::read_dir(&reports)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("verify_download-")
            });
    assert!(
        !has_vd_report,
        "successful verify_download should not write a report"
    );

    // Corrupt local file size -> verify must fail and a report IS written.
    let mut bytes = fs::read(&local_file).unwrap();
    bytes.extend_from_slice(b"junk");
    fs::write(&local_file, &bytes).unwrap();
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
    assert!(
        !status.success(),
        "verify_download should fail on size mismatch"
    );
    let has_vd_report = fs::read_dir(&reports)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("verify_download-")
        });
    assert!(
        has_vd_report,
        "failing verify_download should write a report"
    );
}

#[test]
fn enrich_works_and_index_skips_works_aws() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let raw_dir = root.join("parquet/works_aws/updated_date=2020-01-01");
    fs::create_dir_all(&raw_dir).unwrap();
    let raw_file = raw_dir.join("part_0000.parquet");
    // Raw works with a JSON-string abstract_inverted_index (as in the official release).
    let status = Command::new("duckdb")
        .args([
            "-c",
            &format!(
                "COPY (SELECT * FROM (VALUES \
                   ('https://openalex.org/W1', '{{\"Hello\":[0],\"world\":[1]}}'), \
                   ('https://openalex.org/W2', NULL) \
                 ) AS t(id, abstract_inverted_index)) TO '{}' (FORMAT PARQUET);",
                raw_file.to_string_lossy().replace('\'', "''")
            ),
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    // enrich works_aws -> works
    let status = Command::new(&exe)
        .args(["enrich", "--root-dir", root.to_str().unwrap()])
        .status()
        .unwrap();
    assert!(status.success());
    let enriched = root.join("parquet/works/updated_date=2020-01-01/part_0000.parquet");
    assert!(enriched.exists(), "enriched works file should exist");

    // abstract reconstructed; row parity preserved.
    let out = Command::new("duckdb")
        .args([
            "-csv",
            "-c",
            &format!(
                "SELECT (SELECT abstract FROM read_parquet('{f}') WHERE id='https://openalex.org/W1') AS a, (SELECT COUNT(*) FROM read_parquet('{f}')) AS n;",
                f = enriched.to_string_lossy().replace('\'', "''")
            ),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("Hello world"),
        "abstract should be reconstructed: {s}"
    );
    assert!(
        s.contains(",2") || s.trim_end().ends_with("2"),
        "row parity (2 rows): {s}"
    );

    // index --dataset all must index works but skip works_aws.
    let status = Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "all",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        root.join("parquet/works_id_idx.parquet").exists(),
        "works index should be built"
    );
    assert!(
        !root.join("parquet/works_aws_id_idx.parquet").exists(),
        "works_aws must be skipped by index --dataset all"
    );
}

#[test]
fn report_lists_latest_and_prune_keeps_latest() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    let global_reports = root.join("openalex-snapshot_metadata/reports");
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
    // repair_convert was removed in favour of auto-repair inside `convert`
    assert!(!c.contains("repair_convert:"));
    assert!(c.contains("auto_repair"));
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

    // `fast` template was removed in v0.5.0 — the old enum it referenced
    // (`Profile::Fast`) no longer exists.  Asserting clap rejects it now.
    let fast = Command::new(&exe)
        .args(["config", "--create", "fast", "--stdout"])
        .output()
        .unwrap();
    assert!(!fast.status.success());
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
    let reports = root.join("openalex-snapshot_metadata/reports");
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
    assert!(txt.contains("download=false verify_download=false index=true verify_index=true"));
    assert!(!txt.contains("convert"));
}

#[test]
fn all_pipeline_runs_without_download_when_disabled() {
    if !has_duckdb() {
        return;
    }
    let td = tempfile::tempdir().unwrap();
    let root = td.path();
    make_parquet(
        &root.join("parquet/authors/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/A1','A')) AS t(id, display_name)",
    );
    let cfg = root.join("openalex-snapshot.yaml");
    fs::write(
        &cfg,
        r#"
all:
  enable_download: false
  enable_verify_download: false
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
    let reports = root.join("openalex-snapshot_metadata/reports");
    let has_all = fs::read_dir(&reports)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .any(|n| n.starts_with("all-") && n.ends_with(".json"));
    assert!(has_all);
}

#[test]
fn extract_returns_matching_records() {
    if !has_duckdb() {
        return;
    }

    let td = tempfile::tempdir().unwrap();
    let root = td.path();

    // --- build a minimal works parquet corpus with two works ---
    make_parquet(
        &root.join("parquet/works/part_000/part1.parquet"),
        "SELECT * FROM (VALUES ('https://openalex.org/W1000000001','Alpha'), ('https://openalex.org/W1000000002','Beta')) AS t(id, title)",
    );

    let exe = PathBuf::from(env!("CARGO_BIN_EXE_openalex-snapshot"));

    // --- index ---
    assert!(Command::new(&exe)
        .args([
            "index",
            "--root-dir",
            root.to_str().unwrap(),
            "--dataset",
            "works"
        ])
        .status()
        .unwrap()
        .success());

    // --- write IDs CSV using short form (no URL prefix) to exercise canonical expansion ---
    let ids_csv = root.join("extract_ids.csv");
    fs::write(&ids_csv, "id\nW1000000001\n").unwrap();

    // --- extract ---
    let output_base = root.join("extracted");
    let status = Command::new(&exe)
        .args([
            "extract",
            "--root-dir",
            root.to_str().unwrap(),
            "--ids",
            ids_csv.to_str().unwrap(),
            "--output",
            output_base.to_str().unwrap(),
        ])
        .status()
        .unwrap();
    assert!(status.success(), "extract command failed");

    // --- output parquet should exist ---
    let out_parquet = root.join("extracted_works.parquet");
    assert!(out_parquet.exists(), "extracted_works.parquet not created");

    // --- verify content via duckdb CLI: exactly 1 row, correct id ---
    let sql = format!(
        "SELECT COUNT(*) AS n FROM read_parquet('{}');",
        out_parquet.to_string_lossy().replace('\'', "''")
    );
    let out = Command::new("duckdb")
        .args(["-csv", "-c", &sql])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    // header + one data row
    assert!(
        s.contains('1'),
        "expected 1 row in extracted parquet, got: {s}"
    );

    // --- report written to metadata/reports/ ---
    let reports = root.join("openalex-snapshot_metadata/reports");
    let has_extract_report = fs::read_dir(&reports)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("extract-"));
    assert!(has_extract_report, "no extract report written");
}
