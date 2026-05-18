use anyhow::{anyhow, bail, Context, Result};
use chrono::{Local, TimeZone};
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::stdout;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use walkdir::WalkDir;

const CLI_LONG_ABOUT: &str = "\
Standalone OpenAlex snapshot tooling.

Argument precedence (highest wins):
  1) explicit CLI arguments
  2) config subcommand section values
  3) config defaults section values
  4) built-in defaults

This binary provides:
- config: create or verify YAML configuration
- all: run full config-driven pipeline
- download: sync OpenAlex snapshot from S3-compatible source (AWS CLI wrapper)
- verify_download: strict integrity validation for downloaded snapshot
- convert: snapshot .json.gz -> parquet
- verify_convert: structural and file-level data checks between snapshot and parquet
- schema: inspect schema from source/cache/parquet, including arrow-r JSON
- verify_schema: assert schema parity across schema sources
- index: build *_id_idx.parquet lookup index (R build_corpus_index equivalent)
- extract: extract rows by OpenAlex IDs using per-dataset indexes
- verify_index: validate index integrity and coverage
- repair_convert: re-convert files that failed prior verify runs
- report: view stored reports
- prune-reports: remove old report files
- progress: monitor live status from reports/logs
- skills: create AI skills starter pack under root_dir/skills
- check: run dependency/path/disk/memory preflight checks

convert (detailed):
  - reads <snapshot_dir>/data/<dataset>/**/*.gz
  - writes <parquet_dir>/<dataset>/... with preserved relative structure
  - infers unified dataset schema and uses cache
  - verification is handled separately by verify_convert

verify_convert (detailed):
  - checks .gz -> .parquet mapping and folder structure parity
  - checks per-file row count parity

schema (detailed):
  - supports sources: auto|source|cache|parquet
  - supports formats: table|json|yaml|arrow-r
  - supports source-vs-source diff via --diff-with

verify_schema (detailed):
  - compares schema sources and exits non-zero on differences
  - default comparison: source vs parquet

index (detailed):
  - stage 1: per-file shard index build (resumable)
  - stage 2: shard combine into *_id_idx.parquet
  - outputs columns: id, id_block, parquet_file, file_row_number

extract (detailed):
  - reads IDs from CSV
  - routes IDs by entity prefix / taxonomy namespace
  - resolves files via *_id_idx.parquet
  - writes one parquet output per dataset

repair_convert (detailed):
  - reads a verify report JSON
  - selects file-level verify failures (phase=verify_metrics)
  - deletes failed parquet files and re-converts only those files
  - runs targeted re-verify for repaired files

download/verify_download (detailed):
  - default sync command:
    aws s3 sync --delete s3://openalex ./snapshot --no-sign-request
  - disk preflight:
    required free space = remote manifest size + 10%
  - strict validation compares remote manifest vs local files
  - validates file presence, size parity, and gzip integrity for .json.gz

Examples:
  openalex-snapshot convert --root-dir /data --dataset works
  openalex-snapshot verify_convert --root-dir /data --dataset works --scope dataset --metadata-level both
  openalex-snapshot schema --root-dir /data --dataset works --format arrow-r
  openalex-snapshot verify_schema --root-dir /data --dataset works
  openalex-snapshot index --root-dir /data --dataset works --profile balanced
  openalex-snapshot extract --root-dir /data --ids /data/ids.csv --output /data/extract.parquet
  openalex-snapshot verify_index --root-dir /data --dataset works
  openalex-snapshot repair_convert --root-dir /data --from-verify-report /data/openalex-snapshot_metadata/reports/verify_convert-123456.json
  openalex-snapshot report --root-dir /data --latest
  openalex-snapshot prune-reports --root-dir /data
  openalex-snapshot skills --root-dir /data
  openalex-snapshot check --root-dir /data --dataset all
  openalex-snapshot download --root-dir /data
  openalex-snapshot verify_download --root-dir /data
  openalex-snapshot convert --root-dir /data --dataset all
  openalex-snapshot verify_convert --root-dir /data --dataset all --scope snapshot
  openalex-snapshot progress --root-dir /data
  openalex-snapshot config --create complete
  openalex-snapshot config --create safe
  openalex-snapshot config --create fast
  openalex-snapshot all --config ./openalex-snapshot.yaml --retry 2
";

const REPAIR_LONG_ABOUT: &str = "\
Repair parquet outputs based on verify report failures.

Behavior:
1) Reads a verify report JSON from --from-verify-report
2) Selects actionable file failures with phase=verify_metrics
3) Deletes mapped parquet output files (if present)
4) Re-converts only selected source .gz files to parquet
5) Re-verifies repaired files and records outcome

Selection rules:
  - only failures with phase=verify_metrics are eligible
  - requires actionable source/output paths (or resolvable rel_path)
  - deduplicates by output parquet file path
  - optional --dataset filter limits selected repairs

Output:
  - shared run report schema written to:
    <root>/openalex-snapshot_metadata/reports/repair_convert-<timestamp>.json
  - non-zero exit if any repair/delete/re-verify failures remain
";

const DOWNLOAD_LONG_ABOUT: &str = "\
Download OpenAlex snapshot via AWS CLI sync.

Defaults (zero-config):
  aws s3 sync --delete s3://openalex ./snapshot --no-sign-request
  dataset scope: all
  disk preflight: remote manifest size + 10% free space required

Optional overrides:
  --root-dir, --s3-uri, --endpoint-url, --region, --aws-bin
  --signed/--no-sign-request
  --delete/--no-delete
  --dataset <name|all>
";

const VALIDATE_DOWNLOAD_LONG_ABOUT: &str = "\
Strictly validate downloaded snapshot integrity.

Validation checks:
  - remote manifest fetch via aws s3api list-objects-v2
  - missing local files
  - local size mismatch
  - unexpected local files under validated scope
  - gzip integrity check for every .json.gz

Defaults:
  root_dir: .
  s3_uri: s3://openalex
  dataset: all
  no-sign-request: true
";

const VERIFY_INDEX_LONG_ABOUT: &str = "\
Verify index integrity for a parquet corpus index file.

Checks:
  - index parquet exists and is readable
  - required columns exist: id, id_block, parquet_file, file_row_number
  - index row count matches total rows across corpus parquet files
  - parquet_file references in index resolve to existing files
";

const EXTRACT_LONG_ABOUT: &str = "\
Extract records by OpenAlex IDs using parquet indexes.

Behavior:
  - reads IDs from CSV
  - routes IDs by entity prefix or taxonomy namespace
  - uses <root>/parquet/<dataset>_id_idx.parquet
  - writes <output_base>_<dataset>.parquet

Entity prefixes:
  W works, A authors, S sources, I institutions, T topics,
  K keywords, P publishers, F funders, G awards, C concepts (deprecated)

Taxonomy namespaces:
  institution-types, work-types, source-types, licenses,
  countries, continents, languages, domains, fields, subfields, sdgs
";

const CONFIG_LONG_ABOUT: &str = "\
Manage openalex-snapshot YAML configuration.

Modes:
  --create <complete|safe|fast>  Generate annotated config template
  --verify  Validate an existing config file strictly

Defaults:
  config path: ./openalex-snapshot.yaml

Argument precedence (highest wins):
  1) explicit CLI arguments
  2) config subcommand section values
  3) config defaults section values
  4) built-in defaults
";

const CONVERT_MIN_FREE_BYTES: u64 = 900u64 * 1024u64 * 1024u64 * 1024u64;
static INDEX_ALL_DEPTH: AtomicUsize = AtomicUsize::new(0);
static VERIFY_INDEX_ALL_DEPTH: AtomicUsize = AtomicUsize::new(0);

const REPORT_LONG_ABOUT: &str = "\
View stored report files from parquet and/or download metadata roots.

By default this lists report summaries. Use --full to print JSON payloads.
Use --latest to show only newest report per command type.
";

const PRUNE_REPORTS_LONG_ABOUT: &str = "\
Prune old report files and keep only newest reports per command type.

Defaults:
  keep_per_command: 1
  source: all (parquet + download report roots)
";

const PROGRESS_LONG_ABOUT: &str = "\
Monitor run progress from report and log files.

Behavior:
  - selects latest active run (unfinished report) by default
  - falls back to newest run with fresh logs in the last 5 minutes
  - shows totals, per-dataset summary, and latest log lines

Defaults:
  watch: true
  interval_sec: 2
";

const SKILLS_LONG_ABOUT: &str = "\
Bootstrap AI skills scaffolding for this project.

Generated skills:
  - cli-operations/SKILL.md    command patterns and decision rules
  - pipeline-runbook/SKILL.md  end-to-end flow and orchestration
  - debug-and-recovery/SKILL.md  failure triage and OOM recovery
  - development/SKILL.md       build, test, deploy, architecture notes
  - release-and-docs/SKILL.md  release checklist and docs hygiene

Behavior:
  - creates <root_dir>/skills
  - default safe mode: create missing files only
  - use --overwrite to rewrite generated files
";

const CHECK_LONG_ABOUT: &str = "\
Run environment and capacity preflight checks.

Checks:
  - required binaries (aws for download; duckdb is bundled — no external binary needed)
  - root/snapshot/parquet/metadata path writability
  - download disk estimate from remote manifest (+10%)
  - convert disk estimate from source inventory (precise)
  - tuning/memory risk hints from profile/workers/memory

Exit behavior:
  - default: warn-only (non-zero only on hard failures)
  - --strict: non-zero on warnings and failures
";

const ALL_LONG_ABOUT: &str = "\
Run the end-to-end pipeline from config.

Behavior:
  - Requires explicit --config (no auto-discovery fallback)
  - Runs enabled stages from config in pipeline order
  - Applies bounded verify/repair loop controlled by --retry

Default stage order:
  1) download
  2) verify_download
  3) convert
  4) verify_convert
  5) repair_convert (loop action)
  6) index
  7) verify_index
";

const INDEX_LONG_ABOUT: &str = "\
Build a parquet lookup index for a parquet dataset corpus.

Behavior matches the R build_corpus_index() approach:
1) Stage 1 creates per-file shard indexes in <index_file>_tmp/
2) Stage 2 combines shards into a single <dataset>_id_idx.parquet

Index columns:
  id, id_block, parquet_file, file_row_number

Defaults:
  dataset: all
  corpus path: <root_dir>/parquet/<dataset>
  index path: <root_dir>/parquet/<dataset>_id_idx.parquet
  existing index: skip (use --overwrite to rebuild)
  --index-file is ignored with --dataset all

Profile / tuning:
  Profile controls the DuckDB memory budget per worker (80% of RAM × fraction,
  clamped to a min/max). Workers is only capped by 'safe'.

  profile    workers cap   memory fraction   memory range
  safe       max 2         15% of usable     1 – 8 GiB
  balanced   (none)        35% of usable     4 – 24 GiB
  fast       (none)        55% of usable     8 – 32 GiB

  Fallback when RAM cannot be detected: safe=2 GiB, balanced=6 GiB, fast=12 GiB.
  Set --max-memory-mb to override the profile memory calculation entirely.
";

const CONVERT_LONG_ABOUT: &str = "\
Convert OpenAlex snapshot JSON.GZ files into parquet files.

Behavior:
1) Discovers source files under <root_dir>/snapshot/data/<dataset>/**/*.gz
2) Infers a unified schema per dataset (with cache + optional refresh)
3) Converts each source file to one parquet file
4) Preserves dataset-relative folder/file structure in output

Output:
  parquet root: <root_dir>/parquet
  dataset path: <root_dir>/parquet/<dataset>/...
  mapping: every input .gz maps to exactly one output .parquet
  optional: limit conversion to selected files via --input-file

Defaults:
  profile: safe
  memory: auto-detected from system RAM unless --max-memory-mb is provided
  disk preflight: requires at least 900 GiB free at <root_dir>/parquet

Profile / tuning:
  Profile controls the global DuckDB memory budget (shared across all in-process
  worker connections, 80% of RAM × fraction, clamped to a min/max range).
  Workers is only capped by 'safe'.

  profile    workers cap   memory fraction   memory range
  auto       (none)        65% of usable     4 – 32 GiB   (alias for balanced)
  safe       max 2         15% of usable     1 –  8 GiB
  balanced   (none)        65% of usable     4 – 32 GiB
  fast       (none)        80% of usable     8 – 48 GiB

  Fallback when RAM cannot be detected: safe=2 GiB, balanced=6 GiB, fast=12 GiB.
  Set --max-memory-mb to override the profile memory calculation entirely.
";

const VERIFY_LONG_ABOUT: &str = "\
Verify snapshot/parquet consistency.

Checks include:
1) Structure parity:
   - .gz -> .parquet mapping exists for all expected files
   - relative folder structure is preserved
   - no unexpected extra parquet files for selected dataset(s)
2) Per-file data parity:
   - row count in each input .gz equals row count in mapped .parquet
Defaults:
  seed: 42

Scope:
  - file: sampled file-pair row-count checks
  - dataset: full structure + full file-pair row-count
  - snapshot: same as dataset, intended for --dataset all
";

const VERIFY_SCHEMA_LONG_ABOUT: &str = "\
Verify schema parity across schema sources.

Behavior:
  - loads left schema from --from
  - loads right schema from --diff-with
  - reports added/removed/changed fields
  - exits non-zero when any difference exists

Defaults:
  from: source
  diff-with: parquet
";

const SCHEMA_LONG_ABOUT: &str = "\
Inspect and compare schemas.

Sources:
  source: infer from snapshot JSON.GZ
  cache: read cached source schema
  parquet: infer from parquet files
  auto: source > cache > parquet

Formats:
  table: human-readable overview
  json: machine-readable schema document
  yaml: machine-readable YAML representation
  arrow-r: stable JSON shape for R Arrow comparisons

Comparison:
  use --diff-with <source> to compare schema variants
  output reports added/removed fields and changed types

Defaults:
  from: auto
  format: table
  output: stdout

Cache contract:
  canonical cache artifact is .<dataset>_metadata/schemata/unified_schema.csv
  source_schema.json is optional derived metadata and is not authoritative
";

/// Short version string shown by `-V` / `--version`.
const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Long version shown by `--version` (not `-V`): adds git hash and build date.
const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("BUILD_GIT_HASH"),
    " ",
    env!("BUILD_DATE"),
    ")"
);

#[derive(Parser, Debug, Clone)]
#[command(name = "openalex-snapshot")]
#[command(about = "Standalone OpenAlex snapshot conversion and validation tool")]
#[command(long_about = CLI_LONG_ABOUT)]
#[command(version = VERSION)]
#[command(long_version = LONG_VERSION)]
struct Cli {
    #[arg(long)]
    #[arg(
        help = "Optional path to config YAML (auto-discovers ./openalex-snapshot.yaml if omitted)"
    )]
    config: Option<PathBuf>,

    #[arg(long)]
    #[arg(
        help = "Optional path to a profiles YAML defining custom stratified profiles (auto-discovers ./openalex-snapshot.profiles.yaml if omitted; built-in profiles `safe` and `stratified-36` are always available)"
    )]
    profiles_config: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Print effective resolved arguments for selected subcommand and exit")]
    print_effective_config: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    #[command(about = "Run full pipeline from config.", long_about = ALL_LONG_ABOUT)]
    All(AllArgs),
    #[command(about = "Download snapshot from S3 using AWS CLI sync.", long_about = DOWNLOAD_LONG_ABOUT)]
    Download(DownloadArgs),
    #[command(
        about = "Strictly validate downloaded snapshot integrity.",
        long_about = VALIDATE_DOWNLOAD_LONG_ABOUT,
        name = "verify_download"
    )]
    ValidateDownload(ValidateDownloadArgs),
    #[command(about = "Convert snapshot .json.gz files to parquet", long_about = CONVERT_LONG_ABOUT)]
    Convert(ConvertArgs),
    #[command(
        about = "Verify structure and data parity between snapshot and parquet",
        long_about = VERIFY_LONG_ABOUT,
        name = "verify_convert"
    )]
    Verify(VerifyArgs),
    #[command(
        about = "Inspect schema from source/cache/parquet and compare schema variants",
        long_about = SCHEMA_LONG_ABOUT
    )]
    Schema(SchemaArgs),
    #[command(about = "Verify schema parity across sources.", long_about = VERIFY_SCHEMA_LONG_ABOUT, name = "verify_schema")]
    VerifySchema(VerifySchemaArgs),
    #[command(about = "Build a parquet lookup index for a parquet corpus.", long_about = INDEX_LONG_ABOUT)]
    Index(IndexArgs),
    #[command(about = "Extract rows by OpenAlex IDs using indexes.", long_about = EXTRACT_LONG_ABOUT)]
    Extract(ExtractArgs),
    #[command(
        about = "Verify index integrity for a parquet corpus.",
        long_about = VERIFY_INDEX_LONG_ABOUT,
        name = "verify_index"
    )]
    VerifyIndex(VerifyIndexArgs),
    #[command(
        about = "Repair failed files from a verify report.",
        long_about = REPAIR_LONG_ABOUT,
        name = "repair_convert"
    )]
    Repair(RepairArgs),
    #[command(about = "View stored reports.", long_about = REPORT_LONG_ABOUT)]
    Report(ReportArgs),
    #[command(about = "Prune old reports.", long_about = PRUNE_REPORTS_LONG_ABOUT)]
    PruneReports(PruneReportsArgs),
    #[command(about = "Monitor live run progress.", long_about = PROGRESS_LONG_ABOUT)]
    Progress(ProgressArgs),
    #[command(about = "Bootstrap project AI skills folder.", long_about = SKILLS_LONG_ABOUT)]
    Skills(SkillsArgs),
    #[command(about = "Run environment and capacity preflight checks.", long_about = CHECK_LONG_ABOUT)]
    Check(CheckArgs),
    #[command(about = "Create or verify config YAML.", long_about = CONFIG_LONG_ABOUT)]
    Config(ConfigArgs),
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Run full pipeline from config")]
#[command(long_about = ALL_LONG_ABOUT)]
struct AllArgs {
    #[arg(long)]
    #[arg(help = "Required config file path for pipeline execution")]
    config: PathBuf,

    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(long, default_value_t = 1)]
    #[arg(help = "Max number of repair_convert attempts after verify_convert failures")]
    retry: usize,

    #[arg(long, default_value_t = false)]
    #[arg(
        help = "Skip free disk space preflight checks in all pipeline stages (overrides config)"
    )]
    skip_disk_check: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain resolved execution plan and exit")]
    explain: bool,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Create or verify config YAML")]
#[command(long_about = CONFIG_LONG_ABOUT)]
struct ConfigArgs {
    #[arg(long, value_enum, num_args = 0..=1, default_missing_value = "complete")]
    #[arg(
        help = "Create config template: complete, safe, or fast (default when omitted: complete)"
    )]
    create: Option<ConfigTemplateMode>,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Verify config file syntax and schema")]
    verify: bool,

    #[arg(long, default_value = "./openalex-snapshot.yaml")]
    #[arg(help = "Config file path")]
    config: PathBuf,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Write template to stdout instead of file (create mode)")]
    stdout: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Overwrite existing config file (create mode)")]
    overwrite: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain action and exit without executing")]
    explain: bool,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
enum ConfigTemplateMode {
    Complete,
    Safe,
    Fast,
}

#[derive(clap::Args, Debug, Clone)]
struct SharedArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    snapshot_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    parquet_dir: PathBuf,

    #[arg(long, default_value = "all")]
    #[arg(help = "Dataset name (works, authors, ...) or 'all'")]
    dataset: String,

    #[arg(long, default_value_t = 0)]
    #[arg(help = "Number of worker threads (0 = auto: cpus-2 for auto profile, 4 otherwise)")]
    workers: usize,

    #[arg(long)]
    #[arg(help = "Path to duckdb executable (default: duckdb in PATH)")]
    duckdb_bin: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Stratified profile types
//
// A profile is either Safe (single conservative configuration) or Stratified
// (a sequence of strata partitioning the file list by gz size; each stratum
// runs as its own rayon parallel pass with its own worker count and DuckDB
// memory limit).  Profile definitions are loaded from `builtin_profiles()`
// and optionally merged with a user-supplied `profiles.yaml`.
// ---------------------------------------------------------------------------

/// One bucket in a stratified profile.  Files with `gz_size_bytes <= max_file_mb * 1MiB`
/// (and larger than the previous stratum's `max_file_mb`) belong to this stratum.
/// `max_file_mb = None` is the catch-all (no upper bound); a stratified profile must
/// have exactly one catch-all stratum, and it must be the last in ascending order.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Stratum {
    /// Upper bound for this stratum in MB (inclusive).  None = catch-all.
    max_file_mb: Option<u64>,
    /// Rayon worker threads for this stratum's parallel pass.  Must be >= 1.
    workers: usize,
    /// DuckDB `memory_limit` per worker, in MB.  Must be >= 256.
    /// Global DuckDB memory limit per stratum is set to `workers * per_worker_mb`.
    per_worker_mb: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ProfileKind {
    /// Conservative single-pass mode.  Workers and memory derived from system RAM
    /// at runtime (see `auto_profile_single_worker_safe_memory_mb`).  `strata` is None.
    Safe,
    /// Multi-pass mode partitioned by gz size.  `strata` is required and non-empty.
    Stratified,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProfileDef {
    kind: ProfileKind,
    description: Option<String>,
    /// Recommended minimum system RAM in GB for this profile.  A warning is emitted
    /// if the host has less RAM than this.  None means no hint.
    min_ram_gb: Option<usize>,
    /// Required for Stratified profiles; must be None for Safe.
    strata: Option<Vec<Stratum>>,
}

/// Minimum per-worker DuckDB memory (MB) when scaling.  Keeps tiny machines from
/// landing in pathological 100-MB-per-worker territory.
/// Used by `derive_stratified_profile_for_ram` (which is currently only wired
/// into the upcoming `config --create-profiles` flow — hence `allow(dead_code)`).
#[allow(dead_code)]
const STRATIFIED_MIN_PER_WORKER_MB: usize = 1280;

/// Hard cap on CPU-bound rayon workers regardless of derived value.  Empirically
/// this session showed workers=6 is already slower than workers=4 on small files
/// due to CPU+I/O contention; cap at 8 to leave a tail of headroom for huge boxes.
#[allow(dead_code)]
const STRATIFIED_MAX_WORKERS: usize = 8;

/// The empirical baseline used both for the built-in `stratified-36` profile and
/// as the seed scaled by `derive_stratified_profile_for_ram`.  These exact values
/// were measured this session on a 36 GB / 8+ core Mac with in-process DuckDB
/// and spill-to-disk enabled.
fn stratified_baseline_36gb_strata() -> Vec<Stratum> {
    vec![
        Stratum {
            max_file_mb: Some(400),
            workers: 4,
            per_worker_mb: 4800,
        },
        Stratum {
            max_file_mb: Some(600),
            workers: 3,
            per_worker_mb: 6400,
        },
        Stratum {
            max_file_mb: Some(800),
            workers: 2,
            per_worker_mb: 9600,
        },
        Stratum {
            max_file_mb: None,
            workers: 1,
            per_worker_mb: 13_000,
        },
    ]
}

/// Built-in profiles shipped with the binary.  User profiles loaded from
/// `profiles.yaml` are merged on top via `profile_registry`.
fn builtin_profiles() -> Vec<(String, ProfileDef)> {
    vec![
        (
            "safe".to_string(),
            ProfileDef {
                kind: ProfileKind::Safe,
                description: Some(
                    "Single-worker, max-memory; the conservative universal default".to_string(),
                ),
                min_ram_gb: None,
                strata: None,
            },
        ),
        (
            "stratified-36".to_string(),
            ProfileDef {
                kind: ProfileKind::Stratified,
                description: Some(
                    "Stratified 4/3/2/1 workers by gz size; empirically tuned for ~36 GB RAM"
                        .to_string(),
                ),
                min_ram_gb: Some(32),
                strata: Some(stratified_baseline_36gb_strata()),
            },
        ),
    ]
}

/// Derive a stratified profile scaled from the 36 GB baseline to match the
/// system's actual RAM.  File-size cutoffs (`max_file_mb`) stay fixed because
/// they reflect the works dataset's compression shape (works gz files expand
/// ~10-15x); `workers` and `per_worker_mb` scale linearly with the RAM ratio,
/// floored at `STRATIFIED_MIN_PER_WORKER_MB` per worker and capped at
/// `STRATIFIED_MAX_WORKERS` (further capped by detected CPU count when known).
///
/// Used by `config --create-profiles` to emit a starter `profiles.yaml`
/// calibrated for the host.  Not registered as a runtime profile — the user
/// reviews/tunes the YAML and then references the profile by name.
#[allow(dead_code)]
fn derive_stratified_profile_for_ram(total_ram_mb: Option<usize>) -> ProfileDef {
    let cpu_cap = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(STRATIFIED_MAX_WORKERS)
        .min(STRATIFIED_MAX_WORKERS);

    let strata = match total_ram_mb {
        Some(mb) if mb > 0 => {
            let ratio = (mb as f64) / 36_864.0; // 36 GB baseline
                                                // System-RAM safety caps applied after linear scaling.  Workers and
                                                // per_worker_mb both scale with ratio, so total commitment scales as
                                                // ratio^2 — that overshoots physical RAM on large hosts.  The 36 GB
                                                // baseline uses ~53% of total RAM per parallel stratum and ~36% for
                                                // the single-worker catch-all; preserve those ratios on every host.
            let parallel_cap_mb = ((mb as f64) * 0.55).floor() as usize;
            let single_cap_mb = ((mb as f64) * 0.40).floor() as usize;
            stratified_baseline_36gb_strata()
                .into_iter()
                .map(|s| {
                    let scaled_workers = ((s.workers as f64) * ratio).round().max(1.0) as usize;
                    let workers = scaled_workers.min(cpu_cap).max(1);
                    let scaled_mb = ((s.per_worker_mb as f64) * ratio).round() as usize;
                    let mut per_worker_mb = scaled_mb.max(STRATIFIED_MIN_PER_WORKER_MB);
                    let global_cap = if workers == 1 {
                        single_cap_mb
                    } else {
                        parallel_cap_mb
                    };
                    if workers.saturating_mul(per_worker_mb) > global_cap {
                        per_worker_mb = (global_cap / workers).max(STRATIFIED_MIN_PER_WORKER_MB);
                    }
                    Stratum {
                        max_file_mb: s.max_file_mb,
                        workers,
                        per_worker_mb,
                    }
                })
                .collect()
        }
        _ => {
            // Total RAM unknown — fall back to a single conservative stratum.
            vec![Stratum {
                max_file_mb: None,
                workers: 1,
                per_worker_mb: 4096,
            }]
        }
    };

    let ram_gb = total_ram_mb.map(|mb| (mb + 512) / 1024);
    ProfileDef {
        kind: ProfileKind::Stratified,
        description: Some(format!(
            "Auto-derived from {} GB system RAM (scaled from the 36 GB baseline)",
            ram_gb
                .map(|g| g.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        )),
        min_ram_gb: ram_gb,
        strata: Some(strata),
    }
}

/// YAML shape of a user-provided profiles config file.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfilesYaml {
    profiles: BTreeMap<String, ProfileDef>,
}

/// Resolved set of profile definitions visible to the binary.  Built-in profiles
/// (see `builtin_profiles`) are always present; entries from a user-supplied
/// `profiles.yaml` (loaded via `--profiles-config` or the default sibling-config
/// path) are merged on top with user values winning on name collision.
#[derive(Clone, Debug)]
struct ProfileRegistry {
    profiles: BTreeMap<String, ProfileDef>,
}

impl ProfileRegistry {
    /// Built-ins only; no YAML loaded.
    fn builtins_only() -> Self {
        let profiles = builtin_profiles().into_iter().collect();
        Self { profiles }
    }

    /// Built-ins plus optional user YAML.  A missing path is fine and yields built-ins only.
    /// An unreadable or invalid YAML returns Err with a helpful message.
    fn load(profiles_config_path: Option<&Path>) -> Result<Self> {
        let mut registry = Self::builtins_only();
        let Some(path) = profiles_config_path else {
            return Ok(registry);
        };
        if !path.exists() {
            return Ok(registry);
        }
        let txt = fs::read_to_string(path)
            .with_context(|| format!("failed to read profiles config: {}", path.display()))?;
        let parsed: ProfilesYaml = serde_yaml::from_str(&txt)
            .with_context(|| format!("failed to parse profiles config YAML: {}", path.display()))?;
        for (name, def) in parsed.profiles {
            validate_profile_def(&name, &def)
                .with_context(|| format!("in profiles config: {}", path.display()))?;
            registry.profiles.insert(name, def); // user wins on collision
        }
        Ok(registry)
    }

    fn get(&self, name: &str) -> Option<&ProfileDef> {
        self.profiles.get(name)
    }

    fn names(&self) -> Vec<&str> {
        self.profiles.keys().map(String::as_str).collect()
    }

    /// Build a clear "unknown profile" error message that lists what IS available.
    fn unknown_profile_error(&self, requested: &str) -> anyhow::Error {
        let mut names: Vec<&str> = self.names();
        names.sort();
        anyhow::anyhow!(
            "unknown profile {:?}. Available: {}",
            requested,
            names.join(", ")
        )
    }
}

/// Validate a single profile definition.  Returns a clear error if the shape
/// violates invariants required by the planner (`build_convert_plan`).
fn validate_profile_def(name: &str, def: &ProfileDef) -> Result<()> {
    match def.kind {
        ProfileKind::Safe => {
            if def.strata.is_some() {
                anyhow::bail!("profile '{name}': kind=safe must not have strata");
            }
        }
        ProfileKind::Stratified => {
            let strata = def.strata.as_ref().ok_or_else(|| {
                anyhow::anyhow!("profile '{name}': kind=stratified requires strata")
            })?;
            if strata.is_empty() {
                anyhow::bail!("profile '{name}': strata must not be empty");
            }
            let catch_all_count = strata.iter().filter(|s| s.max_file_mb.is_none()).count();
            if catch_all_count != 1 {
                anyhow::bail!(
                    "profile '{name}': must have exactly one stratum with max_file_mb omitted (catch-all); found {catch_all_count}"
                );
            }
            // Last entry must be the catch-all.
            let last_is_catch_all = strata.last().is_some_and(|s| s.max_file_mb.is_none());
            if !last_is_catch_all {
                anyhow::bail!(
                    "profile '{name}': the catch-all stratum (max_file_mb omitted) must be the LAST entry"
                );
            }
            // max_file_mb must be strictly ascending across bounded strata.
            let mut prev: Option<u64> = None;
            for (i, s) in strata.iter().enumerate() {
                if s.workers < 1 {
                    anyhow::bail!("profile '{name}' stratum {i}: workers must be >= 1");
                }
                if s.per_worker_mb < 256 {
                    anyhow::bail!(
                        "profile '{name}' stratum {i}: per_worker_mb must be >= 256 (got {})",
                        s.per_worker_mb
                    );
                }
                if let Some(curr) = s.max_file_mb {
                    if let Some(p) = prev {
                        if curr <= p {
                            anyhow::bail!(
                                "profile '{name}' stratum {i}: max_file_mb must be strictly ascending; got {curr} MB after {p} MB"
                            );
                        }
                    }
                    prev = Some(curr);
                }
            }
        }
    }
    Ok(())
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum VerifyScope {
    File,
    Dataset,
    Snapshot,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum VerifyMetadataLevel {
    RowCount,
    IdHash,
    Both,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SchemaFrom {
    Auto,
    Source,
    Cache,
    Parquet,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum SchemaFormat {
    Table,
    Json,
    Yaml,
    ArrowR,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ReportSource {
    All,
    Parquet,
    Download,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
enum DiskCheckScope {
    Dataset,
    File,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Convert snapshot .json.gz files to parquet")]
#[command(long_about = CONVERT_LONG_ABOUT)]
struct ConvertArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long, default_value = "safe")]
    #[arg(
        help = "Performance/memory profile (default: safe). Built-in: safe, stratified-36 (fixed 36 GB baseline). For other RAM sizes run `config --create-profiles` to scaffold a tuned profiles.yaml."
    )]
    profile: String,

    #[arg(long)]
    #[arg(
        help = "Per-worker memory cap override in MB (auto-detected from system RAM if omitted)"
    )]
    max_memory_mb: Option<usize>,

    #[arg(long, default_value_t = 100_000)]
    #[arg(help = "Parquet row group size")]
    row_group_rows: usize,

    #[arg(long, default_value_t = 5_000)]
    #[arg(help = "Batch rows hint (reserved for future streaming backend)")]
    batch_rows: usize,

    #[arg(long, default_value = "snappy")]
    #[arg(help = "Parquet compression codec (e.g., snappy, zstd)")]
    compression: String,

    #[arg(long, default_value_t = 100)]
    #[arg(help = "Number of source files sampled for schema inference")]
    sample_size: usize,

    #[arg(long = "input-file")]
    #[arg(
        help = "Convert only selected source .gz file(s); can be repeated (absolute path or dataset-relative path)"
    )]
    input_files: Vec<PathBuf>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = 42)]
    #[arg(help = "Random seed for schema sampling")]
    seed: u64,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Skip free disk space preflight checks")]
    skip_disk_check: bool,

    #[arg(long, value_enum, default_value = "dataset")]
    #[arg(help = "Disk check scope: dataset preflight or per-file")]
    disk_check_scope: DiskCheckScope,

    #[arg(long, default_value = "0")]
    #[arg(
        help = "Split gz files larger than this before converting (e.g. 512mb, 1gib). 0 = auto (balanced_mem/15)"
    )]
    split_size: String,

    #[arg(long)]
    #[arg(
        help = "Directory for temporary split gz chunks (must be on the same filesystem as parquet output). Defaults to <parquet_dir>/.split_tmp"
    )]
    split_temp_dir: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Refresh schema cache before conversion")]
    refresh_cache: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Verify structure and data parity between snapshot and parquet")]
#[command(long_about = VERIFY_LONG_ABOUT)]
struct VerifyArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long)]
    #[arg(
        help = "Per-worker memory cap override in MB (auto-detected from system RAM if omitted)"
    )]
    max_memory_mb: Option<usize>,

    #[arg(long, value_enum, default_value = "dataset")]
    #[arg(help = "Verification scope: file|dataset|snapshot")]
    scope: VerifyScope,

    #[arg(long, value_enum, default_value = "both")]
    #[arg(help = "Metadata level: row_count|id_hash|both")]
    metadata_level: VerifyMetadataLevel,

    #[arg(long, default_value_t = 50)]
    #[arg(help = "Sample size for file scope")]
    file_sample_n: usize,

    #[arg(long, default_value_t = 42)]
    #[arg(help = "Random seed for random mode")]
    seed: u64,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Inspect schema from source/cache/parquet and compare schema variants")]
#[command(long_about = SCHEMA_LONG_ABOUT)]
struct SchemaArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long)]
    #[arg(
        help = "Per-worker memory cap override in MB (auto-detected from system RAM if omitted)"
    )]
    max_memory_mb: Option<usize>,

    #[arg(long, value_enum, default_value = "auto")]
    #[arg(help = "Schema source: auto|source|cache|parquet")]
    from: SchemaFrom,

    #[arg(long, value_enum, default_value = "table")]
    #[arg(help = "Output format: table|json|yaml|arrow-r")]
    format: SchemaFormat,

    #[arg(long, value_enum)]
    #[arg(help = "Compare selected --from schema against this source")]
    diff_with: Option<SchemaFrom>,

    #[arg(long)]
    #[arg(help = "Output path (stdout if omitted)")]
    output: Option<PathBuf>,

    #[arg(long, default_value_t = 100)]
    #[arg(help = "Source sampling size when inferring schema")]
    sample_size: usize,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Refresh schema cache before reading schema")]
    refresh_cache: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush schema inference state every N sampled files (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Verify schema parity across sources")]
#[command(long_about = VERIFY_SCHEMA_LONG_ABOUT)]
struct VerifySchemaArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long)]
    #[arg(
        help = "Per-worker memory cap override in MB (auto-detected from system RAM if omitted)"
    )]
    max_memory_mb: Option<usize>,

    #[arg(long, value_enum, default_value = "source")]
    #[arg(help = "Left schema source")]
    from: SchemaFrom,

    #[arg(long, value_enum, default_value = "parquet")]
    #[arg(help = "Right schema source")]
    diff_with: SchemaFrom,

    #[arg(long, default_value_t = 100)]
    #[arg(help = "Source sampling size when inferring schema")]
    sample_size: usize,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Refresh schema cache before reading schema")]
    refresh_cache: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush schema inference state every N sampled files (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(long_about = INDEX_LONG_ABOUT)]
struct IndexArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(long, default_value = "all")]
    #[arg(help = "Dataset name to index (e.g. works, authors, sources, or all)")]
    dataset: String,

    #[arg(long)]
    #[arg(help = "Optional output index file path")]
    index_file: Option<PathBuf>,

    #[arg(long, default_value_t = 0)]
    #[arg(help = "Number of workers (0 = auto: cpus-2, same semantics as convert)")]
    workers: usize,

    #[arg(long)]
    #[arg(help = "Per-worker memory cap override in MB (same semantics as convert)")]
    max_memory_mb: Option<usize>,

    #[arg(long)]
    #[arg(help = "Path to duckdb executable (default: duckdb in PATH)")]
    duckdb_bin: Option<PathBuf>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show stage progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Overwrite existing index file")]
    overwrite: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Extract rows by OpenAlex IDs using indexes")]
#[command(long_about = EXTRACT_LONG_ABOUT)]
struct ExtractArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long)]
    #[arg(help = "Input CSV containing OpenAlex IDs")]
    ids: PathBuf,

    #[arg(long)]
    #[arg(help = "Output parquet base path (writes <base>_<dataset>.parquet)")]
    output: PathBuf,

    #[arg(long)]
    #[arg(
        help = "Per-worker memory cap override in MB (auto-detected from system RAM if omitted)"
    )]
    max_memory_mb: Option<usize>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Repair failed files from a verify report")]
#[command(long_about = REPAIR_LONG_ABOUT)]
struct RepairArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long)]
    #[arg(
        help = "Path to verify_convert report JSON; if omitted, the latest verify_convert report under <root>/openalex-snapshot_metadata/reports/ is used automatically"
    )]
    from_verify_report: Option<PathBuf>,

    #[arg(long, default_value = "safe")]
    #[arg(
        help = "Performance/memory profile (default: safe). Built-in: safe, stratified-36 (fixed 36 GB baseline). For other RAM sizes run `config --create-profiles` to scaffold a tuned profiles.yaml."
    )]
    profile: String,

    #[arg(long)]
    #[arg(
        help = "Per-worker memory cap override in MB (auto-detected from system RAM if omitted)"
    )]
    max_memory_mb: Option<usize>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Download snapshot from S3 using AWS CLI sync")]
#[command(long_about = DOWNLOAD_LONG_ABOUT)]
struct DownloadArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    snapshot_dir: PathBuf,

    #[arg(long, default_value = "s3://openalex")]
    #[arg(help = "Source S3 URI")]
    s3_uri: String,

    #[arg(long, default_value = "all")]
    #[arg(help = "Dataset name (works, authors, ...) or 'all'")]
    dataset: String,

    #[arg(long, default_value = "aws")]
    #[arg(help = "Path to aws executable (default: aws in PATH)")]
    aws_bin: PathBuf,

    #[arg(long)]
    #[arg(help = "Optional custom S3 endpoint URL")]
    endpoint_url: Option<String>,

    #[arg(long)]
    #[arg(help = "Optional AWS region")]
    region: Option<String>,

    #[arg(long)]
    #[arg(help = "Optional AWS profile name")]
    profile_name: Option<String>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Use --no-sign-request for public OpenAlex access")]
    no_sign_request: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Use signed AWS requests (overrides --no-sign-request)")]
    signed: bool,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Mirror remote content by deleting local files not present remotely")]
    delete_files: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Disable remote-delete mirroring")]
    no_delete: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Skip free disk space preflight checks")]
    skip_disk_check: bool,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Strictly validate downloaded snapshot integrity")]
#[command(long_about = VALIDATE_DOWNLOAD_LONG_ABOUT)]
struct ValidateDownloadArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    snapshot_dir: PathBuf,

    #[arg(long, default_value = "s3://openalex")]
    #[arg(help = "Source S3 URI for remote manifest")]
    s3_uri: String,

    #[arg(long, default_value = "all")]
    #[arg(help = "Dataset name (works, authors, ...) or 'all'")]
    dataset: String,

    #[arg(long, default_value = "aws")]
    #[arg(help = "Path to aws executable (default: aws in PATH)")]
    aws_bin: PathBuf,

    #[arg(long)]
    #[arg(help = "Optional custom S3 endpoint URL")]
    endpoint_url: Option<String>,

    #[arg(long)]
    #[arg(help = "Optional AWS region")]
    region: Option<String>,

    #[arg(long)]
    #[arg(help = "Optional AWS profile name")]
    profile_name: Option<String>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Use --no-sign-request for public OpenAlex access")]
    no_sign_request: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Use signed AWS requests (overrides --no-sign-request)")]
    signed: bool,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Detect extra local files not present remotely")]
    check_extra: bool,

    #[arg(long, default_value_t = 0)]
    #[arg(help = "Number of worker threads for local integrity checks (0 = auto: cpus-2)")]
    workers: usize,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,

    #[arg(long, default_value_t = 25)]
    #[arg(help = "Flush state/report every N items (for crash resilience)")]
    state_flush_every: usize,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Verify index integrity for a parquet corpus")]
#[command(long_about = VERIFY_INDEX_LONG_ABOUT)]
struct VerifyIndexArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(long, default_value = "all")]
    #[arg(help = "Dataset name to verify index for (e.g. works, authors, sources, or all)")]
    dataset: String,

    #[arg(long)]
    #[arg(help = "Optional index file path (default: <parquet_dir>/<dataset>_id_idx.parquet)")]
    index_file: Option<PathBuf>,

    #[arg(long, default_value_t = 0)]
    #[arg(help = "Number of workers (0 = auto: cpus-2)")]
    workers: usize,

    #[arg(long)]
    #[arg(help = "Per-worker memory cap override in MB")]
    max_memory_mb: Option<usize>,

    #[arg(long)]
    #[arg(help = "Path to duckdb executable (default: duckdb in PATH)")]
    duckdb_bin: Option<PathBuf>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "View stored reports")]
#[command(long_about = REPORT_LONG_ABOUT)]
struct ReportArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    snapshot_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    parquet_dir: PathBuf,

    #[arg(long, value_enum, default_value = "all")]
    #[arg(help = "Report source to scan")]
    source: ReportSource,

    #[arg(long)]
    #[arg(help = "Filter to a command name (e.g., verify, convert, download)")]
    command: Option<String>,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Show only the latest report per command")]
    latest: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Show only aggregate totals; suppress per-dataset breakdown")]
    summary: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Print full pretty JSON after each summary line")]
    full: bool,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Prune old reports")]
#[command(long_about = PRUNE_REPORTS_LONG_ABOUT)]
struct PruneReportsArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    snapshot_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    parquet_dir: PathBuf,

    #[arg(long, value_enum, default_value = "all")]
    #[arg(help = "Report source to scan")]
    source: ReportSource,

    #[arg(long)]
    #[arg(help = "Filter to a command name (e.g., verify, convert, download)")]
    command: Option<String>,

    #[arg(long, default_value_t = 1)]
    #[arg(help = "Number of newest reports to keep per command")]
    keep_per_command: usize,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Show what would be pruned without deleting files")]
    dry_run: bool,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Monitor live run progress")]
#[command(long_about = PROGRESS_LONG_ABOUT)]
struct ProgressArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    snapshot_dir: PathBuf,

    #[arg(skip = PathBuf::new())]
    parquet_dir: PathBuf,

    #[arg(long)]
    #[arg(help = "Filter by command")]
    command: Option<String>,

    #[arg(long, default_value = "all")]
    #[arg(help = "Filter by dataset or 'all'")]
    dataset: String,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Continuously watch for updates")]
    watch: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Print once and exit")]
    once: bool,

    #[arg(long, default_value_t = 2)]
    #[arg(help = "Refresh interval in seconds while watching")]
    interval_sec: u64,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Print machine-readable JSON output")]
    json: bool,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Bootstrap project AI skills folder")]
#[command(long_about = SKILLS_LONG_ABOUT)]
struct SkillsArgs {
    #[arg(long, default_value = ".")]
    #[arg(
        help = "Root directory containing openalex-snapshot/, parquet/, and openalex-snapshot_metadata/"
    )]
    root_dir: PathBuf,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Rewrite generated template files")]
    overwrite: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Print generated file manifest/content preview")]
    stdout: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,
}

#[derive(clap::Args, Debug, Clone)]
#[command(about = "Run environment and capacity preflight checks")]
#[command(long_about = CHECK_LONG_ABOUT)]
struct CheckArgs {
    #[command(flatten)]
    shared: SharedArgs,

    #[arg(long, default_value = "aws")]
    #[arg(help = "Path to aws executable (default: aws in PATH)")]
    aws_bin: PathBuf,

    #[arg(long, default_value = "s3://openalex")]
    #[arg(help = "Source S3 URI used for download preflight estimate")]
    s3_uri: String,

    #[arg(long)]
    #[arg(help = "Optional custom S3 endpoint URL")]
    endpoint_url: Option<String>,

    #[arg(long)]
    #[arg(help = "Optional AWS region")]
    region: Option<String>,

    #[arg(long)]
    #[arg(help = "Optional AWS profile name")]
    profile_name: Option<String>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Use --no-sign-request for public OpenAlex access")]
    no_sign_request: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Use signed AWS requests (overrides --no-sign-request)")]
    signed: bool,

    #[arg(long)]
    #[arg(help = "Per-worker memory cap override in MB")]
    max_memory_mb: Option<usize>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Precise source inventory estimate for convert checks")]
    precise: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Fail on warnings as well as errors")]
    strict: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Print machine-readable JSON output")]
    json: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppConfig {
    defaults: Option<ConfigDefaults>,
    all: Option<AllConfig>,
    convert: Option<ConvertConfig>,
    verify_convert: Option<VerifyConfig>,
    schema: Option<SchemaConfig>,
    index: Option<IndexConfig>,
    extract: Option<ExtractConfig>,
    repair_convert: Option<RepairConfig>,
    download: Option<DownloadConfig>,
    verify_download: Option<ValidateDownloadConfig>,
    verify_index: Option<VerifyIndexConfig>,
    report: Option<ReportConfig>,
    prune_reports: Option<PruneReportsConfig>,
    progress: Option<ProgressConfig>,
    check: Option<CheckConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllConfig {
    enable_download: Option<bool>,
    enable_verify_download: Option<bool>,
    enable_convert: Option<bool>,
    enable_verify_convert: Option<bool>,
    enable_repair_convert: Option<bool>,
    enable_index: Option<bool>,
    enable_verify_index: Option<bool>,
    retry: Option<usize>,
    skip_disk_check: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDefaults {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    profile: Option<String>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConvertConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    profile: Option<String>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    row_group_rows: Option<usize>,
    batch_rows: Option<usize>,
    compression: Option<String>,
    sample_size: Option<usize>,
    seed: Option<u64>,
    refresh_cache: Option<bool>,
    skip_disk_check: Option<bool>,
    split_size: Option<String>,
    split_temp_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    scope: Option<VerifyScope>,
    metadata_level: Option<VerifyMetadataLevel>,
    file_sample_n: Option<usize>,
    seed: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    max_memory_mb: Option<usize>,
    state_flush_every: Option<usize>,
    from: Option<SchemaFrom>,
    format: Option<SchemaFormat>,
    diff_with: Option<SchemaFrom>,
    output: Option<PathBuf>,
    sample_size: Option<usize>,
    refresh_cache: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    index_file: Option<PathBuf>,
    overwrite: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtractConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    ids: Option<PathBuf>,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    profile: Option<String>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    from_verify_report: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadConfig {
    root_dir: Option<PathBuf>,
    s3_uri: Option<String>,
    dataset: Option<String>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    aws_bin: Option<PathBuf>,
    endpoint_url: Option<String>,
    region: Option<String>,
    profile_name: Option<String>,
    no_sign_request: Option<bool>,
    signed: Option<bool>,
    delete_files: Option<bool>,
    no_delete: Option<bool>,
    skip_disk_check: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidateDownloadConfig {
    root_dir: Option<PathBuf>,
    s3_uri: Option<String>,
    dataset: Option<String>,
    workers: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    aws_bin: Option<PathBuf>,
    endpoint_url: Option<String>,
    region: Option<String>,
    profile_name: Option<String>,
    no_sign_request: Option<bool>,
    signed: Option<bool>,
    check_extra: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyIndexConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    index_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportConfig {
    root_dir: Option<PathBuf>,
    source: Option<ReportSource>,
    command: Option<String>,
    latest: Option<bool>,
    summary: Option<bool>,
    full: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PruneReportsConfig {
    root_dir: Option<PathBuf>,
    source: Option<ReportSource>,
    command: Option<String>,
    keep_per_command: Option<usize>,
    dry_run: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressConfig {
    root_dir: Option<PathBuf>,
    command: Option<String>,
    dataset: Option<String>,
    interval_sec: Option<u64>,
    watch: Option<bool>,
    json: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    duckdb_bin: Option<PathBuf>,
    aws_bin: Option<PathBuf>,
    s3_uri: Option<String>,
    endpoint_url: Option<String>,
    region: Option<String>,
    profile_name: Option<String>,
    no_sign_request: Option<bool>,
    signed: Option<bool>,
    max_memory_mb: Option<usize>,
    precise: Option<bool>,
    strict: Option<bool>,
    json: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FieldDef {
    name: String,
    r#type: String,
    nullable: bool,
    children: Vec<FieldDef>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaDoc {
    dataset: String,
    source: String,
    generated_at_unix: i64,
    fields: Vec<FieldDef>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct FilePair {
    input_gz: PathBuf,
    output_parquet: PathBuf,
    rel: PathBuf,
    gz_size_bytes: u64,
}

#[derive(Debug, Clone)]
struct RepairTarget {
    dataset: String,
    source_path: PathBuf,
    output_path: PathBuf,
    rel: PathBuf,
}

#[derive(Debug, Clone)]
struct ExtractInput {
    raw: String,
    normalized: String,
    /// Full-URL form matching what the index stores, e.g. "https://openalex.org/W1234"
    canonical: String,
    dataset: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteObject {
    key: String,
    size: u64,
    etag: String,
    last_modified: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceMetricRow {
    rel_path: String,
    source_size: u64,
    source_mtime_unix: i64,
    row_count: u64,
    id_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParquetMetricRow {
    rel_path: String,
    parquet_size: u64,
    parquet_mtime_unix: i64,
    row_count: u64,
    id_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DatasetReportSummary {
    dataset: String,
    items_scanned: u64,
    succeeded: u64,
    failed: u64,
    skipped: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FailureEntry {
    dataset: String,
    phase: String,
    rel_path: Option<String>,
    source_path: Option<String>,
    output_path: Option<String>,
    error_message: String,
    suggested_recovery: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StepRunSummary {
    step: String,
    status: String,
    report_path: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunReport {
    command: String,
    #[serde(default)]
    cli_version: String,
    #[serde(default)]
    report_nonce: u128,
    started_at_unix: i64,
    finished_at_unix: Option<i64>,
    duration_seconds: Option<f64>,
    args: BTreeMap<String, String>,
    totals_items_scanned: u64,
    totals_succeeded: u64,
    totals_failed: u64,
    totals_skipped: u64,
    datasets: Vec<DatasetReportSummary>,
    failures: Vec<FailureEntry>,
    #[serde(default)]
    step_runs: Vec<StepRunSummary>,
}

struct RecursionDepthGuard(&'static AtomicUsize);

impl RecursionDepthGuard {
    fn enter(counter: &'static AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl Drop for RecursionDepthGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn main() -> Result<()> {
    let matches = Cli::command().get_matches();
    let cli =
        Cli::from_arg_matches(&matches).map_err(|e| anyhow!("failed to parse CLI args: {e}"))?;
    let sub_matches = matches.subcommand().map(|(_, m)| m);
    if let Commands::Config(args) = cli.command.clone() {
        return run_config(args);
    }
    let cfg = load_optional_config(cli.config.as_deref())?;
    match cli.command {
        Commands::All(mut args) => {
            let all_cfg = load_optional_config(Some(&args.config))?
                .ok_or_else(|| anyhow!("all requires --config <path>"))?;
            if let Some(c) = all_cfg.defaults.as_ref() {
                if !cli_explicit(sub_matches, "root_dir") {
                    if let Some(v) = &c.root_dir {
                        args.root_dir = v.clone();
                    }
                }
            }
            try_migrate_metadata_root(&args.root_dir);
            run_all(args, &all_cfg, cli.profiles_config.as_deref())
        }
        Commands::Convert(mut args) => {
            fill_shared_dirs(&mut args.shared);
            apply_convert_config(&mut args, cfg.as_ref(), sub_matches);
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            if cli.print_effective_config {
                explain_convert(
                    &args,
                    &resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?,
                    &duckdb_bin(&args.shared),
                );
                return Ok(());
            }
            run_convert(args, cli.profiles_config.as_deref())
        }
        Commands::Verify(mut args) => {
            fill_shared_dirs(&mut args.shared);
            apply_verify_config(&mut args, cfg.as_ref(), sub_matches);
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            if cli.print_effective_config {
                explain_verify(
                    &args,
                    &resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?,
                    &duckdb_bin(&args.shared),
                    &light_tuning_with_override(args.shared.workers, args.max_memory_mb),
                );
                return Ok(());
            }
            run_verify(args)
        }
        Commands::Schema(mut args) => {
            fill_shared_dirs(&mut args.shared);
            apply_schema_config(&mut args, cfg.as_ref(), sub_matches);
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            if cli.print_effective_config {
                explain_schema(
                    &args,
                    &resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?,
                    &duckdb_bin(&args.shared),
                    &light_tuning_with_override(args.shared.workers, args.max_memory_mb),
                );
                return Ok(());
            }
            run_schema(args)
        }
        Commands::VerifySchema(mut args) => {
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            run_verify_schema(args)
        }
        Commands::Index(mut args) => {
            apply_index_config(&mut args, cfg.as_ref(), sub_matches);
            try_migrate_metadata_root(&args.root_dir);
            if cli.print_effective_config {
                let corpus_dir = args.root_dir.join("parquet").join(&args.dataset);
                explain_index(
                    &args,
                    &duckdb_bin_from_option(&args.duckdb_bin),
                    &corpus_dir,
                    &args
                        .index_file
                        .clone()
                        .unwrap_or_else(|| PathBuf::from("auto")),
                    &light_tuning_with_override(args.workers, args.max_memory_mb),
                );
                return Ok(());
            }
            run_index(args)
        }
        Commands::Extract(mut args) => {
            fill_shared_dirs(&mut args.shared);
            apply_extract_config(&mut args, cfg.as_ref(), sub_matches);
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            if cli.print_effective_config {
                explain_extract(
                    &args,
                    &duckdb_bin(&args.shared),
                    &light_tuning_with_override(args.shared.workers, args.max_memory_mb),
                );
                return Ok(());
            }
            run_extract(args)
        }
        Commands::Repair(mut args) => {
            fill_shared_dirs(&mut args.shared);
            apply_repair_config(&mut args, cfg.as_ref(), sub_matches);
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            run_repair(args, cli.profiles_config.as_deref())
        }
        Commands::Download(mut args) => {
            fill_download_dirs(&mut args);
            apply_download_config(&mut args, cfg.as_ref(), sub_matches);
            fill_download_dirs(&mut args);
            try_migrate_metadata_root(&args.root_dir);
            run_download(args)
        }
        Commands::ValidateDownload(mut args) => {
            fill_validate_download_dirs(&mut args);
            apply_validate_download_config(&mut args, cfg.as_ref(), sub_matches);
            fill_validate_download_dirs(&mut args);
            try_migrate_metadata_root(&args.root_dir);
            run_validate_download(args)
        }
        Commands::VerifyIndex(mut args) => {
            apply_verify_index_config(&mut args, cfg.as_ref(), sub_matches);
            try_migrate_metadata_root(&args.root_dir);
            run_verify_index(args)
        }
        Commands::Report(mut args) => {
            fill_report_dirs(&mut args);
            apply_report_config(&mut args, cfg.as_ref(), sub_matches);
            fill_report_dirs(&mut args);
            try_migrate_metadata_root(&args.root_dir);
            run_report(args)
        }
        Commands::PruneReports(mut args) => {
            fill_prune_report_dirs(&mut args);
            apply_prune_reports_config(&mut args, cfg.as_ref(), sub_matches);
            fill_prune_report_dirs(&mut args);
            try_migrate_metadata_root(&args.root_dir);
            run_prune_reports(args)
        }
        Commands::Progress(mut args) => {
            fill_progress_dirs(&mut args);
            apply_progress_config(&mut args, cfg.as_ref(), sub_matches);
            fill_progress_dirs(&mut args);
            try_migrate_metadata_root(&args.root_dir);
            run_progress(args)
        }
        Commands::Skills(args) => run_skills(args),
        Commands::Check(mut args) => {
            fill_shared_dirs(&mut args.shared);
            apply_check_config(&mut args, cfg.as_ref(), sub_matches);
            fill_shared_dirs(&mut args.shared);
            try_migrate_metadata_root(&args.shared.root_dir);
            run_check(args)
        }
        Commands::Config(_) => unreachable!(),
    }
}

fn load_optional_config(explicit: Option<&Path>) -> Result<Option<AppConfig>> {
    let path = if let Some(p) = explicit {
        Some(p.to_path_buf())
    } else {
        let p = PathBuf::from("./openalex-snapshot.yaml");
        if p.exists() {
            Some(p)
        } else {
            None
        }
    };
    let Some(path) = path else {
        return Ok(None);
    };
    let txt = fs::read_to_string(&path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let cfg: AppConfig = serde_yaml::from_str(&txt)
        .with_context(|| format!("invalid config YAML {}", path.display()))?;
    Ok(Some(cfg))
}

fn fill_shared_dirs(shared: &mut SharedArgs) {
    shared.snapshot_dir = shared.root_dir.join("snapshot");
    shared.parquet_dir = shared.root_dir.join("parquet");
}

fn fill_download_dirs(args: &mut DownloadArgs) {
    args.snapshot_dir = args.root_dir.join("snapshot");
}

fn fill_validate_download_dirs(args: &mut ValidateDownloadArgs) {
    args.snapshot_dir = args.root_dir.join("snapshot");
}

fn fill_report_dirs(args: &mut ReportArgs) {
    args.snapshot_dir = args.root_dir.join("snapshot");
    args.parquet_dir = args.root_dir.join("parquet");
}

fn fill_prune_report_dirs(args: &mut PruneReportsArgs) {
    args.snapshot_dir = args.root_dir.join("snapshot");
    args.parquet_dir = args.root_dir.join("parquet");
}

fn fill_progress_dirs(args: &mut ProgressArgs) {
    args.snapshot_dir = args.root_dir.join("snapshot");
    args.parquet_dir = args.root_dir.join("parquet");
}

fn try_migrate_metadata_root(root_dir: &Path) {
    // Step 1: rename .openalex-snapshot_metadata -> openalex-snapshot_metadata
    let old_dot = root_dir.join(".openalex-snapshot_metadata");
    let new_root = root_dir.join("openalex-snapshot_metadata");
    if old_dot.exists() && !new_root.exists() {
        let _ = fs::rename(&old_dot, &new_root);
    }
    let _ = fs::create_dir_all(&new_root);

    // Step 2: migrate datasets/ subfolder -> directly under metadata root
    let datasets_subdir = new_root.join("datasets");
    if datasets_subdir.exists() {
        if let Ok(entries) = fs::read_dir(&datasets_subdir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    let name = e.file_name();
                    let target = new_root.join(&name);
                    if !target.exists() {
                        let _ = fs::rename(&p, &target);
                    } else {
                        let _ = merge_dir_with_fallback(&p, &target);
                    }
                }
            }
        }
        // Remove empty datasets/ dir
        let _ = fs::remove_dir(&datasets_subdir);
    }

    // Step 3: migrate old per-dataset logs/ -> per-step subdirs
    if let Ok(entries) = fs::read_dir(&new_root) {
        for e in entries.flatten() {
            let ds_dir = e.path();
            if !ds_dir.is_dir() {
                continue;
            }
            let name = e.file_name();
            let name = name.to_string_lossy();
            if matches!(name.as_ref(), "reports" | "archived" | "download") {
                continue;
            }
            let logs_dir = ds_dir.join("logs");
            if logs_dir.exists() {
                // Move convert.log -> convert/convert.log
                for (log_name, step_dir) in &[
                    ("convert.log", "convert"),
                    ("index.log", "index"),
                    ("verify.log", "conversion-verify"),
                    ("verify-index.log", "index-verify"),
                    ("verify_convert.log", "conversion-verify"),
                    ("verify_index.log", "index-verify"),
                ] {
                    let old_log = logs_dir.join(log_name);
                    if old_log.exists() {
                        let dest_dir = ds_dir.join(step_dir);
                        let _ = fs::create_dir_all(&dest_dir);
                        let _ = fs::rename(&old_log, dest_dir.join(log_name));
                    }
                }
                // Remove empty logs/ dir
                let _ = fs::remove_dir(&logs_dir);
            }
            // Migrate verify/ -> conversion-verify/
            let old_verify = ds_dir.join("verify");
            let new_conv_verify = ds_dir.join("conversion-verify");
            if old_verify.exists() && !new_conv_verify.exists() {
                let _ = fs::rename(&old_verify, &new_conv_verify);
            } else if old_verify.exists() && new_conv_verify.exists() {
                let _ = merge_dir_with_fallback(&old_verify, &new_conv_verify);
            }
            // Remove per-dataset reports/ dir (no longer used)
            let ds_reports = ds_dir.join("reports");
            if ds_reports.exists() {
                // Move any report JSON files up to global reports dir
                let global = new_root.join("reports");
                let _ = fs::create_dir_all(&global);
                if let Ok(rents) = fs::read_dir(&ds_reports) {
                    for rent in rents.flatten() {
                        let rp = rent.path();
                        if rp.is_file() {
                            let fname = rent.file_name();
                            let dest = global.join(&fname);
                            if !dest.exists() {
                                let _ = fs::rename(&rp, &dest);
                            }
                        }
                    }
                }
                let _ = fs::remove_dir(&ds_reports);
            }
        }
    }

    // Step 4: migrate old download logs/download.log -> download/download.log
    let old_dl_log_dir = new_root.join("download").join("logs");
    if old_dl_log_dir.exists() {
        let dl_dir = new_root.join("download");
        for log_name in &["download.log", "verify_download.log"] {
            let old_log = old_dl_log_dir.join(log_name);
            if old_log.exists() {
                let dest = dl_dir.join("download.log");
                if !dest.exists() {
                    let _ = fs::rename(&old_log, &dest);
                }
            }
        }
        let _ = fs::remove_dir(&old_dl_log_dir);
    }

    // Step 5: migrate legacy very-old paths
    let parquet = root_dir.join("parquet");
    let snapshot = root_dir.join("openalex-snapshot");
    let old_global = parquet.join(".openalex_metadata");
    if old_global.exists() {
        let target = new_root.join("reports");
        let _ = merge_dir_with_fallback(&old_global.join("reports"), &target);
    }
    if parquet.exists() {
        if let Ok(entries) = fs::read_dir(&parquet) {
            for e in entries.flatten() {
                let p = e.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if name.starts_with('.') && name.ends_with("_metadata") {
                        let ds = name
                            .trim_start_matches('.')
                            .trim_end_matches("_metadata")
                            .to_string();
                        let target = new_root.join(&ds);
                        let _ = merge_dir_with_fallback(&p, &target);
                    }
                }
            }
        }
    }
    let old_download = snapshot.join(".openalex_download_metadata");
    if old_download.exists() {
        let target = new_root.join("download");
        let _ = merge_dir_with_fallback(&old_download, &target);
    }
}

fn cli_explicit(matches: Option<&ArgMatches>, id: &str) -> bool {
    matches.and_then(|m| m.value_source(id)) == Some(ValueSource::CommandLine)
}

fn apply_shared_defaults(
    shared: &mut SharedArgs,
    d: &ConfigDefaults,
    matches: Option<&ArgMatches>,
) {
    if !cli_explicit(matches, "root_dir") {
        if let Some(v) = &d.root_dir {
            shared.root_dir = v.clone();
        }
    }
    if !cli_explicit(matches, "dataset") {
        if let Some(v) = &d.dataset {
            shared.dataset = v.clone();
        }
    }
    if !cli_explicit(matches, "workers") {
        if let Some(v) = d.workers {
            shared.workers = v;
        }
    }
    if !cli_explicit(matches, "duckdb_bin") {
        if let Some(v) = &d.duckdb_bin {
            shared.duckdb_bin = Some(v.clone());
        }
    }
}

fn apply_convert_config(
    args: &mut ConvertArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        apply_shared_defaults(&mut args.shared, d, matches);
        if !cli_explicit(matches, "profile") {
            if let Some(v) = &d.profile {
                args.profile = v.clone();
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.convert {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "profile") {
            if let Some(v) = &c.profile {
                args.profile = v.clone();
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "row_group_rows") {
            if let Some(v) = c.row_group_rows {
                args.row_group_rows = v;
            }
        }
        if !cli_explicit(matches, "batch_rows") {
            if let Some(v) = c.batch_rows {
                args.batch_rows = v;
            }
        }
        if !cli_explicit(matches, "compression") {
            if let Some(v) = &c.compression {
                args.compression = v.clone();
            }
        }
        if !cli_explicit(matches, "sample_size") {
            if let Some(v) = c.sample_size {
                args.sample_size = v;
            }
        }
        if !cli_explicit(matches, "seed") {
            if let Some(v) = c.seed {
                args.seed = v;
            }
        }
        if !cli_explicit(matches, "refresh_cache") {
            if let Some(v) = c.refresh_cache {
                args.refresh_cache = v;
            }
        }
        if !cli_explicit(matches, "skip_disk_check") {
            if let Some(v) = c.skip_disk_check {
                args.skip_disk_check = v;
            }
        }
        if !cli_explicit(matches, "split_size") {
            if let Some(v) = &c.split_size {
                args.split_size = v.clone();
            }
        }
        if !cli_explicit(matches, "split_temp_dir") {
            if let Some(v) = &c.split_temp_dir {
                args.split_temp_dir = Some(v.clone());
            }
        }
    }
}

fn apply_verify_config(
    args: &mut VerifyArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        apply_shared_defaults(&mut args.shared, d, matches);
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.verify_convert {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "scope") {
            if let Some(v) = &c.scope {
                args.scope = v.clone();
            }
        }
        if !cli_explicit(matches, "metadata_level") {
            if let Some(v) = &c.metadata_level {
                args.metadata_level = v.clone();
            }
        }
        if !cli_explicit(matches, "file_sample_n") {
            if let Some(v) = c.file_sample_n {
                args.file_sample_n = v;
            }
        }
        if !cli_explicit(matches, "seed") {
            if let Some(v) = c.seed {
                args.seed = v;
            }
        }
    }
}

fn apply_schema_config(
    args: &mut SchemaArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        apply_shared_defaults(&mut args.shared, d, matches);
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.schema {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "from") {
            if let Some(v) = &c.from {
                args.from = v.clone();
            }
        }
        if !cli_explicit(matches, "format") {
            if let Some(v) = &c.format {
                args.format = v.clone();
            }
        }
        if !cli_explicit(matches, "diff_with") {
            args.diff_with = c.diff_with.clone();
        }
        if !cli_explicit(matches, "output") {
            args.output = c.output.clone();
        }
        if !cli_explicit(matches, "sample_size") {
            if let Some(v) = c.sample_size {
                args.sample_size = v;
            }
        }
        if !cli_explicit(matches, "refresh_cache") {
            if let Some(v) = c.refresh_cache {
                args.refresh_cache = v;
            }
        }
    }
}

fn apply_index_config(args: &mut IndexArgs, cfg: Option<&AppConfig>, matches: Option<&ArgMatches>) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &d.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &d.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = d.workers {
                args.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &d.duckdb_bin {
                args.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.index {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "index_file") {
            args.index_file = c.index_file.clone();
        }
        if !cli_explicit(matches, "overwrite") {
            if let Some(v) = c.overwrite {
                args.overwrite = v;
            }
        }
    }
}

fn apply_extract_config(
    args: &mut ExtractArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        apply_shared_defaults(&mut args.shared, d, matches);
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.extract {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "ids") {
            if let Some(v) = &c.ids {
                args.ids = v.clone();
            }
        }
        if !cli_explicit(matches, "output") {
            if let Some(v) = &c.output {
                args.output = v.clone();
            }
        }
    }
}

fn apply_repair_config(
    args: &mut RepairArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        apply_shared_defaults(&mut args.shared, d, matches);
        if !cli_explicit(matches, "profile") {
            if let Some(v) = &d.profile {
                args.profile = v.clone();
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.repair_convert {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "profile") {
            if let Some(v) = &c.profile {
                args.profile = v.clone();
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "from_verify_report") {
            if let Some(v) = &c.from_verify_report {
                args.from_verify_report = Some(v.clone());
            }
        }
    }
}

fn apply_download_config(
    args: &mut DownloadArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &d.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &d.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.download {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "s3_uri") {
            if let Some(v) = &c.s3_uri {
                args.s3_uri = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "aws_bin") {
            if let Some(v) = &c.aws_bin {
                args.aws_bin = v.clone();
            }
        }
        if !cli_explicit(matches, "endpoint_url") {
            args.endpoint_url = c.endpoint_url.clone();
        }
        if !cli_explicit(matches, "region") {
            args.region = c.region.clone();
        }
        if !cli_explicit(matches, "profile_name") {
            args.profile_name = c.profile_name.clone();
        }
        if !cli_explicit(matches, "no_sign_request") {
            if let Some(v) = c.no_sign_request {
                args.no_sign_request = v;
            }
        }
        if !cli_explicit(matches, "signed") {
            if let Some(v) = c.signed {
                args.signed = v;
            }
        }
        if !cli_explicit(matches, "delete_files") {
            if let Some(v) = c.delete_files {
                args.delete_files = v;
            }
        }
        if !cli_explicit(matches, "no_delete") {
            if let Some(v) = c.no_delete {
                args.no_delete = v;
            }
        }
        if !cli_explicit(matches, "skip_disk_check") {
            if let Some(v) = c.skip_disk_check {
                args.skip_disk_check = v;
            }
        }
    }
}

fn apply_validate_download_config(
    args: &mut ValidateDownloadArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &d.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &d.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = d.workers {
                args.workers = v;
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = d.state_flush_every {
                args.state_flush_every = v;
            }
        }
    }
    if let Some(c) = &cfg.verify_download {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "s3_uri") {
            if let Some(v) = &c.s3_uri {
                args.s3_uri = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.workers = v;
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "state_flush_every") {
            if let Some(v) = c.state_flush_every {
                args.state_flush_every = v;
            }
        }
        if !cli_explicit(matches, "aws_bin") {
            if let Some(v) = &c.aws_bin {
                args.aws_bin = v.clone();
            }
        }
        if !cli_explicit(matches, "endpoint_url") {
            args.endpoint_url = c.endpoint_url.clone();
        }
        if !cli_explicit(matches, "region") {
            args.region = c.region.clone();
        }
        if !cli_explicit(matches, "profile_name") {
            args.profile_name = c.profile_name.clone();
        }
        if !cli_explicit(matches, "no_sign_request") {
            if let Some(v) = c.no_sign_request {
                args.no_sign_request = v;
            }
        }
        if !cli_explicit(matches, "signed") {
            if let Some(v) = c.signed {
                args.signed = v;
            }
        }
        if !cli_explicit(matches, "check_extra") {
            if let Some(v) = c.check_extra {
                args.check_extra = v;
            }
        }
    }
}

fn apply_verify_index_config(
    args: &mut VerifyIndexArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(d) = &cfg.defaults {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &d.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &d.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = d.workers {
                args.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &d.duckdb_bin {
                args.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            args.max_memory_mb = d.max_memory_mb;
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = d.progress {
                args.progress = v;
            }
        }
    }
    if let Some(c) = &cfg.verify_index {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "progress") {
            if let Some(v) = c.progress {
                args.progress = v;
            }
        }
        if !cli_explicit(matches, "index_file") {
            args.index_file = c.index_file.clone();
        }
    }
}

fn apply_report_config(
    args: &mut ReportArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(c) = &cfg.report {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "source") {
            if let Some(v) = &c.source {
                args.source = v.clone();
            }
        }
        if !cli_explicit(matches, "command") {
            args.command = c.command.clone();
        }
        if !cli_explicit(matches, "latest") {
            if let Some(v) = c.latest {
                args.latest = v;
            }
        }
        if !cli_explicit(matches, "summary") {
            if let Some(v) = c.summary {
                args.summary = v;
            }
        }
        if !cli_explicit(matches, "full") {
            if let Some(v) = c.full {
                args.full = v;
            }
        }
    }
}

fn apply_prune_reports_config(
    args: &mut PruneReportsArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(c) = &cfg.prune_reports {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "source") {
            if let Some(v) = &c.source {
                args.source = v.clone();
            }
        }
        if !cli_explicit(matches, "command") {
            args.command = c.command.clone();
        }
        if !cli_explicit(matches, "keep_per_command") {
            if let Some(v) = c.keep_per_command {
                args.keep_per_command = v;
            }
        }
        if !cli_explicit(matches, "dry_run") {
            if let Some(v) = c.dry_run {
                args.dry_run = v;
            }
        }
    }
}

fn apply_progress_config(
    args: &mut ProgressArgs,
    cfg: Option<&AppConfig>,
    matches: Option<&ArgMatches>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    if let Some(c) = &cfg.progress {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "command") {
            args.command = c.command.clone();
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "interval_sec") {
            if let Some(v) = c.interval_sec {
                args.interval_sec = v;
            }
        }
        if !cli_explicit(matches, "watch") {
            if let Some(v) = c.watch {
                args.watch = v;
            }
        }
        if !cli_explicit(matches, "json") {
            if let Some(v) = c.json {
                args.json = v;
            }
        }
    }
}

fn apply_check_config(args: &mut CheckArgs, cfg: Option<&AppConfig>, matches: Option<&ArgMatches>) {
    if let Some(d) = cfg.and_then(|c| c.defaults.as_ref()) {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &d.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &d.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = d.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &d.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = d.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
    }
    if let Some(c) = cfg.and_then(|x| x.check.as_ref()) {
        if !cli_explicit(matches, "root_dir") {
            if let Some(v) = &c.root_dir {
                args.shared.root_dir = v.clone();
            }
        }
        if !cli_explicit(matches, "dataset") {
            if let Some(v) = &c.dataset {
                args.shared.dataset = v.clone();
            }
        }
        if !cli_explicit(matches, "workers") {
            if let Some(v) = c.workers {
                args.shared.workers = v;
            }
        }
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.shared.duckdb_bin = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "aws_bin") {
            if let Some(v) = &c.aws_bin {
                args.aws_bin = v.clone();
            }
        }
        if !cli_explicit(matches, "s3_uri") {
            if let Some(v) = &c.s3_uri {
                args.s3_uri = v.clone();
            }
        }
        if !cli_explicit(matches, "endpoint_url") {
            if let Some(v) = &c.endpoint_url {
                args.endpoint_url = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "region") {
            if let Some(v) = &c.region {
                args.region = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "profile_name") {
            if let Some(v) = &c.profile_name {
                args.profile_name = Some(v.clone());
            }
        }
        if !cli_explicit(matches, "no_sign_request") {
            if let Some(v) = c.no_sign_request {
                args.no_sign_request = v;
            }
        }
        if !cli_explicit(matches, "signed") {
            if let Some(v) = c.signed {
                args.signed = v;
            }
        }
        if !cli_explicit(matches, "max_memory_mb") {
            if let Some(v) = c.max_memory_mb {
                args.max_memory_mb = Some(v);
            }
        }
        if !cli_explicit(matches, "precise") {
            if let Some(v) = c.precise {
                args.precise = v;
            }
        }
        if !cli_explicit(matches, "strict") {
            if let Some(v) = c.strict {
                args.strict = v;
            }
        }
        if !cli_explicit(matches, "json") {
            if let Some(v) = c.json {
                args.json = v;
            }
        }
    }
}

fn config_template(mode: ConfigTemplateMode) -> String {
    match mode {
        ConfigTemplateMode::Complete => config_template_complete(),
        ConfigTemplateMode::Safe => config_template_safe(),
        ConfigTemplateMode::Fast => config_template_fast(),
    }
}

fn config_template_safe() -> String {
    r#"# openalex-snapshot.yaml (safe)
# Minimal low-memory preset.
# Only profile-relevant overrides are set here.
# Everything else falls back to built-in defaults (or CLI).

defaults:
  # Keep root explicit so path model remains obvious.
  root_dir: .

  # Safe profile: conservative memory and throughput.
  profile: safe
  workers: 1

  # Optional explicit cap for constrained systems.
  # max_memory_mb: 4096
"#
    .to_string()
}

fn config_template_fast() -> String {
    r#"# openalex-snapshot.yaml (fast)
# Minimal high-throughput preset.
# Only profile-relevant overrides are set here.
# Everything else falls back to built-in defaults (or CLI).

defaults:
  # Keep root explicit so path model remains obvious.
  root_dir: .

  # Fast profile: favors throughput, may increase resource usage.
  profile: fast
  workers: 8

  # Optional explicit cap for high-memory hosts.
  # max_memory_mb: 16384
"#
    .to_string()
}

fn config_template_complete() -> String {
    r#"# openalex-snapshot.yaml (complete)
# Full operational template for openalex-snapshot.
# This template is intentionally verbose and designed as living documentation.
# Read top-to-bottom once before first pipeline run.
#
# ---------------------------------------------------------------------------
# 0) Quick start (recommended)
# ---------------------------------------------------------------------------
# 1) Edit root_dir to your working location.
# 2) Keep profile/workers conservative until first successful full run.
# 3) Verify config:
#      openalex-snapshot config --verify --config ./openalex-snapshot.yaml
# 4) Run pipeline:
#      openalex-snapshot all --config ./openalex-snapshot.yaml --retry 2
#
# ---------------------------------------------------------------------------
# 1) How values are resolved
# ---------------------------------------------------------------------------
# Highest precedence wins:
#   1) explicit CLI flags
#   2) config file values (defaults + command section)
#   3) built-in defaults
#
# Example:
# - config sets workers: 4
# - CLI passes --workers 1
# -> effective workers = 1 for that run only.
#
# ---------------------------------------------------------------------------
# 2) Path model (root-centric)
# ---------------------------------------------------------------------------
# With root_dir="." the tool uses:
#   ./snapshot                         (download/source snapshot)
#   ./parquet                         (converted parquet outputs)
#   ./openalex-snapshot_metadata      (reports, logs, caches, manifests)
#
# Metadata is centralized under openalex-snapshot_metadata.
# Avoid editing metadata files manually unless debugging.
#
# ---------------------------------------------------------------------------
# 3) Pipeline order (all command)
# ---------------------------------------------------------------------------
#   1) download
#   2) verify_download
#   3) convert
#   4) verify_convert
#   5) repair_convert (only if verify_convert fails)
#   6) index
#   7) verify_index
#
# Stages can be disabled in `all:` for partial/local workflows.
#
# This is the default template mode for:
#   openalex-snapshot config --create

defaults:
  # ---------------------------------------------------------------------------
  # Global defaults
  # Applied to most commands unless overridden by command section and/or CLI.
  # ---------------------------------------------------------------------------

  # Global default root for root-based commands.
  # allowed values: any valid path
  root_dir: .

  # Default dataset scope — set once here, applies to all commands.
  # Use "all" or a single dataset name (works, authors, ...).
  # allowed values: all | <dataset-name>
  dataset: all

  # Shared runtime defaults — leave commented to use built-in auto mode.
  # allowed values: any valid executable path
  # duckdb_bin: /usr/local/bin/duckdb
  # Profile controls DuckDB memory budget and worker count.
  # safe (default) — single-pass, single worker, generous per-worker memory
  #   (45% of usable RAM, clamped 8-24 GiB).  Reliable on any host; uses DuckDB
  #   spill-to-disk for files larger than the memory budget.
  # stratified-36 — multi-pass; partitions files by gz size and runs one rayon
  #   pass per non-empty stratum (4-/3-/2-/1-workers on <400/400-600/600-800/800+ MB).
  #   Empirically tuned for ~36 GB RAM hosts.
  # Custom profiles for other RAM tiers go in a sibling `openalex-snapshot.profiles.yaml`
  # (auto-discovered) — see `docs/commands/convert.md` for the schema.
  # allowed values: safe | stratified-36 | <user-defined>
  # profile: safe
  # Workers: 0 (default) = auto-detect per profile. `--workers N` on a stratified
  # profile collapses it into a single flat pass.
  # Override only if you want to pin a specific value.
  # allowed values: integer >= 0 (0 = auto)
  # workers: 0
  # allowed values: integer >= 1
  # max_memory_mb: 8192
  # allowed values: true | false
  progress: true

  # Crash-resilience: flush state/report every N items.
  # allowed values: integer >= 1
  state_flush_every: 25

all:
  # ---------------------------------------------------------------------------
  # Full pipeline orchestrator (openalex-snapshot all --config ...)
  # ---------------------------------------------------------------------------

  # Max number of repair_convert attempts in verify/repair loop.
  # 0 means: run verify_convert once and fail immediately on errors.
  # allowed values: integer >= 0
  retry: 1

  # Stage toggles (default pipeline shown below).
  # Disable stages you do not want in `all` (e.g., skip download for local runs).
  # allowed values: true | false
  enable_download: true
  # allowed values: true | false
  enable_verify_download: true
  # allowed values: true | false
  enable_convert: true
  # allowed values: true | false
  enable_verify_convert: true
  # allowed values: true | false
  enable_repair_convert: true
  # allowed values: true | false
  enable_index: true
  # allowed values: true | false
  enable_verify_index: true

  # Skip disk space checks for all stages that support it (download, convert).
  # Use when disk check estimates are too conservative for partial dataset runs.
  # allowed values: true | false
  # skip_disk_check: false

download:
  # ---------------------------------------------------------------------------
  # Download snapshot from OpenAlex S3
  # Uses AWS CLI wrapper behavior; defaults follow OpenAlex guidance.
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # progress: true
  # state_flush_every: 25

  # Defaults mirror OpenAlex recommendation.
  # allowed values: any valid path
  root_dir: .
  # allowed values: any valid s3:// URI
  s3_uri: s3://openalex
  # allowed values: any valid executable path
  aws_bin: aws
  # allowed values: any valid URL
  # endpoint_url: https://s3.amazonaws.com
  # allowed values: any valid AWS region string
  # region: us-east-1
  # allowed values: any configured AWS profile name
  # profile_name: default
  # allowed values: true | false
  no_sign_request: true
  # allowed values: true | false
  signed: false
  # allowed values: true | false
  delete_files: true
  # allowed values: true | false
  no_delete: false

  # Skip free disk space preflight check for download.
  # allowed values: true | false
  # skip_disk_check: false

verify_download:
  # ---------------------------------------------------------------------------
  # Verify downloaded snapshot against remote manifest + gzip integrity
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # progress: true
  # state_flush_every: 25

  # allowed values: any valid path
  root_dir: .
  # allowed values: any valid s3:// URI
  # s3_uri: s3://openalex
  # allowed values: any valid executable path
  aws_bin: aws
  # allowed values: true | false
  no_sign_request: true
  # allowed values: true | false
  signed: false
  # allowed values: true | false
  check_extra: true

convert:
  # ---------------------------------------------------------------------------
  # Convert snapshot JSON.GZ files into parquet (core data build step)
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # profile: safe
  # max_memory_mb: 8192
  # progress: true
  # state_flush_every: 25

  # Parquet write behavior.
  # row_group_rows:
  #   Larger row groups can improve scan speed but increase write memory pressure.
  # batch_rows:
  #   Controls chunk size for conversion internals; smaller can reduce peak memory.
  # allowed values: integer >= 1
  row_group_rows: 100000
  # allowed values: integer >= 1
  batch_rows: 5000
  # allowed values: snappy | zstd | gzip | uncompressed
  compression: snappy

  # Schema inference behavior.
  # sample_size controls how many files are sampled during schema inference.
  # refresh_cache forces schema cache rebuild.
  # allowed values: integer >= 1
  sample_size: 100
  # allowed values: true | false
  refresh_cache: false

  # allowed values: integer >= 0
  seed: 42

  # Skip free disk space preflight check for conversion.
  # Useful when converting a single small dataset where the global estimate is too conservative.
  # allowed values: true | false
  # skip_disk_check: false

  # Pre-split large gz files before converting.
  # Files larger than split_size are decompressed and split into chunks of this size,
  # then each chunk is converted separately.
  # 0 (default) = disabled: in-process DuckDB handles large files via streaming
  # and memory-limit spill without needing pre-splitting.
  # Only set this if a specific file causes DuckDB to OOM even with memory limits.
  # Accepts human-readable sizes: 0 | 128mb | 256mb | 512mb | 1gb | 1gib etc.
  # allowed values: 0 (disabled) | <size with suffix>
  split_size: 0
  # Directory for temporary split gz chunks. Must be on the same filesystem as parquet_dir
  # to allow efficient renames. Defaults to <parquet_dir>/.split_tmp if not set.
  # allowed values: any valid path
  # split_temp_dir: /Volumes/openalex/.split_tmp

verify_convert:
  # ---------------------------------------------------------------------------
  # Verify converted parquet against snapshot source (integrity gate)
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # max_memory_mb: 8192
  # progress: true
  # state_flush_every: 25

  # Verification scope:
  # - file: sample of file pairs
  # - dataset: all files in dataset
  # - snapshot: all selected datasets
  # allowed values: file | dataset | snapshot
  scope: dataset
  # allowed values: row-count | id-hash | both
  metadata_level: both
  # allowed values: integer >= 1
  file_sample_n: 50
  # allowed values: integer >= 0
  seed: 42

repair_convert:
  # ---------------------------------------------------------------------------
  # Repair failed conversion outputs based on verify_convert report
  # Typical use: rerun only broken files after a failed verify_convert.
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # profile: safe
  # max_memory_mb: 8192
  # progress: true
  # state_flush_every: 25

  # Repair is driven by an existing verify_convert report.
  # No corpus_dir here by design (root_dir + dataset model).
  # allowed values: any valid report path
  # from_verify_report: ./openalex-snapshot_metadata/reports/verify_convert-123456.json

index:
  # ---------------------------------------------------------------------------
  # Build *_id_idx.parquet lookup index for parquet corpus (ID lookups)
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # max_memory_mb: 8192
  # progress: true
  # state_flush_every: 25

  # allowed values: any valid path
  root_dir: .

  # Optional index output file.
  # allowed values: any valid path
  # index_file: ./parquet/all_id_idx.parquet

  # allowed values: true | false
  overwrite: false

verify_index:
  # ---------------------------------------------------------------------------
  # Verify index integrity and corpus coverage
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # max_memory_mb: 8192
  # progress: true

  # allowed values: any valid path
  root_dir: .
  # allowed values: any valid path
  # index_file: ./parquet/all_id_idx.parquet

schema:
  # ---------------------------------------------------------------------------
  # Schema inspection and cache management (source/cache/parquet)
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # max_memory_mb: 8192
  # state_flush_every: 25

  # Schema source preference: auto|source|cache|parquet
  # allowed values: auto | source | cache | parquet
  from: auto

  # Output format: table|json|yaml|arrow-r
  # allowed values: table | json | yaml | arrow-r
  format: table

  # Optional comparison source.
  # allowed values: source | cache | parquet
  # diff_with: parquet

  # Optional output file (stdout if omitted).
  # allowed values: any valid path
  # output: ./schema.json

  # allowed values: integer >= 1
  sample_size: 100
  # allowed values: true | false
  refresh_cache: false

extract:
  # ---------------------------------------------------------------------------
  # Extract rows by OpenAlex IDs from CSV using per-dataset indexes
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
  # duckdb_bin: /usr/local/bin/duckdb
  # max_memory_mb: 8192
  # progress: true
  # state_flush_every: 25

  # Input CSV with IDs (column auto-detected: id/openalex_id/work_id/first column).
  # allowed values: any valid path
  # ids: ./ids.csv

  # Output base path. Command writes one file per dataset:
  # <base>_<dataset>.parquet
  # allowed values: any valid path
  # output: ./extract.parquet

report:
  # ---------------------------------------------------------------------------
  # Read and display stored reports
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .

  # allowed values: any valid path
  root_dir: .
  # allowed values: all | parquet-global | download
  source: all
  # allowed values: any command name
  # command: verify_convert
  # allowed values: true | false
  latest: false
  # allowed values: true | false
  full: false
  # Show only aggregate totals; suppress per-dataset breakdown (default: false = show per-dataset)
  # allowed values: true | false
  # summary: false

prune_reports:
  # ---------------------------------------------------------------------------
  # Prune old reports while keeping the newest per command
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .

  # allowed values: any valid path
  root_dir: .
  # allowed values: all | parquet-global | download
  source: all
  # allowed values: any command name
  # command: verify_convert
  # allowed values: integer >= 1
  keep_per_command: 1
  # allowed values: true | false
  dry_run: false

progress:
  # ---------------------------------------------------------------------------
  # Monitor active/recent runs from reports and logs
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all

  # allowed values: any valid path
  root_dir: .
  # allowed values: any command name
  # command: convert
  # allowed values: integer >= 1
  interval_sec: 2
  # allowed values: true | false
  watch: true
  # allowed values: true | false
  json: false

check:
  # ---------------------------------------------------------------------------
  # Preflight dependency/path/disk/memory checks before running pipeline
  # Recommended before first full run on a new machine or mount.
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all

  # allowed values: any valid path
  root_dir: .
  # allowed values: true | false
  precise: true
  # allowed values: true | false
  strict: false
  # allowed values: true | false
  json: false

# ---------------------------------------------------------------------------
# 4) Example workflow
# ---------------------------------------------------------------------------
#   openalex-snapshot config --verify --config ./openalex-snapshot.yaml
#   openalex-snapshot all --config ./openalex-snapshot.yaml --retry 2
"#
    .to_string()
}

fn run_config(args: ConfigArgs) -> Result<()> {
    let modes = (args.create.is_some() as u8) + (args.verify as u8);
    if modes != 1 {
        bail!("config requires exactly one mode: use --create <complete|safe|fast> or --verify");
    }
    if args.explain {
        let create_mode = args
            .create
            .as_ref()
            .map(|m| match m {
                ConfigTemplateMode::Complete => "complete",
                ConfigTemplateMode::Safe => "safe",
                ConfigTemplateMode::Fast => "fast",
            })
            .unwrap_or("-");
        println!(
            "--explain: config mode={} create_template={} path={} stdout={} overwrite={}",
            if args.create.is_some() {
                "create"
            } else {
                "verify"
            },
            create_mode,
            args.config.display(),
            args.stdout,
            args.overwrite
        );
        return Ok(());
    }
    if let Some(mode) = args.create {
        let tpl = config_template(mode);
        if args.stdout {
            print!("{tpl}");
            return Ok(());
        }
        if args.config.exists() && !args.overwrite {
            bail!(
                "config file already exists: {} (use --overwrite)",
                args.config.display()
            );
        }
        if let Some(parent) = args.config.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&args.config, tpl)
            .with_context(|| format!("failed to write {}", args.config.display()))?;
        println!("[config] created {}", args.config.display());
        return Ok(());
    }

    let txt = fs::read_to_string(&args.config)
        .with_context(|| format!("failed to read {}", args.config.display()))?;
    let raw: serde_yaml::Value = serde_yaml::from_str(&txt)
        .with_context(|| format!("invalid YAML {}", args.config.display()))?;
    let _parsed: AppConfig = serde_yaml::from_str(&txt)
        .with_context(|| format!("schema validation failed {}", args.config.display()))?;

    let sections = raw
        .as_mapping()
        .map(|m| {
            let mut v = m
                .keys()
                .filter_map(|k| k.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>();
            v.sort();
            v
        })
        .unwrap_or_default();
    println!(
        "[config] path={} sections=[{}] status=ok",
        args.config.display(),
        sections.join(", ")
    );
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct CheckFinding {
    name: String,
    status: String,
    details: String,
    recommendation: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CheckJsonOutput {
    status: String,
    strict: bool,
    warnings: usize,
    failures: usize,
    findings: Vec<CheckFinding>,
    report_paths: Vec<String>,
}

fn run_skills(args: SkillsArgs) -> Result<()> {
    let skills_dir = args.root_dir.join("skills");
    let files = skills_templates(&args.root_dir);
    if args.explain {
        println!(
            "--explain: skills root_dir={} overwrite={} stdout={} files={}",
            args.root_dir.display(),
            args.overwrite,
            args.stdout,
            files.len()
        );
        for (p, _) in &files {
            println!("would_write: {}", p.display());
        }
        return Ok(());
    }
    if args.stdout {
        for (p, content) in &files {
            println!("### {}", p.display());
            println!("{content}");
            println!();
        }
        return Ok(());
    }
    fs::create_dir_all(&skills_dir)?;
    let mut created = 0usize;
    let mut skipped = 0usize;
    let mut overwritten = 0usize;
    for (path, content) in files {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if path.exists() {
            if args.overwrite {
                fs::write(&path, content)?;
                overwritten += 1;
            } else {
                skipped += 1;
            }
        } else {
            fs::write(&path, content)?;
            created += 1;
        }
    }
    eprintln!(
        "[skills] root={} created={} overwritten={} skipped={}",
        skills_dir.display(),
        created,
        overwritten,
        skipped
    );
    Ok(())
}

fn run_check(args: CheckArgs) -> Result<()> {
    if args.explain {
        println!("--explain: check");
        println!("root_dir: {}", args.shared.root_dir.display());
        println!("snapshot_dir: {}", args.shared.snapshot_dir.display());
        println!("parquet_dir: {}", args.shared.parquet_dir.display());
        println!("dataset: {}", args.shared.dataset);
        println!("duckdb_bin: {}", duckdb_bin(&args.shared).display());
        println!("aws_bin: {}", args.aws_bin.display());
        println!("s3_uri: {}", args.s3_uri);
        println!("strict: {}", args.strict);
        println!("precise: {}", args.precise);
        return Ok(());
    }
    let _ = cleanup_command_reports(&args.shared.parquet_dir, "check");

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        args.shared.root_dir.to_string_lossy().to_string(),
    );
    report_args.insert("dataset".to_string(), args.shared.dataset.clone());
    report_args.insert("strict".to_string(), args.strict.to_string());
    report_args.insert("precise".to_string(), args.precise.to_string());
    report_args.insert("workers".to_string(), args.shared.workers.to_string());
    report_args.insert(
        "memory_mb".to_string(),
        format!("{:?}", args.max_memory_mb.clone()),
    );
    let mut report = report_new("check", report_args);

    let mut findings: Vec<CheckFinding> = Vec::new();
    let mut warns = 0usize;
    let mut fails = 0usize;

    // DuckDB is statically linked via the bundled crate — no external binary required.
    findings.push(CheckFinding {
        name: "duckdb".to_string(),
        status: "ok".to_string(),
        details: "bundled (statically linked — no external binary required)".to_string(),
        recommendation: None,
    });
    match ensure_aws_cli(&args.aws_bin) {
        Ok(()) => findings.push(CheckFinding {
            name: "aws".to_string(),
            status: "ok".to_string(),
            details: format!("available at {}", args.aws_bin.display()),
            recommendation: None,
        }),
        Err(e) => {
            fails += 1;
            findings.push(CheckFinding {
                name: "aws".to_string(),
                status: "fail".to_string(),
                details: format!("{e:#}"),
                recommendation: Some("install aws cli or use --aws-bin".to_string()),
            });
            report.failures.push(FailureEntry {
                dataset: args.shared.dataset.clone(),
                phase: "check_dependency".to_string(),
                rel_path: None,
                source_path: None,
                output_path: Some(args.aws_bin.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("install aws cli or use --aws-bin".to_string()),
            });
        }
    }

    let meta_dir = args.shared.root_dir.join("openalex-snapshot_metadata");
    for (name, p) in [
        ("root_dir", args.shared.root_dir.clone()),
        ("snapshot_dir", args.shared.snapshot_dir.clone()),
        ("parquet_dir", args.shared.parquet_dir.clone()),
        ("metadata_dir", meta_dir.clone()),
    ] {
        match check_path_writable(&p) {
            Ok(()) => findings.push(CheckFinding {
                name: name.to_string(),
                status: "ok".to_string(),
                details: format!("writable {}", p.display()),
                recommendation: None,
            }),
            Err(e) => {
                fails += 1;
                findings.push(CheckFinding {
                    name: name.to_string(),
                    status: "fail".to_string(),
                    details: format!("{e:#}"),
                    recommendation: Some(
                        "fix path permissions or choose another --root-dir".to_string(),
                    ),
                });
                report.failures.push(FailureEntry {
                    dataset: args.shared.dataset.clone(),
                    phase: "check_path".to_string(),
                    rel_path: None,
                    source_path: None,
                    output_path: Some(p.to_string_lossy().to_string()),
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some(
                        "fix path permissions or choose another --root-dir".to_string(),
                    ),
                });
            }
        }
    }

    if let Ok(remote) = fetch_remote_manifest(
        &ValidateDownloadArgs {
            root_dir: args.shared.root_dir.clone(),
            snapshot_dir: args.shared.snapshot_dir.clone(),
            s3_uri: args.s3_uri.clone(),
            dataset: args.shared.dataset.clone(),
            aws_bin: args.aws_bin.clone(),
            endpoint_url: args.endpoint_url.clone(),
            region: args.region.clone(),
            profile_name: args.profile_name.clone(),
            no_sign_request: args.no_sign_request,
            signed: args.signed,
            check_extra: true,
            workers: args.shared.workers,
            progress: false,
            explain: false,
            state_flush_every: 25,
        },
        false,
    ) {
        let remote_total_bytes: u64 = remote.iter().map(|o| o.size).sum();
        let required_bytes = remote_total_bytes.saturating_mul(11).saturating_div(10);
        match available_disk_bytes(&args.shared.snapshot_dir) {
            Ok(free) if free >= required_bytes => findings.push(CheckFinding {
                name: "download_disk".to_string(),
                status: "ok".to_string(),
                details: format!(
                    "available={} GiB required={} GiB (remote={} GiB +10%)",
                    bytes_to_gib(free),
                    bytes_to_gib(required_bytes),
                    bytes_to_gib(remote_total_bytes)
                ),
                recommendation: None,
            }),
            Ok(free) => {
                fails += 1;
                let msg = format!(
                    "available={} GiB required={} GiB (remote={} GiB +10%)",
                    bytes_to_gib(free),
                    bytes_to_gib(required_bytes),
                    bytes_to_gib(remote_total_bytes)
                );
                findings.push(CheckFinding {
                    name: "download_disk".to_string(),
                    status: "fail".to_string(),
                    details: msg.clone(),
                    recommendation: Some(
                        "Free up disk space, or set skip_disk_check: true under download: in your config, or pass --skip-disk-check to the download/all command".to_string(),
                    ),
                });
                report.failures.push(FailureEntry {
                    dataset: args.shared.dataset.clone(),
                    phase: "check_download_disk".to_string(),
                    rel_path: None,
                    source_path: Some(args.s3_uri.clone()),
                    output_path: Some(args.shared.snapshot_dir.to_string_lossy().to_string()),
                    error_message: msg,
                    suggested_recovery: Some(
                        "Free up disk space, or set skip_disk_check: true under download: in your config, or pass --skip-disk-check to the download/all command".to_string(),
                    ),
                });
            }
            Err(e) => {
                warns += 1;
                findings.push(CheckFinding {
                    name: "download_disk".to_string(),
                    status: "warn".to_string(),
                    details: format!("cannot evaluate disk: {e:#}"),
                    recommendation: Some("check disk manually before download".to_string()),
                });
            }
        }
    } else {
        warns += 1;
        findings.push(CheckFinding {
            name: "download_manifest".to_string(),
            status: "warn".to_string(),
            details: "remote manifest unavailable for estimate".to_string(),
            recommendation: Some("check aws connectivity/settings and rerun check".to_string()),
        });
    }

    let convert_estimate = if args.precise {
        estimate_convert_input_bytes_precise(&args.shared.snapshot_dir, &args.shared.dataset)?
    } else {
        0
    };
    let convert_required = if args.precise {
        convert_estimate.saturating_mul(12).saturating_div(10)
    } else {
        convert_min_free_bytes(&args.shared.parquet_dir)
    };
    match available_disk_bytes(&args.shared.parquet_dir) {
        Ok(free) if free >= convert_required => findings.push(CheckFinding {
            name: "convert_disk".to_string(),
            status: "ok".to_string(),
            details: format!(
                "available={} GiB required={} GiB{}",
                bytes_to_gib(free),
                bytes_to_gib(convert_required),
                if args.precise {
                    format!(" (source={} GiB +20%)", bytes_to_gib(convert_estimate))
                } else {
                    "".to_string()
                }
            ),
            recommendation: None,
        }),
        Ok(free) => {
            fails += 1;
            let msg = format!(
                "available={} GiB required={} GiB{}",
                bytes_to_gib(free),
                bytes_to_gib(convert_required),
                if args.precise {
                    format!(" (source={} GiB +20%)", bytes_to_gib(convert_estimate))
                } else {
                    "".to_string()
                }
            );
            findings.push(CheckFinding {
                name: "convert_disk".to_string(),
                status: "fail".to_string(),
                details: msg.clone(),
                recommendation: Some(
                    "Free up disk space, or set skip_disk_check: true under convert: in your config, or pass --skip-disk-check to the convert/all command".to_string(),
                ),
            });
            report.failures.push(FailureEntry {
                dataset: args.shared.dataset.clone(),
                phase: "check_convert_disk".to_string(),
                rel_path: None,
                source_path: Some(args.shared.snapshot_dir.to_string_lossy().to_string()),
                output_path: Some(args.shared.parquet_dir.to_string_lossy().to_string()),
                error_message: msg,
                suggested_recovery: Some(
                    "Free up disk space, or set skip_disk_check: true under convert: in your config, or pass --skip-disk-check to the convert/all command".to_string(),
                ),
            });
        }
        Err(e) => {
            warns += 1;
            findings.push(CheckFinding {
                name: "convert_disk".to_string(),
                status: "warn".to_string(),
                details: format!("cannot evaluate disk: {e:#}"),
                recommendation: Some("check disk manually before convert".to_string()),
            });
        }
    }

    let tuning = light_tuning_with_override(args.shared.workers, args.max_memory_mb);
    let total = detect_total_memory_mb();
    if let Some(total_mb) = total {
        let mem = tuning.memory_mb.unwrap_or(0);
        let ratio = if total_mb > 0 {
            mem as f64 / total_mb as f64
        } else {
            0.0
        };
        if ratio > 0.7 {
            warns += 1;
            findings.push(CheckFinding {
                name: "memory_tuning".to_string(),
                status: "warn".to_string(),
                details: format!(
                    "configured memory={} MB on system={} MB (high ratio)",
                    mem, total_mb
                ),
                recommendation: Some(
                    "reduce --workers or --max-memory-mb, or use --profile safe".to_string(),
                ),
            });
        } else {
            findings.push(CheckFinding {
                name: "memory_tuning".to_string(),
                status: "ok".to_string(),
                details: format!("configured memory={} MB on system={} MB", mem, total_mb),
                recommendation: None,
            });
        }
    } else {
        warns += 1;
        findings.push(CheckFinding {
            name: "memory_tuning".to_string(),
            status: "warn".to_string(),
            details: "system memory could not be detected".to_string(),
            recommendation: Some("set --max-memory-mb explicitly".to_string()),
        });
    }

    report.datasets.push(DatasetReportSummary {
        dataset: args.shared.dataset.clone(),
        items_scanned: findings.len() as u64,
        succeeded: findings.iter().filter(|f| f.status == "ok").count() as u64,
        failed: fails as u64,
        skipped: 0,
    });
    report.totals_items_scanned = findings.len() as u64;
    report.totals_succeeded = findings.iter().filter(|f| f.status == "ok").count() as u64;
    report.totals_failed = fails as u64;
    report.totals_skipped = 0;
    report
        .args
        .insert("warnings".to_string(), warns.to_string());
    report_finalize(&mut report);
    let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;

    if args.json {
        let payload = CheckJsonOutput {
            status: if fails > 0 || (args.strict && warns > 0) {
                "failed".to_string()
            } else if warns > 0 {
                "warn".to_string()
            } else {
                "ok".to_string()
            },
            strict: args.strict,
            warnings: warns,
            failures: fails,
            findings,
            report_paths: report_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect(),
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "{:<20} {:<8} {:<70} recommendation",
            "check", "status", "details"
        );
        println!("{}", "-".repeat(140));
        for f in &findings {
            println!(
                "{:<20} {:<8} {:<70} {}",
                f.name,
                f.status,
                f.details,
                f.recommendation.clone().unwrap_or_default()
            );
        }
        eprintln!(
            "[check] summary ok={} warn={} failed={} reports={}",
            findings.iter().filter(|f| f.status == "ok").count(),
            warns,
            fails,
            report_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    if fails > 0 {
        bail!("[check] failures detected: {fails}");
    }
    if args.strict && warns > 0 {
        bail!("[check] strict mode failed due to warnings: {warns}");
    }
    Ok(())
}

fn run_report(args: ReportArgs) -> Result<()> {
    let mut records = load_report_records(&args.snapshot_dir, &args.parquet_dir, &args.source)?;
    if let Some(cmd_filter) = &args.command {
        records.retain(|r| r.report.command == *cmd_filter);
    }
    if args.latest {
        let mut newest: BTreeMap<String, ReportRecord> = BTreeMap::new();
        for rec in records {
            let cmd = rec.report.command.clone();
            match newest.get(&cmd) {
                Some(existing) => {
                    if rec.timestamp > existing.timestamp {
                        newest.insert(cmd, rec);
                    }
                }
                None => {
                    newest.insert(cmd, rec);
                }
            }
        }
        records = newest.into_values().collect();
    }
    records.sort_by(|a, b| {
        command_flow_rank(&a.report.command)
            .cmp(&command_flow_rank(&b.report.command))
            .then_with(|| b.timestamp.cmp(&a.timestamp))
            .then_with(|| a.path.cmp(&b.path))
    });

    if records.is_empty() {
        println!("[report] no matching reports found");
        return Ok(());
    }

    if args.summary {
        // Aggregate-only view (one line per report file).
        println!(
            "{:<15} {:<18} {:<8} {:>8} {:>10} {:>8} {:<19} {:>8}  path",
            "source",
            "command",
            "status",
            "failed",
            "succeeded",
            "skipped",
            "started_local",
            "runtime"
        );
        println!("{}", "-".repeat(140));
        for rec in &records {
            let status = if rec.report.totals_failed == 0 {
                "ok"
            } else {
                "FAILED"
            };
            let started_local = Local
                .timestamp_opt(rec.report.started_at_unix, 0)
                .single()
                .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| rec.report.started_at_unix.to_string());
            let runtime = rec
                .report
                .duration_seconds
                .map(format_duration)
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<15} {:<18} {:<8} {:>8} {:>10} {:>8} {:<19} {:>8}  {}",
                rec.source_kind,
                rec.report.command,
                status,
                rec.report.totals_failed,
                rec.report.totals_succeeded,
                rec.report.totals_skipped,
                started_local,
                runtime,
                rec.path.display(),
            );
            if args.full {
                println!("{}", serde_json::to_string_pretty(&rec.report)?);
            }
        }
    } else {
        // Default: per-dataset breakdown grouped under each report header.
        for rec in &records {
            let status = if rec.report.totals_failed == 0 {
                "ok"
            } else {
                "FAILED"
            };
            let started_local = Local
                .timestamp_opt(rec.report.started_at_unix, 0)
                .single()
                .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| rec.report.started_at_unix.to_string());
            let runtime = rec
                .report
                .duration_seconds
                .map(format_duration)
                .unwrap_or_else(|| "-".to_string());
            println!(
                "=== {} [{}  {}  {}]  {}",
                rec.report.command,
                started_local,
                runtime,
                status,
                rec.path.file_name().unwrap_or_default().to_string_lossy()
            );
            if !rec.report.datasets.is_empty() {
                println!(
                    "  {:<22} {:>8} {:>8} {:>8} {:>8}",
                    "dataset", "scanned", "ok", "failed", "skipped"
                );
                println!("  {}", "-".repeat(54));
                for ds in &rec.report.datasets {
                    let ds_status = if ds.failed > 0 { "  !" } else { "" };
                    println!(
                        "  {:<22} {:>8} {:>8} {:>8} {:>8}{}",
                        ds.dataset,
                        ds.items_scanned,
                        ds.succeeded,
                        ds.failed,
                        ds.skipped,
                        ds_status
                    );
                }
            } else if !rec.report.step_runs.is_empty() {
                // all-command style: show step results instead of datasets
                for step in &rec.report.step_runs {
                    let st = if step.status == "ok" {
                        "ok"
                    } else {
                        &step.status
                    };
                    println!("  {:<22} {}", step.step, st);
                }
            } else {
                println!(
                    "  totals: scanned={} ok={} failed={} skipped={}",
                    rec.report.totals_items_scanned,
                    rec.report.totals_succeeded,
                    rec.report.totals_failed,
                    rec.report.totals_skipped,
                );
            }
            if args.full {
                println!("{}", serde_json::to_string_pretty(&rec.report)?);
            }
            println!();
        }
    }
    println!("[report] listed {} report file(s)", records.len());
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct LiveDatasetStatus {
    dataset: String,
    status: String, // "done" | "in-progress" | "pending" | "error"
    scanned: u64,
    todo: u64,
    converted: u64,
    ok: u64,
    failed: u64,
    skipped: u64,
    last_log: String,
}

fn parse_u64_field(line: &str, field: &str) -> Option<u64> {
    let key = format!("{}=", field);
    let pos = line.find(key.as_str())?;
    let rest = &line[pos + key.len()..];
    rest.split_whitespace().next()?.parse().ok()
}

fn live_dataset_status(log_path: &Path) -> LiveDatasetStatus {
    let dataset = log_path
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();

    let Ok(txt) = fs::read_to_string(log_path) else {
        return LiveDatasetStatus {
            dataset,
            status: "pending".to_string(),
            scanned: 0,
            todo: 0,
            converted: 0,
            ok: 0,
            failed: 0,
            skipped: 0,
            last_log: String::new(),
        };
    };

    let last_log = txt.lines().last().unwrap_or("").to_string();
    let mut scanned = 0u64;
    let mut todo = 0u64;
    let mut converted = 0u64;
    let mut ok = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut done = false;
    let mut errored = false;

    for line in txt.lines() {
        if let Some(v) = parse_u64_field(line, "source_files") {
            scanned = v;
        }
        if let Some(v) = parse_u64_field(line, "todo_files") {
            todo = v;
        }
        // count per-file completion lines emitted by convert/index/verify
        if line.contains(" converted ") || line.contains(" indexed ") || line.contains(" verified ")
        {
            converted += 1;
        }
        // summary line written at end of a stage
        if line.contains(" summary ") {
            if let Some(v) = parse_u64_field(line, "ok") {
                ok = v;
            }
            if let Some(v) = parse_u64_field(line, "failed") {
                failed = v;
            }
            if let Some(v) = parse_u64_field(line, "scanned") {
                scanned = v;
            }
            if failed > 0 {
                errored = true;
            }
            done = true;
        }
        if line.contains("all files already converted")
            || line.contains("all files already indexed")
            || line.contains("all files already verified")
        {
            done = true;
            skipped = scanned;
        }
    }

    let status = if errored {
        "error"
    } else if done {
        "done"
    } else if scanned > 0 || converted > 0 {
        "in-progress"
    } else {
        "pending"
    }
    .to_string();

    LiveDatasetStatus {
        dataset,
        status,
        scanned,
        todo,
        converted,
        ok,
        failed,
        skipped,
        last_log,
    }
}

fn live_progress(parquet_dir: &Path, lock: &LockInfo) -> Vec<LiveDatasetStatus> {
    let meta_root = metadata_root(parquet_dir);
    let Ok(entries) = fs::read_dir(&meta_root) else {
        return Vec::new();
    };
    let mut statuses: Vec<LiveDatasetStatus> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .filter(|e| {
            let name = e.file_name();
            let n = name.to_string_lossy();
            !n.starts_with('.') && n != "reports" && n != "archived" && n != "download"
        })
        .map(|e| {
            let log_dir = dataset_log_dir_for_command(
                parquet_dir,
                &e.file_name().to_string_lossy(),
                &lock.command,
            );
            let log_path = log_dir.join(format!("{}.log", &lock.command));
            if log_path.exists() {
                live_dataset_status(&log_path)
            } else {
                LiveDatasetStatus {
                    dataset: e.file_name().to_string_lossy().into_owned(),
                    status: "pending".to_string(),
                    scanned: 0,
                    todo: 0,
                    converted: 0,
                    ok: 0,
                    failed: 0,
                    skipped: 0,
                    last_log: String::new(),
                }
            }
        })
        .collect();
    statuses.sort_by(|a, b| a.dataset.cmp(&b.dataset));
    statuses
}

fn print_live_progress(lock: &LockInfo, datasets: &[LiveDatasetStatus]) {
    let runtime = (now_unix() - lock.started_at_unix).max(0) as f64;
    let done = datasets.iter().filter(|d| d.status == "done").count();
    let in_progress = datasets
        .iter()
        .filter(|d| d.status == "in-progress")
        .count();
    let errors = datasets.iter().filter(|d| d.status == "error").count();
    let pending = datasets.iter().filter(|d| d.status == "pending").count();

    // aggregate cross-dataset totals for a pipeline-level ETA
    let total_converted: u64 = datasets.iter().map(|d| d.converted).sum();
    let total_todo: u64 = datasets.iter().map(|d| d.todo).sum();
    let eta_str = if total_todo > 0 && total_converted > 0 && runtime > 0.0 {
        let rate = total_converted as f64 / runtime; // files/sec
        let remaining = total_todo.saturating_sub(total_converted) as f64;
        let eta_secs = remaining / rate;
        format!(" eta={}", format_duration(eta_secs))
    } else {
        String::new()
    };
    let progress_str = if total_todo > 0 {
        format!(" ({} of {})", total_converted, total_todo)
    } else {
        String::new()
    };

    println!(
        "[progress] command={} pid={} started={} runtime={}{}{}",
        lock.command,
        lock.pid,
        lock.started_at_unix,
        format_duration(runtime),
        progress_str,
        eta_str,
    );
    println!(
        "[progress] datasets: done={} in-progress={} pending={} error={}",
        done, in_progress, pending, errors
    );
    for ds in datasets {
        if ds.status == "pending" && ds.last_log.is_empty() {
            continue; // omit datasets not yet started
        }
        let ds_progress = if ds.todo > 0 {
            format!(" ({} of {})", ds.converted, ds.todo)
        } else if ds.converted > 0 {
            format!(" converted={}", ds.converted)
        } else {
            String::new()
        };
        let ds_eta = if ds.todo > 0 && ds.converted > 0 && runtime > 0.0 {
            let rate = ds.converted as f64 / runtime;
            let remaining = ds.todo.saturating_sub(ds.converted) as f64;
            format!(" eta={}", format_duration(remaining / rate))
        } else {
            String::new()
        };
        print!(
            "[progress] dataset={} status={}{}{}",
            ds.dataset, ds.status, ds_progress, ds_eta
        );
        if ds.ok > 0 || ds.failed > 0 || ds.skipped > 0 {
            print!(" ok={} failed={} skipped={}", ds.ok, ds.failed, ds.skipped);
        }
        println!();
        if !ds.last_log.is_empty() && ds.status != "done" {
            // strip timestamp prefix for readability
            let msg = ds.last_log.splitn(3, ' ').nth(2).unwrap_or(&ds.last_log);
            println!("          {}", msg);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ProgressView {
    command: String,
    report_path: String,
    started_at_unix: i64,
    finished_at_unix: Option<i64>,
    runtime_seconds: f64,
    totals_items_scanned: u64,
    totals_succeeded: u64,
    totals_failed: u64,
    totals_skipped: u64,
    datasets: Vec<DatasetReportSummary>,
    last_logs: Vec<String>,
}

fn canonical_command_name(s: &str) -> String {
    match s {
        "verify_convert" | "verify-convert" | "verify" => "verify".to_string(),
        "verify_download" | "verify-download" | "validate-download" => {
            "verify_download".to_string()
        }
        "verify_schema" | "verify-schema" => "verify_schema".to_string(),
        "verify_index" | "verify-index" => "verify-index".to_string(),
        other => other.to_string(),
    }
}

fn read_last_log_lines(path: &Path, n: usize) -> Vec<String> {
    let Ok(txt) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut lines: Vec<String> = txt.lines().map(|s| s.to_string()).collect();
    if lines.len() > n {
        lines = lines.split_off(lines.len() - n);
    }
    lines
}

fn log_paths_for_report(
    snapshot_dir: &Path,
    parquet_dir: &Path,
    report: &RunReport,
) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if report.command == "download" || report.command == "verify_download" {
        out.push(download_log_path(snapshot_dir));
        return out;
    }
    for ds in &report.datasets {
        let dir = dataset_log_dir_for_command(parquet_dir, &ds.dataset, &report.command);
        out.push(dir.join(format!("{}.log", report.command)));
    }
    out
}

fn select_progress_record(
    snapshot_dir: &Path,
    parquet_dir: &Path,
    records: Vec<ReportRecord>,
    command: &Option<String>,
    dataset: &str,
) -> Option<ReportRecord> {
    let cmd_filter = command.as_ref().map(|c| canonical_command_name(c));
    let dataset_filter = if dataset == "all" {
        None
    } else {
        Some(dataset.to_string())
    };

    let filtered: Vec<ReportRecord> = records
        .into_iter()
        .filter(|r| {
            if let Some(cf) = &cmd_filter {
                canonical_command_name(&r.report.command) == *cf
            } else {
                true
            }
        })
        .filter(|r| {
            if let Some(df) = &dataset_filter {
                r.report.datasets.iter().any(|d| &d.dataset == df)
                    || r.report.failures.iter().any(|f| &f.dataset == df)
            } else {
                true
            }
        })
        .collect();

    if filtered.is_empty() {
        return None;
    }

    let mut active: Vec<ReportRecord> = filtered
        .iter()
        .filter(|r| r.report.finished_at_unix.is_none())
        .cloned()
        .collect();
    active.sort_by_key(|r| std::cmp::Reverse(r.timestamp));
    if let Some(r) = active.into_iter().next() {
        return Some(r);
    }

    let now = now_unix();
    let mut recent: Vec<ReportRecord> = filtered
        .into_iter()
        .filter(|r| {
            let logs = log_paths_for_report(snapshot_dir, parquet_dir, &r.report);
            logs.iter().any(|p| {
                fs::metadata(p)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| now.saturating_sub(d.as_secs() as i64) <= 300)
                    .unwrap_or(false)
            })
        })
        .collect();
    recent.sort_by_key(|r| std::cmp::Reverse(r.timestamp));
    recent.into_iter().next()
}

fn progress_snapshot(args: &ProgressArgs) -> Result<ProgressView> {
    let records = load_report_records(&args.snapshot_dir, &args.parquet_dir, &ReportSource::All)?;
    let rec = select_progress_record(
        &args.snapshot_dir,
        &args.parquet_dir,
        records,
        &args.command,
        &args.dataset,
    )
    .ok_or_else(|| anyhow!("no matching active/recent run found"))?;
    let runtime_seconds = rec
        .report
        .duration_seconds
        .unwrap_or_else(|| (now_unix() - rec.report.started_at_unix).max(0) as f64);
    let mut last_logs = Vec::new();
    for lp in log_paths_for_report(&args.snapshot_dir, &args.parquet_dir, &rec.report) {
        for line in read_last_log_lines(&lp, 2) {
            last_logs.push(format!("{} | {}", lp.display(), line));
        }
    }
    Ok(ProgressView {
        command: rec.report.command.clone(),
        report_path: rec.path.display().to_string(),
        started_at_unix: rec.report.started_at_unix,
        finished_at_unix: rec.report.finished_at_unix,
        runtime_seconds,
        totals_items_scanned: rec.report.totals_items_scanned,
        totals_succeeded: rec.report.totals_succeeded,
        totals_failed: rec.report.totals_failed,
        totals_skipped: rec.report.totals_skipped,
        datasets: rec.report.datasets.clone(),
        last_logs,
    })
}

fn print_progress_human(view: &ProgressView) {
    println!(
        "[progress] command={} started={} runtime={} report={}",
        view.command,
        view.started_at_unix,
        format_duration(view.runtime_seconds),
        view.report_path
    );
    println!(
        "[progress] totals scanned={} ok={} failed={} skipped={}",
        view.totals_items_scanned, view.totals_succeeded, view.totals_failed, view.totals_skipped
    );
    for ds in &view.datasets {
        println!(
            "[progress] dataset={} scanned={} ok={} failed={} skipped={}",
            ds.dataset, ds.items_scanned, ds.succeeded, ds.failed, ds.skipped
        );
    }
    for l in &view.last_logs {
        println!("[progress] log {}", l);
    }
}

#[derive(Debug, Clone)]
struct AllResolved {
    root_dir: PathBuf,
    retry: usize,
    enable_download: bool,
    enable_verify_download: bool,
    enable_convert: bool,
    enable_verify_convert: bool,
    enable_repair_convert: bool,
    enable_index: bool,
    enable_verify_index: bool,
    skip_disk_check: bool,
}

fn resolve_all_settings(args: &AllArgs, cfg: &AppConfig) -> AllResolved {
    let c = cfg.all.clone().unwrap_or_default();
    // CLI --skip-disk-check wins; fall back to all.skip_disk_check in config
    let skip_disk_check = args.skip_disk_check || c.skip_disk_check.unwrap_or(false);
    AllResolved {
        root_dir: args.root_dir.clone(),
        retry: c.retry.unwrap_or(args.retry),
        enable_download: c.enable_download.unwrap_or(true),
        enable_verify_download: c.enable_verify_download.unwrap_or(true),
        enable_convert: c.enable_convert.unwrap_or(true),
        enable_verify_convert: c.enable_verify_convert.unwrap_or(true),
        enable_repair_convert: c.enable_repair_convert.unwrap_or(true),
        enable_index: c.enable_index.unwrap_or(true),
        enable_verify_index: c.enable_verify_index.unwrap_or(true),
        skip_disk_check,
    }
}

fn record_all_step(
    report: &mut RunReport,
    step_failed: &mut bool,
    snapshot_dir: &Path,
    parquet_dir: &Path,
    name: &str,
    res: Result<()>,
    msg: Option<String>,
) {
    let rp = latest_report_path_for_command(snapshot_dir, parquet_dir, name)
        .map(|p| p.to_string_lossy().to_string());
    match res {
        Ok(()) => report.step_runs.push(StepRunSummary {
            step: name.to_string(),
            status: "ok".to_string(),
            report_path: rp,
            message: msg,
        }),
        Err(e) => {
            *step_failed = true;
            report.step_runs.push(StepRunSummary {
                step: name.to_string(),
                status: "failed".to_string(),
                report_path: rp.clone(),
                message: Some(format!("{e:#}")),
            });
            report.failures.push(FailureEntry {
                dataset: "all".to_string(),
                phase: format!("all_{name}"),
                rel_path: None,
                source_path: None,
                output_path: rp,
                error_message: format!("{e:#}"),
                suggested_recovery: None,
            });
        }
    }
}

fn latest_report_path_for_command(
    snapshot_dir: &Path,
    parquet_dir: &Path,
    cmd: &str,
) -> Option<PathBuf> {
    let records = load_report_records(snapshot_dir, parquet_dir, &ReportSource::All).ok()?;
    records
        .into_iter()
        .filter(|r| canonical_command_name(&r.report.command) == canonical_command_name(cmd))
        .max_by_key(|r| r.timestamp)
        .map(|r| r.path)
}

fn run_all(args: AllArgs, cfg: &AppConfig, profiles_config: Option<&Path>) -> Result<()> {
    let resolved = resolve_all_settings(&args, cfg);
    let snapshot_dir = resolved.root_dir.join("snapshot");
    let parquet_dir = resolved.root_dir.join("parquet");
    fs::create_dir_all(&parquet_dir)?;

    if args.explain {
        println!("--explain: all");
        println!("root_dir: {}", resolved.root_dir.display());
        println!("retry: {}", resolved.retry);
        println!(
            "steps: download={} verify_download={} convert={} verify_convert={} repair_convert={} index={} verify_index={}",
            resolved.enable_download,
            resolved.enable_verify_download,
            resolved.enable_convert,
            resolved.enable_verify_convert,
            resolved.enable_repair_convert,
            resolved.enable_index,
            resolved.enable_verify_index
        );
        return Ok(());
    }
    let _lock = acquire_lock(&parquet_dir, "all")?;
    let _ = archive_completed_run(&parquet_dir, &snapshot_dir);
    let _ = cleanup_command_reports(&parquet_dir, "all");

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        resolved.root_dir.to_string_lossy().to_string(),
    );
    report_args.insert("retry".to_string(), resolved.retry.to_string());
    let mut report = report_new("all", report_args);
    report.datasets.push(DatasetReportSummary {
        dataset: "all".to_string(),
        items_scanned: 0,
        succeeded: 0,
        failed: 0,
        skipped: 0,
    });

    let mut step_failed = false;

    if resolved.enable_download {
        let mut da = DownloadArgs {
            root_dir: resolved.root_dir.clone(),
            snapshot_dir: PathBuf::new(),
            s3_uri: "s3://openalex".to_string(),
            dataset: "all".to_string(),
            aws_bin: PathBuf::from("aws"),
            endpoint_url: None,
            region: None,
            profile_name: None,
            no_sign_request: true,
            signed: false,
            delete_files: true,
            no_delete: false,
            skip_disk_check: resolved.skip_disk_check,
            progress: true,
            explain: false,
            state_flush_every: 25,
        };
        fill_download_dirs(&mut da);
        apply_download_config(&mut da, Some(cfg), None);
        fill_download_dirs(&mut da);
        // CLI --skip-disk-check always wins over config
        if resolved.skip_disk_check {
            da.skip_disk_check = true;
        }
        record_all_step(
            &mut report,
            &mut step_failed,
            &snapshot_dir,
            &parquet_dir,
            "download",
            run_download(da),
            None,
        );
        if step_failed {
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            bail!("[all] aborting after download failure");
        }
    }

    if resolved.enable_verify_download {
        let mut va = ValidateDownloadArgs {
            root_dir: resolved.root_dir.clone(),
            snapshot_dir: PathBuf::new(),
            s3_uri: "s3://openalex".to_string(),
            dataset: "all".to_string(),
            aws_bin: PathBuf::from("aws"),
            endpoint_url: None,
            region: None,
            profile_name: None,
            no_sign_request: true,
            signed: false,
            check_extra: true,
            workers: 0,
            progress: true,
            explain: false,
            state_flush_every: 25,
        };
        fill_validate_download_dirs(&mut va);
        apply_validate_download_config(&mut va, Some(cfg), None);
        fill_validate_download_dirs(&mut va);
        record_all_step(
            &mut report,
            &mut step_failed,
            &snapshot_dir,
            &parquet_dir,
            "verify_download",
            run_validate_download(va),
            None,
        );
        if step_failed {
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            bail!("[all] aborting after verify_download failure");
        }
    }

    if resolved.enable_convert {
        let mut ca = ConvertArgs {
            shared: SharedArgs {
                root_dir: resolved.root_dir.clone(),
                snapshot_dir: PathBuf::new(),
                parquet_dir: PathBuf::new(),
                dataset: "all".to_string(),
                workers: 0,
                duckdb_bin: None,
            },
            profile: "safe".to_string(),
            max_memory_mb: None,
            row_group_rows: 100_000,
            batch_rows: 5_000,
            compression: "snappy".to_string(),
            sample_size: 100,
            input_files: Vec::new(),
            progress: true,
            seed: 42,
            skip_disk_check: resolved.skip_disk_check,
            disk_check_scope: DiskCheckScope::Dataset,
            refresh_cache: false,
            split_size: "0".to_string(),
            split_temp_dir: None,
            explain: false,
            state_flush_every: 25,
        };
        fill_shared_dirs(&mut ca.shared);
        apply_convert_config(&mut ca, Some(cfg), None);
        fill_shared_dirs(&mut ca.shared);
        // CLI --skip-disk-check always wins over config
        if resolved.skip_disk_check {
            ca.skip_disk_check = true;
        }
        record_all_step(
            &mut report,
            &mut step_failed,
            &snapshot_dir,
            &parquet_dir,
            "convert",
            run_convert(ca, profiles_config),
            None,
        );
        if step_failed {
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            bail!("[all] aborting after convert failure");
        }
    }

    if resolved.enable_verify_convert {
        let mut verify_ok = false;
        let mut attempts = 0usize;
        loop {
            let mut va = VerifyArgs {
                shared: SharedArgs {
                    root_dir: resolved.root_dir.clone(),
                    snapshot_dir: PathBuf::new(),
                    parquet_dir: PathBuf::new(),
                    dataset: "all".to_string(),
                    workers: 0,
                    duckdb_bin: None,
                },
                max_memory_mb: None,
                scope: VerifyScope::Snapshot,
                metadata_level: VerifyMetadataLevel::Both,
                file_sample_n: 50,
                seed: 42,
                progress: true,
                explain: false,
                state_flush_every: 25,
            };
            fill_shared_dirs(&mut va.shared);
            apply_verify_config(&mut va, Some(cfg), None);
            fill_shared_dirs(&mut va.shared);
            let verify_res = run_verify(va);
            let verify_failed = verify_res.is_err();
            record_all_step(
                &mut report,
                &mut step_failed,
                &snapshot_dir,
                &parquet_dir,
                "verify_convert",
                verify_res,
                Some(format!("attempt={}", attempts + 1)),
            );
            if !verify_failed {
                verify_ok = true;
                break;
            }
            if !resolved.enable_repair_convert || attempts >= resolved.retry {
                break;
            }
            attempts += 1;
            let report_path =
                latest_report_path_for_command(&snapshot_dir, &parquet_dir, "verify_convert")
                    .ok_or_else(|| {
                        anyhow!("[all] cannot locate latest verify_convert report for repair loop")
                    })?;
            let mut ra = RepairArgs {
                shared: SharedArgs {
                    root_dir: resolved.root_dir.clone(),
                    snapshot_dir: PathBuf::new(),
                    parquet_dir: PathBuf::new(),
                    dataset: "all".to_string(),
                    workers: 0,
                    duckdb_bin: None,
                },
                from_verify_report: Some(report_path.clone()),
                profile: "safe".to_string(),
                max_memory_mb: None,
                progress: true,
                explain: false,
                state_flush_every: 25,
            };
            fill_shared_dirs(&mut ra.shared);
            apply_repair_config(&mut ra, Some(cfg), None);
            fill_shared_dirs(&mut ra.shared);
            // Always repair from loop-selected verify report.
            ra.from_verify_report = Some(report_path);
            record_all_step(
                &mut report,
                &mut step_failed,
                &snapshot_dir,
                &parquet_dir,
                "repair_convert",
                run_repair(ra, profiles_config),
                Some(format!("attempt={}", attempts)),
            );
        }
        if !verify_ok {
            report.failures.push(FailureEntry {
                dataset: "all".to_string(),
                phase: "all_verify_repair_loop".to_string(),
                rel_path: None,
                source_path: None,
                output_path: None,
                error_message: format!(
                    "verify_convert did not pass after {} repair attempt(s)",
                    resolved.retry
                ),
                suggested_recovery: Some(
                    "rerun repair_convert manually with higher memory profile".to_string(),
                ),
            });
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            bail!("[all] verify/repair loop exhausted");
        }
    }

    if resolved.enable_index {
        let mut ia = IndexArgs {
            root_dir: resolved.root_dir.clone(),
            dataset: "all".to_string(),
            index_file: None,
            workers: 0,
            max_memory_mb: None,
            duckdb_bin: None,
            progress: true,
            overwrite: false,
            explain: false,
            state_flush_every: 25,
        };
        apply_index_config(&mut ia, Some(cfg), None);
        record_all_step(
            &mut report,
            &mut step_failed,
            &snapshot_dir,
            &parquet_dir,
            "index",
            run_index(ia),
            None,
        );
        if step_failed {
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            bail!("[all] aborting after index failure");
        }
    }

    if resolved.enable_verify_index {
        let mut via = VerifyIndexArgs {
            root_dir: resolved.root_dir.clone(),
            dataset: "all".to_string(),
            index_file: None,
            workers: 0,
            max_memory_mb: None,
            duckdb_bin: None,
            progress: true,
            explain: false,
        };
        apply_verify_index_config(&mut via, Some(cfg), None);
        record_all_step(
            &mut report,
            &mut step_failed,
            &snapshot_dir,
            &parquet_dir,
            "verify_index",
            run_verify_index(via),
            None,
        );
    }

    if step_failed {
        report.datasets[0].failed = 1;
    } else {
        report.datasets[0].succeeded = 1;
    }
    report_finalize(&mut report);
    let report_paths = write_run_reports(&parquet_dir, &report)?;
    eprintln!(
        "[all] summary ok={} failed={} reports={}",
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if step_failed {
        bail!("[all] pipeline failed");
    }
    Ok(())
}

fn run_progress(mut args: ProgressArgs) -> Result<()> {
    if args.once {
        args.watch = false;
    }

    let show_once = |args: &ProgressArgs| match check_lock(&args.parquet_dir) {
        Some(lock) => {
            let statuses = live_progress(&args.parquet_dir, &lock);
            if args.json {
                let _ = serde_json::to_string_pretty(&statuses).map(|s| println!("{}", s));
            } else {
                print_live_progress(&lock, &statuses);
            }
        }
        None => match progress_snapshot(args) {
            Ok(view) => {
                if args.json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&view).unwrap_or_default()
                    );
                } else {
                    print_progress_human(&view);
                }
            }
            Err(e) => eprintln!("[progress] no active run and no recent report: {e}"),
        },
    };

    if !args.watch {
        show_once(&args);
        return Ok(());
    }

    loop {
        print!("\x1B[2J\x1B[H");
        show_once(&args);
        stdout().flush()?;
        if check_lock(&args.parquet_dir).is_none() {
            println!("[progress] pipeline finished");
            return Ok(());
        }
        thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
    }
}

fn run_verify_index(args: VerifyIndexArgs) -> Result<()> {
    if args.dataset == "all" {
        let _guard = RecursionDepthGuard::enter(&VERIFY_INDEX_ALL_DEPTH);
        if args.index_file.is_some() {
            bail!("--index-file cannot be used with --dataset all");
        }
        let parquet_dir = args.root_dir.join("parquet");
        let _ = cleanup_command_reports(&parquet_dir, "verify-index");
        let _ = cleanup_command_dataset_logs(&parquet_dir, "verify-index");
        let mut datasets: Vec<String> = fs::read_dir(&parquet_dir)
            .with_context(|| format!("failed to read {}", parquet_dir.display()))?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let p = e.path();
                if p.is_dir() {
                    e.file_name().to_str().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .filter(|s| !s.starts_with('.'))
            .collect();
        datasets.sort();
        if datasets.is_empty() {
            bail!("no parquet datasets found under {}", parquet_dir.display());
        }
        let mut failures = 0u64;
        for ds in datasets {
            let mut sub = args.clone();
            sub.dataset = ds;
            if let Err(e) = run_verify_index(sub) {
                failures += 1;
                eprintln!("[verify-index] dataset failure: {e:#}");
            }
        }
        if failures > 0 {
            bail!(
                "[verify-index] failures detected across datasets: {}",
                failures
            );
        }
        return Ok(());
    }

    let bin = duckdb_bin_from_option(&args.duckdb_bin);
    ensure_duckdb_bin(&bin)?;
    let parquet_dir = args.root_dir.join("parquet");
    let dataset = args.dataset.clone();
    let corpus_dir = parquet_dir.join(&dataset);
    if !corpus_dir.exists() {
        bail!("corpus_dir does not exist: {}", corpus_dir.display());
    }
    let index_file = args
        .index_file
        .clone()
        .unwrap_or_else(|| parquet_dir.join(format!("{dataset}_id_idx.parquet")));
    let tuning = light_tuning_with_override(args.workers, args.max_memory_mb);
    if args.explain {
        println!("--explain: verify-index");
        println!("duckdb_bin: {}", bin.display());
        println!("root_dir: {}", args.root_dir.display());
        println!("dataset: {}", dataset);
        println!("corpus_dir: {}", corpus_dir.display());
        println!("index_file: {}", index_file.display());
        println!("workers: {}", tuning.workers);
        println!("memory_mb: {:?}", tuning.memory_mb);
        return Ok(());
    }
    if VERIFY_INDEX_ALL_DEPTH.load(Ordering::SeqCst) == 0 {
        let _ = cleanup_command_reports(&parquet_dir, "verify-index");
        let _ = cleanup_command_dataset_logs(&parquet_dir, "verify-index");
    }
    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        args.root_dir.to_string_lossy().to_string(),
    );
    report_args.insert("dataset".to_string(), dataset.clone());
    report_args.insert(
        "index_file".to_string(),
        index_file.to_string_lossy().to_string(),
    );
    let mut report = report_new("verify-index", report_args);
    let mut ds = DatasetReportSummary {
        dataset: dataset.clone(),
        items_scanned: 1,
        ..Default::default()
    };
    let pb = make_progress_bar(args.progress, 4, "verify-index");

    if !index_file.exists() {
        ds.failed += 1;
        report.failures.push(FailureEntry {
            dataset: dataset.clone(),
            phase: "verify_index_exists".to_string(),
            rel_path: None,
            source_path: Some(index_file.to_string_lossy().to_string()),
            output_path: None,
            error_message: "index file missing".to_string(),
            suggested_recovery: Some("run index subcommand first".to_string()),
        });
    }
    pb.inc(1);

    if report.failures.is_empty() {
        let cols = describe_parquet_glob(&bin, &index_file.to_string_lossy(), tuning.memory_mb)?;
        for req in ["id", "id_block", "parquet_file", "file_row_number"] {
            if !cols.contains_key(req) {
                ds.failed += 1;
                report.failures.push(FailureEntry {
                    dataset: dataset.clone(),
                    phase: "verify_index_schema".to_string(),
                    rel_path: None,
                    source_path: Some(index_file.to_string_lossy().to_string()),
                    output_path: None,
                    error_message: format!("required column missing: {req}"),
                    suggested_recovery: Some(
                        "rebuild index with `openalex-snapshot index --overwrite`".to_string(),
                    ),
                });
            }
        }
    }
    pb.inc(1);

    let parquet_files = list_parquet_files(&corpus_dir)?;
    if parquet_files.is_empty() {
        ds.failed += 1;
        report.failures.push(FailureEntry {
            dataset: dataset.clone(),
            phase: "verify_index_corpus".to_string(),
            rel_path: None,
            source_path: Some(corpus_dir.to_string_lossy().to_string()),
            output_path: None,
            error_message: "no parquet files found in corpus".to_string(),
            suggested_recovery: None,
        });
    } else if report.failures.is_empty() {
        let count_sql = with_session_settings(
            &format!(
                "SELECT (SELECT COUNT(*) FROM read_parquet({})) AS idx_count, (SELECT COUNT(*) FROM read_parquet({})) AS corpus_count;",
                sql_quote(&index_file.to_string_lossy()),
                sql_quote(&corpus_dir.join("**/*.parquet").to_string_lossy()),
            ),
            tuning.memory_mb,
            Some(tuning.workers),
        );
        let row = query_one_row(&bin, &count_sql)?;
        let idx_count = row
            .get("idx_count")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let corpus_count = row
            .get("corpus_count")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        if idx_count != corpus_count {
            ds.failed += 1;
            report.failures.push(FailureEntry {
                dataset: dataset.clone(),
                phase: "verify_index_count".to_string(),
                rel_path: None,
                source_path: Some(index_file.to_string_lossy().to_string()),
                output_path: Some(corpus_dir.to_string_lossy().to_string()),
                error_message: format!(
                    "row count mismatch: index={idx_count} corpus={corpus_count}"
                ),
                suggested_recovery: Some(
                    "rebuild index with `openalex-snapshot index --overwrite`".to_string(),
                ),
            });
        }
    }
    pb.inc(1);

    if report.failures.is_empty() {
        let refs_sql = with_session_settings(
            &format!(
                "SELECT DISTINCT parquet_file FROM read_parquet({});",
                sql_quote(&index_file.to_string_lossy())
            ),
            tuning.memory_mb,
            Some(tuning.workers),
        );
        let refs = run_duckdb_csv(&bin, &refs_sql)?;
        for r in refs {
            if let Some(rel) = r.get("parquet_file") {
                let p = parquet_dir.join(rel);
                if !p.exists() {
                    ds.failed += 1;
                    report.failures.push(FailureEntry {
                        dataset: dataset.clone(),
                        phase: "verify_index_path".to_string(),
                        rel_path: Some(rel.clone()),
                        source_path: Some(index_file.to_string_lossy().to_string()),
                        output_path: Some(p.to_string_lossy().to_string()),
                        error_message: "index references missing parquet file".to_string(),
                        suggested_recovery: Some(
                            "re-run convert for missing files then rebuild index".to_string(),
                        ),
                    });
                }
            }
        }
    }
    pb.inc(1);
    pb.finish_with_message("verify-index complete");

    if ds.failed == 0 {
        ds.succeeded = 1;
    }
    report.datasets = vec![ds];
    report_finalize(&mut report);
    let report_paths = write_run_reports(&parquet_dir, &report)?;
    eprintln!(
        "[verify-index] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[verify-index] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn format_duration(seconds: f64) -> String {
    let s = seconds.round().max(0.0) as u64;
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let sec = s % 60;
    if h > 0 {
        format!("{h}h{m:02}m{sec:02}s")
    } else if m > 0 {
        format!("{m}m{sec:02}s")
    } else {
        format!("{sec}s")
    }
}

fn run_prune_reports(args: PruneReportsArgs) -> Result<()> {
    let mut records = load_report_records(&args.snapshot_dir, &args.parquet_dir, &args.source)?;
    if let Some(cmd_filter) = &args.command {
        records.retain(|r| r.report.command == *cmd_filter);
    }
    if records.is_empty() {
        println!("[prune-reports] no matching reports found");
        return Ok(());
    }

    let keep_n = args.keep_per_command.max(1);
    let roots = report_roots(&args.snapshot_dir, &args.parquet_dir, &args.source);
    let mut by_command: BTreeMap<String, Vec<ReportRecord>> = BTreeMap::new();
    for rec in records {
        by_command
            .entry(rec.report.command.clone())
            .or_default()
            .push(rec);
    }

    let mut prune_names: BTreeSet<String> = BTreeSet::new();
    let mut kept = 0usize;
    for (_cmd, mut items) in by_command {
        items.sort_by_key(|r| std::cmp::Reverse(r.timestamp));
        kept += items.len().min(keep_n);
        for rec in items.into_iter().skip(keep_n) {
            if let Some(name) = rec.path.file_name().and_then(|s| s.to_str()) {
                prune_names.insert(name.to_string());
            }
        }
    }

    if prune_names.is_empty() {
        println!("[prune-reports] nothing to prune (keep_per_command={keep_n})");
        return Ok(());
    }

    let mut deleted = 0usize;
    let mut missing = 0usize;
    for root in roots {
        for name in &prune_names {
            let p = root.join(name);
            if p.exists() {
                if args.dry_run {
                    println!("[prune-reports] would delete {}", p.display());
                } else {
                    fs::remove_file(&p)
                        .with_context(|| format!("failed to delete {}", p.display()))?;
                    println!("[prune-reports] deleted {}", p.display());
                }
                deleted += 1;
            } else {
                missing += 1;
            }
        }
    }

    println!(
        "[prune-reports] kept={} pruned_names={} deleted_files={} missing={} dry_run={}",
        kept,
        prune_names.len(),
        deleted,
        missing,
        args.dry_run
    );
    Ok(())
}

fn run_convert(args: ConvertArgs, profiles_config: Option<&Path>) -> Result<()> {
    fs::create_dir_all(&args.shared.parquet_dir)?;
    let datasets = resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?;
    let duckdb_bin = duckdb_bin(&args.shared);

    let total_mb = detect_total_memory_mb();
    let profile_registry =
        ProfileRegistry::load(discover_profiles_config(profiles_config).as_deref())?;
    let resolved_profile = profile_registry
        .get(&args.profile)
        .ok_or_else(|| profile_registry.unknown_profile_error(&args.profile))?
        .clone();

    // Representative tuning for log lines / report metadata.  The actual
    // execution may use different per-stratum values (see build_convert_plan).
    // For Safe: workers/memory derived from the safe profile.  For Stratified:
    // workers from the FIRST stratum (smallest files, highest parallelism) and
    // memory from the CATCH-ALL stratum (biggest files, most per-worker mem) —
    // gives the reader a rough sense of the run's shape in one line.
    let tuning = representative_tuning(
        &args.profile,
        &resolved_profile,
        args.shared.workers,
        args.max_memory_mb,
        total_mb,
    );

    if args.explain {
        explain_convert(&args, &datasets, &duckdb_bin);
        return Ok(());
    }
    let _lock = acquire_lock(&args.shared.parquet_dir, "convert")?;
    let convert_start = Instant::now();
    eprintln!(
        "[convert] profile={} workers_override={} max_memory_mb_override={:?}",
        args.profile,
        if args.shared.workers == 0 {
            "auto".to_string()
        } else {
            args.shared.workers.to_string()
        },
        args.max_memory_mb,
    );
    let _ = archive_completed_run(&args.shared.parquet_dir, &args.shared.snapshot_dir);
    let _ = cleanup_command_reports(&args.shared.parquet_dir, "convert");
    let _ = cleanup_command_dataset_logs(&args.shared.parquet_dir, "convert");
    let mut report_args = BTreeMap::new();
    report_args.insert("dataset".to_string(), args.shared.dataset.clone());
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert("memory_mb".to_string(), format!("{:?}", tuning.memory_mb));
    report_args.insert("compression".to_string(), args.compression.clone());
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("convert", report_args);
    let flush_every = args.state_flush_every.max(1);
    if !args.skip_disk_check && args.disk_check_scope == DiskCheckScope::Dataset {
        let free_bytes = available_disk_bytes(&args.shared.parquet_dir)?;
        let required_min_bytes = convert_min_free_bytes(&args.shared.parquet_dir);
        if free_bytes < required_min_bytes {
            report.failures.push(FailureEntry {
                dataset: args.shared.dataset.clone(),
                phase: "convert_disk_space".to_string(),
                rel_path: None,
                source_path: None,
                output_path: Some(args.shared.parquet_dir.to_string_lossy().to_string()),
                error_message: format!(
                    "insufficient free disk space: available={} GiB required_min={} GiB",
                    bytes_to_gib(free_bytes),
                    bytes_to_gib(required_min_bytes)
                ),
                suggested_recovery: Some(
                    "Free up disk space, or set skip_disk_check: true under convert: in your config file, or pass --skip-disk-check to the convert/all command.".to_string()
                ),
            });
            report_finalize(&mut report);
            let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;
            eprintln!(
                "[convert] summary scanned={} ok={} failed={} reports={}",
                report.totals_items_scanned,
                report.totals_succeeded,
                report.totals_failed,
                report_paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            bail!(
                "[convert] Not enough disk space.\n  Location : {}\n  Available: {} GiB\n  Required : {} GiB (estimated 20% overhead over source size)\n\nTo proceed anyway, either:\n  - Set skip_disk_check: true under the convert: section in your config file\n  - Pass --skip-disk-check when running the convert or all command",
                args.shared.parquet_dir.display(),
                bytes_to_gib(free_bytes),
                bytes_to_gib(required_min_bytes)
            );
        }
    }

    for dataset in &datasets {
        let dataset_start = Instant::now();
        let mut ds = DatasetReportSummary {
            dataset: dataset.clone(),
            ..Default::default()
        };
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "convert",
            &format!(
                "start workers={} memory_mb={:?} compression={} row_group_rows={}",
                tuning.workers, tuning.memory_mb, args.compression, args.row_group_rows
            ),
        );
        eprintln!("[convert] dataset={dataset} scanning input files ...");
        let mut pairs =
            enumerate_pairs(&args.shared.snapshot_dir, &args.shared.parquet_dir, dataset)?;
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "convert",
            &format!("scanned source_files={}", pairs.len()),
        );
        if !args.input_files.is_empty() {
            pairs = filter_pairs_by_input_files(
                &pairs,
                &args.input_files,
                &args.shared.snapshot_dir,
                dataset,
            )?;
            if pairs.is_empty() {
                eprintln!("[convert] dataset={dataset} no matching --input-file entries");
                try_log_dataset(
                    &args.shared.parquet_dir,
                    dataset,
                    "convert",
                    "no matching --input-file entries",
                );
                continue;
            }
        }
        if pairs.is_empty() {
            eprintln!("[convert] dataset={dataset} no source files found");
            try_log_dataset(
                &args.shared.parquet_dir,
                dataset,
                "convert",
                "no source files found",
            );
            report.datasets.push(ds);
            continue;
        }
        let pairs_len = pairs.len();

        let mut todo: Vec<FilePair> = pairs
            .into_iter()
            .filter(|p| !p.output_parquet.exists() && !split_parquets_exist(&p.output_parquet))
            .collect();
        // Process largest files first in all passes to minimise tail-latency stragglers.
        todo.sort_by_key(|p| std::cmp::Reverse(p.gz_size_bytes));

        if todo.is_empty() {
            eprintln!("[convert] dataset={dataset} all files already converted");
            try_log_dataset(
                &args.shared.parquet_dir,
                dataset,
                "convert",
                "all files already converted",
            );
            ds.skipped = pairs_len as u64;
            report.datasets.push(ds);
            continue;
        }

        // Pre-split phase: expand large gz files into chunk FilePairs.
        // chunk_source maps chunk rel → original source rel (for state accounting + cleanup).
        // All chunks + small files are then processed by the single parallel balanced pass.
        let split_target =
            parse_size_str(&args.split_size).context("invalid --split-size value")?;
        // split_size=0 (auto/default) means no splitting: in-process DuckDB handles large
        // files via streaming and memory-limit spill without needing pre-splitting.
        let split_target_bytes: usize = if split_target == 0 {
            usize::MAX
        } else {
            split_target
        };
        let split_tmp_root = args
            .split_temp_dir
            .clone()
            .unwrap_or_else(|| args.shared.parquet_dir.join(".split_tmp"));

        let large_count = todo
            .iter()
            .filter(|p| p.gz_size_bytes as usize > split_target_bytes)
            .count();
        let small_count = todo.len() - large_count;
        let already_done = pairs_len - todo.len();
        if large_count > 0 {
            let target_mb_display = split_target_bytes / 1_000_000;
            eprintln!(
                "[convert] dataset={dataset} pre-split: {large_count} large (>{target_mb_display}MB) + {small_count} small remaining, {already_done} already done"
            );
        } else {
            eprintln!(
                "[convert] dataset={dataset} todo={} already_done={already_done}",
                todo.len()
            );
        }
        let split_pb = make_progress_bar(
            args.progress && large_count > 0,
            large_count as u64,
            &format!("split:{dataset}"),
        );

        // Split large files in parallel; each item produces either a passthrough or a set of chunks.
        enum SplitOutcome {
            Pass(FilePair),
            Chunks {
                source_rel: PathBuf,
                pairs: Vec<(PathBuf, FilePair)>, // (chunk_rel, chunk_pair)
            },
            Failed(FailureEntry),
        }

        let outcomes: Vec<SplitOutcome> = todo
            .into_par_iter()
            .map(|pair| {
                if pair.gz_size_bytes as usize <= split_target_bytes {
                    return SplitOutcome::Pass(pair);
                }
                let stem = pair
                    .output_parquet
                    .file_stem()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let chunk_dir = split_tmp_root
                    .join(dataset)
                    .join(pair.rel.parent().unwrap_or(Path::new("")));
                let chunk_out_dir = pair
                    .output_parquet
                    .parent()
                    .unwrap_or(Path::new(""))
                    .to_path_buf();
                let result = split_gz_lines(&pair.input_gz, &chunk_dir, &stem, split_target_bytes);
                split_pb.inc(1);
                match result {
                    Ok(chunks) => {
                        let pairs = chunks
                            .into_iter()
                            .enumerate()
                            .map(|(i, chunk_path)| {
                                let chunk_out =
                                    chunk_out_dir.join(format!("{}_{:03}.parquet", stem, i + 1));
                                let chunk_rel = pair
                                    .rel
                                    .parent()
                                    .unwrap_or(Path::new(""))
                                    .join(format!("{}_{:03}.json", stem, i + 1));
                                (
                                    chunk_rel.clone(),
                                    FilePair {
                                        input_gz: chunk_path,
                                        output_parquet: chunk_out,
                                        rel: chunk_rel,
                                        gz_size_bytes: 0,
                                    },
                                )
                            })
                            .collect();
                        SplitOutcome::Chunks {
                            source_rel: pair.rel,
                            pairs,
                        }
                    }
                    Err(e) => SplitOutcome::Failed(FailureEntry {
                        dataset: dataset.clone(),
                        phase: "split".to_string(),
                        rel_path: Some(pair.rel.to_string_lossy().to_string()),
                        source_path: Some(pair.input_gz.to_string_lossy().to_string()),
                        output_path: Some(pair.output_parquet.to_string_lossy().to_string()),
                        error_message: format!("split failed: {e:#}"),
                        suggested_recovery: Some(
                            "retry with a larger --split-size or fix disk space".to_string(),
                        ),
                    }),
                }
            })
            .collect();

        split_pb.finish_and_clear();

        // Maps chunk_rel → source_rel (for cleanup and success accounting)
        let mut chunk_source: HashMap<PathBuf, PathBuf> = HashMap::new();
        // Track how many chunks each source has, for accounting
        let mut source_chunk_counts: HashMap<PathBuf, u64> = HashMap::new();
        let mut expanded: Vec<FilePair> = Vec::new();

        for outcome in outcomes {
            match outcome {
                SplitOutcome::Pass(pair) => expanded.push(pair),
                SplitOutcome::Chunks { source_rel, pairs } => {
                    source_chunk_counts.insert(source_rel.clone(), pairs.len() as u64);
                    for (chunk_rel, pair) in pairs {
                        chunk_source.insert(chunk_rel, source_rel.clone());
                        expanded.push(pair);
                    }
                }
                SplitOutcome::Failed(f) => {
                    ds.failed += 1;
                    report.failures.push(f);
                }
            }
        }
        let split_count: usize = source_chunk_counts.values().map(|&n| n as usize).sum();
        let unsplit_count = expanded.len() - split_count;
        if !source_chunk_counts.is_empty() {
            let target_mb = split_target_bytes / 1_000_000;
            eprintln!(
                "[convert] dataset={dataset} split: {} source files → {} chunks (target={}MB, unsplit={})",
                source_chunk_counts.len(), split_count, target_mb, unsplit_count,
            );
            try_log_dataset(
                &args.shared.parquet_dir,
                dataset,
                "convert",
                &format!(
                    "split sources={} chunks={} target_mb={}",
                    source_chunk_counts.len(),
                    split_count,
                    target_mb
                ),
            );
        }
        let todo = expanded;

        ds.items_scanned = todo.len() as u64 + ds.failed; // include split failures

        let schema_start = Instant::now();
        eprintln!(
            "[convert] dataset={dataset} todo_files={} (starting schema inference)",
            todo.len()
        );
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "convert",
            &format!("todo_files={} schema_inference_start", todo.len()),
        );
        fs::create_dir_all(dataset_cache_dir(&args.shared.parquet_dir, dataset))?;
        let schema = match load_or_infer_source_schema(
            &duckdb_bin,
            &args.shared.snapshot_dir,
            &args.shared.parquet_dir,
            dataset,
            args.sample_size,
            args.refresh_cache,
            tuning.memory_mb,
            tuning.workers,
            args.state_flush_every,
        ) {
            Ok(s) => s,
            Err(e) => {
                ds.failed += ds.items_scanned;
                report.failures.push(FailureEntry {
                    dataset: dataset.clone(),
                    phase: "schema_infer".to_string(),
                    rel_path: None,
                    source_path: Some(
                        args.shared
                            .snapshot_dir
                            .join("data")
                            .join(dataset)
                            .to_string_lossy()
                            .to_string(),
                    ),
                    output_path: Some(
                        args.shared
                            .parquet_dir
                            .join(dataset)
                            .to_string_lossy()
                            .to_string(),
                    ),
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some(
                        "refresh schema cache or reduce sample size".to_string(),
                    ),
                });
                ds.succeeded = 0;
                report.datasets.push(ds);
                report_finalize(&mut report);
                let _ = write_run_reports(&args.shared.parquet_dir, &report);
                continue;
            }
        };
        eprintln!(
            "[convert] dataset={dataset} schema inference complete elapsed={}",
            format_duration(schema_start.elapsed().as_secs_f64())
        );
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "convert",
            "schema inference complete",
        );
        let columns_clause = to_duckdb_columns_clause(&schema.fields);

        let start = Instant::now();
        let pb = make_progress_bar(
            args.progress,
            todo.len() as u64,
            &format!("convert:{dataset}"),
        );

        // Enable spill-to-disk so DuckDB can handle files larger than memory_limit.
        // Without a temp_directory an in-memory connection cannot spill and will OOM.
        // The OnceLock inside this function makes it idempotent across strata + datasets.
        {
            let spill_dir = metadata_root(&args.shared.parquet_dir).join("duckdb_tmp");
            set_duckdb_temp_directory(&spill_dir);
        }

        // Build the stratified execution plan.  Safe profile produces a single
        // stratum; stratified profiles partition `todo` by gz size and emit one
        // stratum per non-empty bucket (largest-files-first).  `--workers`
        // collapses a stratified plan into one flat pass.
        let workers_override = if args.shared.workers == 0 {
            None
        } else {
            Some(args.shared.workers)
        };
        let plan = build_convert_plan(
            &args.profile,
            workers_override,
            args.max_memory_mb,
            total_mb,
            todo.clone(),
            &profile_registry,
        )?;
        eprintln!(
            "[convert] dataset={dataset} profile={} strata={} flat={}",
            plan.profile_name,
            plan.strata.len(),
            plan.flat,
        );

        let schema_arc = Arc::new(columns_clause);
        let duckdb_arc = Arc::new(duckdb_bin.clone());
        let compression = args.compression.clone();
        let row_group_rows = args.row_group_rows;
        let parquet_root = Arc::new(args.shared.parquet_dir.clone());
        let dataset_name = dataset.clone();
        let extra_json_options = if dataset == "works" {
            ", maximum_object_size=1000000000".to_string()
        } else {
            "".to_string()
        };

        for (stratum_idx, stratum) in plan.strata.iter().enumerate() {
            eprintln!(
                "[convert] dataset={dataset} stratum {}/{}: files={} workers={} per_worker_mb={}",
                stratum_idx + 1,
                plan.strata.len(),
                stratum.files.len(),
                stratum.workers,
                stratum.memory_mb,
            );

            // memory_limit is a global DuckDB setting shared by all in-process
            // connections.  Set it freshly per stratum so each pass gets the
            // appropriate per_worker × workers budget.  Safe to change between
            // strata (unlike temp_directory, which is OnceLock-guarded).
            set_duckdb_memory_limit(stratum.memory_mb.saturating_mul(stratum.workers));

            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(stratum.workers)
                .build()
                .context("failed to build rayon thread pool")?;

            let memory_mb = Some(stratum.memory_mb);

            for chunk in stratum.files.chunks(flush_every) {
                let results: Vec<Option<FailureEntry>> = pool.install(|| {
                    chunk
                        .par_iter()
                        .map(|pair| {
                            if !args.skip_disk_check && args.disk_check_scope == DiskCheckScope::File {
                                match available_disk_bytes(parquet_root.as_path()) {
                                    Ok(free_bytes) => {
                                        let input_size = fs::metadata(&pair.input_gz).map(|m| m.len()).unwrap_or(0);
                                        let required_bytes = input_size.saturating_add(64 * 1024 * 1024);
                                        if free_bytes < required_bytes {
                                            pb.inc(1);
                                            return Some(FailureEntry {
                                                dataset: dataset.clone(),
                                                phase: "convert_disk_space".to_string(),
                                                rel_path: Some(pair.rel.to_string_lossy().to_string()),
                                                source_path: Some(pair.input_gz.to_string_lossy().to_string()),
                                                output_path: Some(pair.output_parquet.to_string_lossy().to_string()),
                                                error_message: format!(
                                                    "insufficient free disk space for file preflight: available={} GiB required={} GiB",
                                                    bytes_to_gib(free_bytes),
                                                    bytes_to_gib(required_bytes)
                                                ),
                                                suggested_recovery: Some("Free up disk space, or set skip_disk_check: true under convert: in your config, or pass --skip-disk-check to the convert/all command".to_string()),
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        pb.inc(1);
                                        return Some(FailureEntry {
                                            dataset: dataset.clone(),
                                            phase: "convert_disk_space".to_string(),
                                            rel_path: Some(pair.rel.to_string_lossy().to_string()),
                                            source_path: Some(pair.input_gz.to_string_lossy().to_string()),
                                            output_path: Some(pair.output_parquet.to_string_lossy().to_string()),
                                            error_message: format!("disk space check failed: {e:#}"),
                                            suggested_recovery: Some("Free up disk space, or set skip_disk_check: true under convert: in your config, or pass --skip-disk-check to the convert/all command".to_string()),
                                        });
                                    }
                                }
                            }
                            let out = convert_one(
                                &duckdb_arc,
                                pair,
                                &schema_arc,
                                &compression,
                                row_group_rows,
                                memory_mb,
                                &extra_json_options,
                            );
                            pb.inc(1);
                            match out {
                                Ok(()) => {
                                    try_log_dataset(
                                        parquet_root.as_path(),
                                        &dataset_name,
                                        "convert",
                                        &format!("file converted {}", pair.rel.to_string_lossy()),
                                    );
                                    None
                                }
                                Err(e) => {
                                    let msg = format!("{e:#}");
                                    let suggestion = if msg.contains("Out of Memory Error") {
                                        Some(format!(
                                            "openalex-snapshot convert --root-dir {} --dataset {} --profile safe",
                                            args.shared.root_dir.display(),
                                            dataset
                                        ))
                                    } else {
                                        Some("retry converting this single file via --input-file".to_string())
                                    };
                                    Some(FailureEntry {
                                        dataset: dataset.clone(),
                                        phase: "convert_file".to_string(),
                                        rel_path: Some(pair.rel.to_string_lossy().to_string()),
                                        source_path: Some(pair.input_gz.to_string_lossy().to_string()),
                                        output_path: Some(pair.output_parquet.to_string_lossy().to_string()),
                                        error_message: msg,
                                        suggested_recovery: suggestion,
                                    })
                                }
                            }
                        })
                        .collect()
                });
                for f in results.into_iter().flatten() {
                    ds.failed += 1;
                    report.failures.push(f);
                }
                ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
                let mut preview = report.clone();
                preview.datasets.retain(|d| d.dataset != *dataset);
                preview.datasets.push(ds.clone());
                report_finalize(&mut preview);
                let _ = write_run_reports(&args.shared.parquet_dir, &preview);
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        pb.finish_with_message(format!("convert:{dataset} done in {:.1}s", elapsed));
        eprintln!(
            "[convert] dataset={dataset} pass done ok={} failed={} elapsed={}",
            ds.items_scanned.saturating_sub(ds.failed),
            ds.failed,
            format_duration(elapsed),
        );

        // Clean up temp split chunks now that all conversions are done.
        if !source_chunk_counts.is_empty() {
            let ds_tmp = split_tmp_root.join(dataset);
            let _ = fs::remove_dir_all(&ds_tmp);
        }

        let dataset_elapsed = dataset_start.elapsed().as_secs_f64();
        eprintln!(
            "[convert] dataset={dataset} done elapsed={}",
            format_duration(dataset_elapsed)
        );
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "convert",
            &format!("conversion stage complete elapsed_s={:.2}", dataset_elapsed),
        );
        ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
        report.datasets.push(ds);
        report_finalize(&mut report);
        let _ = write_run_reports(&args.shared.parquet_dir, &report);
    }

    report_finalize(&mut report);
    let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;
    let total_elapsed = convert_start.elapsed().as_secs_f64();
    eprintln!(
        "[convert] summary scanned={} ok={} failed={} elapsed={} profile={} workers={} memory_mb={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        format_duration(total_elapsed),
        format!("{:?}", args.profile).to_lowercase(),
        tuning.workers,
        tuning.memory_mb.unwrap_or(0),
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[convert] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn run_index(args: IndexArgs) -> Result<()> {
    if args.dataset == "all" {
        let _guard = RecursionDepthGuard::enter(&INDEX_ALL_DEPTH);
        if args.index_file.is_some() {
            eprintln!(
                "[index] --index-file is ignored when --dataset all; using per-dataset default paths"
            );
        }
        let parquet_dir = args.root_dir.join("parquet");
        let _ = cleanup_command_reports(&parquet_dir, "index");
        let _ = cleanup_command_dataset_logs(&parquet_dir, "index");
        let mut datasets: Vec<String> = fs::read_dir(&parquet_dir)
            .with_context(|| format!("failed to read {}", parquet_dir.display()))?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let p = e.path();
                if p.is_dir() {
                    e.file_name().to_str().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .filter(|s| !s.starts_with('.'))
            .collect();
        datasets.sort();
        if datasets.is_empty() {
            bail!("no parquet datasets found under {}", parquet_dir.display());
        }
        let mut failures = 0u64;
        for ds in datasets {
            let corpus_dir = parquet_dir.join(&ds);
            match list_parquet_files(&corpus_dir) {
                Ok(v) if v.is_empty() => {
                    eprintln!(
                        "[index] dataset={} skipped (no parquet files under {})",
                        ds,
                        corpus_dir.display()
                    );
                    continue;
                }
                Err(e) => {
                    failures += 1;
                    eprintln!("[index] dataset={} scan failure: {e:#}", ds);
                    continue;
                }
                Ok(_) => {}
            }
            let mut sub = args.clone();
            sub.dataset = ds;
            sub.index_file = None;
            if let Err(e) = run_index(sub) {
                failures += 1;
                eprintln!("[index] dataset failure: {e:#}");
            }
        }
        if failures > 0 {
            bail!("[index] failures detected across datasets: {}", failures);
        }
        return Ok(());
    }

    let bin = duckdb_bin_from_option(&args.duckdb_bin);
    ensure_duckdb_bin(&bin)?;
    let parquet_dir = args.root_dir.join("parquet");
    let dataset = args.dataset.clone();
    let corpus_dir = parquet_dir.join(&dataset);
    if !corpus_dir.exists() {
        bail!("corpus_dir does not exist: {}", corpus_dir.display());
    }

    let index_file = args
        .index_file
        .clone()
        .unwrap_or_else(|| parquet_dir.join(format!("{dataset}_id_idx.parquet")));
    let tuning = light_tuning_with_override(args.workers, args.max_memory_mb);
    if args.explain {
        explain_index(&args, &bin, &corpus_dir, &index_file, &tuning);
        return Ok(());
    }
    if INDEX_ALL_DEPTH.load(Ordering::SeqCst) == 0 {
        let _ = cleanup_command_reports(&parquet_dir, "index");
        let _ = cleanup_command_dataset_logs(&parquet_dir, "index");
    }
    let snapshot_dir = args.root_dir.join("snapshot");
    let _lock = acquire_lock(&parquet_dir, "index")?;
    let _ = archive_completed_run(&parquet_dir, &snapshot_dir);
    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        args.root_dir.to_string_lossy().to_string(),
    );
    report_args.insert("dataset".to_string(), dataset.clone());
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert("memory_mb".to_string(), format!("{:?}", tuning.memory_mb));
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("index", report_args);
    let mut ds = DatasetReportSummary {
        dataset: dataset.clone(),
        ..Default::default()
    };
    let flush_every = args.state_flush_every.max(1);

    if index_file.exists() {
        if args.overwrite {
            fs::remove_file(&index_file)
                .with_context(|| format!("failed to remove {}", index_file.display()))?;
        } else {
            eprintln!(
                "[index] index_file exists - skipped (use --overwrite): {}",
                index_file.display()
            );
            try_log_dataset(
                &parquet_dir,
                &dataset,
                "index",
                &format!("index_file exists skipped {}", index_file.display()),
            );
            ds.skipped = 1;
            report.datasets.push(ds);
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            return Ok(());
        }
    }

    let files = list_parquet_files(&corpus_dir)?;
    if files.is_empty() {
        bail!("no parquet files found under {}", corpus_dir.display());
    }
    try_log_dataset(
        &parquet_dir,
        &dataset,
        "index",
        &format!(
            "start files={} workers={} memory_mb={:?}",
            files.len(),
            tuning.workers,
            tuning.memory_mb
        ),
    );
    ds.items_scanned = files.len() as u64;

    let start = Instant::now();
    let temp_dir = PathBuf::from(format!("{}_tmp", index_file.to_string_lossy()));
    fs::create_dir_all(&temp_dir)?;
    let _ = fs::write(temp_dir.join(".metadata_never_index"), b"");

    let stage1 = make_progress_bar(args.progress, files.len() as u64, "index:stage1");

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(tuning.workers)
        .build()
        .context("failed to build rayon thread pool")?;

    let bin_arc = Arc::new(bin.clone());
    let temp_arc = Arc::new(temp_dir.clone());
    let base_arc = Arc::new(parquet_dir.clone());

    for (chunk_idx, chunk) in files.chunks(flush_every).enumerate() {
        let outcomes: Vec<(bool, Option<FailureEntry>)> = pool.install(|| {
            chunk
                .par_iter()
                .enumerate()
                .map(|(offset, pf)| {
                    let i = chunk_idx * flush_every + offset;
                    let out_file = temp_arc.join(format!("idx_{:05}.parquet", i + 1));
                    if out_file.exists() {
                        stage1.inc(1);
                        return (true, None);
                    }

                    let rel = match pf.strip_prefix(base_arc.as_path()) {
                        Ok(r) => r.to_path_buf(),
                        Err(e) => {
                            stage1.inc(1);
                            return (false, Some(FailureEntry {
                                dataset: dataset.clone(),
                                phase: "index_stage1".to_string(),
                                rel_path: None,
                                source_path: Some(pf.to_string_lossy().to_string()),
                                output_path: Some(out_file.to_string_lossy().to_string()),
                                error_message: format!("{e:#}"),
                                suggested_recovery: None,
                            }));
                        }
                    };
                    let rel_s = rel.to_string_lossy().replace('\\', "/");

                    let mut sql = String::new();
                    sql.push_str("SET preserve_insertion_order = false;");
                    sql.push_str("SET threads = 1;");
                    if let Some(mb) = tuning.memory_mb {
                        sql.push_str(&format!("SET memory_limit='{}MB';", mb));
                    }
                    sql.push_str(&format!(
                        "COPY (SELECT id, \
                         CAST(FLOOR(TRY_CAST(regexp_extract(CAST(id AS VARCHAR), '([0-9]+)$', 1) AS BIGINT) / 10000) AS INTEGER) AS id_block, \
                         '{}' AS parquet_file, \
                         file_row_number \
                         FROM read_parquet({}, file_row_number = true)) \
                         TO {} (FORMAT PARQUET, COMPRESSION SNAPPY);",
                        rel_s.replace('\'', "''"),
                        sql_quote(&pf.to_string_lossy()),
                        sql_quote(&out_file.to_string_lossy())
                    ));
                    stage1.inc(1);
                    match run_duckdb_sql(&bin_arc, &sql) {
                        Ok(()) => (false, None),
                        Err(e) => (false, Some(FailureEntry {
                            dataset: dataset.clone(),
                            phase: "index_stage1".to_string(),
                            rel_path: Some(rel_s),
                            source_path: Some(pf.to_string_lossy().to_string()),
                            output_path: Some(out_file.to_string_lossy().to_string()),
                            error_message: format!("{e:#}"),
                            suggested_recovery: Some("repair/reconvert parquet file and rerun index".to_string()),
                        })),
                    }
                })
                .collect()
        });
        for (skipped, failure) in outcomes {
            if skipped {
                ds.skipped += 1;
            }
            if let Some(f) = failure {
                ds.failed += 1;
                report.failures.push(f);
            }
        }
        ds.succeeded = ds.items_scanned.saturating_sub(ds.failed + ds.skipped);
        let mut preview = report.clone();
        preview.datasets = vec![ds.clone()];
        report_finalize(&mut preview);
        let _ = write_run_reports(&parquet_dir, &preview);
    }
    stage1.finish_with_message("index:stage1 complete");

    if ds.failed == 0 {
        let stage2 = make_progress_bar(args.progress, 1, "index:stage2");
        let combine_sql = format!(
            "COPY (SELECT * FROM read_parquet({})) TO {} (FORMAT PARQUET, COMPRESSION SNAPPY);",
            sql_quote(&temp_dir.join("*.parquet").to_string_lossy()),
            sql_quote(&index_file.to_string_lossy())
        );
        if let Err(e) = run_duckdb_sql(&bin, &combine_sql) {
            ds.failed += 1;
            report.failures.push(FailureEntry {
                dataset: dataset.clone(),
                phase: "index_stage2".to_string(),
                rel_path: None,
                source_path: Some(temp_dir.to_string_lossy().to_string()),
                output_path: Some(index_file.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("fix stage1 failures and rerun index".to_string()),
            });
        } else {
            stage2.inc(1);
            stage2.finish_with_message("index:stage2 complete");
        }
    } else {
        report.failures.push(FailureEntry {
            dataset: dataset.clone(),
            phase: "index_stage2".to_string(),
            rel_path: None,
            source_path: Some(temp_dir.to_string_lossy().to_string()),
            output_path: Some(index_file.to_string_lossy().to_string()),
            error_message: format!("stage1 failed for {} shard(s), stage2 skipped", ds.failed),
            suggested_recovery: Some("fix/reconvert failed files and rerun index".to_string()),
        });
    }

    fs::remove_dir_all(&temp_dir)
        .with_context(|| format!("failed to remove temp dir {}", temp_dir.display()))?;

    let sz = fs::metadata(&index_file).map(|m| m.len()).unwrap_or(0);
    eprintln!(
        "[index] done in {:.1}s, size {:.2} MB, file={}",
        start.elapsed().as_secs_f64(),
        (sz as f64) / (1024.0 * 1024.0),
        index_file.display()
    );
    try_log_dataset(
        &parquet_dir,
        &dataset,
        "index",
        &format!(
            "done elapsed_s={:.2} size_mb={:.2} file={}",
            start.elapsed().as_secs_f64(),
            (sz as f64) / (1024.0 * 1024.0),
            index_file.display()
        ),
    );
    ds.succeeded = ds.items_scanned.saturating_sub(ds.failed + ds.skipped);
    report.datasets = vec![ds];
    report_finalize(&mut report);
    let report_paths = write_run_reports(&parquet_dir, &report)?;
    eprintln!(
        "[index] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[index] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn run_extract(args: ExtractArgs) -> Result<()> {
    let bin = duckdb_bin(&args.shared);
    let parquet_dir = args.shared.parquet_dir.clone();
    let tuning = light_tuning_with_override(args.shared.workers, args.max_memory_mb);
    if args.explain {
        explain_extract(&args, &bin, &tuning);
        return Ok(());
    }

    let _ = cleanup_command_reports(&parquet_dir, "extract");
    let _ = cleanup_command_dataset_logs(&parquet_dir, "extract");
    let _lock = acquire_lock(&parquet_dir, "extract")?;

    let inputs = read_extract_ids(&args.ids)?;
    let allowed_dataset = if args.shared.dataset == "all" {
        None
    } else {
        Some(args.shared.dataset.clone())
    };

    let mut unknown_rows: Vec<(String, String, String)> = Vec::new();
    let mut dataset_ids: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for inp in inputs {
        let Some(ds) = inp.dataset.clone() else {
            unknown_rows.push((
                inp.raw,
                inp.normalized,
                "unmapped id format/prefix".to_string(),
            ));
            continue;
        };
        if let Some(only) = &allowed_dataset {
            if &ds != only {
                unknown_rows.push((
                    inp.raw,
                    inp.normalized,
                    format!("filtered by --dataset={only}"),
                ));
                continue;
            }
        }
        // Use canonical (full-URL) form so it matches what the index stores.
        dataset_ids.entry(ds).or_default().insert(inp.canonical);
    }

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        args.shared.root_dir.to_string_lossy().to_string(),
    );
    report_args.insert("ids".to_string(), args.ids.to_string_lossy().to_string());
    report_args.insert(
        "output".to_string(),
        args.output.to_string_lossy().to_string(),
    );
    report_args.insert("dataset".to_string(), args.shared.dataset.clone());
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert("memory_mb".to_string(), format!("{:?}", tuning.memory_mb));
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("extract", report_args);
    let flush_every = args.state_flush_every.max(1);
    let ds_names: Vec<String> = dataset_ids.keys().cloned().collect();
    let pb = make_progress_bar(args.progress, ds_names.len() as u64, "extract");

    let output_base = args.output.clone();
    let output_parent = output_base
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&output_parent)
        .with_context(|| format!("failed to create output dir {}", output_parent.display()))?;

    let reports_dir = global_reports_dir(&parquet_dir);
    fs::create_dir_all(&reports_dir)?;
    let ts = now_unix();
    let unknown_report_path = reports_dir.join(format!("extract-unknown-ids-{}.csv", ts));
    let missing_report_path = reports_dir.join(format!("extract-missing-ids-{}.csv", ts));
    let mut missing_rows: Vec<(String, String)> = Vec::new();

    for (i, (dataset, ids)) in dataset_ids.iter().enumerate() {
        if i > 0 && i % flush_every == 0 {
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            // Continue updating report in-memory after checkpoint write.
            report.finished_at_unix = None;
            report.duration_seconds = None;
        }
        let mut ds = DatasetReportSummary {
            dataset: dataset.clone(),
            items_scanned: ids.len() as u64,
            ..Default::default()
        };
        try_log_dataset(
            &parquet_dir,
            dataset,
            "extract",
            &format!(
                "start ids={} workers={} memory_mb={:?}",
                ids.len(),
                tuning.workers,
                tuning.memory_mb
            ),
        );

        let index_file = parquet_dir.join(format!("{dataset}_id_idx.parquet"));
        if !index_file.exists() {
            ds.failed += ds.items_scanned.max(1);
            report.datasets.push(ds);
            report.failures.push(FailureEntry {
                dataset: dataset.clone(),
                phase: "extract_index_read".to_string(),
                rel_path: None,
                source_path: Some(index_file.to_string_lossy().to_string()),
                output_path: None,
                error_message: "index file missing".to_string(),
                suggested_recovery: Some(format!(
                    "run openalex-snapshot index --root-dir {} --dataset {}",
                    args.shared.root_dir.display(),
                    dataset
                )),
            });
            pb.inc(1);
            continue;
        }

        let req_ids_tmp = reports_dir.join(format!("extract-req-{}-{}.csv", dataset, ts));
        {
            let mut wtr = csv::Writer::from_path(&req_ids_tmp)?;
            wtr.write_record(["id"])?;
            for id in ids {
                wtr.write_record([id])?;
            }
            wtr.flush()?;
        }

        let idx_sql = with_session_settings(
            &format!(
                "SELECT DISTINCT CAST(i.id AS VARCHAR) AS id, i.parquet_file AS parquet_file \
                 FROM read_parquet({}) i \
                 INNER JOIN read_csv_auto({}, HEADER=true, ALL_VARCHAR=true) r \
                 ON CAST(i.id AS VARCHAR)=CAST(r.id AS VARCHAR);",
                sql_quote(&index_file.to_string_lossy()),
                sql_quote(&req_ids_tmp.to_string_lossy())
            ),
            tuning.memory_mb,
            Some(1),
        );
        let idx_rows = match run_duckdb_csv(&bin, &idx_sql) {
            Ok(v) => v,
            Err(e) => {
                ds.failed += ds.items_scanned.max(1);
                report.datasets.push(ds);
                report.failures.push(FailureEntry {
                    dataset: dataset.clone(),
                    phase: "extract_index_read".to_string(),
                    rel_path: None,
                    source_path: Some(index_file.to_string_lossy().to_string()),
                    output_path: None,
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some("rebuild index and retry extract".to_string()),
                });
                let _ = fs::remove_file(&req_ids_tmp);
                pb.inc(1);
                continue;
            }
        };
        let mut matched_ids = BTreeSet::<String>::new();
        let mut dataset_files = BTreeSet::<PathBuf>::new();
        for row in idx_rows {
            if let Some(id) = row.get("id") {
                matched_ids.insert(id.clone());
            }
            if let Some(rel_file) = row.get("parquet_file") {
                dataset_files.insert(parquet_dir.join(rel_file));
            }
        }
        let missing: Vec<String> = ids
            .iter()
            .filter(|id| !matched_ids.contains(*id))
            .cloned()
            .collect();
        for id in &missing {
            missing_rows.push((dataset.clone(), id.clone()));
        }
        ds.skipped = missing.len() as u64;

        let out_path = extract_output_path(&output_base, dataset);
        if matched_ids.is_empty() {
            ds.succeeded = 0;
            report.datasets.push(ds);
            let _ = fs::remove_file(&req_ids_tmp);
            pb.inc(1);
            continue;
        }

        let matched_tmp = reports_dir.join(format!("extract-match-{}-{}.csv", dataset, ts));
        {
            let mut wtr = csv::Writer::from_path(&matched_tmp)?;
            wtr.write_record(["id"])?;
            for id in &matched_ids {
                wtr.write_record([id])?;
            }
            wtr.flush()?;
        }
        let file_list_sql = format!(
            "[{}]",
            dataset_files
                .iter()
                .map(|p| sql_quote(&p.to_string_lossy()))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let copy_sql = with_session_settings(
            &format!(
                "COPY (SELECT s.* \
                 FROM read_parquet({}) s \
                 INNER JOIN read_csv_auto({}, HEADER=true, ALL_VARCHAR=true) m \
                 ON CAST(s.id AS VARCHAR)=CAST(m.id AS VARCHAR)) \
                 TO {} (FORMAT PARQUET, COMPRESSION SNAPPY);",
                file_list_sql,
                sql_quote(&matched_tmp.to_string_lossy()),
                sql_quote(&out_path.to_string_lossy())
            ),
            tuning.memory_mb,
            Some(1),
        );
        if let Err(e) = run_duckdb_sql(&bin, &copy_sql) {
            ds.failed += matched_ids.len() as u64;
            report.failures.push(FailureEntry {
                dataset: dataset.clone(),
                phase: "extract_write_output".to_string(),
                rel_path: None,
                source_path: Some(index_file.to_string_lossy().to_string()),
                output_path: Some(out_path.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("verify parquet/index and retry extract".to_string()),
            });
        } else {
            ds.succeeded = matched_ids.len() as u64;
            try_log_dataset(
                &parquet_dir,
                dataset,
                "extract",
                &format!(
                    "done output={} matched={} missing={}",
                    out_path.display(),
                    ds.succeeded,
                    ds.skipped
                ),
            );
        }
        report.datasets.push(ds);
        let _ = fs::remove_file(&req_ids_tmp);
        let _ = fs::remove_file(&matched_tmp);
        pb.inc(1);
    }
    pb.finish_with_message("extract complete");

    if !unknown_rows.is_empty() {
        let mut wtr = csv::Writer::from_path(&unknown_report_path)?;
        wtr.write_record(["raw_id", "normalized_id", "reason"])?;
        for (raw, norm, reason) in &unknown_rows {
            wtr.write_record([raw, norm, reason])?;
        }
        wtr.flush()?;
    }
    if !missing_rows.is_empty() {
        let mut wtr = csv::Writer::from_path(&missing_report_path)?;
        wtr.write_record(["dataset", "id"])?;
        for (dataset, id) in &missing_rows {
            wtr.write_record([dataset, id])?;
        }
        wtr.flush()?;
    }
    if !unknown_rows.is_empty() {
        report.args.insert(
            "unknown_report".to_string(),
            unknown_report_path.to_string_lossy().to_string(),
        );
    }
    if !missing_rows.is_empty() {
        report.args.insert(
            "missing_report".to_string(),
            missing_report_path.to_string_lossy().to_string(),
        );
    }
    report
        .args
        .insert("unknown_ids".to_string(), unknown_rows.len().to_string());
    report
        .args
        .insert("missing_ids".to_string(), missing_rows.len().to_string());
    report_finalize(&mut report);
    let report_paths = write_run_reports(&parquet_dir, &report)?;
    eprintln!(
        "[extract] summary scanned={} ok={} failed={} skipped={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report.totals_skipped,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[extract] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn read_extract_ids(path: &Path) -> Result<Vec<ExtractInput>> {
    let mut rdr = csv::Reader::from_path(path)
        .with_context(|| format!("cannot read ids CSV {}", path.display()))?;
    let headers = rdr.headers()?.clone();
    let id_idx = headers
        .iter()
        .position(|h| matches!(h, "id" | "openalex_id" | "work_id"))
        .unwrap_or(0);

    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        let raw = rec.get(id_idx).unwrap_or_default().trim().to_string();
        if raw.is_empty() {
            continue;
        }
        let normalized = normalize_openalex_id(&raw);
        let canonical = canonical_openalex_id(&raw);
        let dataset = extract_dataset_from_id(&normalized);
        out.push(ExtractInput {
            raw,
            normalized,
            canonical,
            dataset,
        });
    }
    Ok(out)
}

fn normalize_openalex_id(id: &str) -> String {
    let t = id.trim().trim_matches('"').trim_matches('\'').to_string();
    if let Some(rest) = t.strip_prefix("https://openalex.org/") {
        return rest.trim_matches('/').to_string();
    }
    if let Some(rest) = t.strip_prefix("http://openalex.org/") {
        return rest.trim_matches('/').to_string();
    }
    t.trim_matches('/').to_string()
}

/// Return the canonical full-URL form of an OpenAlex ID, matching what the index stores.
/// Short IDs (e.g. "W1234") are expanded to "https://openalex.org/W1234".
fn canonical_openalex_id(raw: &str) -> String {
    let t = raw.trim().trim_matches('"').trim_matches('\'');
    if t.starts_with("https://openalex.org/") || t.starts_with("http://openalex.org/") {
        t.to_string()
    } else {
        format!("https://openalex.org/{}", t.trim_matches('/'))
    }
}

fn extract_dataset_from_id(id: &str) -> Option<String> {
    let t = id.trim();
    if t.is_empty() {
        return None;
    }
    if t.contains('/') {
        let ns = t.split('/').next()?.to_ascii_lowercase();
        let ds = match ns.as_str() {
            "institution-types" => "institution-types",
            "work-types" => "work-types",
            "source-types" => "source-types",
            "licenses" => "licenses",
            "countries" => "countries",
            "continents" => "continents",
            "languages" => "languages",
            "domains" => "domains",
            "fields" => "fields",
            "subfields" => "subfields",
            "sdgs" => "sdgs",
            _ => return None,
        };
        return Some(ds.to_string());
    }
    let first = t.chars().next()?.to_ascii_uppercase();
    let ds = match first {
        'W' => "works",
        'A' => "authors",
        'S' => "sources",
        'I' => "institutions",
        'T' => "topics",
        'K' => "keywords",
        'P' => "publishers",
        'F' => "funders",
        'G' => "awards",
        'C' => "concepts",
        _ => return None,
    };
    Some(ds.to_string())
}

fn extract_output_path(base: &Path, dataset: &str) -> PathBuf {
    let parent = base.parent().map(Path::to_path_buf).unwrap_or_default();
    let stem = base
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("extract");
    parent.join(format!("{stem}_{dataset}.parquet"))
}

fn run_verify(args: VerifyArgs) -> Result<()> {
    let datasets = resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?;
    let duckdb_bin = duckdb_bin(&args.shared);
    let tuning = light_tuning_with_override(args.shared.workers, args.max_memory_mb);
    if args.explain {
        explain_verify(&args, &datasets, &duckdb_bin, &tuning);
        return Ok(());
    }
    let _lock = acquire_lock(&args.shared.parquet_dir, "verify")?;
    let _ = archive_completed_run(&args.shared.parquet_dir, &args.shared.snapshot_dir);
    let _ = cleanup_command_reports(&args.shared.parquet_dir, "verify_convert");
    let _ = cleanup_command_dataset_logs(&args.shared.parquet_dir, "verify");
    let mut report_args = BTreeMap::new();
    report_args.insert("dataset".to_string(), args.shared.dataset.clone());
    report_args.insert("scope".to_string(), format!("{:?}", args.scope));
    report_args.insert(
        "metadata_level".to_string(),
        format!("{:?}", args.metadata_level),
    );
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert("memory_mb".to_string(), format!("{:?}", tuning.memory_mb));
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("verify_convert", report_args);

    let flush_every = args.state_flush_every.max(1);

    for dataset in &datasets {
        let dataset_start = Instant::now();
        let mut ds = DatasetReportSummary {
            dataset: dataset.clone(),
            ..Default::default()
        };
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "verify",
            &format!(
                "start scope={:?} file_sample_n={} metadata_level={:?}",
                args.scope, args.file_sample_n, args.metadata_level
            ),
        );
        let pairs = enumerate_pairs(&args.shared.snapshot_dir, &args.shared.parquet_dir, dataset)?;
        if pairs.is_empty() {
            try_log_dataset(
                &args.shared.parquet_dir,
                dataset,
                "verify",
                "no file pairs found",
            );
            report.datasets.push(ds);
            continue;
        }
        let mut source_metrics = load_source_metrics_cache(&args.shared.parquet_dir, dataset)?;
        let mut parquet_metrics = load_parquet_metrics_cache(&args.shared.parquet_dir, dataset)?;

        match args.scope {
            VerifyScope::File => {
                let mut candidates: Vec<&FilePair> = pairs.iter().collect();
                let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
                candidates.shuffle(&mut rng);
                let n = args.file_sample_n.min(candidates.len());
                let sample = &candidates[..n];
                ds.items_scanned = sample.len() as u64;

                let pb = make_progress_bar(
                    args.progress,
                    sample.len() as u64,
                    &format!("verify-file:{dataset}"),
                );
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(tuning.workers)
                    .build()
                    .context("failed to build rayon thread pool")?;
                let source_metrics_arc = Arc::new(std::sync::Mutex::new(source_metrics));
                let parquet_metrics_arc = Arc::new(std::sync::Mutex::new(parquet_metrics));

                for chunk in sample.chunks(flush_every) {
                    let failures: Vec<Option<FailureEntry>> = pool.install(|| {
                        chunk
                            .par_iter()
                            .map(|p| {
                                if !p.output_parquet.exists() {
                                    pb.inc(1);
                                    return Some(FailureEntry {
                                        dataset: dataset.clone(),
                                        phase: "verify_metrics".to_string(),
                                        rel_path: Some(p.rel.to_string_lossy().to_string()),
                                        source_path: Some(p.input_gz.to_string_lossy().to_string()),
                                        output_path: Some(
                                            p.output_parquet.to_string_lossy().to_string(),
                                        ),
                                        error_message: "missing parquet file".to_string(),
                                        suggested_recovery: Some(
                                            "re-run convert for this file".to_string(),
                                        ),
                                    });
                                }
                                let out = verify_file_metrics(
                                    &duckdb_bin,
                                    p,
                                    args.metadata_level.clone(),
                                    &source_metrics_arc,
                                    &parquet_metrics_arc,
                                    tuning.memory_mb,
                                );
                                pb.inc(1);
                                match out {
                                    Ok(()) => None,
                                    Err(e) => Some(FailureEntry {
                                        dataset: dataset.clone(),
                                        phase: "verify_metrics".to_string(),
                                        rel_path: Some(p.rel.to_string_lossy().to_string()),
                                        source_path: Some(p.input_gz.to_string_lossy().to_string()),
                                        output_path: Some(
                                            p.output_parquet.to_string_lossy().to_string(),
                                        ),
                                        error_message: format!("{e:#}"),
                                        suggested_recovery: Some(
                                            "reconvert file and re-run verify".to_string(),
                                        ),
                                    }),
                                }
                            })
                            .collect()
                    });
                    for f in failures.into_iter().flatten() {
                        ds.failed += 1;
                        report.failures.push(f);
                    }
                    ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
                    source_metrics = source_metrics_arc
                        .lock()
                        .map_err(|_| anyhow!("verify metrics cache lock poisoned"))?
                        .clone();
                    parquet_metrics = parquet_metrics_arc
                        .lock()
                        .map_err(|_| anyhow!("parquet metrics cache lock poisoned"))?
                        .clone();
                    save_source_metrics_cache(&args.shared.parquet_dir, dataset, &source_metrics)?;
                    save_parquet_metrics_cache(
                        &args.shared.parquet_dir,
                        dataset,
                        &parquet_metrics,
                    )?;
                    let mut preview = report.clone();
                    preview.datasets.retain(|d| d.dataset != *dataset);
                    preview.datasets.push(ds.clone());
                    report_finalize(&mut preview);
                    let _ = write_run_reports(&args.shared.parquet_dir, &preview);
                }
                pb.finish_with_message(format!("verify-file:{dataset} ok"));
            }
            VerifyScope::Dataset | VerifyScope::Snapshot => {
                let mut missing = Vec::new();
                for p in &pairs {
                    if !p.output_parquet.exists() {
                        missing.push(p.rel.clone());
                    }
                }
                if !missing.is_empty() {
                    for rel in missing {
                        ds.failed += 1;
                        report.failures.push(FailureEntry {
                            dataset: dataset.clone(),
                            phase: "structure".to_string(),
                            rel_path: Some(rel.to_string_lossy().to_string()),
                            source_path: None,
                            output_path: Some(
                                args.shared
                                    .parquet_dir
                                    .join(dataset)
                                    .join(rel.with_extension("parquet"))
                                    .to_string_lossy()
                                    .to_string(),
                            ),
                            error_message: "missing parquet file".to_string(),
                            suggested_recovery: Some(
                                "re-run convert for missing files".to_string(),
                            ),
                        });
                    }
                }

                // Build expected parquet set: for each source gz, if split chunks exist
                // use all matching NNN chunks; otherwise expect the single parquet.
                let ds_parquet_root = args.shared.parquet_dir.join(dataset);
                let actual_rel: BTreeSet<PathBuf> = list_parquet_rel(&ds_parquet_root)?;
                let mut expected_rel: BTreeSet<PathBuf> = BTreeSet::new();
                for p in &pairs {
                    let single = p.rel.with_extension("parquet");
                    let stem = single
                        .file_stem()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    let parent = single.parent().unwrap_or(Path::new(""));
                    // Collect any split chunks that exist in the actual set.
                    let chunks: Vec<PathBuf> = actual_rel
                        .iter()
                        .filter(|r| {
                            r.parent().unwrap_or(Path::new("")) == parent
                                && r.file_stem()
                                    .and_then(|s| s.to_str())
                                    .map(|s| {
                                        s.starts_with(&format!("{stem}_"))
                                            && s.len() == stem.len() + 4
                                            && s[stem.len() + 1..].parse::<u32>().is_ok()
                                    })
                                    .unwrap_or(false)
                        })
                        .cloned()
                        .collect();
                    if !chunks.is_empty() {
                        expected_rel.extend(chunks);
                    } else {
                        expected_rel.insert(single);
                    }
                }
                if expected_rel != actual_rel {
                    let extra = actual_rel.difference(&expected_rel).count();
                    let miss = expected_rel.difference(&actual_rel).count();
                    report.failures.push(FailureEntry {
                        dataset: dataset.clone(),
                        phase: "structure".to_string(),
                        rel_path: None,
                        source_path: None,
                        output_path: Some(
                            args.shared
                                .parquet_dir
                                .join(dataset)
                                .to_string_lossy()
                                .to_string(),
                        ),
                        error_message: format!(
                            "structure mismatch (missing={miss}, extra={extra})"
                        ),
                        suggested_recovery: Some(
                            "re-run convert and clean unexpected parquet files".to_string(),
                        ),
                    });
                    ds.failed += 1;
                }

                ds.items_scanned = pairs.len() as u64;
                let pb = make_progress_bar(
                    args.progress,
                    pairs.len() as u64,
                    &format!("verify-count:{dataset}"),
                );
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(tuning.workers)
                    .build()
                    .context("failed to build rayon thread pool")?;
                let source_metrics_arc = Arc::new(std::sync::Mutex::new(source_metrics));
                let parquet_metrics_arc = Arc::new(std::sync::Mutex::new(parquet_metrics));

                for chunk in pairs.chunks(flush_every) {
                    let failures: Vec<Option<FailureEntry>> = pool.install(|| {
                        chunk
                            .par_iter()
                            .map(|p| {
                                let out = verify_file_metrics(
                                    &duckdb_bin,
                                    p,
                                    args.metadata_level.clone(),
                                    &source_metrics_arc,
                                    &parquet_metrics_arc,
                                    tuning.memory_mb,
                                );
                                pb.inc(1);
                                match out {
                                    Ok(()) => None,
                                    Err(e) => Some(FailureEntry {
                                        dataset: dataset.clone(),
                                        phase: "verify_metrics".to_string(),
                                        rel_path: Some(p.rel.to_string_lossy().to_string()),
                                        source_path: Some(p.input_gz.to_string_lossy().to_string()),
                                        output_path: Some(
                                            p.output_parquet.to_string_lossy().to_string(),
                                        ),
                                        error_message: format!("{e:#}"),
                                        suggested_recovery: Some(
                                            "reconvert file and re-run verify".to_string(),
                                        ),
                                    }),
                                }
                            })
                            .collect()
                    });
                    for f in failures.into_iter().flatten() {
                        ds.failed += 1;
                        report.failures.push(f);
                    }
                    ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
                    source_metrics = source_metrics_arc
                        .lock()
                        .map_err(|_| anyhow!("verify metrics cache lock poisoned"))?
                        .clone();
                    parquet_metrics = parquet_metrics_arc
                        .lock()
                        .map_err(|_| anyhow!("parquet metrics cache lock poisoned"))?
                        .clone();
                    save_source_metrics_cache(&args.shared.parquet_dir, dataset, &source_metrics)?;
                    save_parquet_metrics_cache(
                        &args.shared.parquet_dir,
                        dataset,
                        &parquet_metrics,
                    )?;
                    let mut preview = report.clone();
                    preview.datasets.retain(|d| d.dataset != *dataset);
                    preview.datasets.push(ds.clone());
                    report_finalize(&mut preview);
                    let _ = write_run_reports(&args.shared.parquet_dir, &preview);
                }
                pb.finish_with_message(format!("verify-count:{dataset} ok"));
            }
        }
        ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
        report.datasets.push(ds.clone());
        try_log_dataset(
            &args.shared.parquet_dir,
            dataset,
            "verify",
            &format!(
                "done pairs={} elapsed_s={:.2}",
                pairs.len(),
                dataset_start.elapsed().as_secs_f64()
            ),
        );
        report_finalize(&mut report);
        let _ = write_run_reports(&args.shared.parquet_dir, &report);
    }
    report_finalize(&mut report);
    let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;
    eprintln!(
        "[verify] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!(
            "[verify] failures detected: {} files/conditions failed",
            report.totals_failed
        );
    }
    Ok(())
}

fn run_schema(args: SchemaArgs) -> Result<()> {
    let datasets = resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?;
    let duckdb_bin = duckdb_bin(&args.shared);
    let tuning = light_tuning_with_override(args.shared.workers, args.max_memory_mb);
    if args.explain {
        explain_schema(&args, &datasets, &duckdb_bin, &tuning);
        return Ok(());
    }
    let _ = cleanup_command_reports(&args.shared.parquet_dir, "schema");
    let _ = cleanup_command_dataset_logs(&args.shared.parquet_dir, "schema");
    let mut report_args = BTreeMap::new();
    report_args.insert("dataset".to_string(), args.shared.dataset.clone());
    report_args.insert("from".to_string(), format!("{:?}", args.from));
    report_args.insert("format".to_string(), format!("{:?}", args.format));
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert("memory_mb".to_string(), format!("{:?}", tuning.memory_mb));
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("schema", report_args);

    for dataset in datasets {
        let dataset_start = Instant::now();
        let mut ds = DatasetReportSummary {
            dataset: dataset.clone(),
            items_scanned: 1,
            ..Default::default()
        };
        try_log_dataset(
            &args.shared.parquet_dir,
            &dataset,
            "schema",
            &format!("start from={:?} format={:?}", args.from, args.format),
        );
        let schema = match load_schema_by_policy(
            &duckdb_bin,
            &args.shared.snapshot_dir,
            &args.shared.parquet_dir,
            &dataset,
            args.from.clone(),
            args.sample_size,
            args.refresh_cache,
            tuning.memory_mb,
            tuning.workers,
            args.state_flush_every,
        ) {
            Ok(s) => s,
            Err(e) => {
                ds.failed = 1;
                report.failures.push(FailureEntry {
                    dataset: dataset.clone(),
                    phase: "schema_load".to_string(),
                    rel_path: None,
                    source_path: Some(
                        args.shared
                            .snapshot_dir
                            .join("data")
                            .join(&dataset)
                            .to_string_lossy()
                            .to_string(),
                    ),
                    output_path: Some(
                        args.shared
                            .parquet_dir
                            .join(&dataset)
                            .to_string_lossy()
                            .to_string(),
                    ),
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some(
                        "check schema cache/source availability and retry".to_string(),
                    ),
                });
                report.datasets.push(ds);
                report_finalize(&mut report);
                let _ = write_run_reports(&args.shared.parquet_dir, &report);
                continue;
            }
        };

        let rendered = if let Some(diff_with) = &args.diff_with {
            let right = match load_schema_by_policy(
                &duckdb_bin,
                &args.shared.snapshot_dir,
                &args.shared.parquet_dir,
                &dataset,
                diff_with.clone(),
                args.sample_size,
                args.refresh_cache,
                tuning.memory_mb,
                tuning.workers,
                args.state_flush_every,
            ) {
                Ok(v) => v,
                Err(e) => {
                    ds.failed = 1;
                    report.failures.push(FailureEntry {
                        dataset: dataset.clone(),
                        phase: "schema_diff_rhs".to_string(),
                        rel_path: None,
                        source_path: None,
                        output_path: None,
                        error_message: format!("{e:#}"),
                        suggested_recovery: Some(
                            "retry without --diff-with or fix right-hand source".to_string(),
                        ),
                    });
                    report.datasets.push(ds);
                    report_finalize(&mut report);
                    let _ = write_run_reports(&args.shared.parquet_dir, &report);
                    continue;
                }
            };
            render_schema_diff(&schema, &right, &args.format)?
        } else {
            match args.format {
                SchemaFormat::Table => render_table(&schema),
                SchemaFormat::Json => serde_json::to_string_pretty(&schema)?,
                SchemaFormat::Yaml => serde_yaml::to_string(&schema)?,
                SchemaFormat::ArrowR => {
                    let value = serde_json::json!({
                        "dataset": schema.dataset,
                        "source": schema.source,
                        "schema": {
                            "fields": schema.fields,
                            "metadata": schema.metadata,
                        }
                    });
                    serde_json::to_string_pretty(&value)?
                }
            }
        };

        if let Some(base) = &args.output {
            let out = if datasets_len_hint(&args.shared.dataset) > 1 {
                base.join(format!(
                    "{}_schema.{}",
                    dataset,
                    extension_for_format(&args.format)
                ))
            } else {
                base.clone()
            };
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)?;
            }
            if let Err(e) = fs::write(&out, rendered.as_bytes()) {
                ds.failed = 1;
                report.failures.push(FailureEntry {
                    dataset: dataset.clone(),
                    phase: "schema_output".to_string(),
                    rel_path: None,
                    source_path: None,
                    output_path: Some(out.to_string_lossy().to_string()),
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some("check output path permissions".to_string()),
                });
            } else {
                eprintln!("wrote schema: {}", out.display());
                ds.succeeded = 1;
            }
            try_log_dataset(
                &args.shared.parquet_dir,
                &dataset,
                "schema",
                &format!("wrote output {}", out.display()),
            );
        } else {
            println!("# dataset={dataset}\n{rendered}");
            ds.succeeded = 1;
            try_log_dataset(
                &args.shared.parquet_dir,
                &dataset,
                "schema",
                "wrote output stdout",
            );
        }
        try_log_dataset(
            &args.shared.parquet_dir,
            &dataset,
            "schema",
            &format!(
                "done fields={} elapsed_s={:.2}",
                schema.fields.len(),
                dataset_start.elapsed().as_secs_f64()
            ),
        );
        if ds.failed > 0 {
            ds.succeeded = 0;
        } else if ds.succeeded == 0 {
            ds.succeeded = 1;
        }
        report.datasets.push(ds);
        report_finalize(&mut report);
        let _ = write_run_reports(&args.shared.parquet_dir, &report);
    }
    report_finalize(&mut report);
    let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;
    eprintln!(
        "[schema] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[schema] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn run_verify_schema(args: VerifySchemaArgs) -> Result<()> {
    let datasets = resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?;
    let duckdb_bin = duckdb_bin(&args.shared);
    let tuning = light_tuning_with_override(args.shared.workers, args.max_memory_mb);
    if args.explain {
        println!(
            "--explain: verify_schema from={:?} diff_with={:?} datasets={} sample_size={} refresh_cache={}",
            args.from,
            args.diff_with,
            datasets.join(","),
            args.sample_size,
            args.refresh_cache
        );
        return Ok(());
    }
    let mut failures = 0usize;
    for dataset in datasets {
        let left = load_schema_by_policy(
            &duckdb_bin,
            &args.shared.snapshot_dir,
            &args.shared.parquet_dir,
            &dataset,
            args.from.clone(),
            args.sample_size,
            args.refresh_cache,
            tuning.memory_mb,
            tuning.workers,
            args.state_flush_every,
        )?;
        let right = load_schema_by_policy(
            &duckdb_bin,
            &args.shared.snapshot_dir,
            &args.shared.parquet_dir,
            &dataset,
            args.diff_with.clone(),
            args.sample_size,
            args.refresh_cache,
            tuning.memory_mb,
            tuning.workers,
            args.state_flush_every,
        )?;

        let left_map: BTreeMap<String, String> = left
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.r#type.clone()))
            .collect();
        let right_map: BTreeMap<String, String> = right
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.r#type.clone()))
            .collect();
        let left_names: BTreeSet<String> = left_map.keys().cloned().collect();
        let right_names: BTreeSet<String> = right_map.keys().cloned().collect();
        let added = right_names.difference(&left_names).count();
        let removed = left_names.difference(&right_names).count();
        let changed = left_names
            .intersection(&right_names)
            .filter(|n| left_map.get(*n) != right_map.get(*n))
            .count();

        if added + removed + changed == 0 {
            eprintln!(
                "[verify_schema] dataset={} status=ok from={:?} diff_with={:?}",
                dataset, args.from, args.diff_with
            );
        } else {
            failures += 1;
            eprintln!(
                "[verify_schema] dataset={} status=failed added={} removed={} changed_types={} from={:?} diff_with={:?}",
                dataset, added, removed, changed, args.from, args.diff_with
            );
        }
    }
    if failures > 0 {
        bail!(
            "[verify_schema] schema differences detected in {} dataset(s)",
            failures
        );
    }
    Ok(())
}

fn explain_repair(
    args: &RepairArgs,
    verify_report_path: &Path,
    datasets: &[String],
    duckdb_bin: &Path,
    tuning: &Tuning,
) -> Result<()> {
    let txt = fs::read_to_string(verify_report_path).with_context(|| {
        format!(
            "failed to read verify report {}",
            verify_report_path.display()
        )
    })?;
    let verify_report: RunReport = serde_json::from_str(&txt).with_context(|| {
        format!(
            "failed to parse verify report {}",
            verify_report_path.display()
        )
    })?;
    let targets = collect_repair_targets(
        &verify_report,
        &datasets.iter().cloned().collect::<BTreeSet<_>>(),
        &args.shared.snapshot_dir,
        &args.shared.parquet_dir,
    );
    println!("--explain: repair_convert");
    println!("duckdb_bin: {}", duckdb_bin.display());
    println!("snapshot_dir: {}", args.shared.snapshot_dir.display());
    println!("parquet_dir: {}", args.shared.parquet_dir.display());
    println!("from_verify_report: {}", verify_report_path.display());
    println!("datasets filter: {}", datasets.join(", "));
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
    println!("state_flush_every: {}", args.state_flush_every);
    println!("selected_files: {}", targets.len());
    println!("post_verify: targeted file-level verify enabled");
    Ok(())
}

fn resolve_verify_report_path(args: &RepairArgs) -> Result<PathBuf> {
    if let Some(p) = &args.from_verify_report {
        return Ok(p.clone());
    }
    let reports_dir = global_reports_dir(&args.shared.parquet_dir);
    if let Some(p) = latest_report_for_command(&args.shared.parquet_dir, "verify_convert") {
        return Ok(p);
    }
    if let Some(p) = latest_report_for_command(&args.shared.parquet_dir, "convert") {
        eprintln!(
            "[repair] no verify_convert report found; using convert report: {}",
            p.display()
        );
        return Ok(p);
    }
    bail!(
        "no verify_convert or convert report found under {}; run convert or verify_convert first, or pass --from-verify-report",
        reports_dir.display()
    )
}

fn run_repair(args: RepairArgs, profiles_config: Option<&Path>) -> Result<()> {
    let duckdb_bin = duckdb_bin(&args.shared);
    let datasets = resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?;
    let allowed: BTreeSet<String> = datasets.iter().cloned().collect();
    let total_mb = detect_total_memory_mb();
    let profile_registry =
        ProfileRegistry::load(discover_profiles_config(profiles_config).as_deref())?;
    let resolved_profile = profile_registry
        .get(&args.profile)
        .ok_or_else(|| profile_registry.unknown_profile_error(&args.profile))?
        .clone();
    let tuning = representative_tuning(
        &args.profile,
        &resolved_profile,
        args.shared.workers,
        args.max_memory_mb,
        total_mb,
    );
    let verify_report_path = resolve_verify_report_path(&args)?;

    if args.explain {
        explain_repair(&args, &verify_report_path, &datasets, &duckdb_bin, &tuning)?;
        return Ok(());
    }
    let _lock = acquire_lock(&args.shared.parquet_dir, "repair")?;
    // Do NOT archive here — repair reads an existing verify report
    let _ = cleanup_command_reports(&args.shared.parquet_dir, "repair_convert");
    let _ = cleanup_command_dataset_logs(&args.shared.parquet_dir, "repair_convert");

    let mut report_args = BTreeMap::new();
    report_args.insert("dataset".to_string(), args.shared.dataset.clone());
    report_args.insert(
        "from_verify_report".to_string(),
        verify_report_path.to_string_lossy().to_string(),
    );
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert("memory_mb".to_string(), format!("{:?}", tuning.memory_mb));
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("repair_convert", report_args);
    let flush_every = args.state_flush_every.max(1);

    let verify_report: RunReport = match fs::read_to_string(&verify_report_path)
        .with_context(|| {
            format!(
                "failed to read verify report {}",
                verify_report_path.display()
            )
        })
        .and_then(|txt| {
            serde_json::from_str(&txt).with_context(|| {
                format!(
                    "failed to parse verify report {}",
                    verify_report_path.display()
                )
            })
        }) {
        Ok(v) => v,
        Err(e) => {
            report.failures.push(FailureEntry {
                dataset: args.shared.dataset.clone(),
                phase: "repair_report_parse".to_string(),
                rel_path: None,
                source_path: Some(verify_report_path.to_string_lossy().to_string()),
                output_path: None,
                error_message: format!("{e:#}"),
                suggested_recovery: Some("provide a valid verify report JSON".to_string()),
            });
            report_finalize(&mut report);
            let _ = write_run_reports(&args.shared.parquet_dir, &report);
            bail!("[repair] failed to parse verify report");
        }
    };

    for f in &verify_report.failures {
        if f.phase != "verify_metrics" || !allowed.contains(&f.dataset) {
            continue;
        }
        let has_source = f
            .source_path
            .as_ref()
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let has_output = f
            .output_path
            .as_ref()
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let has_rel = f.rel_path.as_ref().map(|s| !s.is_empty()).unwrap_or(false);
        if !(has_rel || has_source && has_output) {
            report.failures.push(FailureEntry {
                dataset: f.dataset.clone(),
                phase: "repair_select".to_string(),
                rel_path: f.rel_path.clone(),
                source_path: f.source_path.clone(),
                output_path: f.output_path.clone(),
                error_message: "verify failure entry is not actionable (missing paths)".to_string(),
                suggested_recovery: Some(
                    "rerun verify to generate complete failure paths".to_string(),
                ),
            });
        }
    }

    let targets = collect_repair_targets(
        &verify_report,
        &allowed,
        &args.shared.snapshot_dir,
        &args.shared.parquet_dir,
    );
    if targets.is_empty() {
        eprintln!("[repair] no eligible verify failures found in report");
        report_finalize(&mut report);
        let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;
        eprintln!(
            "[repair] summary scanned=0 ok=0 failed=0 reports={}",
            report_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        return Ok(());
    }

    let mut by_dataset: BTreeMap<String, Vec<RepairTarget>> = BTreeMap::new();
    for t in targets {
        by_dataset.entry(t.dataset.clone()).or_default().push(t);
    }

    for (dataset, ds_targets) in by_dataset {
        try_log_dataset(
            &args.shared.parquet_dir,
            &dataset,
            "repair_convert",
            &format!(
                "start files={} workers={} memory_mb={:?}",
                ds_targets.len(),
                tuning.workers,
                tuning.memory_mb
            ),
        );
        let mut ds = DatasetReportSummary {
            dataset: dataset.clone(),
            items_scanned: ds_targets.len() as u64,
            ..Default::default()
        };
        let schema = match load_or_infer_source_schema(
            &duckdb_bin,
            &args.shared.snapshot_dir,
            &args.shared.parquet_dir,
            &dataset,
            100,
            false,
            tuning.memory_mb,
            tuning.workers,
            args.state_flush_every,
        ) {
            Ok(s) => s,
            Err(e) => {
                ds.failed = ds.items_scanned;
                report.failures.push(FailureEntry {
                    dataset: dataset.clone(),
                    phase: "repair_select".to_string(),
                    rel_path: None,
                    source_path: None,
                    output_path: None,
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some("refresh schema cache and retry repair".to_string()),
                });
                report.datasets.push(ds);
                report_finalize(&mut report);
                let _ = write_run_reports(&args.shared.parquet_dir, &report);
                continue;
            }
        };
        let columns_clause = Arc::new(to_duckdb_columns_clause(&schema.fields));
        let pb = make_progress_bar(
            args.progress,
            ds_targets.len() as u64,
            &format!("repair:{dataset}"),
        );
        // Spill directory (OnceLock-guarded; idempotent across strata + datasets).
        {
            let spill_dir = metadata_root(&args.shared.parquet_dir).join("duckdb_tmp");
            set_duckdb_temp_directory(&spill_dir);
        }

        // Build FilePairs from repair targets so the planner can partition by gz size.
        let repair_pairs: Vec<FilePair> = ds_targets
            .iter()
            .map(|t| FilePair {
                input_gz: t.source_path.clone(),
                output_parquet: t.output_path.clone(),
                rel: t.rel.clone(),
                gz_size_bytes: fs::metadata(&t.source_path).map(|m| m.len()).unwrap_or(0),
            })
            .collect();

        let workers_override = if args.shared.workers == 0 {
            None
        } else {
            Some(args.shared.workers)
        };
        let plan = build_convert_plan(
            &args.profile,
            workers_override,
            args.max_memory_mb,
            total_mb,
            repair_pairs,
            &profile_registry,
        )?;
        eprintln!(
            "[repair] dataset={dataset} profile={} strata={} flat={}",
            plan.profile_name,
            plan.strata.len(),
            plan.flat,
        );

        let duckdb_arc = Arc::new(duckdb_bin.clone());
        let dataset_arc = Arc::new(dataset.clone());
        let snapshot_root_arc = Arc::new(args.shared.snapshot_dir.clone());
        let parquet_root_arc = Arc::new(args.shared.parquet_dir.clone());
        let extra_json_options = if dataset == "works" {
            ", maximum_object_size=1000000000".to_string()
        } else {
            "".to_string()
        };
        let compression = "snappy".to_string();
        let row_group_rows = 100_000usize;

        let source_metrics_arc = Arc::new(std::sync::Mutex::new(load_source_metrics_cache(
            &args.shared.parquet_dir,
            &dataset,
        )?));
        let parquet_metrics_arc = Arc::new(std::sync::Mutex::new(load_parquet_metrics_cache(
            &args.shared.parquet_dir,
            &dataset,
        )?));

        for (stratum_idx, stratum) in plan.strata.iter().enumerate() {
            eprintln!(
                "[repair] dataset={dataset} stratum {}/{}: files={} workers={} per_worker_mb={}",
                stratum_idx + 1,
                plan.strata.len(),
                stratum.files.len(),
                stratum.workers,
                stratum.memory_mb,
            );

            set_duckdb_memory_limit(stratum.memory_mb.saturating_mul(stratum.workers));

            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(stratum.workers)
                .build()
                .context("failed to build rayon thread pool")?;

            let memory_mb = Some(stratum.memory_mb);

            for chunk in stratum.files.chunks(flush_every) {
                let failures: Vec<Option<FailureEntry>> = pool.install(|| {
                    chunk
                        .par_iter()
                        .map(|pair| {
                            if pair.output_parquet.exists() {
                                if let Err(e) = fs::remove_file(&pair.output_parquet) {
                                    pb.inc(1);
                                    return Some(FailureEntry {
                                        dataset: dataset_arc.to_string(),
                                        phase: "repair_delete".to_string(),
                                        rel_path: Some(pair.rel.to_string_lossy().to_string()),
                                        source_path: Some(
                                            pair.input_gz.to_string_lossy().to_string(),
                                        ),
                                        output_path: Some(
                                            pair.output_parquet.to_string_lossy().to_string(),
                                        ),
                                        error_message: format!("{e:#}"),
                                        suggested_recovery: Some(
                                            "fix file permissions or remove file manually"
                                                .to_string(),
                                        ),
                                    });
                                }
                            }

                            if let Err(e) = convert_one(
                                &duckdb_arc,
                                pair,
                                &columns_clause,
                                &compression,
                                row_group_rows,
                                memory_mb,
                                &extra_json_options,
                            ) {
                                pb.inc(1);
                                return Some(FailureEntry {
                                    dataset: dataset_arc.to_string(),
                                    phase: "repair_convert".to_string(),
                                    rel_path: Some(pair.rel.to_string_lossy().to_string()),
                                    source_path: Some(pair.input_gz.to_string_lossy().to_string()),
                                    output_path: Some(
                                        pair.output_parquet.to_string_lossy().to_string(),
                                    ),
                                    error_message: format!("{e:#}"),
                                    suggested_recovery: Some(
                                        "retry with --profile safe (single-worker, max memory)"
                                            .to_string(),
                                    ),
                                });
                            }

                            if let Err(e) = verify_file_metrics(
                                &duckdb_arc,
                                pair,
                                VerifyMetadataLevel::Both,
                                &source_metrics_arc,
                                &parquet_metrics_arc,
                                memory_mb,
                            ) {
                                pb.inc(1);
                                return Some(FailureEntry {
                                    dataset: dataset_arc.to_string(),
                                    phase: "repair_verify".to_string(),
                                    rel_path: Some(pair.rel.to_string_lossy().to_string()),
                                    source_path: Some(pair.input_gz.to_string_lossy().to_string()),
                                    output_path: Some(
                                        pair.output_parquet.to_string_lossy().to_string(),
                                    ),
                                    error_message: format!("{e:#}"),
                                    suggested_recovery: Some(
                                        "run verify for this file and inspect mismatch".to_string(),
                                    ),
                                });
                            }
                            try_log_dataset(
                                parquet_root_arc.as_path(),
                                &dataset_arc,
                                "repair_convert",
                                &format!("repaired {}", pair.rel.to_string_lossy()),
                            );
                            let _ = snapshot_root_arc;
                            pb.inc(1);
                            None
                        })
                        .collect()
                });
                for f in failures.into_iter().flatten() {
                    ds.failed += 1;
                    report.failures.push(f);
                }
                let source_metrics = source_metrics_arc
                    .lock()
                    .map_err(|_| anyhow!("repair source metrics cache lock poisoned"))?
                    .clone();
                let parquet_metrics = parquet_metrics_arc
                    .lock()
                    .map_err(|_| anyhow!("repair parquet metrics cache lock poisoned"))?
                    .clone();
                save_source_metrics_cache(&args.shared.parquet_dir, &dataset, &source_metrics)?;
                save_parquet_metrics_cache(&args.shared.parquet_dir, &dataset, &parquet_metrics)?;

                ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
                let mut preview = report.clone();
                preview.datasets.retain(|d| d.dataset != dataset);
                preview.datasets.push(ds.clone());
                report_finalize(&mut preview);
                let _ = write_run_reports(&args.shared.parquet_dir, &preview);
            }
        }
        pb.finish_with_message(format!("repair:{dataset} done"));
        ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
        report.datasets.push(ds.clone());
        try_log_dataset(
            &args.shared.parquet_dir,
            &dataset,
            "repair_convert",
            &format!("done files={} failed={}", ds.items_scanned, ds.failed),
        );
        report_finalize(&mut report);
        let _ = write_run_reports(&args.shared.parquet_dir, &report);
    }

    report_finalize(&mut report);
    let report_paths = write_run_reports(&args.shared.parquet_dir, &report)?;
    eprintln!(
        "[repair] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[repair] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn run_download(args: DownloadArgs) -> Result<()> {
    ensure_aws_cli(&args.aws_bin)?;
    fs::create_dir_all(&args.snapshot_dir)?;
    if args.explain {
        explain_download(&args)?;
        return Ok(());
    }
    let parquet_dir = args.root_dir.join("parquet");
    let _lock = acquire_lock(&parquet_dir, "download")?;
    let _ = archive_completed_run(&parquet_dir, &args.snapshot_dir);
    let _ = cleanup_download_reports(&args.snapshot_dir, "download");
    let _ = cleanup_download_log(&args.snapshot_dir, "download");
    let mut report_args = BTreeMap::new();
    report_args.insert(
        "snapshot_dir".to_string(),
        args.snapshot_dir.to_string_lossy().to_string(),
    );
    report_args.insert("s3_uri".to_string(), args.s3_uri.clone());
    report_args.insert("dataset".to_string(), args.dataset.clone());
    let effective_no_sign = args.no_sign_request && !args.signed;
    let effective_delete = args.delete_files && !args.no_delete;
    report_args.insert("no_sign_request".to_string(), effective_no_sign.to_string());
    report_args.insert("delete_files".to_string(), effective_delete.to_string());
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("download", report_args);

    let preflight_validate_args = ValidateDownloadArgs {
        root_dir: args.root_dir.clone(),
        snapshot_dir: args.snapshot_dir.clone(),
        s3_uri: args.s3_uri.clone(),
        dataset: args.dataset.clone(),
        aws_bin: args.aws_bin.clone(),
        endpoint_url: args.endpoint_url.clone(),
        region: args.region.clone(),
        profile_name: args.profile_name.clone(),
        no_sign_request: effective_no_sign,
        signed: args.signed,
        check_extra: effective_delete,
        workers: 0,
        progress: false,
        explain: false,
        state_flush_every: args.state_flush_every,
    };
    let remote_manifest = match fetch_remote_manifest(&preflight_validate_args, false) {
        Ok(v) => v,
        Err(e) => {
            report.failures.push(FailureEntry {
                dataset: args.dataset.clone(),
                phase: "download_manifest_fetch".to_string(),
                rel_path: None,
                source_path: Some(args.s3_uri.clone()),
                output_path: Some(args.snapshot_dir.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("check aws CLI/network/endpoint settings".to_string()),
            });
            report_finalize(&mut report);
            let report_paths = write_download_reports(&args.snapshot_dir, &report)?;
            eprintln!(
                "[download] summary scanned={} ok={} failed={} reports={}",
                report.totals_items_scanned,
                report.totals_succeeded,
                report.totals_failed,
                report_paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            bail!("[download] remote manifest fetch failed");
        }
    };
    if !args.skip_disk_check {
        let remote_total_bytes: u64 = remote_manifest.iter().map(|o| o.size).sum();
        let required_bytes = remote_total_bytes.saturating_mul(11).saturating_div(10);
        let free_bytes = available_disk_bytes(&args.snapshot_dir)?;
        eprintln!(
            "[download] disk preflight available={} GiB required={} GiB (remote={} GiB +10%)",
            bytes_to_gib(free_bytes),
            bytes_to_gib(required_bytes),
            bytes_to_gib(remote_total_bytes)
        );
        if free_bytes < required_bytes {
            report.failures.push(FailureEntry {
                dataset: args.dataset.clone(),
                phase: "download_disk_space".to_string(),
                rel_path: None,
                source_path: Some(args.s3_uri.clone()),
                output_path: Some(args.snapshot_dir.to_string_lossy().to_string()),
                error_message: format!(
                    "insufficient free disk space: available={} GiB required={} GiB",
                    bytes_to_gib(free_bytes),
                    bytes_to_gib(required_bytes)
                ),
                suggested_recovery: Some(
                    "Free up disk space, or set skip_disk_check: true under download: in your config file, or pass --skip-disk-check to the download/all command.".to_string()
                ),
            });
            report_finalize(&mut report);
            let report_paths = write_download_reports(&args.snapshot_dir, &report)?;
            eprintln!(
                "[download] summary scanned={} ok={} failed={} reports={}",
                report.totals_items_scanned,
                report.totals_succeeded,
                report.totals_failed,
                report_paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            bail!(
                "[download] Not enough disk space.\n  Location : {}\n  Available: {} GiB\n  Required : {} GiB (remote {} GiB + 10% buffer)\n\nTo proceed anyway, either:\n  - Set skip_disk_check: true under the download: section in your config file\n  - Pass --skip-disk-check when running the download or all command",
                args.snapshot_dir.display(),
                bytes_to_gib(free_bytes),
                bytes_to_gib(required_bytes),
                bytes_to_gib(remote_total_bytes)
            );
        }
    }

    let sync = aws_sync_command(&args)?;
    if let Err(e) = run_aws(&args.aws_bin, &sync) {
        report.failures.push(FailureEntry {
            dataset: args.dataset.clone(),
            phase: "download_sync".to_string(),
            rel_path: None,
            source_path: Some(args.s3_uri.clone()),
            output_path: Some(args.snapshot_dir.to_string_lossy().to_string()),
            error_message: format!("{e:#}"),
            suggested_recovery: Some("check aws CLI/network/endpoint settings".to_string()),
        });
        report_finalize(&mut report);
        let report_paths = write_download_reports(&args.snapshot_dir, &report)?;
        eprintln!(
            "[download] summary scanned={} ok={} failed={} reports={}",
            report.totals_items_scanned,
            report.totals_succeeded,
            report.totals_failed,
            report_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        bail!("[download] sync failed");
    }
    append_download_log(&args.snapshot_dir, "download", "sync complete")?;

    report.datasets.push(DatasetReportSummary {
        dataset: args.dataset.clone(),
        items_scanned: 1,
        succeeded: 1,
        failed: 0,
        skipped: 0,
    });

    report_finalize(&mut report);
    let report_paths = write_download_reports(&args.snapshot_dir, &report)?;
    eprintln!(
        "[download] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!("[download] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn run_validate_download(args: ValidateDownloadArgs) -> Result<()> {
    ensure_aws_cli(&args.aws_bin)?;
    fs::create_dir_all(&args.snapshot_dir)?;
    let tuning = light_tuning_with_override(args.workers, None);
    if args.explain {
        explain_validate_download(&args, &tuning);
        return Ok(());
    }
    let _ = cleanup_download_reports(&args.snapshot_dir, "verify_download");
    let _ = cleanup_download_log(&args.snapshot_dir, "verify_download");
    let _ = cleanup_download_manifests(&args.snapshot_dir);

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "snapshot_dir".to_string(),
        args.snapshot_dir.to_string_lossy().to_string(),
    );
    report_args.insert("s3_uri".to_string(), args.s3_uri.clone());
    report_args.insert("dataset".to_string(), args.dataset.clone());
    report_args.insert("check_extra".to_string(), args.check_extra.to_string());
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    report_args.insert(
        "state_flush_every".to_string(),
        args.state_flush_every.to_string(),
    );
    let mut report = report_new("verify_download", report_args);

    let remote = match fetch_remote_manifest(&args, args.progress) {
        Ok(v) => v,
        Err(e) => {
            report.failures.push(FailureEntry {
                dataset: args.dataset.clone(),
                phase: "validate_manifest_fetch".to_string(),
                rel_path: None,
                source_path: Some(args.s3_uri.clone()),
                output_path: Some(args.snapshot_dir.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("check aws CLI credentials/network/endpoint".to_string()),
            });
            report_finalize(&mut report);
            let _ = write_download_reports(&args.snapshot_dir, &report);
            bail!("[verify_download] remote manifest fetch failed");
        }
    };
    write_manifest_jsonl(
        &download_manifests_dir(&args.snapshot_dir)
            .join(format!("remote_manifest-{}.jsonl", report.started_at_unix)),
        &remote,
    )?;

    let local = build_local_manifest(&args.snapshot_dir, &args.dataset)?;
    write_manifest_jsonl(
        &download_manifests_dir(&args.snapshot_dir)
            .join(format!("local_manifest-{}.jsonl", report.started_at_unix)),
        &local,
    )?;

    let mut ds_map: BTreeMap<String, DatasetReportSummary> = BTreeMap::new();
    let remote_map: BTreeMap<String, &RemoteObject> =
        remote.iter().map(|o| (o.key.clone(), o)).collect();
    let local_map: BTreeMap<String, &RemoteObject> =
        local.iter().map(|o| (o.key.clone(), o)).collect();
    let flush_every = args.state_flush_every.max(1);

    let compare_pb = make_progress_bar(args.progress, remote_map.len() as u64, "validate-compare");
    for (idx, (key, ro)) in remote_map.iter().enumerate() {
        let ds_name = dataset_from_key(key);
        let ds = ds_map
            .entry(ds_name.clone())
            .or_insert_with(|| DatasetReportSummary {
                dataset: ds_name.clone(),
                ..Default::default()
            });
        ds.items_scanned += 1;
        let lp = args.snapshot_dir.join(key);
        match local_map.get(key) {
            None => {
                ds.failed += 1;
                report.failures.push(FailureEntry {
                    dataset: ds_name,
                    phase: "validate_file_presence".to_string(),
                    rel_path: Some(key.clone()),
                    source_path: Some(format!("{}/{}", args.s3_uri.trim_end_matches('/'), key)),
                    output_path: Some(lp.to_string_lossy().to_string()),
                    error_message: "missing local file".to_string(),
                    suggested_recovery: Some("rerun download".to_string()),
                });
            }
            Some(lo) => {
                if lo.size != ro.size {
                    ds.failed += 1;
                    report.failures.push(FailureEntry {
                        dataset: ds_name,
                        phase: "validate_file_size".to_string(),
                        rel_path: Some(key.clone()),
                        source_path: Some(format!("{}/{}", args.s3_uri.trim_end_matches('/'), key)),
                        output_path: Some(lp.to_string_lossy().to_string()),
                        error_message: format!(
                            "size mismatch local={} remote={}",
                            lo.size, ro.size
                        ),
                        suggested_recovery: Some("rerun download".to_string()),
                    });
                } else {
                    ds.succeeded += 1;
                }
            }
        }
        if (idx + 1) % flush_every == 0 {
            report.datasets = ds_map.values().cloned().collect();
            report_finalize(&mut report);
            let _ = write_download_reports(&args.snapshot_dir, &report);
        }
        compare_pb.inc(1);
    }
    compare_pb.finish_with_message("validate-compare complete");

    if args.check_extra {
        let extra_pb = make_progress_bar(args.progress, local_map.len() as u64, "validate-extra");
        for key in local_map.keys() {
            if !remote_map.contains_key(key) {
                let ds_name = dataset_from_key(key);
                let ds = ds_map
                    .entry(ds_name.clone())
                    .or_insert_with(|| DatasetReportSummary {
                        dataset: ds_name.clone(),
                        ..Default::default()
                    });
                ds.failed += 1;
                report.failures.push(FailureEntry {
                    dataset: ds_name,
                    phase: "validate_file_presence".to_string(),
                    rel_path: Some(key.clone()),
                    source_path: None,
                    output_path: Some(args.snapshot_dir.join(key).to_string_lossy().to_string()),
                    error_message: "unexpected local file".to_string(),
                    suggested_recovery: Some("rerun download with --delete".to_string()),
                });
            }
            extra_pb.inc(1);
        }
        extra_pb.finish_with_message("validate-extra complete");
    }

    let gz_files = list_scoped_gz_files(&args.snapshot_dir, &args.dataset)?;
    let pb = make_progress_bar(args.progress, gz_files.len() as u64, "validate-gzip");
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(tuning.workers)
        .build()
        .context("failed to build rayon thread pool")?;
    let failures: Vec<Option<FailureEntry>> = pool.install(|| {
        gz_files
            .par_iter()
            .map(|p| {
                let out = gzip_integrity_ok(p);
                pb.inc(1);
                if out.is_ok() {
                    None
                } else {
                    let rel = p
                        .strip_prefix(&args.snapshot_dir)
                        .map(|x| x.to_string_lossy().to_string())
                        .unwrap_or_else(|_| p.to_string_lossy().to_string());
                    Some(FailureEntry {
                        dataset: dataset_from_key(&rel),
                        phase: "validate_gzip_integrity".to_string(),
                        rel_path: Some(rel.clone()),
                        source_path: Some(format!("{}/{}", args.s3_uri.trim_end_matches('/'), rel)),
                        output_path: Some(p.to_string_lossy().to_string()),
                        error_message: format!(
                            "{:#}",
                            out.err().unwrap_or_else(|| anyhow!("gzip check failed"))
                        ),
                        suggested_recovery: Some("rerun download for this file".to_string()),
                    })
                }
            })
            .collect()
    });
    pb.finish_with_message("validate-gzip complete");
    for f in failures.into_iter().flatten() {
        let ds = ds_map
            .entry(f.dataset.clone())
            .or_insert_with(|| DatasetReportSummary {
                dataset: f.dataset.clone(),
                ..Default::default()
            });
        ds.failed += 1;
        report.failures.push(f);
    }

    report.datasets = ds_map.values().cloned().collect();
    report_finalize(&mut report);
    let report_paths = write_download_reports(&args.snapshot_dir, &report)?;
    append_download_log(
        &args.snapshot_dir,
        "verify_download",
        &format!(
            "done scanned={} failed={}",
            report.totals_items_scanned, report.totals_failed
        ),
    )?;
    eprintln!(
        "[verify_download] summary scanned={} ok={} failed={} reports={}",
        report.totals_items_scanned,
        report.totals_succeeded,
        report.totals_failed,
        report_paths
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if report.totals_failed > 0 {
        bail!(
            "[verify_download] failures detected: {}",
            report.totals_failed
        );
    }
    Ok(())
}

fn render_schema_diff(
    left: &SchemaDoc,
    right: &SchemaDoc,
    format: &SchemaFormat,
) -> Result<String> {
    let lmap: BTreeMap<String, String> = left
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.r#type.clone()))
        .collect();
    let rmap: BTreeMap<String, String> = right
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.r#type.clone()))
        .collect();

    let left_names: BTreeSet<String> = lmap.keys().cloned().collect();
    let right_names: BTreeSet<String> = rmap.keys().cloned().collect();
    let removed: Vec<String> = left_names.difference(&right_names).cloned().collect();
    let added: Vec<String> = right_names.difference(&left_names).cloned().collect();

    let mut changed = Vec::<serde_json::Value>::new();
    for name in left_names.intersection(&right_names) {
        let lt = lmap.get(name).unwrap();
        let rt = rmap.get(name).unwrap();
        if lt != rt {
            changed.push(serde_json::json!({
                "name": name,
                "left_type": lt,
                "right_type": rt
            }));
        }
    }

    let diff = serde_json::json!({
        "dataset": left.dataset,
        "left_source": left.source,
        "right_source": right.source,
        "added": added,
        "removed": removed,
        "changed_types": changed
    });

    let out = match format {
        SchemaFormat::Table => {
            let mut s = String::new();
            s.push_str(&format!(
                "dataset: {}\nleft: {}\nright: {}\n",
                left.dataset, left.source, right.source
            ));
            s.push_str(&format!(
                "added: {}\nremoved: {}\nchanged_types: {}\n",
                diff["added"].as_array().map(|a| a.len()).unwrap_or(0),
                diff["removed"].as_array().map(|a| a.len()).unwrap_or(0),
                diff["changed_types"]
                    .as_array()
                    .map(|a| a.len())
                    .unwrap_or(0)
            ));
            s
        }
        SchemaFormat::Json | SchemaFormat::ArrowR => serde_json::to_string_pretty(&diff)?,
        SchemaFormat::Yaml => serde_yaml::to_string(&diff)?,
    };
    Ok(out)
}

fn datasets_len_hint(dataset_arg: &str) -> usize {
    if dataset_arg == "all" {
        2
    } else {
        1
    }
}

fn extension_for_format(fmt: &SchemaFormat) -> &'static str {
    match fmt {
        SchemaFormat::Table => "txt",
        SchemaFormat::Json | SchemaFormat::ArrowR => "json",
        SchemaFormat::Yaml => "yaml",
    }
}

fn explain_convert(args: &ConvertArgs, datasets: &[String], duckdb_bin: &Path) {
    println!("--explain: convert");
    println!("duckdb_bin: {}", duckdb_bin.display());
    println!("snapshot_dir: {}", args.shared.snapshot_dir.display());
    println!("parquet_dir: {}", args.shared.parquet_dir.display());
    println!("datasets: {}", datasets.join(", "));
    println!("profile: {}", args.profile);
    println!(
        "workers: {}",
        if args.shared.workers == 0 {
            "auto (per profile)".to_string()
        } else {
            args.shared.workers.to_string()
        }
    );
    println!("max_memory_mb override: {:?}", args.max_memory_mb);
    println!("compression: {}", args.compression);
    println!("row_group_rows: {}", args.row_group_rows);
    println!("sample_size(schema): {}", args.sample_size);
    println!("refresh_cache: {}", args.refresh_cache);
    if args.input_files.is_empty() {
        println!("input_filter: all files");
    } else {
        println!("input_filter: {}", args.input_files.len());
    }
    println!("verify: not part of convert; run verify_convert separately");
}

fn explain_verify(args: &VerifyArgs, datasets: &[String], duckdb_bin: &Path, tuning: &Tuning) {
    println!("--explain: verify");
    println!("duckdb_bin: {}", duckdb_bin.display());
    println!("snapshot_dir: {}", args.shared.snapshot_dir.display());
    println!("parquet_dir: {}", args.shared.parquet_dir.display());
    println!("datasets: {}", datasets.join(", "));
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
    println!(
        "plan: scope={:?}, metadata_level={:?}, file_sample_n={}, seed={}",
        args.scope, args.metadata_level, args.file_sample_n, args.seed
    );
}

fn explain_schema(args: &SchemaArgs, datasets: &[String], duckdb_bin: &Path, tuning: &Tuning) {
    println!("--explain: schema");
    println!("duckdb_bin: {}", duckdb_bin.display());
    println!("snapshot_dir: {}", args.shared.snapshot_dir.display());
    println!("parquet_dir: {}", args.shared.parquet_dir.display());
    println!("datasets: {}", datasets.join(", "));
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
    println!(
        "plan: from={:?}, format={:?}, diff_with={:?}, sample_size={}, refresh_cache={}, state_flush_every={}",
        args.from, args.format, args.diff_with, args.sample_size, args.refresh_cache, args.state_flush_every
    );
    println!(
        "output: {}",
        args.output
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "stdout".to_string())
    );
}

fn explain_index(
    args: &IndexArgs,
    duckdb_bin: &Path,
    corpus_dir: &Path,
    index_file: &Path,
    tuning: &Tuning,
) {
    println!("--explain: index");
    println!("duckdb_bin: {}", duckdb_bin.display());
    println!("corpus_dir: {}", corpus_dir.display());
    println!("index_file: {}", index_file.display());
    println!("overwrite: {}", args.overwrite);
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
}

fn explain_extract(args: &ExtractArgs, duckdb_bin: &Path, tuning: &Tuning) {
    println!("--explain: extract");
    println!("duckdb_bin: {}", duckdb_bin.display());
    println!("snapshot_dir: {}", args.shared.snapshot_dir.display());
    println!("parquet_dir: {}", args.shared.parquet_dir.display());
    println!("dataset filter: {}", args.shared.dataset);
    println!("ids csv: {}", args.ids.display());
    println!("output base: {}", args.output.display());
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
}

fn ensure_duckdb_bin(bin: &Path) -> Result<()> {
    let out = Command::new(bin).arg("--version").output();
    match out {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => bail!(
            "duckdb check failed for {}: {}",
            bin.display(),
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => bail!("duckdb binary not available at {}: {}", bin.display(), e),
    }
}

fn duckdb_bin(shared: &SharedArgs) -> PathBuf {
    duckdb_bin_from_option(&shared.duckdb_bin)
}

fn duckdb_bin_from_option(p: &Option<PathBuf>) -> PathBuf {
    p.clone().unwrap_or_else(|| PathBuf::from("duckdb"))
}

/// Tuning for parquet-side commands (verify, schema, index, extract, verify_index,
/// check, …) where the heavy lifting is parquet read/scan rather than JSON gz
/// parsing.  Workers scale with detected CPU count, capped at 4 (these
/// workloads don't benefit from more parallelism on local disk); per-worker
/// memory defaults to 8 GiB.  An explicit `--workers N` or `--max-memory-mb N`
/// wins.
fn light_tuning_with_override(workers: usize, memory_mb_override: Option<usize>) -> Tuning {
    let workers = if workers > 0 {
        workers
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get().min(4))
            .unwrap_or(2)
    };
    let memory_mb = memory_mb_override.unwrap_or(8192);
    Tuning {
        workers,
        memory_mb: Some(memory_mb),
    }
}

/// Resolve the path to a profiles config YAML.  If `cli_arg` is set, use that
/// directly.  Otherwise auto-discover `./openalex-snapshot.profiles.yaml` if it
/// exists.  Returns `None` when no profiles config is in use (built-ins only).
fn discover_profiles_config(cli_arg: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = cli_arg {
        return Some(p.to_path_buf());
    }
    let default = PathBuf::from("openalex-snapshot.profiles.yaml");
    if default.exists() {
        Some(default)
    } else {
        None
    }
}

/// Single-pair representative of a profile's resolved workers/memory for use
/// in log lines and report metadata that pre-date the stratified machinery
/// and still expect a single Tuning.  Not used to drive actual execution —
/// `build_convert_plan` builds the real schedule per stratum.
fn representative_tuning(
    profile_name: &str,
    def: &ProfileDef,
    workers_override: usize,
    max_memory_mb_override: Option<usize>,
    total_ram_mb: Option<usize>,
) -> Tuning {
    match def.kind {
        ProfileKind::Safe => {
            // Workers: explicit override wins, else 1 (single-worker safe is the default).
            let workers = if workers_override > 0 {
                workers_override.clamp(1, 2)
            } else {
                1
            };
            let memory_mb = if let Some(mb) = max_memory_mb_override {
                Some(mb)
            } else {
                let mut mb = auto_profile_safe_memory_mb(total_ram_mb);
                if workers == 1 {
                    mb = mb.max(auto_profile_single_worker_safe_memory_mb(total_ram_mb));
                }
                Some(mb)
            };
            Tuning { workers, memory_mb }
        }
        ProfileKind::Stratified => {
            let strata = def
                .strata
                .as_ref()
                .expect("validate_profile_def guarantees strata for Stratified");
            // workers = explicit override > FIRST stratum's workers (sample of the smallest-file pass)
            let workers = if workers_override > 0 {
                workers_override
            } else {
                strata.first().map(|s| s.workers).unwrap_or(1)
            };
            // memory = explicit override > CATCH-ALL stratum's per_worker_mb (biggest files = biggest mem)
            let memory_mb = if let Some(mb) = max_memory_mb_override {
                Some(mb)
            } else {
                strata
                    .iter()
                    .find(|s| s.max_file_mb.is_none())
                    .or_else(|| strata.last())
                    .map(|s| s.per_worker_mb)
            };
            let _ = profile_name; // for symmetry; intentionally unused
            Tuning { workers, memory_mb }
        }
    }
}

// ---------------------------------------------------------------------------
// Convert plan
//
// A `ConvertPlan` is the parallel-execution schedule for a dataset's `todo`
// file list, derived from the chosen profile + CLI overrides + system RAM.
// `run_convert` walks `plan.strata` in order, configuring DuckDB and rayon
// fresh for each stratum so worker count and memory budget can vary per
// file-size bucket.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StratumPlan {
    workers: usize,
    /// DuckDB `memory_limit` per worker, in MB.  Multiplied by `workers` to get
    /// the global cap set on the master DuckDB connection.
    memory_mb: usize,
    files: Vec<FilePair>,
}

#[derive(Debug, Clone)]
struct ConvertPlan {
    /// Strata in execution order (largest files first under stratified mode).
    strata: Vec<StratumPlan>,
    /// True when the plan is a single flat parallel pass — either because the
    /// profile is Safe (single configuration) or because `--workers N` was
    /// passed and collapsed a stratified profile into one pass.
    flat: bool,
    /// Resolved profile name (after lookup).  Used for logging.
    profile_name: String,
}

/// Build the execution plan for `run_convert` / `run_repair`.
///
/// Behaviour by profile kind:
///   - Safe: one stratum.  Workers clamped to [1, 2].  Memory derived from RAM
///     (with single-worker boost on workers=1) unless `max_memory_mb_override`
///     is set.  All `todo` files placed in the single stratum.
///   - Stratified + `workers_override.is_some()`: one flat stratum.  Memory =
///     `max_memory_mb_override` if set, else the per_worker_mb from the profile's
///     largest stratum.  Files sorted largest-first.
///   - Stratified without override: one stratum per profile stratum (empty ones
///     dropped).  Files partitioned by gz size against each stratum's
///     `max_file_mb` upper bound.  Strata emitted largest-files-first (catch-all
///     runs first) so failures surface early on the riskiest data.
fn build_convert_plan(
    profile_name: &str,
    workers_override: Option<usize>,
    max_memory_mb_override: Option<usize>,
    total_ram_mb: Option<usize>,
    todo: Vec<FilePair>,
    registry: &ProfileRegistry,
) -> Result<ConvertPlan> {
    let def = registry
        .get(profile_name)
        .ok_or_else(|| registry.unknown_profile_error(profile_name))?
        .clone();

    match def.kind {
        ProfileKind::Safe => {
            let workers = workers_override.map(|w| w.clamp(1, 2)).unwrap_or(1).max(1);
            let memory_mb = if let Some(mb) = max_memory_mb_override {
                mb
            } else {
                let mut mb = auto_profile_safe_memory_mb(total_ram_mb);
                if workers == 1 {
                    mb = mb.max(auto_profile_single_worker_safe_memory_mb(total_ram_mb));
                }
                mb
            };
            Ok(ConvertPlan {
                profile_name: profile_name.to_string(),
                flat: true,
                strata: vec![StratumPlan {
                    workers,
                    memory_mb,
                    files: todo,
                }],
            })
        }
        ProfileKind::Stratified => {
            let strata_defs = def
                .strata
                .as_ref()
                .expect("validate_profile_def guarantees strata for Stratified");
            let largest_stratum_mb = strata_defs
                .iter()
                .map(|s| s.per_worker_mb)
                .max()
                .unwrap_or(4096);

            // --workers override collapses into a single flat pass.
            if let Some(w) = workers_override {
                let workers = w.max(1);
                let memory_mb = max_memory_mb_override.unwrap_or(largest_stratum_mb);
                let mut files = todo;
                files.sort_by(|a, b| b.gz_size_bytes.cmp(&a.gz_size_bytes));
                return Ok(ConvertPlan {
                    profile_name: profile_name.to_string(),
                    flat: true,
                    strata: vec![StratumPlan {
                        workers,
                        memory_mb,
                        files,
                    }],
                });
            }

            // Partition files by size against each stratum's max_file_mb bound.
            // Strata are in ascending max_file_mb order with catch-all last.
            // For each file, walk the strata in order and place it in the first
            // one whose bound covers its size.
            let mut sorted = todo;
            sorted.sort_by(|a, b| b.gz_size_bytes.cmp(&a.gz_size_bytes));
            let n_strata = strata_defs.len();
            let mut buckets: Vec<Vec<FilePair>> = (0..n_strata).map(|_| Vec::new()).collect();
            for pair in sorted {
                let size_mb = pair.gz_size_bytes / (1024 * 1024);
                let placed = strata_defs.iter().position(|s| match s.max_file_mb {
                    Some(cap_mb) => size_mb <= cap_mb,
                    None => true, // catch-all
                });
                // validate_profile_def guarantees the catch-all is present, so a
                // position is always found.
                let idx = placed.expect("catch-all stratum must always match");
                buckets[idx].push(pair);
            }

            // Emit StratumPlan(s) in LARGEST-files-first order: walk strata
            // backwards so the catch-all (most-risky big files) runs first.
            let plan_strata: Vec<StratumPlan> = strata_defs
                .iter()
                .zip(buckets)
                .rev()
                .filter(|(_, files)| !files.is_empty())
                .map(|(sdef, files)| {
                    let memory_mb = max_memory_mb_override.unwrap_or(sdef.per_worker_mb);
                    StratumPlan {
                        workers: sdef.workers.max(1),
                        memory_mb,
                        files,
                    }
                })
                .collect();

            Ok(ConvertPlan {
                profile_name: profile_name.to_string(),
                flat: false,
                strata: plan_strata,
            })
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Tuning {
    workers: usize,
    memory_mb: Option<usize>,
}

fn auto_profile_single_worker_safe_memory_mb(total_mb: Option<usize>) -> usize {
    let t = match total_mb {
        Some(v) if v > 0 => v,
        _ => return 8192,
    };
    let usable = (t as f64 * 0.80).floor() as usize;
    // For single-worker safe mode, prefer higher memory to avoid OOM on large nested records.
    let mb = ((usable as f64) * 0.45).floor() as usize;
    mb.clamp(8192, 24_576)
}

/// Memory budget (MB) for the multi-worker `safe` profile path: 15% of usable
/// RAM, clamped to [1024, 8192].  Used as the floor by `representative_tuning`
/// when `workers > 1`.  `auto_profile_single_worker_safe_memory_mb` is used as
/// the floor when `workers == 1` (it overrides with a more generous cap so
/// huge files can be processed without OOM).
fn auto_profile_safe_memory_mb(total_mb: Option<usize>) -> usize {
    let t = match total_mb {
        Some(v) if v > 0 => v,
        _ => return 2048,
    };
    let usable = (t as f64 * 0.80).floor() as usize;
    let mb = ((usable as f64) * 0.15).floor() as usize;
    mb.clamp(1024, 8192)
}

/// Parse a human-readable size string to bytes.
/// "0" → 0 (sentinel for auto). Supports kb/mb/gb (SI) and kib/mib/gib (binary), case-insensitive.
fn parse_size_str(s: &str) -> Result<usize> {
    let s = s.trim().to_lowercase();
    if s == "0" {
        return Ok(0);
    }
    let (num_s, mult): (&str, usize) = if s.ends_with("gib") {
        (&s[..s.len() - 3], 1024 * 1024 * 1024)
    } else if s.ends_with("mib") {
        (&s[..s.len() - 3], 1024 * 1024)
    } else if s.ends_with("kib") {
        (&s[..s.len() - 3], 1024)
    } else if s.ends_with("gb") {
        (&s[..s.len() - 2], 1_000_000_000)
    } else if s.ends_with("mb") {
        (&s[..s.len() - 2], 1_000_000)
    } else if s.ends_with("kb") {
        (&s[..s.len() - 2], 1_000)
    } else if s.ends_with('b') {
        (&s[..s.len() - 1], 1)
    } else {
        (s.as_str(), 1)
    };
    let n: usize = num_s.trim().parse().with_context(|| {
        format!("invalid size '{s}': expected number with optional suffix (kb/mb/gb/kib/mib/gib)")
    })?;
    Ok(n * mult)
}

fn detect_total_memory_mb() -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        let out = Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let bytes: u128 = s.parse().ok()?;
        return Some((bytes / (1024 * 1024) as u128) as usize);
    }
    #[cfg(target_os = "linux")]
    {
        let txt = fs::read_to_string("/proc/meminfo").ok()?;
        let line = txt.lines().find(|l| l.starts_with("MemTotal:"))?;
        let kb: u128 = line.split_whitespace().nth(1)?.parse().ok()?;
        return Some((kb / 1024) as usize);
    }
    #[allow(unreachable_code)]
    None
}

fn resolve_datasets(snapshot_dir: &Path, dataset_arg: &str) -> Result<Vec<String>> {
    if dataset_arg != "all" {
        return Ok(vec![dataset_arg.to_string()]);
    }

    let root = snapshot_dir.join("data");
    let mut out = Vec::new();
    for entry in fs::read_dir(&root).with_context(|| format!("cannot read {}", root.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name != "merged_ids" {
                out.push(name);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn enumerate_pairs(
    snapshot_dir: &Path,
    parquet_dir: &Path,
    dataset: &str,
) -> Result<Vec<FilePair>> {
    let data_root = snapshot_dir.join("data").join(dataset);
    let out_root = parquet_dir.join(dataset);
    let mut pairs = Vec::new();

    if !data_root.exists() {
        return Ok(pairs);
    }

    for entry in WalkDir::new(&data_root).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) != Some("gz") {
            continue;
        }
        let rel = p.strip_prefix(&data_root)?.to_path_buf();
        let mut out_rel = rel.clone();
        out_rel.set_extension("parquet");
        let out_path = out_root.join(&out_rel);

        let gz_size_bytes = fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        // A source gz is done if either the single parquet or split chunks exist.
        // The caller's `!p.output_parquet.exists()` check handles the single-file case;
        // here we set gz_size_bytes=0 on split-done files so the caller can detect them
        // via split_parquets_exist, but we still emit the pair so skipped count is accurate.
        pairs.push(FilePair {
            input_gz: p.to_path_buf(),
            output_parquet: out_path,
            rel,
            gz_size_bytes,
        });
    }

    pairs.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(pairs)
}

fn filter_pairs_by_input_files(
    pairs: &[FilePair],
    inputs: &[PathBuf],
    snapshot_dir: &Path,
    dataset: &str,
) -> Result<Vec<FilePair>> {
    let mut wanted_abs = BTreeSet::<String>::new();
    let mut wanted_rel = BTreeSet::<String>::new();
    let mut wanted_base = BTreeSet::<String>::new();

    let ds_root = snapshot_dir.join("data").join(dataset);
    for inp in inputs {
        if inp.is_absolute() {
            wanted_abs.insert(inp.to_string_lossy().replace('\\', "/"));
            if let Ok(rel) = inp.strip_prefix(&ds_root) {
                wanted_rel.insert(rel.to_string_lossy().replace('\\', "/"));
            }
        } else {
            wanted_rel.insert(inp.to_string_lossy().replace('\\', "/"));
            if let Some(b) = inp.file_name().and_then(|s| s.to_str()) {
                wanted_base.insert(b.to_string());
            }
        }
    }

    let mut out = Vec::new();
    for p in pairs {
        let abs = p.input_gz.to_string_lossy().replace('\\', "/");
        let rel = p.rel.to_string_lossy().replace('\\', "/");
        let base = p
            .input_gz
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if wanted_abs.contains(&abs) || wanted_rel.contains(&rel) || wanted_base.contains(&base) {
            out.push(p.clone());
        }
    }
    Ok(out)
}

fn list_parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("parquet")
        {
            out.push(entry.path().to_path_buf());
        }
    }
    out.sort();
    Ok(out)
}

fn collect_repair_targets(
    verify_report: &RunReport,
    allowed_datasets: &BTreeSet<String>,
    snapshot_dir: &Path,
    parquet_dir: &Path,
) -> Vec<RepairTarget> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::new();
    for f in &verify_report.failures {
        if f.phase != "verify_metrics" && f.phase != "convert_file" {
            continue;
        }
        if !allowed_datasets.contains(&f.dataset) {
            continue;
        }
        let source_path = f
            .source_path
            .as_ref()
            .map(|p| resolve_report_path_for_root(p, snapshot_dir, parquet_dir));
        let output_path = f
            .output_path
            .as_ref()
            .map(|p| resolve_report_path_for_root(p, snapshot_dir, parquet_dir));
        let rel_path = f.rel_path.as_ref().map(PathBuf::from);
        let dataset = f.dataset.clone();

        let source = source_path.or_else(|| {
            rel_path
                .as_ref()
                .map(|r| snapshot_dir.join("data").join(&dataset).join(r))
        });
        let output = output_path.or_else(|| {
            rel_path
                .as_ref()
                .map(|r| parquet_dir.join(&dataset).join(r.with_extension("parquet")))
        });

        let (source, output) = match (source, output) {
            (Some(s), Some(o)) => (s, o),
            _ => continue,
        };
        let rel = if let Some(r) = rel_path {
            r
        } else if let Ok(r) = source.strip_prefix(snapshot_dir.join("data").join(&dataset)) {
            r.to_path_buf()
        } else if let Ok(r) = output.strip_prefix(parquet_dir.join(&dataset)) {
            r.with_extension("gz")
        } else {
            PathBuf::from(
                source
                    .file_name()
                    .and_then(|x| x.to_str())
                    .unwrap_or("unknown.gz"),
            )
        };

        let key = output.to_string_lossy().to_string();
        if seen.insert(key) {
            out.push(RepairTarget {
                dataset,
                source_path: source,
                output_path: output,
                rel,
            });
        }
    }
    out
}

fn resolve_report_path_for_root(
    path_str: &str,
    snapshot_dir: &Path,
    _parquet_dir: &Path,
) -> PathBuf {
    let p = PathBuf::from(path_str);
    if p.is_absolute() {
        return p;
    }
    let root = snapshot_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let norm = path_str.trim_start_matches("./");
    if norm == "snapshot" || norm.starts_with("snapshot/") {
        return root.join(norm);
    }
    if norm == "parquet" || norm.starts_with("parquet/") {
        return root.join(norm);
    }
    p
}

fn ensure_aws_cli(bin: &Path) -> Result<()> {
    let out = Command::new(bin).arg("--version").output();
    match out {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => bail!(
            "aws cli check failed for {}: {}",
            bin.display(),
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(e) => bail!("aws binary not available at {}: {}", bin.display(), e),
    }
}

fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let u = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("invalid s3 uri: {uri}"))?;
    let mut parts = u.splitn(2, '/');
    let bucket = parts
        .next()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("invalid s3 uri bucket: {uri}"))?;
    let prefix = parts.next().unwrap_or("").trim_matches('/').to_string();
    Ok((bucket, prefix))
}

fn aws_common_flags_from(
    endpoint_url: &Option<String>,
    region: &Option<String>,
    profile_name: &Option<String>,
    no_sign_request: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(v) = endpoint_url {
        out.push("--endpoint-url".to_string());
        out.push(v.clone());
    }
    if let Some(v) = region {
        out.push("--region".to_string());
        out.push(v.clone());
    }
    if let Some(v) = profile_name {
        out.push("--profile".to_string());
        out.push(v.clone());
    }
    if no_sign_request {
        out.push("--no-sign-request".to_string());
    }
    out
}

fn aws_sync_command(args: &DownloadArgs) -> Result<Vec<String>> {
    let mut cmd = vec!["s3".to_string(), "sync".to_string()];
    let effective_delete = args.delete_files && !args.no_delete;
    let effective_no_sign = args.no_sign_request && !args.signed;
    if effective_delete {
        cmd.push("--delete".to_string());
    }
    let src = if args.dataset == "all" {
        args.s3_uri.trim_end_matches('/').to_string()
    } else {
        format!(
            "{}/data/{}/",
            args.s3_uri.trim_end_matches('/'),
            args.dataset
        )
    };
    let dst = if args.dataset == "all" {
        args.snapshot_dir.to_string_lossy().to_string()
    } else {
        args.snapshot_dir
            .join("data")
            .join(&args.dataset)
            .to_string_lossy()
            .to_string()
    };
    cmd.push(src);
    cmd.push(dst);
    cmd.extend(aws_common_flags_from(
        &args.endpoint_url,
        &args.region,
        &args.profile_name,
        effective_no_sign,
    ));
    Ok(cmd)
}

fn run_aws(bin: &Path, args: &[String]) -> Result<String> {
    let out = Command::new(bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to run aws {} {}", bin.display(), args.join(" ")))?;
    if !out.status.success() {
        bail!(
            "aws command failed: {}\n{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn download_metadata_root(snapshot_dir: &Path) -> PathBuf {
    let root = snapshot_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    root.join("openalex-snapshot_metadata").join("download")
}

fn download_manifests_dir(snapshot_dir: &Path) -> PathBuf {
    download_metadata_root(snapshot_dir).join("manifests")
}

fn download_reports_dir(snapshot_dir: &Path) -> PathBuf {
    let root = snapshot_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    root.join("openalex-snapshot_metadata").join("reports")
}

fn download_log_path(snapshot_dir: &Path) -> PathBuf {
    download_metadata_root(snapshot_dir).join("download.log")
}

fn append_download_log(snapshot_dir: &Path, command: &str, msg: &str) -> Result<()> {
    let path = download_log_path(snapshot_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(f, "{} [{}] {}", now_unix(), command, msg)?;
    Ok(())
}

fn write_download_reports(snapshot_dir: &Path, report: &RunReport) -> Result<Vec<PathBuf>> {
    let fname = report_file_name(&report.command, report.started_at_unix, report.report_nonce);
    let payload = serde_json::to_vec_pretty(report)?;
    let p = download_reports_dir(snapshot_dir).join(fname);
    write_json_atomic(&p, &payload)?;
    Ok(vec![p])
}

fn cleanup_download_reports(snapshot_dir: &Path, command: &str) -> Result<()> {
    let dir = download_reports_dir(snapshot_dir);
    if !dir.exists() {
        return Ok(());
    }
    let prefix = format!("{}-", sanitize_command_name(command));
    for ent in fs::read_dir(&dir)? {
        let ent = ent?;
        if !ent.path().is_file() {
            continue;
        }
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&prefix) && name.ends_with(".json") {
            let _ = fs::remove_file(ent.path());
        }
    }
    Ok(())
}

fn cleanup_download_log(snapshot_dir: &Path, _command: &str) -> Result<()> {
    let p = download_log_path(snapshot_dir);
    if p.exists() {
        let _ = fs::remove_file(p);
    }
    Ok(())
}

fn cleanup_download_manifests(snapshot_dir: &Path) -> Result<()> {
    let dir = download_manifests_dir(snapshot_dir);
    if !dir.exists() {
        return Ok(());
    }
    for ent in fs::read_dir(&dir)? {
        let ent = ent?;
        if !ent.path().is_file() {
            continue;
        }
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if (name.starts_with("remote_manifest-") || name.starts_with("local_manifest-"))
            && name.ends_with(".jsonl")
        {
            let _ = fs::remove_file(ent.path());
        }
    }
    Ok(())
}

fn write_manifest_jsonl(path: &Path, items: &[RemoteObject]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut out = String::new();
    for it in items {
        out.push_str(&serde_json::to_string(it)?);
        out.push('\n');
    }
    fs::write(path, out.as_bytes())?;
    Ok(())
}

fn dataset_prefix(dataset: &str) -> String {
    if dataset == "all" {
        "".to_string()
    } else {
        format!("data/{}/", dataset)
    }
}

fn dataset_from_key(key: &str) -> String {
    let k = key.trim_start_matches('/');
    let mut parts = k.split('/');
    if parts.next() == Some("data") {
        parts.next().unwrap_or("unknown").to_string()
    } else {
        "snapshot".to_string()
    }
}

fn fetch_remote_manifest(args: &ValidateDownloadArgs, progress: bool) -> Result<Vec<RemoteObject>> {
    let effective_no_sign = args.no_sign_request && !args.signed;
    let (bucket, base_prefix) = parse_s3_uri(&args.s3_uri)?;
    let ds_prefix = dataset_prefix(&args.dataset);
    let prefix = if base_prefix.is_empty() {
        ds_prefix
    } else if ds_prefix.is_empty() {
        format!("{}/", base_prefix.trim_end_matches('/'))
    } else {
        format!("{}/{ds_prefix}", base_prefix.trim_end_matches('/'))
    };

    let mut all = Vec::new();
    let manifest_pb = if progress {
        let pb = ProgressBar::new_spinner();
        pb.set_prefix("validate-manifest");
        pb.set_message("fetching remote manifest pages");
        pb.enable_steady_tick(std::time::Duration::from_millis(120));
        Some(pb)
    } else {
        None
    };
    let mut token: Option<String> = None;
    let mut pages: u64 = 0;
    loop {
        let mut cmd = vec![
            "s3api".to_string(),
            "list-objects-v2".to_string(),
            "--bucket".to_string(),
            bucket.clone(),
            "--prefix".to_string(),
            prefix.clone(),
            "--output".to_string(),
            "json".to_string(),
        ];
        if let Some(t) = &token {
            cmd.push("--continuation-token".to_string());
            cmd.push(t.clone());
        }
        cmd.extend(aws_common_flags_from(
            &args.endpoint_url,
            &args.region,
            &args.profile_name,
            effective_no_sign,
        ));
        let out = run_aws(&args.aws_bin, &cmd)?;
        let v: serde_json::Value =
            serde_json::from_str(&out).context("invalid aws list-objects-v2 JSON")?;
        pages += 1;
        if let Some(pb) = &manifest_pb {
            pb.set_message(format!("pages={} objects={}", pages, all.len()));
        }
        if let Some(arr) = v.get("Contents").and_then(|x| x.as_array()) {
            for item in arr {
                let key = item
                    .get("Key")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                if key.is_empty() {
                    continue;
                }
                let size = item.get("Size").and_then(|x| x.as_u64()).unwrap_or(0);
                let etag = item
                    .get("ETag")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let last_modified = item
                    .get("LastModified")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                all.push(RemoteObject {
                    key,
                    size,
                    etag,
                    last_modified,
                });
            }
            if let Some(pb) = &manifest_pb {
                pb.set_message(format!("pages={} objects={}", pages, all.len()));
            }
        }
        let truncated = v
            .get("IsTruncated")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        if truncated {
            token = v
                .get("NextContinuationToken")
                .and_then(|x| x.as_str())
                .map(str::to_string);
            if token.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    if let Some(pb) = manifest_pb {
        pb.finish_with_message(format!(
            "validate-manifest pages={} objects={}",
            pages,
            all.len()
        ));
    }
    all.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(all)
}

fn build_local_manifest(snapshot_dir: &Path, dataset: &str) -> Result<Vec<RemoteObject>> {
    let mut out = Vec::new();
    let root = if dataset == "all" {
        snapshot_dir.to_path_buf()
    } else {
        snapshot_dir.join("data").join(dataset)
    };
    if !root.exists() {
        return Ok(out);
    }
    for e in WalkDir::new(&root).into_iter().filter_map(|x| x.ok()) {
        if !e.file_type().is_file() {
            continue;
        }
        let p = e.path();
        if p.components()
            .any(|c| c.as_os_str() == ".openalex_download_metadata")
        {
            continue;
        }
        let rel = p
            .strip_prefix(snapshot_dir)
            .map(|x| x.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| p.to_string_lossy().replace('\\', "/"));
        let m = fs::metadata(p)?;
        let last_modified = m
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default();
        out.push(RemoteObject {
            key: rel,
            size: m.len(),
            etag: "".to_string(),
            last_modified,
        });
    }
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}

fn list_scoped_gz_files(snapshot_dir: &Path, dataset: &str) -> Result<Vec<PathBuf>> {
    let root = if dataset == "all" {
        snapshot_dir.join("data")
    } else {
        snapshot_dir.join("data").join(dataset)
    };
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    for e in WalkDir::new(&root).into_iter().filter_map(|x| x.ok()) {
        if e.file_type().is_file() && e.path().extension().and_then(|s| s.to_str()) == Some("gz") {
            out.push(e.path().to_path_buf());
        }
    }
    out.sort();
    Ok(out)
}

fn gzip_integrity_ok(path: &Path) -> Result<()> {
    use flate2::read::GzDecoder;
    use std::io::Read;
    let f = fs::File::open(path)?;
    let mut d = GzDecoder::new(f);
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = d.read(&mut buf)?;
        if n == 0 {
            break;
        }
    }
    Ok(())
}

fn explain_download(args: &DownloadArgs) -> Result<()> {
    let cmd = aws_sync_command(args)?;
    let effective_no_sign = args.no_sign_request && !args.signed;
    let effective_delete = args.delete_files && !args.no_delete;
    println!("--explain: download");
    println!("aws_bin: {}", args.aws_bin.display());
    println!("snapshot_dir: {}", args.snapshot_dir.display());
    println!("dataset: {}", args.dataset);
    println!("no_sign_request: {}", effective_no_sign);
    println!("delete_files: {}", effective_delete);
    println!("sync_command: {} {}", args.aws_bin.display(), cmd.join(" "));
    println!("verify: not part of download; run verify_download separately");
    Ok(())
}

fn explain_validate_download(args: &ValidateDownloadArgs, tuning: &Tuning) {
    println!("--explain: validate-download");
    println!("aws_bin: {}", args.aws_bin.display());
    println!("snapshot_dir: {}", args.snapshot_dir.display());
    println!("s3_uri: {}", args.s3_uri);
    println!("dataset: {}", args.dataset);
    println!("check_extra: {}", args.check_extra);
    println!("workers: {}", tuning.workers);
}

#[derive(Debug, Clone)]
struct ReportRecord {
    source_kind: String,
    path: PathBuf,
    timestamp: i64,
    report: RunReport,
}

fn parse_report_filename(name: &str) -> Option<(String, i64)> {
    let stem = name.strip_suffix(".json")?;
    let mut parts = stem.rsplitn(2, '-');
    let ts = parts.next()?.parse::<i64>().ok()?;
    let cmd = parts.next()?.to_string();
    if cmd.is_empty() {
        return None;
    }
    Some((cmd, ts))
}

fn report_roots(snapshot_dir: &Path, _parquet_dir: &Path, _source: &ReportSource) -> Vec<PathBuf> {
    // All commands (including download) write to the same global reports dir.
    vec![download_reports_dir(snapshot_dir)]
}

fn load_report_records(
    snapshot_dir: &Path,
    parquet_dir: &Path,
    source: &ReportSource,
) -> Result<Vec<ReportRecord>> {
    let roots = report_roots(snapshot_dir, parquet_dir, source);
    let mut out = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in
            fs::read_dir(&root).with_context(|| format!("failed to read {}", root.display()))?
        {
            let entry = entry?;
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let name = match p.file_name().and_then(|s| s.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let (_cmd, ts) = match parse_report_filename(name) {
                Some(v) => v,
                None => continue,
            };
            let txt = fs::read_to_string(&p)
                .with_context(|| format!("failed to read report {}", p.display()))?;
            let report: RunReport = serde_json::from_str(&txt)
                .with_context(|| format!("invalid report JSON {}", p.display()))?;
            let source_kind = if p.starts_with(download_metadata_root(snapshot_dir)) {
                "download".to_string()
            } else if p.starts_with(global_reports_dir(parquet_dir)) {
                "parquet-global".to_string()
            } else {
                "parquet-dataset".to_string()
            };
            out.push(ReportRecord {
                source_kind,
                path: p,
                timestamp: ts,
                report,
            });
        }
    }
    Ok(out)
}

fn metadata_root(parquet_dir: &Path) -> PathBuf {
    let root = parquet_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    root.join("openalex-snapshot_metadata")
}

fn dataset_metadata_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    metadata_root(parquet_dir).join(dataset)
}

fn prev_dataset_metadata_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    parquet_dir.join(format!(".{dataset}_conversion_metadata"))
}

fn legacy_dataset_cache_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    parquet_dir.join(dataset).join(".schema_cache")
}

fn dataset_cache_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    dataset_metadata_dir(parquet_dir, dataset).join("schemata")
}

fn dataset_convert_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    dataset_metadata_dir(parquet_dir, dataset).join("convert")
}

fn dataset_conversion_verify_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    dataset_metadata_dir(parquet_dir, dataset).join("conversion-verify")
}

fn dataset_index_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    dataset_metadata_dir(parquet_dir, dataset).join("index")
}

fn dataset_index_verify_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    dataset_metadata_dir(parquet_dir, dataset).join("index-verify")
}

fn archived_root_dir(parquet_dir: &Path) -> PathBuf {
    metadata_root(parquet_dir).join("archived")
}

fn lock_file_path(parquet_dir: &Path) -> PathBuf {
    metadata_root(parquet_dir).join("openalex-snapshot.lock")
}

fn global_reports_dir(parquet_dir: &Path) -> PathBuf {
    metadata_root(parquet_dir).join("reports")
}

fn latest_report_for_command(parquet_dir: &Path, command: &str) -> Option<PathBuf> {
    let dir = global_reports_dir(parquet_dir);
    let prefix = format!("{}-", sanitize_command_name(command));
    let mut candidates: Vec<PathBuf> = fs::read_dir(&dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix) && n.ends_with(".json"))
                    .unwrap_or(false)
        })
        .collect();
    candidates.sort();
    candidates.into_iter().last()
}

fn cleanup_command_reports(parquet_dir: &Path, command: &str) -> Result<()> {
    let prefix = format!("{}-", sanitize_command_name(command));
    let dir = global_reports_dir(parquet_dir);
    if !dir.exists() {
        return Ok(());
    }
    for ent in fs::read_dir(&dir)? {
        let ent = ent?;
        if !ent.path().is_file() {
            continue;
        }
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&prefix) && name.ends_with(".json") {
            let _ = fs::remove_file(ent.path());
        }
    }
    Ok(())
}

fn cleanup_command_dataset_logs(parquet_dir: &Path, log_command: &str) -> Result<()> {
    let meta_root = metadata_root(parquet_dir);
    if !meta_root.exists() {
        return Ok(());
    }
    for ent in fs::read_dir(&meta_root)? {
        let ent = ent?;
        if !ent.path().is_dir() {
            continue;
        }
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if matches!(name.as_ref(), "reports" | "archived" | "download") {
            continue;
        }
        // Clean logs from step subdirectories only — schemata/ is a persistent cache
        // and must never be removed by log cleanup.
        for step in &["convert", "conversion-verify", "index", "index-verify"] {
            let p = ent.path().join(step).join(format!("{log_command}.log"));
            if p.exists() {
                let _ = fs::remove_file(p);
            }
        }
    }
    Ok(())
}

fn schema_csv_candidates(parquet_dir: &Path, dataset: &str) -> Vec<PathBuf> {
    vec![
        dataset_cache_dir(parquet_dir, dataset).join("unified_schema.csv"),
        legacy_dataset_cache_dir(parquet_dir, dataset).join("unified_schema.csv"),
    ]
}

fn schema_json_candidates(parquet_dir: &Path, dataset: &str, file_name: &str) -> Vec<PathBuf> {
    vec![
        dataset_cache_dir(parquet_dir, dataset).join(file_name),
        legacy_dataset_cache_dir(parquet_dir, dataset).join(file_name),
    ]
}

fn move_dir_with_fallback(src: &Path, dst: &Path) -> Result<()> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::rename(src, dst) {
        Ok(()) => Ok(()),
        Err(_) => {
            fs::create_dir_all(dst)?;
            for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
                let rel = entry.path().strip_prefix(src)?;
                let target = dst.join(rel);
                if entry.file_type().is_dir() {
                    fs::create_dir_all(&target)?;
                } else if entry.file_type().is_file() {
                    if let Some(parent) = target.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::copy(entry.path(), &target)?;
                }
            }
            fs::remove_dir_all(src)?;
            Ok(())
        }
    }
}

fn merge_dir_with_fallback(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
        let rel = entry.path().strip_prefix(src)?;
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            if !target.exists() {
                fs::copy(entry.path(), &target)?;
            }
        }
    }
    fs::remove_dir_all(src)?;
    Ok(())
}

fn migrate_previous_metadata_root_if_needed(parquet_dir: &Path, dataset: &str) -> Result<()> {
    let new_root = dataset_metadata_dir(parquet_dir, dataset);
    let old_root = prev_dataset_metadata_dir(parquet_dir, dataset);
    if !old_root.exists() {
        return Ok(());
    }
    if !new_root.exists() {
        eprintln!(
            "[meta] dataset={dataset} migrating prior metadata root {} -> {}",
            old_root.display(),
            new_root.display()
        );
        move_dir_with_fallback(&old_root, &new_root)?;
    } else {
        eprintln!(
            "[meta] dataset={dataset} merging prior metadata root {} -> {}",
            old_root.display(),
            new_root.display()
        );
        merge_dir_with_fallback(&old_root, &new_root)?;
    }
    Ok(())
}

fn migrate_legacy_schema_cache_if_needed(parquet_dir: &Path, dataset: &str) -> Result<()> {
    migrate_previous_metadata_root_if_needed(parquet_dir, dataset)?;
    let new_dir = dataset_cache_dir(parquet_dir, dataset);
    let old_conversion = prev_dataset_metadata_dir(parquet_dir, dataset).join("schema_cache");
    let old_in_dataset = legacy_dataset_cache_dir(parquet_dir, dataset);
    if !new_dir.exists() && old_conversion.exists() {
        eprintln!(
            "[schema] dataset={dataset} migrating prior cache {} -> {}",
            old_conversion.display(),
            new_dir.display()
        );
        move_dir_with_fallback(&old_conversion, &new_dir)?;
    }
    if !new_dir.exists() && old_in_dataset.exists() {
        eprintln!(
            "[schema] dataset={dataset} migrating legacy cache {} -> {}",
            old_in_dataset.display(),
            new_dir.display()
        );
        move_dir_with_fallback(&old_in_dataset, &new_dir)?;
    }
    Ok(())
}

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn bytes_to_gib(bytes: u64) -> u64 {
    bytes / (1024 * 1024 * 1024)
}

fn now_unix_millis() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockInfo {
    pid: u32,
    command: String,
    started_at_unix: i64,
}

struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn pid_is_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn acquire_lock(parquet_dir: &Path, command: &str) -> Result<LockGuard> {
    let path = lock_file_path(parquet_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let info = LockInfo {
        pid: std::process::id(),
        command: command.to_string(),
        started_at_unix: now_unix(),
    };
    let payload = serde_json::to_vec_pretty(&info)?;
    write_json_atomic(&path, &payload)?;
    Ok(LockGuard { path })
}

fn check_lock(parquet_dir: &Path) -> Option<LockInfo> {
    let path = lock_file_path(parquet_dir);
    let txt = fs::read_to_string(&path).ok()?;
    let info: LockInfo = serde_json::from_str(&txt).ok()?;
    if pid_is_alive(info.pid) {
        Some(info)
    } else {
        None // stale lock
    }
}

fn archive_completed_run(parquet_dir: &Path, snapshot_dir: &Path) -> Result<()> {
    let meta_root = metadata_root(parquet_dir);
    let archived_root = archived_root_dir(parquet_dir);
    let timestamp = now_unix();
    let archive_base = archived_root.join(timestamp.to_string());

    let mut moved_any = false;

    // Move finished reports
    let reports_dir = global_reports_dir(parquet_dir);
    if reports_dir.exists() {
        for entry in fs::read_dir(&reports_dir)?.flatten() {
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let txt = fs::read_to_string(&p).unwrap_or_default();
            if let Ok(r) = serde_json::from_str::<RunReport>(&txt) {
                if r.finished_at_unix.is_some() {
                    let dest = archive_base.join("reports").join(p.file_name().unwrap());
                    fs::create_dir_all(dest.parent().unwrap())?;
                    fs::rename(&p, &dest)?;
                    moved_any = true;
                }
            }
        }
    }

    // Move download log
    let dl_log = download_log_path(snapshot_dir);
    if dl_log.exists() {
        let dest = archive_base.join("download").join("download.log");
        fs::create_dir_all(dest.parent().unwrap())?;
        fs::rename(&dl_log, &dest)?;
        moved_any = true;
    }

    // Move dataset logs — schemata/ is intentionally excluded: it is a persistent
    // cache that should survive across runs and must not be archived or removed.
    if meta_root.exists() {
        for entry in fs::read_dir(&meta_root)?.flatten() {
            let ds_dir = entry.path();
            if !ds_dir.is_dir() {
                continue;
            }
            let name = ds_dir.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if matches!(name, "reports" | "archived" | "download") {
                continue;
            }
            for step in &["convert", "conversion-verify", "index", "index-verify"] {
                let log_dir = ds_dir.join(step);
                if log_dir.exists() {
                    let dest = archive_base.join(name).join(step);
                    fs::create_dir_all(&dest)?;
                    for f in fs::read_dir(&log_dir)?.flatten() {
                        let fp = f.path();
                        if fp.is_file() {
                            fs::rename(&fp, dest.join(fp.file_name().unwrap()))?;
                            moved_any = true;
                        }
                    }
                }
            }
        }
    }

    if !moved_any && archive_base.exists() {
        let _ = fs::remove_dir_all(&archive_base);
    }

    Ok(())
}

fn check_path_writable(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    let probe = path.join(".openalex-check-write-probe.tmp");
    fs::write(&probe, b"ok")?;
    fs::remove_file(&probe)?;
    Ok(())
}

fn estimate_convert_input_bytes_precise(snapshot_dir: &Path, dataset: &str) -> Result<u64> {
    let mut total = 0u64;
    let data_root = snapshot_dir.join("data");
    if dataset == "all" {
        if !data_root.exists() {
            return Ok(0);
        }
        for e in WalkDir::new(&data_root).into_iter().filter_map(|e| e.ok()) {
            if e.file_type().is_file()
                && e.path().extension().and_then(|x| x.to_str()) == Some("gz")
            {
                total = total.saturating_add(e.metadata()?.len());
            }
        }
        return Ok(total);
    }
    let ds_root = data_root.join(dataset);
    if !ds_root.exists() {
        return Ok(0);
    }
    for e in WalkDir::new(&ds_root).into_iter().filter_map(|e| e.ok()) {
        if e.file_type().is_file() && e.path().extension().and_then(|x| x.to_str()) == Some("gz") {
            total = total.saturating_add(e.metadata()?.len());
        }
    }
    Ok(total)
}

fn skills_templates(root_dir: &Path) -> Vec<(PathBuf, String)> {
    let base = root_dir.join("skills");
    vec![
        (
            base.join("README.md"),
            r#"# Project Skills

These skills help AI coding agents operate and develop `openalex-snapshot` safely and consistently.

## Program Summary

`openalex-snapshot` is a root-dir-first CLI for OpenAlex snapshot workflows:

1. `download` / `verify_download`
2. `convert` / `verify_convert` / `repair_convert`
3. `index` / `verify_index`
4. `extract`
5. `schema` / `verify_schema`
6. reporting and progress (`report`, `prune-reports`, `progress`)

Runtime requirements:
- `aws` CLI for download/verify_download paths only
- No external `duckdb` binary needed — DuckDB is statically linked in the binary

Argument precedence to apply in all commands:
1. explicit CLI flags
2. config subcommand section values
3. config defaults section values
4. built-in defaults

Note: `--config` is a **global** flag and must precede the subcommand:
  `openalex-snapshot --config ./openalex-snapshot.yaml report`

## How to use these skills

- Start with `cli-operations/SKILL.md` for day-to-day operation.
- Use `pipeline-runbook/SKILL.md` for end-to-end execution.
- Use `debug-and-recovery/SKILL.md` when any command fails.
- Use `development/SKILL.md` when modifying source code.
- Use `release-and-docs/SKILL.md` when releasing or updating docs.

Canonical references:
- `ARCHITECTURE_AND_DECISIONS.md`
- `CLAUDE.md`
- `NEWS.md`
- CLI help and man pages
"#
            .to_string(),
        ),
        (
            base.join("cli-operations").join("SKILL.md"),
            r#"# CLI Operations Skill

## Purpose
Run subcommands with correct root-dir model and predictable outputs.

## Required Inputs
- `root_dir`
- target `dataset` or `all`
- resource settings (`profile`, `workers`, `max-memory-mb`) when needed

## Command Pattern
- Always prefer `--root-dir` or a `--config` file.
- `--config` is a global flag — place it before the subcommand:
  `openalex-snapshot --config ./openalex-snapshot.yaml <subcommand>`
- Use `--explain` before long runs to preview what will happen.
- Use `report` and `progress` for run-state visibility.

## Common command snippets

```bash
# Preflight check
openalex-snapshot check --root-dir <root> --dataset all

# Convert one dataset — default `safe` profile (single-worker, max memory) works on any host
openalex-snapshot convert --root-dir <root> --dataset works

# Faster on 32+ GB hosts: empirically tuned stratified profile partitions files by gz size
openalex-snapshot convert --root-dir <root> --dataset works --profile stratified-36

# Verify one dataset
openalex-snapshot verify_convert --root-dir <root> --dataset works --scope dataset --metadata-level both

# Show latest reports with per-dataset breakdown
openalex-snapshot --config ./openalex-snapshot.yaml report --latest

# Show aggregate totals only
openalex-snapshot --config ./openalex-snapshot.yaml report --latest --summary

# Extract by IDs
openalex-snapshot extract --root-dir <root> --ids <ids.csv> --output <extract.parquet>

# Repair from verify report
openalex-snapshot repair_convert --root-dir <root> --from-verify-report <report.json>
```

## Failure Handling
- On non-zero exit, inspect latest report: `report --latest --full`.
- Datasets with failures are marked `!` in the default report view.
- Use `repair_convert` for verify-driven reconversion.

## Decision rules
- Default profile is `safe` for `convert` / `repair_convert`: single worker, generous per-worker memory (~45% of usable RAM, clamped 8–24 GiB on single-worker mode).  Works on any host; the most reliable choice for the worst-case files.
- On a 32+ GB host, use `--profile stratified-36` for a faster run: it partitions the file list by gz size (4-/3-/2-/1-worker buckets) and runs one rayon pass per bucket.
- Custom RAM tiers (e.g. 16 GB, 64 GB) need a user-supplied `openalex-snapshot.profiles.yaml` — see [`docs/commands/convert.md`](../../docs/commands/convert.md#custom-profiles-via-profilesyaml).
- To isolate a single problematic file: repeated `--input-file` on `convert`.
- Prefer `verify_convert --scope file` for quick spot checks; `--scope dataset|snapshot` for full checks.
- Run `index` before `extract`; extraction requires `<dataset>_id_idx.parquet`.

## Done Criteria
- Command exits 0.
- Expected report written to `openalex-snapshot_metadata/reports/`.
"#
            .to_string(),
        ),
        (
            base.join("pipeline-runbook").join("SKILL.md"),
            r#"# Pipeline Runbook Skill

## Purpose
Execute the recommended end-to-end flow safely.

## Full flow
1. `check`
2. `download`
3. `verify_download`
4. `convert`
5. `verify_convert`
6. `repair_convert` (only when verify reports failures)
7. `index`
8. `verify_index`
9. `extract`

## Auto orchestration (recommended)
```bash
openalex-snapshot all --config <path> --retry 2
```
Runs all enabled stages in order with a bounded verify/repair loop.
Edit `all:` section in the config to disable stages you don't need (e.g. `enable_download: false`).

## Local snapshot already present (skip download)
```bash
openalex-snapshot convert --root-dir <root> --dataset all
openalex-snapshot verify_convert --root-dir <root> --scope snapshot
openalex-snapshot index --root-dir <root> --dataset all
openalex-snapshot verify_index --root-dir <root>
```

## Decision Rules
- Default profile is `safe`: single worker, generous memory, works on any host.  Use this for unattended runs and unfamiliar hardware.
- On a 32+ GB host where speed matters, set `profile: stratified-36` under `defaults:` in the config (or pass `--profile stratified-36` to `convert`).  It partitions files by gz size and parallelises each bucket.
- For other RAM tiers, supply a user-defined profile in `openalex-snapshot.profiles.yaml` (sibling to the main config or via `--profiles-config`).
- Use `--dataset <name>` to rerun a single dataset without touching others.
- Check `report --latest` after each stage to confirm success before proceeding.
- Keep reports: they drive `repair_convert` and provide audit trails.
"#
            .to_string(),
        ),
        (
            base.join("debug-and-recovery").join("SKILL.md"),
            r#"# Debug and Recovery Skill

## Purpose
Triage failures using metadata and reports.

## Steps
1. `openalex-snapshot --config <cfg> report --latest` — scan per-dataset table for `!` rows
2. `openalex-snapshot --config <cfg> report --latest --full` — full JSON for root cause
3. `progress --once` — check if a run is still live
4. Run targeted command with `--explain` to preview what it would do
5. Rerun failed dataset or use `repair_convert` as indicated

## Common Traps
- Wrong `root-dir` (snapshot/parquet/metadata dirs won't be found)
- Missing `aws` binary (only needed for download steps)
- Low disk space (`check --root-dir <root>` reports estimates)
- Using `--config` after the subcommand instead of before it

## Failure phase hints
- `check_dependency`: missing `aws` binary (duckdb is bundled — not an external dep)
- `check_download_disk` / `check_convert_disk`: insufficient free space
- `verify_metrics`: file-level parity mismatch — run `repair_convert`
- `download_sync`: S3 sync / auth / endpoint failure
- `validate_gzip_integrity`: corrupted `.json.gz` file

## OOM during convert
- `safe` is the default profile and should handle the largest works files via DuckDB spill-to-disk.  If you're explicitly running another profile, retry with `--profile safe`.
- Run `check --root-dir <root>` to see memory estimates.
- Use `--max-memory-mb <N>` to force a smaller DuckDB cap so spill kicks in earlier.
- Use `--split-size 256mb` to pre-chunk very large gz files before conversion.
- If a specific file is always failing, isolate it with `--input-file <rel-path>` and retry.
"#
            .to_string(),
        ),
        (
            base.join("development").join("SKILL.md"),
            r#"# Development Skill

## Purpose
Build, test, and deploy `openalex-snapshot` source changes safely.

## Repository layout
- All logic lives in `src/main.rs` (single file, ~10 000+ lines).
- Tests live in `tests/cli_smoke.rs`.
- Skills templates are embedded in `skills_templates()` near the end of `src/main.rs`.
- Config templates are embedded as `config_template_*()` functions in `src/main.rs`.

## Build / test loop
```bash
cargo build --release                     # production binary
cargo test --all-targets --locked         # run all 27 tests
cargo clippy --all-targets -- -D warnings # lint (must be clean)
cargo fmt --all                           # format (CI enforces)
```

Tests require the `duckdb` CLI binary in PATH for parquet-reading verification steps;
they skip gracefully when it is absent. The main binary does NOT need it — DuckDB is
statically linked via `duckdb = { version = "1", features = ["bundled", "json", "parquet"] }`.

## Deploy pattern
```bash
cargo build --release
cp target/release/openalex-snapshot <target-dir>/openalex-snapshot
```

## DuckDB in-process architecture
- A global `Connection` lives in `OnceLock<Mutex<Connection>>` (see `master_conn()`).
- Each rayon worker thread calls `master.try_clone()` once and stores it in `thread_local!`.
- The global DuckDB memory limit (`SET memory_limit`) is shared across ALL connections on
  the same database.  It's set **per stratum** to `stratum.memory_mb × stratum.workers` at
  the start of each rayon pass; changes are safe between passes.
- Spill-to-disk is enabled by `SET temp_directory='<root>/openalex-snapshot_metadata/duckdb_tmp/'`
  on the master connection (OnceLock-guarded so the assignment runs exactly once per process).
  Without this, an in-memory connection has no temp dir and OOMs when memory_limit is hit.

## Profiles and the stratified plan
- `convert` and `repair_convert` resolve `--profile <name>` against a `ProfileRegistry`
  populated from `builtin_profiles()` plus an optional user `openalex-snapshot.profiles.yaml`.
- Built-in profiles:
  - `safe` (default) — single pass, workers clamped 1..=2, generous per-worker memory
    (`auto_profile_single_worker_safe_memory_mb` returns 45% of usable RAM clamped 8–24 GiB
    on workers=1).
  - `stratified-36` — 4 strata tuned for ~36 GB hosts (4×4800 / 3×6400 / 2×9600 / 1×13000).
- `build_convert_plan(profile, workers_override, max_mem_mb_override, total_ram_mb, todo, &registry)`
  produces a `ConvertPlan { strata: Vec<StratumPlan>, flat: bool }`.
  - Safe → one flat stratum.
  - Stratified → file list partitioned by `gz_size_bytes`; one StratumPlan per non-empty
    bucket; largest-files-first execution order.
  - `--workers N` collapses stratified into one flat pass (largest stratum's memory).
- `run_convert` / `run_repair` iterate `plan.strata`, reconfiguring DuckDB + rayon per stratum.

## Worktree and PR conventions
- All changes go through a PR from a `claude/<name>` worktree branch.
- Never commit directly to `main` except for trivial fixes.
- Do NOT delete `claude/*` branches after merging — kept for AI audit trail.
- Tag releases on `main` after merge; pushing a `v*` tag triggers the release workflow.

## Required updates on any behavior change
1. `NEWS.md` — add entry under `[Unreleased]`
2. `docs/commands/<name>.md` — update affected command doc
3. `ARCHITECTURE_AND_DECISIONS.md` — update if invariants changed
4. `CLAUDE.md` — update if architecture or build model changed
5. Help text in `src/main.rs` (`*_LONG_ABOUT` constants, `#[arg(help = ...)]`)
6. Config template embedded in `src/main.rs` (if new options added)
7. Skills templates in `skills_templates()` in `src/main.rs` (if operational behavior changed)

## Adding a subcommand
1. Add `*_LONG_ABOUT` constant and register in top-level `CLI_LONG_ABOUT`.
2. Add `*Args` struct with `#[command]` derive.
3. Add config section struct and `apply_*_config()` wiring.
4. Add report persistence if command has operational outcomes.
5. Add tests in `tests/cli_smoke.rs`.
6. Update `NEWS.md`, `docs/commands/<name>.md`, `AI_SKILLS_USAGE.md`.

## Done Criteria
- `cargo test --all-targets --locked` passes (all 27 tests green).
- `cargo clippy --all-targets -- -D warnings` is clean.
- Binary deployed and smoke-tested against real data.
- `NEWS.md` and affected docs updated in the same commit.
"#
            .to_string(),
        ),
        (
            base.join("release-and-docs").join("SKILL.md"),
            r#"# Release and Docs Hygiene Skill

## Purpose
Keep docs, help text, and release notes in sync with behavior changes.

## Required updates on any feature change
- `NEWS.md` — add entry under `[Unreleased]`
- `docs/commands/<name>.md` — update command-specific docs
- `CLAUDE.md` — update if architecture or build model changed
- `ARCHITECTURE_AND_DECISIONS.md` — update if invariants changed
- Help text in `src/main.rs` (`*_LONG_ABOUT`, `#[arg(help = ...)]`, profile tables)
- Config template in `src/main.rs` (if new options added)
- Skills templates in `skills_templates()` in `src/main.rs` (if operational behavior changed)
- `AI_SKILLS_USAGE.md` — if skill structure changes

## Acceptance criteria
- New flags/commands appear in: `--help`, `README.md`, `docs/`, and `NEWS.md`
- Profile/memory tables in docs and help text match `builtin_profiles()` (in particular
  `stratified_baseline_36gb_strata()`) and `auto_profile_single_worker_safe_memory_mb`
- Tests cover CLI parsing + behavior + edge cases
- `openalex-snapshot --version` reflects the correct `Cargo.toml` version

## Release checklist
```bash
# 1. Bump version in Cargo.toml
# 2. Move [Unreleased] entries to [X.Y.Z] - YYYY-MM-DD in NEWS.md
cargo test --all-targets --locked
cargo clippy --all-targets -- -D warnings
cargo build --release
openalex-snapshot --help
openalex-snapshot --version
# 3. Commit, merge PR to main, push v* tag to trigger release workflow
```
"#
            .to_string(),
        ),
        (
            base.join("_templates").join("skill-template.md"),
            r#"# Skill Name

## Purpose
One-sentence objective.

## Required Inputs
- required context/flags

## Commands
Concrete command patterns.

## Decision Rules
When to choose one path vs another.

## Failure Handling
How to diagnose and recover.

## Done Criteria
What must be true to mark complete.
"#
            .to_string(),
        ),
    ]
}

fn convert_min_free_bytes(path: &Path) -> u64 {
    if let Ok(v) = std::env::var("OPENALEX_CONVERT_MIN_FREE_GB") {
        if let Ok(gb) = v.parse::<u64>() {
            return gb.saturating_mul(1024 * 1024 * 1024);
        }
    }
    let tmp = std::env::temp_dir();
    if path.starts_with(&tmp) {
        return 1024u64 * 1024u64 * 1024u64;
    }
    CONVERT_MIN_FREE_BYTES
}

fn available_disk_bytes(path: &Path) -> Result<u64> {
    let out = Command::new("df")
        .arg("-k")
        .arg(path)
        .output()
        .with_context(|| format!("failed to run df for {}", path.display()))?;
    if !out.status.success() {
        bail!(
            "df failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let txt = String::from_utf8_lossy(&out.stdout);
    let line = txt
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .ok_or_else(|| anyhow!("unexpected df output"))?;
    let cols: Vec<&str> = line.split_whitespace().collect();
    if cols.len() < 4 {
        bail!("unexpected df output row: {line}");
    }
    let avail_kb: u64 = cols[3]
        .parse()
        .with_context(|| format!("cannot parse df available kb from row: {line}"))?;
    Ok(avail_kb.saturating_mul(1024))
}

fn dataset_log_dir_for_command(parquet_dir: &Path, dataset: &str, command: &str) -> PathBuf {
    match command {
        "convert" | "convert-preview" => dataset_convert_dir(parquet_dir, dataset),
        "index" | "index-preview" => dataset_index_dir(parquet_dir, dataset),
        "verify" | "verify_convert" | "verify-convert" => {
            dataset_conversion_verify_dir(parquet_dir, dataset)
        }
        "verify-index" | "verify_index" => dataset_index_verify_dir(parquet_dir, dataset),
        _ => dataset_convert_dir(parquet_dir, dataset),
    }
}

fn append_dataset_log(parquet_dir: &Path, dataset: &str, command: &str, msg: &str) -> Result<()> {
    let dir = dataset_log_dir_for_command(parquet_dir, dataset, command);
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{command}.log"));
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(f, "{} [{}] {}", now_unix(), command, msg)?;
    Ok(())
}

fn try_log_dataset(parquet_dir: &Path, dataset: &str, command: &str, msg: &str) {
    if let Err(e) = append_dataset_log(parquet_dir, dataset, command, msg) {
        eprintln!("[log] dataset={dataset} command={command} write failed: {e}");
    }
}

fn sanitize_command_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn command_flow_rank(cmd: &str) -> usize {
    match cmd {
        "all" => 0,
        "download" => 1,
        "verify_download" | "validate-download" | "verify-download" => 2,
        "check" => 3,
        "convert" => 4,
        "verify" | "verify_convert" | "verify-convert" => 5,
        "verify_schema" | "verify-schema" => 6,
        "index" => 7,
        "extract" => 8,
        "verify-index" | "verify_index" => 9,
        "repair_convert" | "repair-convert" | "repair" => 10,
        _ => 100,
    }
}

fn report_file_name(command: &str, started_at_unix: i64, report_nonce: u128) -> String {
    if report_nonce == 0 {
        format!(
            "{}-{}.json",
            sanitize_command_name(command),
            started_at_unix
        )
    } else {
        format!(
            "{}-{}-{}.json",
            sanitize_command_name(command),
            started_at_unix,
            report_nonce
        )
    }
}

fn write_json_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn write_run_reports(parquet_dir: &Path, report: &RunReport) -> Result<Vec<PathBuf>> {
    let mut out_paths = Vec::new();
    let fname = report_file_name(&report.command, report.started_at_unix, report.report_nonce);
    let payload = serde_json::to_vec_pretty(report)?;

    let global = global_reports_dir(parquet_dir).join(&fname);
    write_json_atomic(&global, &payload)?;
    out_paths.push(global);

    Ok(out_paths)
}

fn report_new(command: &str, args: BTreeMap<String, String>) -> RunReport {
    RunReport {
        command: command.to_string(),
        cli_version: env!("CARGO_PKG_VERSION").to_string(),
        report_nonce: now_unix_millis(),
        started_at_unix: now_unix(),
        finished_at_unix: None,
        duration_seconds: None,
        args,
        totals_items_scanned: 0,
        totals_succeeded: 0,
        totals_failed: 0,
        totals_skipped: 0,
        datasets: Vec::new(),
        failures: Vec::new(),
        step_runs: Vec::new(),
    }
}

fn report_finalize(report: &mut RunReport) {
    let finished = now_unix();
    report.finished_at_unix = Some(finished);
    report.duration_seconds = Some((finished - report.started_at_unix) as f64);
    report.totals_items_scanned = report.datasets.iter().map(|d| d.items_scanned).sum();
    report.totals_succeeded = report.datasets.iter().map(|d| d.succeeded).sum();
    report.totals_failed = report.datasets.iter().map(|d| d.failed).sum();
    report.totals_skipped = report.datasets.iter().map(|d| d.skipped).sum();
}

#[allow(clippy::too_many_arguments)]
fn load_or_infer_source_schema(
    duckdb_bin: &Path,
    snapshot_dir: &Path,
    parquet_dir: &Path,
    dataset: &str,
    sample_size: usize,
    refresh: bool,
    memory_mb: Option<usize>,
    workers: usize,
    state_flush_every: usize,
) -> Result<SchemaDoc> {
    migrate_legacy_schema_cache_if_needed(parquet_dir, dataset)?;
    let cache_file = dataset_cache_dir(parquet_dir, dataset).join("source_schema.json");
    let unified_csv = dataset_cache_dir(parquet_dir, dataset).join("unified_schema.csv");

    // Canonical cache precedence: unified_schema.csv first.
    if !refresh {
        for csv in schema_csv_candidates(parquet_dir, dataset) {
            if csv.exists() {
                eprintln!(
                    "[schema] dataset={dataset} using canonical cache: {}",
                    csv.display()
                );
                let doc = read_unified_schema_csv(dataset, &csv)?;
                // Optional mirror cache for inspection/debugging only.
                fs::create_dir_all(dataset_cache_dir(parquet_dir, dataset))?;
                fs::write(&cache_file, serde_json::to_vec_pretty(&doc)?)?;
                return Ok(doc);
            }
        }
    }

    let mut files = Vec::new();
    let root = snapshot_dir.join("data").join(dataset);
    for entry in WalkDir::new(&root).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("gz")
        {
            files.push(entry.path().to_path_buf());
        }
    }
    files.sort();

    if files.is_empty() {
        bail!("no source .gz files found for dataset {dataset}");
    }

    let sample: Vec<PathBuf> = if sample_size == 0 || files.len() <= sample_size {
        files
    } else {
        let step = (files.len() / sample_size).max(1);
        files.into_iter().step_by(step).take(sample_size).collect()
    };

    let n_workers = workers.max(1);
    eprintln!(
        "[schema] dataset={dataset} inferring schema from {} sampled source files (workers={n_workers})",
        sample.len()
    );
    let extra = if dataset == "works" {
        ", maximum_object_size=1000000000"
    } else {
        ""
    };
    // Run describe calls in parallel, collect (index, result) to preserve order for logging.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_workers)
        .build()?;
    let duckdb_arc = std::sync::Arc::new(duckdb_bin.to_path_buf());
    let results: Vec<(usize, Result<BTreeMap<String, String>>)> = pool.install(|| {
        sample
            .par_iter()
            .enumerate()
            .map(|(i, f)| {
                let cols = describe_json_file(&duckdb_arc, f, extra, memory_mb);
                (i, cols)
            })
            .collect()
    });

    let mut merged: HashMap<String, String> = HashMap::new();
    let flush_every = state_flush_every.max(1);
    let in_progress_csv =
        dataset_cache_dir(parquet_dir, dataset).join("unified_schema.in_progress.csv");
    let in_progress_json =
        dataset_cache_dir(parquet_dir, dataset).join("source_schema.in_progress.json");
    let total = results.len();
    for (i, cols_result) in results {
        let cols = cols_result?;
        for (k, t) in cols {
            merged
                .entry(k)
                .and_modify(|existing| {
                    if existing != &t {
                        *existing = widen_type(existing, &t);
                    }
                })
                .or_insert(t);
        }
        if (i + 1) % flush_every == 0 || i + 1 == total {
            let checkpoint_doc = schema_doc_from_merged(dataset, &merged);
            write_unified_schema_csv(&in_progress_csv, &checkpoint_doc)?;
            write_json_atomic(
                &in_progress_json,
                &serde_json::to_vec_pretty(&checkpoint_doc)?,
            )?;
            eprintln!(
                "[schema] dataset={dataset} processed {}/{} schema sample files",
                i + 1,
                total
            );
        }
    }
    let doc = schema_doc_from_merged(dataset, &merged);

    fs::create_dir_all(dataset_cache_dir(parquet_dir, dataset))?;
    write_unified_schema_csv(&unified_csv, &doc)?;
    fs::write(&cache_file, serde_json::to_vec_pretty(&doc)?)?;
    let _ = fs::remove_file(&in_progress_csv);
    let _ = fs::remove_file(&in_progress_json);
    eprintln!(
        "[schema] dataset={dataset} wrote canonical schema cache: {}",
        unified_csv.display()
    );
    Ok(doc)
}

fn load_parquet_schema(
    duckdb_bin: &Path,
    parquet_dir: &Path,
    dataset: &str,
    refresh: bool,
    memory_mb: Option<usize>,
) -> Result<SchemaDoc> {
    migrate_legacy_schema_cache_if_needed(parquet_dir, dataset)?;
    let cache_file = dataset_cache_dir(parquet_dir, dataset).join("parquet_schema.json");
    if !refresh {
        for p in schema_json_candidates(parquet_dir, dataset, "parquet_schema.json") {
            if p.exists() {
                let txt = fs::read_to_string(&p)?;
                return serde_json::from_str(&txt).context("invalid parquet schema cache");
            }
        }
    }

    let glob = parquet_dir
        .join(dataset)
        .join("**")
        .join("*.parquet")
        .to_string_lossy()
        .to_string();

    let cols = describe_parquet_glob(duckdb_bin, &glob, memory_mb)?;
    let mut fields: Vec<FieldDef> = cols
        .into_iter()
        .map(|(name, t)| FieldDef {
            name,
            r#type: duckdb_to_arrow_type(&t),
            nullable: true,
            children: Vec::new(),
            metadata: {
                let mut md = BTreeMap::new();
                md.insert("duckdb_type".to_string(), t);
                md
            },
        })
        .collect();
    fields.sort_by(|a, b| a.name.cmp(&b.name));

    let doc = SchemaDoc {
        dataset: dataset.to_string(),
        source: "parquet".to_string(),
        generated_at_unix: now_unix(),
        fields,
        metadata: BTreeMap::new(),
    };

    fs::create_dir_all(dataset_cache_dir(parquet_dir, dataset))?;
    fs::write(&cache_file, serde_json::to_vec_pretty(&doc)?)?;
    Ok(doc)
}

fn schema_doc_from_merged(dataset: &str, merged: &HashMap<String, String>) -> SchemaDoc {
    let mut fields: Vec<FieldDef> = merged
        .iter()
        .map(|(name, t)| FieldDef {
            name: name.clone(),
            r#type: duckdb_to_arrow_type(t),
            nullable: true,
            children: Vec::new(),
            metadata: {
                let mut md = BTreeMap::new();
                md.insert("duckdb_type".to_string(), t.clone());
                md
            },
        })
        .collect();
    fields.sort_by(|a, b| a.name.cmp(&b.name));

    if dataset == "works" {
        if let Some(f) = fields
            .iter_mut()
            .find(|x| x.name == "abstract_inverted_index")
        {
            f.r#type = "utf8".to_string();
        }
    }

    SchemaDoc {
        dataset: dataset.to_string(),
        source: "source".to_string(),
        generated_at_unix: now_unix(),
        fields,
        metadata: BTreeMap::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn load_schema_by_policy(
    duckdb_bin: &Path,
    snapshot_dir: &Path,
    parquet_dir: &Path,
    dataset: &str,
    from: SchemaFrom,
    sample_size: usize,
    refresh: bool,
    memory_mb: Option<usize>,
    workers: usize,
    state_flush_every: usize,
) -> Result<SchemaDoc> {
    migrate_legacy_schema_cache_if_needed(parquet_dir, dataset)?;
    let cache_file = dataset_cache_dir(parquet_dir, dataset).join("source_schema.json");
    let unified_csv = dataset_cache_dir(parquet_dir, dataset).join("unified_schema.csv");

    match from {
        SchemaFrom::Source => load_or_infer_source_schema(
            duckdb_bin,
            snapshot_dir,
            parquet_dir,
            dataset,
            sample_size,
            refresh,
            memory_mb,
            workers,
            state_flush_every,
        ),
        SchemaFrom::Cache => {
            for p in schema_csv_candidates(parquet_dir, dataset) {
                if p.exists() {
                    return read_unified_schema_csv(dataset, &p);
                }
            }
            for p in schema_json_candidates(parquet_dir, dataset, "source_schema.json") {
                if p.exists() {
                    let txt = fs::read_to_string(&p)?;
                    return serde_json::from_str(&txt).context("invalid legacy cache schema");
                }
            }
            let first = schema_csv_candidates(parquet_dir, dataset)
                .into_iter()
                .next()
                .unwrap_or(unified_csv);
            let _ = cache_file;
            bail!("cache not found: {}", first.display())
        }
        SchemaFrom::Parquet => {
            load_parquet_schema(duckdb_bin, parquet_dir, dataset, refresh, memory_mb)
        }
        SchemaFrom::Auto => {
            let src_root = snapshot_dir.join("data").join(dataset);
            if src_root.exists() {
                return load_or_infer_source_schema(
                    duckdb_bin,
                    snapshot_dir,
                    parquet_dir,
                    dataset,
                    sample_size,
                    refresh,
                    memory_mb,
                    workers,
                    state_flush_every,
                );
            }
            for p in schema_csv_candidates(parquet_dir, dataset) {
                if p.exists() {
                    return read_unified_schema_csv(dataset, &p);
                }
            }
            for p in schema_json_candidates(parquet_dir, dataset, "source_schema.json") {
                if p.exists() {
                    let txt = fs::read_to_string(&p)?;
                    return serde_json::from_str(&txt).context("invalid legacy cache schema");
                }
            }
            load_parquet_schema(duckdb_bin, parquet_dir, dataset, refresh, memory_mb)
        }
    }
}

/// Split a gzipped line-delimited JSON file into smaller gz chunks.
/// Each chunk targets at most `target_uncompressed_bytes` of uncompressed data.
/// Chunks are written to `chunk_dir/{stem}_001.gz`, `{stem}_002.gz`, …
/// Returns the list of chunk paths.
fn split_gz_lines(
    input: &Path,
    chunk_dir: &Path,
    stem: &str,
    target_uncompressed_bytes: usize,
) -> Result<Vec<PathBuf>> {
    use flate2::read::GzDecoder;
    use std::io::{BufRead, BufReader, BufWriter, Write};

    fs::create_dir_all(chunk_dir)
        .with_context(|| format!("failed to create split temp dir {}", chunk_dir.display()))?;

    let file =
        fs::File::open(input).with_context(|| format!("failed to open {}", input.display()))?;
    let gz_reader = BufReader::new(GzDecoder::new(BufReader::new(file)));

    let mut chunk_paths: Vec<PathBuf> = Vec::new();
    let mut chunk_idx: usize = 1;
    let mut bytes_in_chunk: usize = 0;
    let mut current_path = chunk_dir.join(format!("{stem}_{chunk_idx:03}.json"));
    let mut writer = BufWriter::new(
        fs::File::create(&current_path)
            .with_context(|| format!("failed to create chunk {}", current_path.display()))?,
    );

    for line in gz_reader.lines() {
        let line = line.with_context(|| format!("read error in {}", input.display()))?;
        if line.is_empty() {
            continue;
        }
        // Roll over to next chunk when current chunk is full.
        if bytes_in_chunk >= target_uncompressed_bytes && bytes_in_chunk > 0 {
            writer.flush()?;
            drop(writer);
            chunk_paths.push(current_path);
            chunk_idx += 1;
            bytes_in_chunk = 0;
            current_path = chunk_dir.join(format!("{stem}_{chunk_idx:03}.json"));
            writer =
                BufWriter::new(fs::File::create(&current_path).with_context(|| {
                    format!("failed to create chunk {}", current_path.display())
                })?);
        }
        let b = line.as_bytes();
        writer.write_all(b)?;
        writer.write_all(b"\n")?;
        bytes_in_chunk += b.len() + 1;
    }
    writer.flush()?;
    drop(writer);
    chunk_paths.push(current_path);
    Ok(chunk_paths)
}

/// Returns true if split parquet chunks exist for the given base output path.
/// e.g. if `part_0000_001.parquet` exists alongside where `part_0000.parquet` would be.
fn split_parquets_exist(out_path: &Path) -> bool {
    let stem = out_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let dir = out_path.parent().unwrap_or(Path::new("."));
    dir.join(format!("{stem}_001.parquet")).exists()
}

// Returns all split chunk parquets for a source in sorted order, or empty if none.
fn list_split_parquets(out_path: &Path) -> Vec<PathBuf> {
    let stem = out_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    let dir = out_path.parent().unwrap_or(Path::new("."));
    let mut chunks: Vec<PathBuf> = (1u32..)
        .map(|i| dir.join(format!("{stem}_{i:03}.parquet")))
        .take_while(|p| p.exists())
        .collect();
    chunks.sort();
    chunks
}

fn convert_one(
    duckdb_bin: &Path,
    pair: &FilePair,
    columns_clause: &str,
    compression: &str,
    row_group_rows: usize,
    memory_mb: Option<usize>,
    extra_json_options: &str,
) -> Result<()> {
    if let Some(parent) = pair.output_parquet.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = pair.output_parquet.with_extension("parquet.tmp");
    let in_q = sql_quote(&pair.input_gz.to_string_lossy());
    let out_q = sql_quote(&tmp.to_string_lossy());

    let mut sql = String::new();
    sql.push_str("SET preserve_insertion_order = false;");
    sql.push_str(&format!(
        "COPY (SELECT * FROM read_json({}, columns = {}, union_by_name = true, ignore_errors = true)) TO {} (FORMAT PARQUET, COMPRESSION {}, ROW_GROUP_SIZE {});",
        in_q,
        columns_clause,
        out_q,
        compression.to_uppercase(),
        row_group_rows
    ));
    if !extra_json_options.is_empty() {
        sql.clear();
        sql.push_str("SET preserve_insertion_order = false;");
        sql.push_str(&format!(
            "COPY (SELECT * FROM read_json({}, columns = {}, union_by_name = true, ignore_errors = true{}) ) TO {} (FORMAT PARQUET, COMPRESSION {}, ROW_GROUP_SIZE {});",
            in_q,
            columns_clause,
            extra_json_options,
            out_q,
            compression.to_uppercase(),
            row_group_rows
        ));
    }
    let _ = memory_mb; // set globally via set_duckdb_memory_limit before the parallel pass

    run_duckdb_sql(duckdb_bin, &sql)?;
    fs::rename(&tmp, &pair.output_parquet)?;
    Ok(())
}

fn list_parquet_rel(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut out = BTreeSet::new();
    if !root.exists() {
        return Ok(out);
    }
    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        out.insert(entry.path().strip_prefix(root)?.to_path_buf());
    }
    Ok(out)
}

fn legacy_verify_cache_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    parquet_dir.join(dataset).join(".verify_cache")
}

fn prev_verify_cache_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    prev_dataset_metadata_dir(parquet_dir, dataset).join("verify_cache")
}

fn verify_cache_dir(parquet_dir: &Path, dataset: &str) -> PathBuf {
    dataset_conversion_verify_dir(parquet_dir, dataset)
}

fn migrate_legacy_verify_cache_if_needed(parquet_dir: &Path, dataset: &str) -> Result<()> {
    migrate_previous_metadata_root_if_needed(parquet_dir, dataset)?;
    let new_dir = verify_cache_dir(parquet_dir, dataset);
    let old_conversion = prev_verify_cache_dir(parquet_dir, dataset);
    let old_in_dataset = legacy_verify_cache_dir(parquet_dir, dataset);
    if !new_dir.exists() && old_conversion.exists() {
        eprintln!(
            "[verify] dataset={dataset} migrating prior cache {} -> {}",
            old_conversion.display(),
            new_dir.display()
        );
        move_dir_with_fallback(&old_conversion, &new_dir)?;
    }
    if !new_dir.exists() && old_in_dataset.exists() {
        eprintln!(
            "[verify] dataset={dataset} migrating legacy cache {} -> {}",
            old_in_dataset.display(),
            new_dir.display()
        );
        move_dir_with_fallback(&old_in_dataset, &new_dir)?;
    }
    Ok(())
}

fn source_metrics_cache_file(parquet_dir: &Path, dataset: &str) -> PathBuf {
    verify_cache_dir(parquet_dir, dataset).join("source_file_metrics.csv")
}

fn parquet_metrics_cache_file(parquet_dir: &Path, dataset: &str) -> PathBuf {
    verify_cache_dir(parquet_dir, dataset).join("parquet_file_metrics.csv")
}

fn load_source_metrics_cache(
    parquet_dir: &Path,
    dataset: &str,
) -> Result<HashMap<String, SourceMetricRow>> {
    migrate_legacy_verify_cache_if_needed(parquet_dir, dataset)?;
    let canonical = source_metrics_cache_file(parquet_dir, dataset);
    let legacy = legacy_verify_cache_dir(parquet_dir, dataset).join("source_file_metrics.csv");
    let f = if canonical.exists() {
        canonical
    } else {
        legacy
    };
    if !f.exists() {
        return Ok(HashMap::new());
    }
    let mut rdr = csv::Reader::from_path(&f)?;
    let mut out = HashMap::new();
    for rec in rdr.deserialize::<SourceMetricRow>() {
        let r = rec?;
        out.insert(r.rel_path.clone(), r);
    }
    Ok(out)
}

fn load_parquet_metrics_cache(
    parquet_dir: &Path,
    dataset: &str,
) -> Result<HashMap<String, ParquetMetricRow>> {
    migrate_legacy_verify_cache_if_needed(parquet_dir, dataset)?;
    let f = parquet_metrics_cache_file(parquet_dir, dataset);
    if !f.exists() {
        return Ok(HashMap::new());
    }
    let mut rdr = csv::Reader::from_path(&f)?;
    let mut out = HashMap::new();
    for rec in rdr.deserialize::<ParquetMetricRow>() {
        let r = rec?;
        out.insert(r.rel_path.clone(), r);
    }
    Ok(out)
}

fn save_source_metrics_cache(
    parquet_dir: &Path,
    dataset: &str,
    map: &HashMap<String, SourceMetricRow>,
) -> Result<()> {
    let dir = verify_cache_dir(parquet_dir, dataset);
    fs::create_dir_all(&dir)?;
    let f = source_metrics_cache_file(parquet_dir, dataset);
    let mut rows: Vec<SourceMetricRow> = map.values().cloned().collect();
    rows.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    let tmp = f.with_extension("csv.tmp");
    let mut wtr = csv::Writer::from_path(&tmp)?;
    for r in rows {
        wtr.serialize(r)?;
    }
    wtr.flush()?;
    fs::rename(&tmp, &f)?;
    Ok(())
}

fn save_parquet_metrics_cache(
    parquet_dir: &Path,
    dataset: &str,
    map: &HashMap<String, ParquetMetricRow>,
) -> Result<()> {
    let dir = verify_cache_dir(parquet_dir, dataset);
    fs::create_dir_all(&dir)?;
    let f = parquet_metrics_cache_file(parquet_dir, dataset);
    let mut rows: Vec<ParquetMetricRow> = map.values().cloned().collect();
    rows.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    let tmp = f.with_extension("csv.tmp");
    let mut wtr = csv::Writer::from_path(&tmp)?;
    for r in rows {
        wtr.serialize(r)?;
    }
    wtr.flush()?;
    fs::rename(&tmp, &f)?;
    Ok(())
}

fn file_meta_signature(path: &Path) -> Result<(u64, i64)> {
    let m = fs::metadata(path)?;
    let sz = m.len();
    let mt = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    Ok((sz, mt))
}

fn verify_file_metrics(
    duckdb_bin: &Path,
    p: &FilePair,
    level: VerifyMetadataLevel,
    source_metrics: &Arc<std::sync::Mutex<HashMap<String, SourceMetricRow>>>,
    parquet_metrics: &Arc<std::sync::Mutex<HashMap<String, ParquetMetricRow>>>,
    memory_mb: Option<usize>,
) -> Result<()> {
    let rel = p.rel.to_string_lossy().replace('\\', "/");
    let (src_sz, src_mt) = file_meta_signature(&p.input_gz)?;

    // Detect split parquets (1 source gz → N parquet chunks).
    let split_chunks = list_split_parquets(&p.output_parquet);
    let is_split = !split_chunks.is_empty();

    // Aggregate size/mtime across all chunks as the cache key.
    let (pq_sz, pq_mt) = if is_split {
        let mut total_sz: u64 = 0;
        let mut max_mt: i64 = 0;
        for chunk in &split_chunks {
            let (sz, mt) = file_meta_signature(chunk)?;
            total_sz += sz;
            max_mt = max_mt.max(mt);
        }
        (total_sz, max_mt)
    } else {
        file_meta_signature(&p.output_parquet)?
    };
    let need_hash = matches!(
        level,
        VerifyMetadataLevel::IdHash | VerifyMetadataLevel::Both
    );
    let cached = {
        let guard = source_metrics
            .lock()
            .map_err(|_| anyhow!("verify metrics cache lock poisoned"))?;
        guard.get(&rel).cloned()
    };
    let src_row = match cached {
        Some(cached)
            if cached.source_size == src_sz
                && cached.source_mtime_unix == src_mt
                && (!need_hash || !cached.id_hash.is_empty()) =>
        {
            cached
        }
        _ => {
            let (n, h) = if need_hash {
                duckdb_metrics_json(duckdb_bin, &p.input_gz, memory_mb)?
            } else {
                (
                    duckdb_count_json(duckdb_bin, &p.input_gz, memory_mb)?,
                    String::new(),
                )
            };
            let row = SourceMetricRow {
                rel_path: rel.clone(),
                source_size: src_sz,
                source_mtime_unix: src_mt,
                row_count: n,
                id_hash: h,
            };
            let mut guard = source_metrics
                .lock()
                .map_err(|_| anyhow!("verify metrics cache lock poisoned"))?;
            guard.insert(rel.clone(), row.clone());
            row
        }
    };
    let pq_cached = {
        let guard = parquet_metrics
            .lock()
            .map_err(|_| anyhow!("parquet metrics cache lock poisoned"))?;
        guard.get(&rel).cloned()
    };
    let pq_display = if is_split {
        format!(
            "{} (+{} chunks)",
            p.output_parquet.display(),
            split_chunks.len()
        )
    } else {
        p.output_parquet.display().to_string()
    };
    let query_parquet_metrics = |need_hash: bool| -> Result<(u64, String)> {
        if is_split {
            if need_hash {
                duckdb_metrics_parquet_list(duckdb_bin, &split_chunks, memory_mb)
            } else {
                duckdb_count_parquet_list(duckdb_bin, &split_chunks, memory_mb)
                    .map(|n| (n, String::new()))
            }
        } else if need_hash {
            duckdb_metrics_parquet(duckdb_bin, &p.output_parquet, memory_mb)
        } else {
            duckdb_count_parquet(duckdb_bin, &p.output_parquet, memory_mb)
                .map(|n| (n, String::new()))
        }
        .with_context(|| {
            format!(
                "parquet metrics failed for {}. \
If this parquet is corrupt/truncated, delete it and reconvert matching source file: {}",
                pq_display,
                p.rel.display()
            )
        })
    };
    let (pq_n, pq_h) = if let Some(c) = pq_cached {
        if c.parquet_size == pq_sz
            && c.parquet_mtime_unix == pq_mt
            && (!need_hash || !c.id_hash.is_empty())
        {
            (c.row_count, c.id_hash)
        } else {
            let (n, h) = query_parquet_metrics(need_hash)?;
            let row = ParquetMetricRow {
                rel_path: rel.clone(),
                parquet_size: pq_sz,
                parquet_mtime_unix: pq_mt,
                row_count: n,
                id_hash: h.clone(),
            };
            let mut guard = parquet_metrics
                .lock()
                .map_err(|_| anyhow!("parquet metrics cache lock poisoned"))?;
            guard.insert(rel.clone(), row);
            (n, h)
        }
    } else {
        let (n, h) = query_parquet_metrics(need_hash)?;
        let row = ParquetMetricRow {
            rel_path: rel.clone(),
            parquet_size: pq_sz,
            parquet_mtime_unix: pq_mt,
            row_count: n,
            id_hash: h.clone(),
        };
        let mut guard = parquet_metrics
            .lock()
            .map_err(|_| anyhow!("parquet metrics cache lock poisoned"))?;
        guard.insert(rel.clone(), row);
        (n, h)
    };

    if matches!(
        level,
        VerifyMetadataLevel::RowCount | VerifyMetadataLevel::Both
    ) && src_row.row_count != pq_n
    {
        bail!(
            "[verify] row count mismatch for {} (json={}, parquet={})",
            p.rel.display(),
            src_row.row_count,
            pq_n
        );
    }
    if matches!(
        level,
        VerifyMetadataLevel::IdHash | VerifyMetadataLevel::Both
    ) && src_row.id_hash != pq_h
    {
        bail!(
            "[verify] id hash mismatch for {} (json_hash={}, parquet_hash={})",
            p.rel.display(),
            src_row.id_hash,
            pq_h
        );
    }
    Ok(())
}

fn duckdb_metrics_json(
    duckdb_bin: &Path,
    path: &Path,
    memory_mb: Option<usize>,
) -> Result<(u64, String)> {
    let sql = with_session_settings(&format!(
        "SELECT COUNT(*) AS n, COALESCE(CAST(bit_xor(hash(CAST(id AS VARCHAR))) AS VARCHAR), '0') AS h FROM read_json_auto({}, union_by_name=true, ignore_errors=true)",
        sql_quote(&path.to_string_lossy())
    ), memory_mb, Some(1));
    let row = query_one_row(duckdb_bin, &sql)?;
    Ok((
        parse_u64(row.get("n"))?,
        row.get("h").cloned().unwrap_or_else(|| "0".to_string()),
    ))
}

fn duckdb_count_json(duckdb_bin: &Path, path: &Path, memory_mb: Option<usize>) -> Result<u64> {
    let sql = with_session_settings(
        &format!(
            "SELECT COUNT(*) AS n FROM read_json_auto({}, union_by_name=true, ignore_errors=true)",
            sql_quote(&path.to_string_lossy())
        ),
        memory_mb,
        Some(1),
    );
    let row = query_one_row(duckdb_bin, &sql)?;
    parse_u64(row.get("n"))
}

fn duckdb_metrics_parquet(
    duckdb_bin: &Path,
    path: &Path,
    memory_mb: Option<usize>,
) -> Result<(u64, String)> {
    let sql = with_session_settings(&format!(
        "SELECT COUNT(*) AS n, COALESCE(CAST(bit_xor(hash(CAST(id AS VARCHAR))) AS VARCHAR), '0') AS h FROM read_parquet({})",
        sql_quote(&path.to_string_lossy())
    ), memory_mb, Some(1));
    let row = query_one_row(duckdb_bin, &sql)?;
    Ok((
        parse_u64(row.get("n"))?,
        row.get("h").cloned().unwrap_or_else(|| "0".to_string()),
    ))
}

fn duckdb_count_parquet(duckdb_bin: &Path, path: &Path, memory_mb: Option<usize>) -> Result<u64> {
    let sql = with_session_settings(
        &format!(
            "SELECT COUNT(*) AS n FROM read_parquet({})",
            sql_quote(&path.to_string_lossy())
        ),
        memory_mb,
        Some(1),
    );
    let row = query_one_row(duckdb_bin, &sql)?;
    parse_u64(row.get("n"))
}

fn sql_path_list(paths: &[PathBuf]) -> String {
    let quoted: Vec<String> = paths
        .iter()
        .map(|p| sql_quote(&p.to_string_lossy()))
        .collect();
    format!("[{}]", quoted.join(", "))
}

fn duckdb_count_parquet_list(
    duckdb_bin: &Path,
    paths: &[PathBuf],
    memory_mb: Option<usize>,
) -> Result<u64> {
    let sql = with_session_settings(
        &format!(
            "SELECT COUNT(*) AS n FROM read_parquet({})",
            sql_path_list(paths)
        ),
        memory_mb,
        Some(1),
    );
    let row = query_one_row(duckdb_bin, &sql)?;
    parse_u64(row.get("n"))
}

fn duckdb_metrics_parquet_list(
    duckdb_bin: &Path,
    paths: &[PathBuf],
    memory_mb: Option<usize>,
) -> Result<(u64, String)> {
    let sql = with_session_settings(
        &format!(
            "SELECT COUNT(*) AS n, COALESCE(CAST(bit_xor(hash(CAST(id AS VARCHAR))) AS VARCHAR), '0') AS h FROM read_parquet({})",
            sql_path_list(paths)
        ),
        memory_mb,
        Some(1),
    );
    let row = query_one_row(duckdb_bin, &sql)?;
    Ok((
        parse_u64(row.get("n"))?,
        row.get("h").cloned().unwrap_or_else(|| "0".to_string()),
    ))
}

fn parse_u64(v: Option<&String>) -> Result<u64> {
    v.ok_or_else(|| anyhow!("missing numeric value"))?
        .parse::<u64>()
        .context("invalid integer in duckdb output")
}

fn describe_json_file(
    duckdb_bin: &Path,
    file: &Path,
    extra_options: &str,
    memory_mb: Option<usize>,
) -> Result<BTreeMap<String, String>> {
    let sql = with_session_settings(&format!(
        "SELECT * FROM (DESCRIBE SELECT * FROM read_json_auto({}, union_by_name=true, ignore_errors=true{}))",
        sql_quote(&file.to_string_lossy()),
        extra_options
    ), memory_mb, Some(1));
    describe_query(duckdb_bin, &sql)
}

fn describe_parquet_glob(
    duckdb_bin: &Path,
    glob: &str,
    memory_mb: Option<usize>,
) -> Result<BTreeMap<String, String>> {
    let sql = with_session_settings(
        &format!(
            "SELECT * FROM (DESCRIBE SELECT * FROM read_parquet({}))",
            sql_quote(glob)
        ),
        memory_mb,
        Some(1),
    );
    describe_query(duckdb_bin, &sql)
}

fn describe_query(duckdb_bin: &Path, sql: &str) -> Result<BTreeMap<String, String>> {
    let rows = run_duckdb_csv(duckdb_bin, sql)?;
    let mut out = BTreeMap::new();
    for row in rows {
        let name = row
            .get("column_name")
            .or_else(|| row.get("name"))
            .cloned()
            .ok_or_else(|| anyhow!("DESCRIBE row missing column name"))?;
        let ty = row
            .get("column_type")
            .or_else(|| row.get("type"))
            .cloned()
            .ok_or_else(|| anyhow!("DESCRIBE row missing column type"))?;
        out.insert(name, ty);
    }
    Ok(out)
}

fn to_duckdb_columns_clause(fields: &[FieldDef]) -> String {
    let defs = fields
        .iter()
        .map(|f| {
            let dt = f
                .metadata
                .get("duckdb_type")
                .cloned()
                .unwrap_or_else(|| arrow_to_duckdb_type(&f.r#type));
            format!("'{}': '{}'", f.name, dt.replace('\'', "''"))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{}}}", defs)
}

fn read_unified_schema_csv(dataset: &str, path: &Path) -> Result<SchemaDoc> {
    let mut rdr =
        csv::Reader::from_path(path).with_context(|| format!("cannot read {}", path.display()))?;
    let mut fields = Vec::<FieldDef>::new();
    for rec in rdr.deserialize::<HashMap<String, String>>() {
        let row = rec.context("invalid row in unified_schema.csv")?;
        let name = row
            .get("col_name")
            .cloned()
            .ok_or_else(|| anyhow!("missing col_name in unified_schema.csv"))?;
        let typ = row
            .get("col_type")
            .cloned()
            .ok_or_else(|| anyhow!("missing col_type in unified_schema.csv"))?;
        let mut md = BTreeMap::new();
        md.insert("duckdb_type".to_string(), typ.clone());
        fields.push(FieldDef {
            name,
            r#type: duckdb_to_arrow_type(&typ),
            nullable: true,
            children: Vec::new(),
            metadata: md,
        });
    }
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(SchemaDoc {
        dataset: dataset.to_string(),
        source: "source".to_string(),
        generated_at_unix: now_unix(),
        fields,
        metadata: BTreeMap::new(),
    })
}

fn write_unified_schema_csv(path: &Path, doc: &SchemaDoc) -> Result<()> {
    let mut wtr =
        csv::Writer::from_path(path).with_context(|| format!("cannot write {}", path.display()))?;
    wtr.write_record(["col_name", "col_type"])?;
    for f in &doc.fields {
        let dt = f
            .metadata
            .get("duckdb_type")
            .cloned()
            .unwrap_or_else(|| arrow_to_duckdb_type(&f.r#type));
        wtr.write_record([f.name.as_str(), dt.as_str()])?;
    }
    wtr.flush()?;
    Ok(())
}

fn arrow_to_duckdb_type(t: &str) -> String {
    match t {
        "bool" => "BOOLEAN".to_string(),
        "int8" => "TINYINT".to_string(),
        "int16" => "SMALLINT".to_string(),
        "int32" => "INTEGER".to_string(),
        "int64" => "BIGINT".to_string(),
        "float32" => "FLOAT".to_string(),
        "float64" => "DOUBLE".to_string(),
        "utf8" => "VARCHAR".to_string(),
        "date32" => "DATE".to_string(),
        "timestamp[us]" => "TIMESTAMP".to_string(),
        other => other.to_uppercase(),
    }
}

fn duckdb_to_arrow_type(t: &str) -> String {
    let tt = t.trim().to_uppercase();
    if tt.starts_with("STRUCT") {
        return "struct".to_string();
    }
    if tt.starts_with("LIST") {
        return "list".to_string();
    }
    match tt.as_str() {
        "BOOLEAN" => "bool".to_string(),
        "TINYINT" => "int8".to_string(),
        "SMALLINT" => "int16".to_string(),
        "INTEGER" | "INT" => "int32".to_string(),
        "BIGINT" | "HUGEINT" => "int64".to_string(),
        "FLOAT" => "float32".to_string(),
        "DOUBLE" | "DECIMAL" => "float64".to_string(),
        "DATE" => "date32".to_string(),
        "TIMESTAMP" => "timestamp[us]".to_string(),
        _ => "utf8".to_string(),
    }
}

fn widen_type(a: &str, b: &str) -> String {
    let na = normalize_duckdb_type(a);
    let nb = normalize_duckdb_type(b);
    if na == nb {
        return na;
    }

    let numeric = [
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE",
    ];
    let pa = numeric.iter().position(|x| *x == na);
    let pb = numeric.iter().position(|x| *x == nb);

    match (pa, pb) {
        (Some(i), Some(j)) => numeric[i.max(j)].to_string(),
        _ => {
            if na.starts_with("STRUCT") || na.starts_with("LIST") || na.starts_with("MAP") {
                na
            } else if nb.starts_with("STRUCT") || nb.starts_with("LIST") || nb.starts_with("MAP") {
                nb
            } else {
                "VARCHAR".to_string()
            }
        }
    }
}

fn normalize_duckdb_type(t: &str) -> String {
    t.trim().to_uppercase()
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

// ---------------------------------------------------------------------------
// In-process DuckDB: one shared database, one Connection per rayon/OS thread.
//
// DuckDB's bundled library has global state (signal handlers, allocators) that
// crashes when multiple separate in-memory databases co-exist in the same
// process. The safe pattern is ONE database shared by all threads, with each
// thread holding its own Connection cloned from a master via try_clone().
// ---------------------------------------------------------------------------

static MASTER_CONN: OnceLock<Mutex<duckdb::Connection>> = OnceLock::new();

fn master_conn() -> &'static Mutex<duckdb::Connection> {
    MASTER_CONN.get_or_init(|| {
        let conn =
            duckdb::Connection::open_in_memory().expect("failed to init master DuckDB database");
        conn.execute_batch(
            "SET autoinstall_known_extensions=false; SET autoload_known_extensions=true;",
        )
        .ok();
        Mutex::new(conn)
    })
}

thread_local! {
    static DUCKDB_CONN: RefCell<Option<duckdb::Connection>> = const { RefCell::new(None) };
}

fn with_conn<F, R>(f: F) -> Result<R>
where
    F: FnOnce(&duckdb::Connection) -> Result<R>,
{
    DUCKDB_CONN.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            // Clone a new connection from the shared master database.
            // Connections to the same database are independent and safe to use
            // concurrently; separate databases in the same process are not.
            let conn = {
                let master = master_conn().lock().expect("master conn lock poisoned");
                master
                    .try_clone()
                    .context("failed to clone DuckDB connection")?
            };
            // One internal DuckDB thread per connection; rayon provides external
            // file-level parallelism and must not compete with DuckDB's own pool.
            conn.execute_batch("SET threads=1;").ok();
            *opt = Some(conn);
        }
        f(opt.as_ref().unwrap())
    })
}

/// Split "SET a=1; SET b=2; QUERY" into (set_prefix_str, query_str).
fn split_set_prefix(sql: &str) -> (&str, &str) {
    let mut cursor = sql;
    loop {
        let trimmed = cursor.trim_start();
        if trimmed.is_empty() {
            let off = trimmed.as_ptr() as usize - sql.as_ptr() as usize;
            return (&sql[..off], "");
        }
        if trimmed.to_uppercase().starts_with("SET ") {
            match trimmed.find(';') {
                Some(i) => cursor = &trimmed[i + 1..],
                None => {
                    let off = trimmed.as_ptr() as usize - sql.as_ptr() as usize;
                    return (&sql[..off], "");
                }
            }
        } else {
            let off = trimmed.as_ptr() as usize - sql.as_ptr() as usize;
            return (&sql[..off], trimmed);
        }
    }
}

fn arrow_col_to_string(col: &dyn duckdb::arrow::array::Array, idx: usize) -> String {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::DataType;
    if col.is_null(idx) {
        return String::new();
    }
    macro_rules! cast_to_string {
        ($array_type:ty) => {
            col.as_any()
                .downcast_ref::<$array_type>()
                .map(|a| a.value(idx).to_string())
                .unwrap_or_default()
        };
    }
    match col.data_type() {
        DataType::Utf8 => cast_to_string!(StringArray),
        DataType::LargeUtf8 => cast_to_string!(LargeStringArray),
        DataType::Int8 => cast_to_string!(Int8Array),
        DataType::Int16 => cast_to_string!(Int16Array),
        DataType::Int32 => cast_to_string!(Int32Array),
        DataType::Int64 => cast_to_string!(Int64Array),
        DataType::UInt8 => cast_to_string!(UInt8Array),
        DataType::UInt16 => cast_to_string!(UInt16Array),
        DataType::UInt32 => cast_to_string!(UInt32Array),
        DataType::UInt64 => cast_to_string!(UInt64Array),
        DataType::Float32 => cast_to_string!(Float32Array),
        DataType::Float64 => cast_to_string!(Float64Array),
        DataType::Boolean => cast_to_string!(BooleanArray),
        _ => String::new(),
    }
}

/// Set the global DuckDB memory limit (database-wide, shared by all cloned connections).
/// With in-process DuckDB, memory_limit is a global setting — must be set once to the
/// total budget (per_worker_mb × workers), not per-worker, or all threads share the smaller value.
fn set_duckdb_memory_limit(total_mb: usize) {
    let master = master_conn().lock().expect("master conn lock poisoned");
    let _ = master.execute_batch(&format!("SET memory_limit='{total_mb}MB';"));
}

/// Set the DuckDB spill-to-disk directory on the master connection.
/// Without this, an in-memory DuckDB connection has no temp_directory and will OOM
/// instead of spilling when it hits the memory_limit.  Must be called before the
/// parallel convert pass so all cloned connections inherit the setting.
///
/// DuckDB only allows `SET temp_directory` once per process (switching after first use
/// is rejected with "Cannot switch temporary directory after the current one has been
/// used"). The OnceLock ensures this is applied exactly once regardless of how many
/// datasets are converted in a single run.
fn set_duckdb_temp_directory(dir: &Path) {
    static TEMP_DIR_INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    TEMP_DIR_INIT.get_or_init(|| {
        if let Err(e) = fs::create_dir_all(dir) {
            eprintln!(
                "[duckdb] warning: could not create temp_directory {}: {e}",
                dir.display()
            );
            return;
        }
        let master = master_conn().lock().expect("master conn lock poisoned");
        let path_str = dir.to_string_lossy();
        if let Err(e) = master.execute_batch(&format!("SET temp_directory='{path_str}';")) {
            eprintln!("[duckdb] warning: could not set temp_directory={path_str}: {e}");
        }
    });
}

fn run_duckdb_sql(_duckdb_bin: &Path, sql: &str) -> Result<()> {
    with_conn(|conn| {
        conn.execute_batch(sql)
            .with_context(|| format!("duckdb execute failed:\n{}", &sql[..sql.len().min(500)]))
    })
}

fn with_session_settings(sql: &str, memory_mb: Option<usize>, threads: Option<usize>) -> String {
    let mut out = String::new();
    if let Some(t) = threads {
        out.push_str(&format!("SET threads = {}; ", t.max(1)));
    }
    if let Some(mb) = memory_mb {
        out.push_str(&format!("SET memory_limit='{}MB'; ", mb));
    }
    out.push_str(sql);
    out
}

fn run_duckdb_csv(_duckdb_bin: &Path, sql: &str) -> Result<Vec<HashMap<String, String>>> {
    with_conn(|conn| {
        let (set_part, query_part) = split_set_prefix(sql);
        if !set_part.is_empty() {
            conn.execute_batch(set_part).context("duckdb SET failed")?;
        }
        let query_part = query_part.trim();
        if query_part.is_empty() {
            return Ok(Vec::new());
        }
        let mut stmt = conn
            .prepare(query_part)
            .with_context(|| format!("duckdb prepare failed:\n{query_part}"))?;
        // query_arrow executes the statement, populating the schema before iteration
        let mut arrow = stmt
            .query_arrow([])
            .with_context(|| format!("duckdb query failed:\n{query_part}"))?;
        let schema = arrow.get_schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let mut rows = Vec::new();
        for batch in &mut arrow {
            for row_i in 0..batch.num_rows() {
                let mut map = HashMap::new();
                for (col_i, name) in col_names.iter().enumerate() {
                    let s = arrow_col_to_string(batch.column(col_i).as_ref(), row_i);
                    map.insert(name.clone(), s);
                }
                rows.push(map);
            }
        }
        Ok(rows)
    })
}

fn query_one_row(duckdb_bin: &Path, sql: &str) -> Result<HashMap<String, String>> {
    let rows = run_duckdb_csv(duckdb_bin, sql)?;
    rows.into_iter()
        .next()
        .ok_or_else(|| anyhow!("query returned no rows"))
}

fn make_progress_bar(enabled: bool, len: u64, prefix: &str) -> ProgressBar {
    if !enabled {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::with_template(
            "{prefix} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} eta {eta}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    pb.set_prefix(prefix.to_string());
    pb
}

fn render_table(schema: &SchemaDoc) -> String {
    let mut out = String::new();
    out.push_str(&format!("dataset: {}\n", schema.dataset));
    out.push_str(&format!("source: {}\n", schema.source));
    out.push_str("\nname\ttype\tnullable\n");
    for f in &schema.fields {
        out.push_str(&format!("{}\t{}\t{}\n", f.name, f.r#type, f.nullable));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_widen_numeric() {
        assert_eq!(widen_type("INTEGER", "DOUBLE"), "DOUBLE");
        assert_eq!(widen_type("SMALLINT", "BIGINT"), "BIGINT");
    }

    #[test]
    fn test_sql_quote() {
        assert_eq!(sql_quote("abc"), "'abc'");
        assert_eq!(sql_quote("a'b"), "'a''b'");
    }

    #[test]
    fn test_arrow_mapping() {
        assert_eq!(duckdb_to_arrow_type("BIGINT"), "int64");
        assert_eq!(duckdb_to_arrow_type("VARCHAR"), "utf8");
        assert_eq!(arrow_to_duckdb_type("utf8"), "VARCHAR");
    }

    #[test]
    fn test_collect_repair_targets_filters_and_dedups() {
        let report = RunReport {
            command: "verify".to_string(),
            cli_version: "0.1.0".to_string(),
            report_nonce: 1,
            started_at_unix: 1,
            finished_at_unix: Some(2),
            duration_seconds: Some(1.0),
            args: BTreeMap::new(),
            totals_items_scanned: 2,
            totals_succeeded: 0,
            totals_failed: 2,
            totals_skipped: 0,
            datasets: vec![],
            failures: vec![
                FailureEntry {
                    dataset: "authors".to_string(),
                    phase: "verify_metrics".to_string(),
                    rel_path: Some("part_000/part1.gz".to_string()),
                    source_path: Some("/tmp/snapshot/data/authors/part_000/part1.gz".to_string()),
                    output_path: Some("/tmp/parquet/authors/part_000/part1.parquet".to_string()),
                    error_message: "x".to_string(),
                    suggested_recovery: None,
                },
                FailureEntry {
                    dataset: "authors".to_string(),
                    phase: "verify_metrics".to_string(),
                    rel_path: Some("part_000/part1.gz".to_string()),
                    source_path: Some("/tmp/snapshot/data/authors/part_000/part1.gz".to_string()),
                    output_path: Some("/tmp/parquet/authors/part_000/part1.parquet".to_string()),
                    error_message: "x".to_string(),
                    suggested_recovery: None,
                },
                FailureEntry {
                    dataset: "works".to_string(),
                    phase: "structure".to_string(),
                    rel_path: Some("part_000/part1.gz".to_string()),
                    source_path: None,
                    output_path: None,
                    error_message: "x".to_string(),
                    suggested_recovery: None,
                },
            ],
            step_runs: vec![],
        };
        let mut allowed = BTreeSet::new();
        allowed.insert("authors".to_string());
        let out = collect_repair_targets(
            &report,
            &allowed,
            Path::new("/tmp/snapshot"),
            Path::new("/tmp/parquet"),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].dataset, "authors");
    }

    #[test]
    fn test_extract_dataset_routing() {
        assert_eq!(
            extract_dataset_from_id("A5073096074").as_deref(),
            Some("authors")
        );
        assert_eq!(extract_dataset_from_id("W123").as_deref(), Some("works"));
        assert_eq!(
            extract_dataset_from_id("institution-types/other").as_deref(),
            Some("institution-types")
        );
        assert_eq!(
            extract_dataset_from_id("continents/europe").as_deref(),
            Some("continents")
        );
        assert_eq!(extract_dataset_from_id("X999"), None);
    }

    #[test]
    fn test_normalize_openalex_id() {
        assert_eq!(
            normalize_openalex_id("https://openalex.org/A5073096074"),
            "A5073096074"
        );
        assert_eq!(
            normalize_openalex_id("http://openalex.org/countries/us"),
            "countries/us"
        );
    }
}
