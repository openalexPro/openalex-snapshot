use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, LargeStringArray, ListArray,
    RecordBatch, StringArray, StructArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{Local, TimeZone};
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use openalex_core::profile::detect_total_memory_mb;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::stdout;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

OpenAlex now publishes the snapshot natively in parquet, so the pipeline is:
  download -> verify_download -> (auto) enrich -> index -> extract

This binary provides:
- config: create or verify YAML configuration
- all: run full config-driven pipeline (download/verify_download/index/verify_index)
- download: sync the official parquet snapshot from S3 (auto-enriches works)
- verify_download: validate the parquet corpus against the published manifest
- enrich: add abstract + citation columns to works (works_aws/ -> works/)
- index: build *_id_idx.parquet lookup index (R build_corpus_index equivalent)
- extract: extract rows by OpenAlex IDs using per-dataset indexes
- verify_index: validate index integrity and coverage
- report: view stored reports
- prune-reports: remove old report files
- progress: monitor live status from reports/logs
- skills: create AI skills starter pack under root_dir/skills
- check: run dependency/path/disk/memory preflight checks

download (detailed):
  - per-dataset sync: aws s3 sync s3://openalex/data/parquet/<ds>/ <root>/parquet/<ds>/
  - works lands in works_aws/ and is auto-enriched into works/ (skip with --no-enrich)
  - dataset list + disk preflight derive from s3://openalex/data/parquet/manifest.json
  - transfer tuning applied via a temporary AWS config (no global ~/.aws change)

verify_download (detailed):
  - validates presence, size (content_length), and row count (record_count) per manifest file
  - default uses footer metadata; --full does a row scan; --quick skips row counts

enrich (detailed):
  - reconstructs abstract from the JSON abstract_inverted_index and builds citation
  - incremental + row-parity checked; raw works_aws/ is never modified

index (detailed):
  - stage 1: per-file shard index build (resumable)
  - stage 2: shard combine into *_id_idx.parquet
  - outputs columns: id, id_block, parquet_file, file_row_number
  - index --dataset all skips *_aws staging dirs

extract (detailed):
  - reads IDs from CSV
  - routes IDs by entity prefix / taxonomy namespace
  - resolves files via *_id_idx.parquet
  - writes one parquet output per dataset

Examples:
  openalex-snapshot download --root-dir /data
  openalex-snapshot verify_download --root-dir /data
  openalex-snapshot enrich --root-dir /data
  openalex-snapshot index --root-dir /data --dataset all
  openalex-snapshot extract --root-dir /data --ids /data/ids.csv --output /data/extract.parquet
  openalex-snapshot verify_index --root-dir /data --dataset works
  openalex-snapshot report --root-dir /data --latest
  openalex-snapshot prune-reports --root-dir /data
  openalex-snapshot skills --root-dir /data
  openalex-snapshot check --root-dir /data --dataset all
  openalex-snapshot all --config ./openalex-snapshot.yaml
  openalex-snapshot progress --root-dir /data
  openalex-snapshot config --create complete
  openalex-snapshot config --create safe
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
Manage the openalex-snapshot YAML configuration.

The MAIN config (./openalex-snapshot.yaml) drives every subcommand: paths,
dataset selection, workers, memory caps, per-stage options.  Required by `all`,
and pointed at by the global `--config <path>` flag.

Modes (exactly one required):
  --create <complete|safe>   Generate the config template (default: complete)
  --verify                   Validate the config file strictly

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
  - required binaries (aws for download; parquet I/O is pure Rust, no duckdb needed)
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

Default stage order:
  1) download         (auto-enriches works -> parquet/works/)
  2) verify_download
  3) index
  4) verify_index
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

Tuning:
  Pure-Rust parquet I/O; reads only the `id` column per file. Memory needs are
  modest — use --workers / --max-memory-mb only to constrain resource use.
";

const ENRICH_LONG_ABOUT: &str = "\
Enrich the downloaded works parquet with derived columns.

Reads the raw official works from <root_dir>/parquet/works_aws/ and writes an
enriched copy to <root_dir>/parquet/works/ that adds two columns:
  abstract  — plain text reconstructed from the JSON abstract_inverted_index
  citation  — \"Author (year)\" / \"A & B (year)\" / \"A et al. (year)\"

Behavior:
  - Mirrors the updated_date=.../part_*.parquet partition layout.
  - Incremental: skips outputs newer than their source (use --overwrite to force).
  - Row-parity self-check: enriched row count must equal the source row count.
  - The raw works_aws/ is left untouched, so it stays manifest-verifiable and the
    next snapshot's incremental `aws s3 sync` is unaffected.

`download` runs this automatically for works unless --no-enrich is given.
Only the 'works' dataset is supported (other datasets need no enrichment).
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
    #[command(about = "Build a parquet lookup index for a parquet corpus.", long_about = INDEX_LONG_ABOUT)]
    Index(IndexArgs),
    #[command(about = "Extract rows by OpenAlex IDs using indexes.", long_about = EXTRACT_LONG_ABOUT)]
    Extract(ExtractArgs),
    #[command(about = "Enrich works parquet with abstract + citation columns.", long_about = ENRICH_LONG_ABOUT)]
    Enrich(EnrichArgs),
    #[command(
        about = "Verify index integrity for a parquet corpus.",
        long_about = VERIFY_INDEX_LONG_ABOUT,
        name = "verify_index"
    )]
    VerifyIndex(VerifyIndexArgs),
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

    #[arg(long, default_value_t = 1, hide = true)]
    #[arg(
        help = "[deprecated, no-op] accepted for back-compat; the convert retry loop was removed"
    )]
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
        help = "Write the MAIN config YAML (default ./openalex-snapshot.yaml). Template style: `complete` (default; documented, all sections) or `safe` (minimal). This is the file `--config` points at and `all` requires."
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
}

// VerifyScope / VerifyMetadataLevel: only parsed from a legacy verify_convert config section.
#[allow(dead_code)]
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
#[command(long_about = ENRICH_LONG_ABOUT)]
struct EnrichArgs {
    #[arg(long, default_value = ".")]
    #[arg(help = "Root directory containing parquet/ and openalex-snapshot_metadata/")]
    root_dir: PathBuf,

    #[arg(long, default_value = "works")]
    #[arg(help = "Dataset to enrich (only 'works' is supported)")]
    dataset: String,

    #[arg(long, default_value_t = 0)]
    #[arg(help = "Number of workers (0 = auto: cpus-2)")]
    workers: usize,

    #[arg(long)]
    #[arg(help = "Per-worker memory cap override in MB")]
    max_memory_mb: Option<usize>,

    #[arg(long, default_value_t = true)]
    #[arg(help = "Show progress bars with rough ETA")]
    progress: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Overwrite existing enriched files instead of skipping up-to-date ones")]
    overwrite: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Explain planned actions and exit without executing")]
    explain: bool,
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

    #[arg(skip = PathBuf::new())]
    parquet_dir: PathBuf,

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

    #[arg(long, default_value_t = 10)]
    #[arg(
        help = "aws s3 max_concurrent_requests (applied via a temp AWS config, no global change)"
    )]
    max_concurrent_requests: usize,

    #[arg(long, default_value_t = 50000)]
    #[arg(help = "aws s3 max_queue_size (applied via a temp AWS config)")]
    max_queue_size: usize,

    #[arg(long, default_value = "32MB")]
    #[arg(help = "aws s3 multipart_chunksize (applied via a temp AWS config)")]
    multipart_chunksize: String,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Skip auto-enrich of works after download (raw works_aws/ only)")]
    no_enrich: bool,

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

    #[arg(skip = PathBuf::new())]
    parquet_dir: PathBuf,

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

    #[arg(long, default_value_t = false)]
    #[arg(help = "Presence + size only; skip parquet row-count checks (fastest)")]
    quick: bool,

    #[arg(long, default_value_t = false)]
    #[arg(help = "Full row scan instead of footer metadata (catches data-page corruption; slow)")]
    full: bool,

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
    // Legacy JSON-pipeline sections — parsed for back-compat but no longer acted on.
    #[allow(dead_code)]
    convert: Option<ConvertConfig>,
    #[allow(dead_code)]
    verify_convert: Option<VerifyConfig>,
    #[allow(dead_code)]
    schema: Option<SchemaConfig>,
    index: Option<IndexConfig>,
    extract: Option<ExtractConfig>,
    enrich: Option<EnrichConfig>,
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
    // Legacy convert toggles + retry — parsed for back-compat but no longer acted on.
    #[allow(dead_code)]
    enable_convert: Option<bool>,
    #[allow(dead_code)]
    enable_verify_convert: Option<bool>,
    enable_index: Option<bool>,
    enable_verify_index: Option<bool>,
    #[allow(dead_code)]
    retry: Option<usize>,
    skip_disk_check: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDefaults {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    // Legacy convert profile selector — parsed for back-compat but no longer acted on.
    #[allow(dead_code)]
    profile: Option<String>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
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
    auto_repair: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
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
#[allow(dead_code)]
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
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    state_flush_every: Option<usize>,
    ids: Option<PathBuf>,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnrichConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    max_memory_mb: Option<usize>,
    progress: Option<bool>,
    overwrite: Option<bool>,
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
    max_concurrent_requests: Option<usize>,
    max_queue_size: Option<usize>,
    multipart_chunksize: Option<String>,
    no_enrich: Option<bool>,
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
    quick: Option<bool>,
    full: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyIndexConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
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

#[derive(Debug, Clone)]
struct ExtractInput {
    raw: String,
    normalized: String,
    /// Full-URL form matching what the index stores, e.g. "https://openalex.org/W1234"
    canonical: String,
    dataset: Option<String>,
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
            run_all(args, &all_cfg)
        }
        Commands::Index(mut args) => {
            apply_index_config(&mut args, cfg.as_ref(), sub_matches);
            try_migrate_metadata_root(&args.root_dir);
            if cli.print_effective_config {
                let corpus_dir = args.root_dir.join("parquet").join(&args.dataset);
                explain_index(
                    &args,
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
                    &light_tuning_with_override(args.shared.workers, args.max_memory_mb),
                );
                return Ok(());
            }
            run_extract(args)
        }
        Commands::Enrich(mut args) => {
            apply_enrich_config(&mut args, cfg.as_ref(), sub_matches);
            try_migrate_metadata_root(&args.root_dir);
            run_enrich(args)
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
    args.parquet_dir = args.root_dir.join("parquet");
}

fn fill_validate_download_dirs(args: &mut ValidateDownloadArgs) {
    args.snapshot_dir = args.root_dir.join("snapshot");
    args.parquet_dir = args.root_dir.join("parquet");
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

fn apply_enrich_config(
    args: &mut EnrichArgs,
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
        if !cli_explicit(matches, "workers") {
            if let Some(v) = d.workers {
                args.workers = v;
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
    if let Some(c) = &cfg.enrich {
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
        if !cli_explicit(matches, "max_concurrent_requests") {
            if let Some(v) = c.max_concurrent_requests {
                args.max_concurrent_requests = v;
            }
        }
        if !cli_explicit(matches, "max_queue_size") {
            if let Some(v) = c.max_queue_size {
                args.max_queue_size = v;
            }
        }
        if !cli_explicit(matches, "multipart_chunksize") {
            if let Some(v) = &c.multipart_chunksize {
                args.multipart_chunksize = v.clone();
            }
        }
        if !cli_explicit(matches, "no_enrich") {
            if let Some(v) = c.no_enrich {
                args.no_enrich = v;
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
        if !cli_explicit(matches, "quick") {
            if let Some(v) = c.quick {
                args.quick = v;
            }
        }
        if !cli_explicit(matches, "full") {
            if let Some(v) = c.full {
                args.full = v;
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
    }
}

fn config_template_safe() -> String {
    r#"# openalex-snapshot.yaml (safe)
# Minimal low-memory preset.
# Everything else falls back to built-in defaults (or CLI).

defaults:
  # Keep root explicit so path model remains obvious.
  root_dir: .

  # Conservative parallelism for constrained systems.
  workers: 1

  # Optional explicit cap for constrained systems.
  # max_memory_mb: 4096
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
#   3) convert        ──┐ looped: convert auto-repairs parquets flagged by
#   4) verify_convert ──┘ the latest verify report, up to --retry attempts
#   5) index
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
  # Workers: 0 (default) = auto-detect (min(cpus, 4)). Override to pin a value.
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
  # Pipeline: download -> verify_download -> index -> verify_index
  # ---------------------------------------------------------------------------

  # Stage toggles (default pipeline shown below).
  # Disable stages you do not want in `all` (e.g., skip download for local runs).
  # allowed values: true | false
  enable_download: true
  # allowed values: true | false
  enable_verify_download: true
  # allowed values: true | false
  enable_index: true
  # allowed values: true | false
  enable_verify_index: true

  # Skip disk space checks for all stages that support it (download).
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

  # Auto-enrich works after download (works_aws/ -> works/). Set true to skip.
  # allowed values: true | false
  no_enrich: false
  # AWS S3 transfer tuning (applied via a temp AWS config; global ~/.aws untouched).
  # allowed values: positive integer
  max_concurrent_requests: 10
  # allowed values: positive integer
  max_queue_size: 50000
  # allowed values: e.g. 8MB, 32MB, 64MB
  multipart_chunksize: 32MB

  # Skip free disk space preflight check for download.
  # allowed values: true | false
  # skip_disk_check: false

verify_download:
  # ---------------------------------------------------------------------------
  # Verify downloaded parquet corpus against the published manifest.json
  # (presence + size + row count). --quick = size-only, --full = row scan.
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

index:
  # ---------------------------------------------------------------------------
  # Build *_id_idx.parquet lookup index for parquet corpus (ID lookups)
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
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
  # max_memory_mb: 8192
  # progress: true

  # allowed values: any valid path
  root_dir: .
  # allowed values: any valid path
  # index_file: ./parquet/all_id_idx.parquet

extract:
  # ---------------------------------------------------------------------------
  # Extract rows by OpenAlex IDs from CSV using per-dataset indexes
  # ---------------------------------------------------------------------------
  # Shared-default overrides supported here (optional, uncomment to override defaults):
  # root_dir: .
  # dataset: all
  # workers: 4
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
        bail!("config requires exactly one mode: use --create <complete|safe> or --verify");
    }
    if args.explain {
        let mode = if args.create.is_some() {
            "create"
        } else {
            "verify"
        };
        let create_mode = args
            .create
            .as_ref()
            .map(|m| match m {
                ConfigTemplateMode::Complete => "complete",
                ConfigTemplateMode::Safe => "safe",
            })
            .unwrap_or("-");
        println!(
            "--explain: config mode={} create_template={} path={} stdout={} overwrite={}",
            mode,
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

    let check_parquet_dir = args.shared.root_dir.join("parquet");
    if let Ok(manifest) = fetch_parquet_manifest(
        &args.aws_bin,
        &args.s3_uri,
        &args.endpoint_url,
        &args.region,
        &args.profile_name,
        args.no_sign_request && !args.signed,
    ) {
        let remote_total_bytes: u64 = if args.shared.dataset == "all" {
            manifest.meta.content_length
        } else {
            manifest
                .entities
                .iter()
                .find(|e| e.entity == args.shared.dataset)
                .map(|e| e.content_length)
                .unwrap_or(0)
        };
        let required_bytes = remote_total_bytes.saturating_mul(11).saturating_div(10);
        match available_disk_bytes(&check_parquet_dir) {
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
                    output_path: Some(check_parquet_dir.to_string_lossy().to_string()),
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
    _snapshot_dir: &Path,
    parquet_dir: &Path,
    report: &RunReport,
) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if report.command == "download" || report.command == "verify_download" {
        // Parquet-native download/verify no longer write a per-step .log file.
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
    enable_download: bool,
    enable_verify_download: bool,
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
        enable_download: c.enable_download.unwrap_or(true),
        enable_verify_download: c.enable_verify_download.unwrap_or(true),
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

fn run_all(args: AllArgs, cfg: &AppConfig) -> Result<()> {
    let resolved = resolve_all_settings(&args, cfg);
    let snapshot_dir = resolved.root_dir.join("snapshot");
    let parquet_dir = resolved.root_dir.join("parquet");
    fs::create_dir_all(&parquet_dir)?;

    if args.explain {
        println!("--explain: all");
        println!("root_dir: {}", resolved.root_dir.display());
        println!(
            "steps: download={} verify_download={} index={} verify_index={}",
            resolved.enable_download,
            resolved.enable_verify_download,
            resolved.enable_index,
            resolved.enable_verify_index
        );
        return Ok(());
    }
    let _lock = acquire_lock(&parquet_dir, "all")?;
    let _ = cleanup_command_reports(&parquet_dir, "all");

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        resolved.root_dir.to_string_lossy().to_string(),
    );
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
            parquet_dir: PathBuf::new(),
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
            max_concurrent_requests: 10,
            max_queue_size: 50000,
            multipart_chunksize: "32MB".to_string(),
            no_enrich: false,
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
            parquet_dir: PathBuf::new(),
            s3_uri: "s3://openalex".to_string(),
            dataset: "all".to_string(),
            aws_bin: PathBuf::from("aws"),
            endpoint_url: None,
            region: None,
            profile_name: None,
            no_sign_request: true,
            signed: false,
            check_extra: true,
            quick: false,
            full: false,
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

    if resolved.enable_index {
        let mut ia = IndexArgs {
            root_dir: resolved.root_dir.clone(),
            dataset: "all".to_string(),
            index_file: None,
            workers: 0,
            max_memory_mb: None,
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
            .filter(|s| !s.ends_with("_aws"))
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
    if args.explain {
        println!("--explain: verify-index");
        println!("root_dir: {}", args.root_dir.display());
        println!("dataset: {}", dataset);
        println!("corpus_dir: {}", corpus_dir.display());
        println!("index_file: {}", index_file.display());
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
        let cols = parquet_top_level_columns(&index_file)?;
        for req in ["id", "id_block", "parquet_file", "file_row_number"] {
            if !cols.iter().any(|c| c == req) {
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
        // Footer-metadata row counts (no data scan): index file vs. the whole corpus.
        let idx_count = parquet_rowcount_meta(&index_file)?;
        let mut corpus_count = 0u64;
        for pf in &parquet_files {
            corpus_count += parquet_rowcount_meta(pf)?;
        }
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
        let refs = parquet_distinct_strings(&index_file, "parquet_file")?;
        for rel in refs {
            let p = parquet_dir.join(&rel);
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
                        "rebuild the index with `openalex-snapshot index --overwrite`".to_string(),
                    ),
                });
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

/// id_block = floor(trailing-digit-run(id) / 10000) as i32, or None when the id has no
/// trailing digits or the run overflows i64. Mirrors the DuckDB
/// `CAST(FLOOR(TRY_CAST(regexp_extract(id,'([0-9]+)$',1) AS BIGINT)/10000) AS INTEGER)`.
fn id_block_of(id: &str) -> Option<i32> {
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

/// Collect a Utf8/LargeUtf8 arrow column into owned `Option<String>` values.
fn string_col_values(arr: &ArrayRef, ctx: &str) -> Result<Vec<Option<String>>> {
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

fn index_shard_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("id_block", DataType::Int32, true),
        Field::new("parquet_file", DataType::Utf8, false),
        Field::new("file_row_number", DataType::Int64, false),
    ]))
}

fn snappy_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

/// Stage 1: read the `id` column of one corpus parquet, derive (id, id_block,
/// parquet_file=rel, file_row_number) and stream it into a shard parquet.
fn build_index_shard(src: &Path, rel: &str, out: &Path) -> Result<()> {
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
fn concat_index_shards(shard_dir: &Path, out: &Path) -> Result<()> {
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
            // Skip raw `*_aws` staging dirs (e.g. works_aws) — the enriched canonical
            // dataset (works) is indexed instead.
            .filter(|s| !s.ends_with("_aws"))
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
        explain_index(&args, &corpus_dir, &index_file, &tuning);
        return Ok(());
    }
    if INDEX_ALL_DEPTH.load(Ordering::SeqCst) == 0 {
        let _ = cleanup_command_reports(&parquet_dir, "index");
        let _ = cleanup_command_dataset_logs(&parquet_dir, "index");
    }
    let _lock = acquire_lock(&parquet_dir, "index")?;
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
                            return (
                                false,
                                Some(FailureEntry {
                                    dataset: dataset.clone(),
                                    phase: "index_stage1".to_string(),
                                    rel_path: None,
                                    source_path: Some(pf.to_string_lossy().to_string()),
                                    output_path: Some(out_file.to_string_lossy().to_string()),
                                    error_message: format!("{e:#}"),
                                    suggested_recovery: None,
                                }),
                            );
                        }
                    };
                    let rel_s = rel.to_string_lossy().replace('\\', "/");
                    stage1.inc(1);
                    match build_index_shard(pf, &rel_s, &out_file) {
                        Ok(()) => (false, None),
                        Err(e) => (
                            false,
                            Some(FailureEntry {
                                dataset: dataset.clone(),
                                phase: "index_stage1".to_string(),
                                rel_path: Some(rel_s),
                                source_path: Some(pf.to_string_lossy().to_string()),
                                output_path: Some(out_file.to_string_lossy().to_string()),
                                error_message: format!("{e:#}"),
                                suggested_recovery: Some(
                                    "re-download the parquet file and rerun index".to_string(),
                                ),
                            }),
                        ),
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
        if let Err(e) = concat_index_shards(&temp_dir, &index_file) {
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

fn explain_enrich(args: &EnrichArgs, src_dir: &Path, out_dir: &Path, tuning: &Tuning) {
    println!("--explain: enrich");
    println!("dataset: {}", args.dataset);
    println!("source: {}", src_dir.display());
    println!("output: {}", out_dir.display());
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
    println!("overwrite: {}", args.overwrite);
}

/// Reconstruct a works abstract from the JSON inverted-index string
/// (`{"word":[pos,...],...}`): emit one (pos, word) per position, sort by position, join
/// with single spaces. Returns None on null/empty/invalid input. Mirrors the SQL helper.
/// Parse `{"word":[pos,...],...}` into (word, positions) entries, **preserving duplicate
/// keys** — OpenAlex sometimes emits the same word as multiple keys, and DuckDB's JSON→MAP
/// cast keeps them all, so a plain HashMap (last-key-wins) would drop positions.
fn parse_inverted_index(json: &str) -> Option<Vec<(String, Vec<i64>)>> {
    use serde::de::{MapAccess, Visitor};
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Vec<(String, Vec<i64>)>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an abstract_inverted_index object")
        }
        fn visit_map<A: MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = Vec::new();
            while let Some((k, v)) = map.next_entry::<String, Vec<i64>>()? {
                out.push((k, v));
            }
            Ok(out)
        }
    }
    let mut de = serde_json::Deserializer::from_str(json);
    serde::Deserializer::deserialize_map(&mut de, V).ok()
}

fn reconstruct_abstract(json: &str) -> Option<String> {
    let entries = parse_inverted_index(json)?;
    let mut pairs: Vec<(i64, &str)> = Vec::new();
    for (word, positions) in &entries {
        for &pos in positions {
            pairs.push((pos, word.as_str()));
        }
    }
    if pairs.is_empty() {
        return Some(String::new());
    }
    // Sort by (pos, word) to match DuckDB's list_sort over {pos, word} structs.
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));
    let mut out = String::new();
    for (i, (_, w)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(w);
    }
    Some(out)
}

/// Read `display_name` at `idx` from an `authorships[*].author` struct slice, or None if null.
fn author_display_name(authors_struct: &StructArray, idx: usize) -> Option<String> {
    let author = authors_struct.column_by_name("author")?;
    let author = author.as_any().downcast_ref::<StructArray>()?;
    let dn = author.column_by_name("display_name")?;
    let dn = dn.as_any().downcast_ref::<StringArray>()?;
    if dn.is_null(idx) {
        None
    } else {
        Some(dn.value(idx).to_string())
    }
}

/// Build the `citation` column for a batch from `authorships` (List<Struct<author:Struct<…>>>)
/// and `publication_year` (Int32). Mirrors `works_citation_expr`: "A (yr)" / "A & B (yr)" /
/// "A et al. (yr)"; null year → "n.d."; null/empty authorships or null first author → null.
fn build_citation_array(authorships: &ArrayRef, years: &ArrayRef, n: usize) -> Result<StringArray> {
    let list = authorships
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("authorships is not a List column"))?;
    let yr = years.as_any().downcast_ref::<Int32Array>();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        if list.is_null(i) {
            out.push(None);
            continue;
        }
        let row = list.value(i);
        let st = match row.as_any().downcast_ref::<StructArray>() {
            Some(s) => s,
            None => {
                out.push(None);
                continue;
            }
        };
        let len = st.len();
        if len == 0 {
            out.push(None);
            continue;
        }
        let year = match yr {
            Some(a) if !a.is_null(i) => a.value(i).to_string(),
            _ => "n.d.".to_string(),
        };
        let a0 = author_display_name(st, 0);
        let cite = match (len, a0) {
            (_, None) => None,
            (1, Some(a)) => Some(format!("{a} ({year})")),
            (2, Some(a)) => author_display_name(st, 1).map(|b| format!("{a} & {b} ({year})")),
            (_, Some(a)) => Some(format!("{a} et al. ({year})")),
        };
        out.push(cite);
    }
    Ok(out.into_iter().collect())
}

/// Enrich one works file: copy all columns through and append `abstract` and/or `citation`.
fn enrich_one(src: &Path, out: &Path, add_abstract: bool, add_citation: bool) -> Result<()> {
    let f = fs::File::open(src).with_context(|| format!("open {}", src.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(f)
        .with_context(|| format!("open parquet {}", src.display()))?;
    let src_schema = builder.schema().clone();
    let mut fields: Vec<Arc<Field>> = src_schema.fields().iter().cloned().collect();
    if add_abstract {
        fields.push(Arc::new(Field::new("abstract", DataType::Utf8, true)));
    }
    if add_citation {
        fields.push(Arc::new(Field::new("citation", DataType::Utf8, true)));
    }
    let out_schema = Arc::new(Schema::new(fields));
    let reader = builder
        .build()
        .with_context(|| format!("read parquet {}", src.display()))?;
    let outf = fs::File::create(out).with_context(|| format!("create {}", out.display()))?;
    let mut writer = ArrowWriter::try_new(outf, out_schema.clone(), Some(snappy_writer_props()))
        .with_context(|| format!("init writer {}", out.display()))?;
    for batch in reader {
        let batch = batch.with_context(|| format!("decode {}", src.display()))?;
        let n = batch.num_rows();
        let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
        if add_abstract {
            let aii = batch
                .column_by_name("abstract_inverted_index")
                .ok_or_else(|| anyhow!("missing abstract_inverted_index in {}", src.display()))?;
            let vals = string_col_values(aii, "abstract_inverted_index")?;
            let arr: StringArray = vals
                .iter()
                .map(|o| o.as_deref().and_then(reconstruct_abstract))
                .collect();
            cols.push(Arc::new(arr) as ArrayRef);
        }
        if add_citation {
            let auth = batch
                .column_by_name("authorships")
                .ok_or_else(|| anyhow!("missing authorships in {}", src.display()))?;
            let yr = batch
                .column_by_name("publication_year")
                .ok_or_else(|| anyhow!("missing publication_year in {}", src.display()))?;
            let arr = build_citation_array(auth, yr, n)?;
            cols.push(Arc::new(arr) as ArrayRef);
        }
        let rb = RecordBatch::try_new(out_schema.clone(), cols)
            .with_context(|| format!("assemble batch for {}", out.display()))?;
        writer
            .write(&rb)
            .with_context(|| format!("write {}", out.display()))?;
    }
    writer
        .close()
        .with_context(|| format!("close {}", out.display()))?;
    Ok(())
}

/// Enrich raw works (`parquet/works_aws/`) into `parquet/works/` with `abstract`
/// + `citation` columns, mirroring the partition layout. Incremental + row-parity checked.
fn run_enrich(args: EnrichArgs) -> Result<()> {
    if args.dataset != "works" {
        bail!(
            "[enrich] only the 'works' dataset is supported (got '{}')",
            args.dataset
        );
    }
    let parquet_dir = args.root_dir.join("parquet");
    let src_dir = parquet_dir.join("works_aws");
    let out_dir = parquet_dir.join("works");
    let tuning = light_tuning_with_override(args.workers, args.max_memory_mb);
    if args.explain {
        explain_enrich(&args, &src_dir, &out_dir, &tuning);
        return Ok(());
    }
    if !src_dir.exists() {
        bail!(
            "[enrich] source corpus not found: {} (run download first)",
            src_dir.display()
        );
    }
    let _lock = acquire_lock(&parquet_dir, "enrich")?;
    let _ = cleanup_command_reports(&parquet_dir, "enrich");

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "root_dir".to_string(),
        args.root_dir.to_string_lossy().to_string(),
    );
    report_args.insert("dataset".to_string(), "works".to_string());
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    let mut report = report_new("enrich", report_args);
    let mut ds = DatasetReportSummary {
        dataset: "works".to_string(),
        ..Default::default()
    };

    let files = list_parquet_files(&src_dir)?;
    if files.is_empty() {
        bail!("[enrich] no parquet files under {}", src_dir.display());
    }
    ds.items_scanned = files.len() as u64;

    // Decide which derived columns the source supports (once, from the first file).
    let cols = parquet_top_level_columns(&files[0])?;
    let has = |c: &str| cols.iter().any(|x| x == c);
    let add_abstract = has("abstract_inverted_index");
    let add_citation = has("authorships") && has("publication_year");
    if !add_abstract && !add_citation {
        bail!("[enrich] source works parquet has neither abstract_inverted_index nor authorships+publication_year");
    }

    let start = Instant::now();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(tuning.workers)
        .build()
        .context("failed to build rayon thread pool")?;
    let pb = make_progress_bar(args.progress, files.len() as u64, "enrich");
    let overwrite = args.overwrite;

    let outcomes: Vec<(bool, Option<FailureEntry>)> = pool.install(|| {
        files
            .par_iter()
            .map(|pf| {
                let rel = match pf.strip_prefix(&src_dir) {
                    Ok(r) => r.to_path_buf(),
                    Err(e) => {
                        pb.inc(1);
                        return (
                            false,
                            Some(FailureEntry {
                                dataset: "works".to_string(),
                                phase: "enrich".to_string(),
                                rel_path: None,
                                source_path: Some(pf.to_string_lossy().to_string()),
                                output_path: None,
                                error_message: format!("{e:#}"),
                                suggested_recovery: None,
                            }),
                        );
                    }
                };
                let out_file = out_dir.join(&rel);
                if !overwrite && out_file.exists() {
                    if let (Ok(om), Ok(sm)) = (fs::metadata(&out_file), fs::metadata(pf)) {
                        let up_to_date = match (om.modified(), sm.modified()) {
                            (Ok(o), Ok(s)) => o >= s,
                            _ => true,
                        };
                        if up_to_date {
                            pb.inc(1);
                            return (true, None);
                        }
                    }
                }
                if let Some(parent) = out_file.parent() {
                    if let Err(e) = fs::create_dir_all(parent) {
                        pb.inc(1);
                        return (
                            false,
                            Some(FailureEntry {
                                dataset: "works".to_string(),
                                phase: "enrich".to_string(),
                                rel_path: Some(rel.to_string_lossy().to_string()),
                                source_path: Some(pf.to_string_lossy().to_string()),
                                output_path: Some(out_file.to_string_lossy().to_string()),
                                error_message: format!("{e:#}"),
                                suggested_recovery: None,
                            }),
                        );
                    }
                }
                if let Err(e) = enrich_one(pf, &out_file, add_abstract, add_citation) {
                    let _ = fs::remove_file(&out_file);
                    pb.inc(1);
                    return (
                        false,
                        Some(FailureEntry {
                            dataset: "works".to_string(),
                            phase: "enrich".to_string(),
                            rel_path: Some(rel.to_string_lossy().to_string()),
                            source_path: Some(pf.to_string_lossy().to_string()),
                            output_path: Some(out_file.to_string_lossy().to_string()),
                            error_message: format!("{e:#}"),
                            suggested_recovery: Some("rerun enrich".to_string()),
                        }),
                    );
                }
                // Row-parity self-check: enriched row count must equal source row count.
                let parity = match (parquet_rowcount_meta(pf), parquet_rowcount_meta(&out_file)) {
                    (Ok(a), Ok(b)) if a == b => None,
                    (Ok(a), Ok(b)) => Some(format!("rowcount mismatch src={a} enriched={b}")),
                    (a, b) => Some(
                        a.err()
                            .or_else(|| b.err())
                            .map(|e| format!("{e:#}"))
                            .unwrap_or_else(|| "rowcount check failed".to_string()),
                    ),
                };
                pb.inc(1);
                match parity {
                    None => (false, None),
                    Some(msg) => {
                        let _ = fs::remove_file(&out_file);
                        (
                            false,
                            Some(FailureEntry {
                                dataset: "works".to_string(),
                                phase: "enrich_rowcount".to_string(),
                                rel_path: Some(rel.to_string_lossy().to_string()),
                                source_path: Some(pf.to_string_lossy().to_string()),
                                output_path: Some(out_file.to_string_lossy().to_string()),
                                error_message: msg,
                                suggested_recovery: Some("rerun enrich".to_string()),
                            }),
                        )
                    }
                }
            })
            .collect()
    });
    pb.finish_with_message("enrich complete");
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
    eprintln!(
        "[enrich] done in {:.1}s: ok={} skipped={} failed={} -> {}",
        start.elapsed().as_secs_f64(),
        ds.succeeded,
        ds.skipped,
        ds.failed,
        out_dir.display()
    );
    report.datasets = vec![ds];
    report_finalize(&mut report);
    if report.totals_failed > 0 {
        let _ = write_run_reports(&parquet_dir, &report);
        bail!("[enrich] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

/// Scan an `*_id_idx.parquet`, returning the matched ids and the (relative) parquet_file
/// paths that contain them — the set of `requested` ids that appear in the index.
fn extract_index_lookup(
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
fn extract_rows_to_parquet(
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

fn run_extract(args: ExtractArgs) -> Result<()> {
    let parquet_dir = args.shared.parquet_dir.clone();
    let tuning = light_tuning_with_override(args.shared.workers, args.max_memory_mb);
    if args.explain {
        explain_extract(&args, &tuning);
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

        let (matched_ids, dataset_files) = match extract_index_lookup(&index_file, ids) {
            Ok((m, rel)) => {
                let files: BTreeSet<PathBuf> = rel.iter().map(|r| parquet_dir.join(r)).collect();
                (m, files)
            }
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
                pb.inc(1);
                continue;
            }
        };
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
            pb.inc(1);
            continue;
        }

        let files_vec: Vec<PathBuf> = dataset_files.iter().cloned().collect();
        match extract_rows_to_parquet(&files_vec, &matched_ids, &out_path) {
            Ok(_n) => {
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
            Err(e) => {
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
            }
        }
        report.datasets.push(ds);
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

fn run_download(args: DownloadArgs) -> Result<()> {
    ensure_aws_cli(&args.aws_bin)?;
    fs::create_dir_all(&args.parquet_dir)?;
    if args.explain {
        explain_download(&args)?;
        return Ok(());
    }
    let _lock = acquire_lock(&args.parquet_dir, "download")?;
    let _ = cleanup_download_reports(&args.snapshot_dir, "download");
    let effective_no_sign = args.no_sign_request && !args.signed;
    let effective_delete = args.delete_files && !args.no_delete;

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "parquet_dir".to_string(),
        args.parquet_dir.to_string_lossy().to_string(),
    );
    report_args.insert("s3_uri".to_string(), args.s3_uri.clone());
    report_args.insert("dataset".to_string(), args.dataset.clone());
    report_args.insert("no_sign_request".to_string(), effective_no_sign.to_string());
    report_args.insert("delete_files".to_string(), effective_delete.to_string());
    report_args.insert("no_enrich".to_string(), args.no_enrich.to_string());
    let mut report = report_new("download", report_args);

    // Fetch the official parquet manifest (drives the dataset list + disk preflight).
    let manifest = match fetch_parquet_manifest(
        &args.aws_bin,
        &args.s3_uri,
        &args.endpoint_url,
        &args.region,
        &args.profile_name,
        effective_no_sign,
    ) {
        Ok(m) => m,
        Err(e) => {
            report.failures.push(FailureEntry {
                dataset: args.dataset.clone(),
                phase: "download_manifest_fetch".to_string(),
                rel_path: None,
                source_path: Some(args.s3_uri.clone()),
                output_path: Some(args.parquet_dir.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("check aws CLI/network/endpoint settings".to_string()),
            });
            report_finalize(&mut report);
            let _ = write_download_reports(&args.snapshot_dir, &report);
            bail!("[download] parquet manifest fetch failed");
        }
    };
    // Persist the fetched manifest once (single audit artifact).
    {
        let dir = download_metadata_root(&args.snapshot_dir);
        let _ = fs::create_dir_all(&dir);
        if let Ok(bytes) = serde_json::to_vec_pretty(&serde_json::json!({
            "date": manifest.date,
            "format": manifest.format,
            "record_count": manifest.meta.record_count,
            "content_length": manifest.meta.content_length,
            "entities": manifest.entities.iter().map(|e| e.entity.clone()).collect::<Vec<_>>(),
        })) {
            let _ = fs::write(dir.join("manifest.json"), bytes);
        }
    }

    let datasets: Vec<String> = if args.dataset == "all" {
        manifest.entities.iter().map(|e| e.entity.clone()).collect()
    } else {
        vec![args.dataset.clone()]
    };

    // Disk preflight from the manifest content_length (+10% buffer).
    if !args.skip_disk_check {
        let remote_total_bytes: u64 = if args.dataset == "all" {
            manifest.meta.content_length
        } else {
            manifest
                .entities
                .iter()
                .find(|e| e.entity == args.dataset)
                .map(|e| e.content_length)
                .unwrap_or(0)
        };
        let required_bytes = remote_total_bytes.saturating_mul(11).saturating_div(10);
        let free_bytes = available_disk_bytes(&args.parquet_dir)?;
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
                output_path: Some(args.parquet_dir.to_string_lossy().to_string()),
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
            let _ = write_download_reports(&args.snapshot_dir, &report);
            bail!(
                "[download] Not enough disk space.\n  Location : {}\n  Available: {} GiB\n  Required : {} GiB (remote {} GiB + 10% buffer)\n\nTo proceed anyway, either:\n  - Set skip_disk_check: true under the download: section in your config file\n  - Pass --skip-disk-check when running the download or all command",
                args.parquet_dir.display(),
                bytes_to_gib(free_bytes),
                bytes_to_gib(required_bytes),
                bytes_to_gib(remote_total_bytes)
            );
        }
    }

    // Apply S3 tuning via a temp AWS config (no global ~/.aws change).
    let aws_config = write_temp_aws_config(&args)?;
    let env = vec![("AWS_CONFIG_FILE", aws_config.to_string_lossy().to_string())];

    // Per-dataset sync (continue-and-report).
    for ds in &datasets {
        let dst = args.parquet_dir.join(local_dir_for(ds));
        if let Err(e) = fs::create_dir_all(&dst) {
            report.failures.push(FailureEntry {
                dataset: ds.clone(),
                phase: "download_mkdir".to_string(),
                rel_path: None,
                source_path: None,
                output_path: Some(dst.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("check permissions / disk".to_string()),
            });
            continue;
        }
        let sync = aws_sync_command(&args, ds)?;
        eprintln!("[download] syncing {ds} -> {}", dst.display());
        match run_aws_env(&args.aws_bin, &sync, &env) {
            Ok(_) => report.datasets.push(DatasetReportSummary {
                dataset: ds.clone(),
                items_scanned: 1,
                succeeded: 1,
                failed: 0,
                skipped: 0,
            }),
            Err(e) => {
                report.datasets.push(DatasetReportSummary {
                    dataset: ds.clone(),
                    items_scanned: 1,
                    succeeded: 0,
                    failed: 1,
                    skipped: 0,
                });
                report.failures.push(FailureEntry {
                    dataset: ds.clone(),
                    phase: "download_sync".to_string(),
                    rel_path: None,
                    source_path: Some(format!(
                        "{}/data/parquet/{}/",
                        args.s3_uri.trim_end_matches('/'),
                        ds
                    )),
                    output_path: Some(dst.to_string_lossy().to_string()),
                    error_message: format!("{e:#}"),
                    suggested_recovery: Some("check aws CLI/network/endpoint settings".to_string()),
                });
            }
        }
    }

    // Auto-enrich works (unless skipped). Only if the works sync succeeded.
    let works_ok = report
        .datasets
        .iter()
        .any(|d| d.dataset == "works" && d.failed == 0);
    if !args.no_enrich && datasets.iter().any(|d| d == "works") && works_ok {
        let ea = EnrichArgs {
            root_dir: args.root_dir.clone(),
            dataset: "works".to_string(),
            workers: 0,
            max_memory_mb: None,
            progress: args.progress,
            overwrite: false,
            explain: false,
        };
        if let Err(e) = run_enrich(ea) {
            report.failures.push(FailureEntry {
                dataset: "works".to_string(),
                phase: "download_enrich".to_string(),
                rel_path: None,
                source_path: Some(
                    args.parquet_dir
                        .join("works_aws")
                        .to_string_lossy()
                        .to_string(),
                ),
                output_path: Some(args.parquet_dir.join("works").to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some(
                    "inspect the latest enrich report and rerun `enrich`".to_string(),
                ),
            });
        }
    }

    report_finalize(&mut report);
    eprintln!(
        "[download] summary datasets={} ok={} failed={}",
        report.datasets.len(),
        report.totals_succeeded,
        report.totals_failed
    );
    if report.totals_failed > 0 {
        let _ = write_download_reports(&args.snapshot_dir, &report);
        bail!("[download] failures detected: {}", report.totals_failed);
    }
    Ok(())
}

fn run_validate_download(args: ValidateDownloadArgs) -> Result<()> {
    ensure_aws_cli(&args.aws_bin)?;
    fs::create_dir_all(&args.parquet_dir)?;
    let tuning = light_tuning_with_override(args.workers, None);
    if args.explain {
        explain_validate_download(&args, &tuning);
        return Ok(());
    }
    let _ = cleanup_download_reports(&args.snapshot_dir, "verify_download");
    let effective_no_sign = args.no_sign_request && !args.signed;
    let mode = if args.quick {
        "quick"
    } else if args.full {
        "full"
    } else {
        "meta"
    };

    let mut report_args = BTreeMap::new();
    report_args.insert(
        "parquet_dir".to_string(),
        args.parquet_dir.to_string_lossy().to_string(),
    );
    report_args.insert("s3_uri".to_string(), args.s3_uri.clone());
    report_args.insert("dataset".to_string(), args.dataset.clone());
    report_args.insert("check_extra".to_string(), args.check_extra.to_string());
    report_args.insert("mode".to_string(), mode.to_string());
    report_args.insert("workers".to_string(), tuning.workers.to_string());
    let mut report = report_new("verify_download", report_args);

    let manifest = match fetch_parquet_manifest(
        &args.aws_bin,
        &args.s3_uri,
        &args.endpoint_url,
        &args.region,
        &args.profile_name,
        effective_no_sign,
    ) {
        Ok(m) => m,
        Err(e) => {
            report.failures.push(FailureEntry {
                dataset: args.dataset.clone(),
                phase: "validate_manifest_fetch".to_string(),
                rel_path: None,
                source_path: Some(args.s3_uri.clone()),
                output_path: Some(args.parquet_dir.to_string_lossy().to_string()),
                error_message: format!("{e:#}"),
                suggested_recovery: Some("check aws CLI credentials/network/endpoint".to_string()),
            });
            report_finalize(&mut report);
            let _ = write_download_reports(&args.snapshot_dir, &report);
            bail!("[verify_download] parquet manifest fetch failed");
        }
    };

    let entities: Vec<&ManifestEntity> = if args.dataset == "all" {
        manifest.entities.iter().collect()
    } else {
        manifest
            .entities
            .iter()
            .filter(|e| e.entity == args.dataset)
            .collect()
    };
    if entities.is_empty() {
        bail!(
            "[verify_download] dataset '{}' not found in manifest",
            args.dataset
        );
    }

    let mut ds_map: BTreeMap<String, DatasetReportSummary> = BTreeMap::new();
    let mut expected_by_ds: BTreeMap<String, BTreeSet<PathBuf>> = BTreeMap::new();
    // Files that passed presence+size, for the row-count pass: (dataset, local, expected_record_count)
    let mut present_files: Vec<(String, PathBuf, u64)> = Vec::new();

    // Presence + size against the manifest.
    let total_files: u64 = entities.iter().map(|e| e.files.len() as u64).sum();
    let pb = make_progress_bar(args.progress, total_files, "validate-presence");
    for ent in &entities {
        let ds = ds_map
            .entry(ent.entity.clone())
            .or_insert_with(|| DatasetReportSummary {
                dataset: ent.entity.clone(),
                ..Default::default()
            });
        for f in &ent.files {
            ds.items_scanned += 1;
            let local = match manifest_url_to_local(&args.parquet_dir, &f.url) {
                Some(p) => p,
                None => {
                    ds.failed += 1;
                    report.failures.push(FailureEntry {
                        dataset: ent.entity.clone(),
                        phase: "validate_manifest_url".to_string(),
                        rel_path: None,
                        source_path: Some(f.url.clone()),
                        output_path: None,
                        error_message: "could not derive local path from manifest url".to_string(),
                        suggested_recovery: None,
                    });
                    pb.inc(1);
                    continue;
                }
            };
            expected_by_ds
                .entry(ent.entity.clone())
                .or_default()
                .insert(local.clone());
            match fs::metadata(&local) {
                Err(_) => {
                    ds.failed += 1;
                    report.failures.push(FailureEntry {
                        dataset: ent.entity.clone(),
                        phase: "validate_file_presence".to_string(),
                        rel_path: Some(local.to_string_lossy().to_string()),
                        source_path: Some(f.url.clone()),
                        output_path: Some(local.to_string_lossy().to_string()),
                        error_message: "missing local file".to_string(),
                        suggested_recovery: Some("rerun download".to_string()),
                    });
                }
                Ok(m) if !m.is_file() => {
                    ds.failed += 1;
                    report.failures.push(FailureEntry {
                        dataset: ent.entity.clone(),
                        phase: "validate_file_presence".to_string(),
                        rel_path: Some(local.to_string_lossy().to_string()),
                        source_path: Some(f.url.clone()),
                        output_path: Some(local.to_string_lossy().to_string()),
                        error_message: "expected a file".to_string(),
                        suggested_recovery: Some("rerun download".to_string()),
                    });
                }
                Ok(m) => {
                    if m.len() != f.meta.content_length {
                        ds.failed += 1;
                        report.failures.push(FailureEntry {
                            dataset: ent.entity.clone(),
                            phase: "validate_file_size".to_string(),
                            rel_path: Some(local.to_string_lossy().to_string()),
                            source_path: Some(f.url.clone()),
                            output_path: Some(local.to_string_lossy().to_string()),
                            error_message: format!(
                                "size mismatch local={} manifest={}",
                                m.len(),
                                f.meta.content_length
                            ),
                            suggested_recovery: Some("rerun download".to_string()),
                        });
                    } else {
                        present_files.push((
                            ent.entity.clone(),
                            local.clone(),
                            f.meta.record_count,
                        ));
                    }
                }
            }
            pb.inc(1);
        }
    }
    pb.finish_with_message("validate-presence complete");

    // Row-count integrity (footer metadata by default; full scan with --full).
    if !args.quick {
        let pb = make_progress_bar(
            args.progress,
            present_files.len() as u64,
            "validate-rowcount",
        );
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(tuning.workers)
            .build()
            .context("failed to build rayon thread pool")?;
        let full = args.full;
        let results: Vec<Option<FailureEntry>> = pool.install(|| {
            present_files
                .par_iter()
                .map(|(ds, local, expected_rc)| {
                    let got = if full {
                        parquet_full_rowcount(local)
                    } else {
                        parquet_rowcount_meta(local)
                    };
                    pb.inc(1);
                    match got {
                        Ok(n) if n == *expected_rc => None,
                        Ok(n) => Some(FailureEntry {
                            dataset: ds.clone(),
                            phase: "validate_parquet_rowcount".to_string(),
                            rel_path: Some(local.to_string_lossy().to_string()),
                            source_path: None,
                            output_path: Some(local.to_string_lossy().to_string()),
                            error_message: format!(
                                "rowcount mismatch local={n} manifest={expected_rc}"
                            ),
                            suggested_recovery: Some("rerun download for this file".to_string()),
                        }),
                        Err(e) => Some(FailureEntry {
                            dataset: ds.clone(),
                            phase: "validate_parquet_rowcount".to_string(),
                            rel_path: Some(local.to_string_lossy().to_string()),
                            source_path: None,
                            output_path: Some(local.to_string_lossy().to_string()),
                            error_message: format!("{e:#}"),
                            suggested_recovery: Some("rerun download for this file".to_string()),
                        }),
                    }
                })
                .collect()
        });
        pb.finish_with_message("validate-rowcount complete");
        for f in results.into_iter().flatten() {
            if let Some(ds) = ds_map.get_mut(&f.dataset) {
                ds.failed += 1;
            }
            report.failures.push(f);
        }
    }

    // Detect unexpected local files (not in the manifest).
    if args.check_extra {
        for (ds_name, expected) in &expected_by_ds {
            let dir = args.parquet_dir.join(local_dir_for(ds_name));
            for f in list_parquet_files(&dir).unwrap_or_default() {
                if !expected.contains(&f) {
                    if let Some(ds) = ds_map.get_mut(ds_name) {
                        ds.failed += 1;
                    }
                    report.failures.push(FailureEntry {
                        dataset: ds_name.clone(),
                        phase: "validate_file_presence".to_string(),
                        rel_path: Some(f.to_string_lossy().to_string()),
                        source_path: None,
                        output_path: Some(f.to_string_lossy().to_string()),
                        error_message: "unexpected local file".to_string(),
                        suggested_recovery: Some("rerun download with --delete".to_string()),
                    });
                }
            }
        }
    }

    for ds in ds_map.values_mut() {
        ds.succeeded = ds.items_scanned.saturating_sub(ds.failed);
    }
    report.datasets = ds_map.values().cloned().collect();
    report_finalize(&mut report);
    eprintln!(
        "[verify_download] summary scanned={} ok={} failed={} mode={}",
        report.totals_items_scanned, report.totals_succeeded, report.totals_failed, mode
    );
    if report.totals_failed > 0 {
        let _ = write_download_reports(&args.snapshot_dir, &report);
        bail!(
            "[verify_download] failures detected: {}",
            report.totals_failed
        );
    }
    Ok(())
}

fn explain_index(args: &IndexArgs, corpus_dir: &Path, index_file: &Path, tuning: &Tuning) {
    println!("--explain: index");
    println!("corpus_dir: {}", corpus_dir.display());
    println!("index_file: {}", index_file.display());
    println!("overwrite: {}", args.overwrite);
    println!("workers: {}", tuning.workers);
}

fn explain_extract(args: &ExtractArgs, tuning: &Tuning) {
    println!("--explain: extract");
    println!("parquet_dir: {}", args.shared.parquet_dir.display());
    println!("dataset filter: {}", args.shared.dataset);
    println!("ids csv: {}", args.ids.display());
    println!("output base: {}", args.output.display());
    println!("workers: {}", tuning.workers);
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

#[derive(Debug, Clone, Copy)]
struct Tuning {
    workers: usize,
    memory_mb: Option<usize>,
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

/// Local subdirectory name (under `<root>/parquet/`) for a remote dataset.
/// `works` is staged raw into `works_aws/` so the enriched corpus can own the
/// canonical `works/` name; every other dataset uses its own name unchanged.
fn local_dir_for(dataset: &str) -> &str {
    if dataset == "works" {
        "works_aws"
    } else {
        dataset
    }
}

// --- Official OpenAlex parquet manifest (`s3://openalex/data/parquet/manifest.json`) ---

#[derive(Debug, Clone, Deserialize)]
struct ParquetManifest {
    #[serde(default)]
    date: Option<String>,
    #[serde(default)]
    format: Option<String>,
    meta: ManifestMeta,
    entities: Vec<ManifestEntity>,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestMeta {
    record_count: u64,
    content_length: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestEntity {
    entity: String,
    content_length: u64,
    files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestFile {
    url: String,
    meta: ManifestFileMeta,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestFileMeta {
    content_length: u64,
    record_count: u64,
}

/// Fetch and parse the top-level parquet manifest via `aws s3 cp <uri> -`.
fn fetch_parquet_manifest(
    aws_bin: &Path,
    s3_uri: &str,
    endpoint_url: &Option<String>,
    region: &Option<String>,
    profile_name: &Option<String>,
    no_sign_request: bool,
) -> Result<ParquetManifest> {
    let uri = format!(
        "{}/data/parquet/manifest.json",
        s3_uri.trim_end_matches('/')
    );
    let mut cmd = vec!["s3".to_string(), "cp".to_string(), uri, "-".to_string()];
    cmd.extend(aws_common_flags_from(
        endpoint_url,
        region,
        profile_name,
        no_sign_request,
    ));
    let out = run_aws(aws_bin, &cmd)?;
    let manifest: ParquetManifest =
        serde_json::from_str(&out).context("failed to parse parquet manifest.json")?;
    Ok(manifest)
}

/// Map a manifest file URL (`s3://<bucket>/data/parquet/<entity>/updated_date=.../part.parquet`)
/// to its local path under `<parquet_dir>/<local_dir_for(entity)>/updated_date=.../part.parquet`.
fn manifest_url_to_local(parquet_dir: &Path, url: &str) -> Option<PathBuf> {
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

/// Row count of a single parquet file from its footer metadata (no column scan).
/// Total row count of a parquet file from its footer metadata (no data scan).
fn parquet_rowcount_meta(path: &Path) -> Result<u64> {
    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(f)
        .with_context(|| format!("read parquet footer {}", path.display()))?;
    Ok(reader.metadata().file_metadata().num_rows().max(0) as u64)
}

/// Total row count by decoding every row group — catches data-page corruption a footer
/// read would miss. Used by `verify_download --full`.
fn parquet_full_rowcount(path: &Path) -> Result<u64> {
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
fn parquet_top_level_columns(path: &Path) -> Result<Vec<String>> {
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
fn parquet_distinct_strings(path: &Path, col: &str) -> Result<Vec<String>> {
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

/// Build a per-dataset `aws s3 sync` command:
///   src = {s3_uri}/data/parquet/{dataset}/   (remote dataset name)
///   dst = {root}/parquet/{local_dir_for(dataset)}/
/// Per-dataset destinations keep enriched `works/`, other datasets, and the
/// `*_id_idx.parquet` files (all one level up) safe from `--delete`.
fn aws_sync_command(args: &DownloadArgs, dataset: &str) -> Result<Vec<String>> {
    let mut cmd = vec!["s3".to_string(), "sync".to_string()];
    let effective_delete = args.delete_files && !args.no_delete;
    let effective_no_sign = args.no_sign_request && !args.signed;
    if effective_delete {
        cmd.push("--delete".to_string());
    }
    let src = format!(
        "{}/data/parquet/{}/",
        args.s3_uri.trim_end_matches('/'),
        dataset
    );
    let dst = args
        .parquet_dir
        .join(local_dir_for(dataset))
        .to_string_lossy()
        .to_string();
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

/// Write a temporary AWS config file applying the S3 transfer tuning, returning its path.
/// These settings (`max_concurrent_requests`, `max_queue_size`, `multipart_chunksize`) have
/// no CLI-flag/env equivalent, so a config file is the only way to apply them without
/// mutating the user's global `~/.aws/config`. Used via `AWS_CONFIG_FILE` on the sync command.
fn write_temp_aws_config(args: &DownloadArgs) -> Result<PathBuf> {
    let dir = download_metadata_root(&args.snapshot_dir);
    fs::create_dir_all(&dir)?;
    let path = dir.join("aws_tuning.config");
    let body = format!(
        "[default]\ns3 =\n    max_concurrent_requests = {}\n    max_queue_size = {}\n    multipart_chunksize = {}\n",
        args.max_concurrent_requests, args.max_queue_size, args.multipart_chunksize
    );
    fs::write(&path, body.as_bytes())
        .with_context(|| format!("failed to write temp aws config {}", path.display()))?;
    Ok(path)
}

fn run_aws(bin: &Path, args: &[String]) -> Result<String> {
    run_aws_env(bin, args, &[])
}

fn run_aws_env(bin: &Path, args: &[String], env: &[(&str, String)]) -> Result<String> {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd
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

fn download_reports_dir(snapshot_dir: &Path) -> PathBuf {
    let root = snapshot_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    root.join("openalex-snapshot_metadata").join("reports")
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

fn explain_download(args: &DownloadArgs) -> Result<()> {
    let effective_no_sign = args.no_sign_request && !args.signed;
    let effective_delete = args.delete_files && !args.no_delete;
    println!("--explain: download (parquet-native)");
    println!("aws_bin: {}", args.aws_bin.display());
    println!("parquet_dir: {}", args.parquet_dir.display());
    println!("dataset: {}", args.dataset);
    println!("no_sign_request: {}", effective_no_sign);
    println!("delete_files: {}", effective_delete);
    println!("auto_enrich (works): {}", !args.no_enrich);
    println!(
        "manifest: {}/data/parquet/manifest.json",
        args.s3_uri.trim_end_matches('/')
    );
    println!(
        "tuning: max_concurrent_requests={} max_queue_size={} multipart_chunksize={} (via temp AWS_CONFIG_FILE)",
        args.max_concurrent_requests, args.max_queue_size, args.multipart_chunksize
    );
    let example_ds = if args.dataset == "all" {
        "works".to_string()
    } else {
        args.dataset.clone()
    };
    let cmd = aws_sync_command(args, &example_ds)?;
    println!(
        "sync_command (per dataset, e.g. {example_ds}): {} {}",
        args.aws_bin.display(),
        cmd.join(" ")
    );
    println!("verify: not part of download; run verify_download separately");
    Ok(())
}

fn explain_validate_download(args: &ValidateDownloadArgs, tuning: &Tuning) {
    let mode = if args.quick {
        "quick (presence+size)"
    } else if args.full {
        "full (row scan)"
    } else {
        "meta (footer rowcount)"
    };
    println!("--explain: verify_download (manifest-driven)");
    println!("aws_bin: {}", args.aws_bin.display());
    println!("parquet_dir: {}", args.parquet_dir.display());
    println!(
        "manifest: {}/data/parquet/manifest.json",
        args.s3_uri.trim_end_matches('/')
    );
    println!("dataset: {}", args.dataset);
    println!("check_extra: {}", args.check_extra);
    println!("mode: {}", mode);
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

fn lock_file_path(parquet_dir: &Path) -> PathBuf {
    metadata_root(parquet_dir).join("openalex-snapshot.lock")
}

fn global_reports_dir(parquet_dir: &Path) -> PathBuf {
    metadata_root(parquet_dir).join("reports")
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

OpenAlex publishes the snapshot natively in parquet, so `openalex-snapshot` is a root-dir-first,
parquet-native CLI:

1. `download` / `verify_download` (download auto-runs `enrich` for works)
2. `enrich` (works_aws/ -> works/, adds abstract + citation)
3. `index` / `verify_index`
4. `extract`
5. reporting and progress (`report`, `prune-reports`, `progress`)

Runtime requirements:
- `aws` CLI for download/verify_download paths only
- No DuckDB — all parquet I/O uses the pure-Rust `arrow`/`parquet` crates

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
- resource settings (`workers`, `max-memory-mb`) when constraining resource use

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

# Download the parquet snapshot (auto-enriches works -> parquet/works/)
openalex-snapshot download --root-dir <root>

# Verify the download against the published manifest (presence + size + row count)
openalex-snapshot verify_download --root-dir <root>          # --quick (size only) / --full (row scan)

# (Re-)enrich works on demand: works_aws/ -> works/ (abstract + citation)
openalex-snapshot enrich --root-dir <root>

# Build indexes for all datasets (skips the raw *_aws staging dirs)
openalex-snapshot index --root-dir <root> --dataset all

# Verify index integrity
openalex-snapshot verify_index --root-dir <root> --dataset all

# Extract by IDs (writes one parquet per resolved dataset)
openalex-snapshot extract --root-dir <root> --ids <ids.csv> --output <extract.parquet>

# Or run the whole pipeline from config
openalex-snapshot all --config ./openalex-snapshot.yaml

# Show latest reports / aggregate totals
openalex-snapshot --config ./openalex-snapshot.yaml report --latest
openalex-snapshot --config ./openalex-snapshot.yaml report --latest --summary
```

## Failure Handling
- A JSON report is written only when a command has failures: `report --latest --full`.
- Datasets with failures are marked `!` in the default report view.
- Re-running any command is safe and incremental (download sync, enrich, index all resume).

## Decision rules
- `download` auto-runs `enrich`; pass `--no-enrich` for a raw-only sync.
- Reading works (`index`/`extract`, `W` IDs) uses `parquet/works/` (enriched); `index --dataset all`
  skips the raw `parquet/works_aws/` staging dir.
- Run `index` before `extract`; extraction requires `<dataset>_id_idx.parquet`.
- Use `--workers` / `--max-memory-mb` only to constrain resources (parquet I/O is light).

## Done Criteria
- Command exits 0.
- A report is written under `openalex-snapshot_metadata/reports/` only if there were failures.
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
2. `download`  (auto-enriches works -> parquet/works/)
3. `verify_download`
4. `enrich`  (only if you downloaded with --no-enrich)
5. `index`
6. `verify_index`
7. `extract`

## Auto orchestration (recommended)
```bash
openalex-snapshot all --config <path>
```
Runs download -> verify_download -> index -> verify_index in order.
Edit the `all:` section in the config to disable stages you don't need (e.g. `enable_download: false`).

## Corpus already present (skip download)
```bash
openalex-snapshot enrich --root-dir <root>            # if only parquet/works_aws/ exists
openalex-snapshot index --root-dir <root> --dataset all
openalex-snapshot verify_index --root-dir <root> --dataset all
```

## Decision Rules
- `download` auto-runs `enrich`; the canonical works corpus is `parquet/works/` (enriched),
  with the raw official copy preserved in `parquet/works_aws/` for clean incremental re-sync.
- Use `--dataset <name>` to rerun a single dataset without touching others.
- Every stage is resumable — re-running is safe and incremental.
- Check `report --latest` (written only on failure) and `--full` for root cause.
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
   (a report is written only when a command had failures)
2. `openalex-snapshot --config <cfg> report --latest --full` — full JSON for root cause
3. `progress --once` — check if a run is still live
4. Run targeted command with `--explain` to preview what it would do
5. Re-run the failed command — download/enrich/index all resume incrementally

## Common Traps
- Wrong `root-dir` (parquet/metadata dirs won't be found)
- Missing `aws` binary (only needed for download / verify_download)
- Low disk space (`check --root-dir <root>` reports estimates)
- Using `--config` after the subcommand instead of before it
- Expecting `parquet/works/` without enriching: `download` builds it unless `--no-enrich`

## Failure phase hints
- `check_download_disk`: insufficient free space for the download
- `download_sync`: S3 sync / auth / endpoint failure
- `validate_file_size` / `validate_parquet_rowcount`: local file differs from the manifest — re-run `download`
- `enrich` / `enrich_rowcount`: enrichment failed or row count drifted — re-run `enrich`
- `index_stage1` / `index_stage2`: a corpus parquet couldn't be read — re-download then re-index
- `extract_index_read`: missing/!built index — run `index` for that dataset first

## OOM
- Parquet I/O streams one file at a time, so memory use is modest.
- If you still hit limits, constrain with `--workers <N>` and/or `--max-memory-mb <N>`.
"#
            .to_string(),
        ),
        (
            base.join("development").join("SKILL.md"),
            r#"# Development Skill

## Purpose
Build, test, and deploy `openalex-snapshot` source changes safely.

## Repository layout
- Cargo workspace: `openalex-snapshot/` (the CLI binary, `src/main.rs`) and `openalex-core/`
  (shared library: profile planner + SQL string helpers, used by the R package).
- CLI tests live in `openalex-snapshot/tests/cli_smoke.rs`; unit tests in `src/main.rs`.
- Skills templates are embedded in `skills_templates()` near the end of `src/main.rs`.
- Config templates are embedded as `config_template_*()` functions in `src/main.rs`.

## Build / test loop
```bash
cargo build --release -p openalex-snapshot   # production binary
cargo test --workspace --locked              # run all tests
cargo clippy --all-targets -- -D warnings    # lint (must be clean)
cargo fmt --all                              # format (CI enforces)
```

Some `cli_smoke.rs` tests use a `duckdb` CLI to build small parquet fixtures and skip
gracefully when it is absent. The binary itself has NO DuckDB dependency.

## Deploy pattern
```bash
cargo build --release -p openalex-snapshot
cp target/release/openalex-snapshot <target-dir>/openalex-snapshot
```

## Parquet I/O (pure Rust)
- All parquet reads/writes use the `arrow` + `parquet` crates — no DuckDB.
- Row counts come from parquet footer metadata (`parquet_rowcount_meta`); `verify_download --full`
  decodes all row groups to catch data-page corruption.
- `index` projects the `id` column, derives `id_block`/`file_row_number`, and writes shards with
  `ArrowWriter` (SNAPPY); `extract` filters rows with `arrow::compute::filter` (preserving nested
  columns); `enrich` reconstructs `abstract` from the JSON inverted index (duplicate-key-preserving
  parse) and builds `citation` from the nested `authorships` struct.
- `rayon` provides per-file parallelism (`--workers`); memory is modest since one file streams at a time.

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
- `cargo test --workspace --locked` passes (all tests green).
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
- Help text in `src/main.rs` (`*_LONG_ABOUT`, `#[arg(help = ...)]`)
- Config template in `src/main.rs` (if new options added)
- Skills templates in `skills_templates()` in `src/main.rs` (if operational behavior changed)
- `AI_SKILLS_USAGE.md` — if skill structure changes

## Acceptance criteria
- New flags/commands appear in: `--help`, `README.md`, `docs/`, and `NEWS.md`
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

#[cfg(test)]
mod tests {
    use super::*;
    use openalex_core::sql::sql_quote;

    // -----------------------------------------------------------------------
    // Stratified profile machinery
    // -----------------------------------------------------------------------

    #[test]
    fn test_sql_quote() {
        assert_eq!(sql_quote("abc"), "'abc'");
        assert_eq!(sql_quote("a'b"), "'a''b'");
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

    #[test]
    fn test_reconstruct_abstract() {
        // words ordered by position
        assert_eq!(
            reconstruct_abstract(r#"{"world":[1],"hello":[0]}"#).as_deref(),
            Some("hello world")
        );
        // a word repeated at several positions
        assert_eq!(
            reconstruct_abstract(r#"{"the":[0,2],"cat":[1]}"#).as_deref(),
            Some("the cat the")
        );
        // DUPLICATE keys must both be kept (not last-wins): "x" at 0 and 2
        assert_eq!(
            reconstruct_abstract(r#"{"x":[0],"y":[1],"x":[2]}"#).as_deref(),
            Some("x y x")
        );
        // escaped key (quote) must not drop the abstract
        assert_eq!(
            reconstruct_abstract(r#"{"a\"b":[0]}"#).as_deref(),
            Some("a\"b")
        );
        // empty object -> empty string; invalid JSON -> None
        assert_eq!(reconstruct_abstract("{}").as_deref(), Some(""));
        assert_eq!(reconstruct_abstract("not json"), None);
    }
}
