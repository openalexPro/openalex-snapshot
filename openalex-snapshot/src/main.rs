use anyhow::{anyhow, bail, Context, Result};
use chrono::{Local, TimeZone};
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use openalex_core::profile::{
    auto_profile_safe_memory_mb, auto_profile_single_worker_safe_memory_mb, build_convert_plan,
    derive_stratified_profile_for_ram, detect_total_memory_mb, FilePair, ProfileDef, ProfileKind,
    ProfileRegistry,
};
use openalex_core::sql::{normalize_duckdb_type, parse_size_str, sql_quote};
use openalex_core::{works_abstract_expr, works_abstract_expr_from_json, works_citation_expr};
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
- convert/verify_convert/schema/verify_schema: [DEPRECATED] legacy JSON-snapshot tools

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
  openalex-snapshot config --create-profiles      # scaffold performance.yaml from detected RAM
  openalex-snapshot config --list-profiles        # show all available profiles
  openalex-snapshot all --config ./openalex-snapshot.yaml --retry 2
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

There are TWO separate config files this command can produce, neither of which
references the other:

  1. MAIN config         (./openalex-snapshot.yaml)
     - Drives every subcommand: paths, dataset selection, workers, memory caps,
       profile name to use, per-stage options.  Required by `all`.
     - Pointed at by the global `--config <path>` flag.
     - Generated by `--create <complete|safe>`.

  2. PROFILES config     (./openalex-snapshot.performance.yaml)
     - Defines named stratified performance profiles (workers + memory per
       gz-size bucket).  Optional; built-ins `safe` and `stratified-36` work
       without it.  Auto-discovered if present alongside the main config.
     - Pointed at by the global `--performance-config <path>` flag.
     - Generated by `--create-profiles` (host-tuned, RAM-derived).

Modes (exactly one required):
  --create <complete|safe>   Generate the MAIN config template (default: complete)
  --create-profiles          Generate the PROFILES config from detected RAM
  --verify                   Validate the MAIN config file strictly
  --list-profiles            Print built-in + loaded profiles with their strata

Defaults:
  main config path:     ./openalex-snapshot.yaml
  profiles config path: ./openalex-snapshot.performance.yaml

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
  3) convert  ──┐
  4) verify_convert ┴── looped up to --retry times: convert auto-repairs any
                       parquet flagged by the latest verify_convert report.
  5) index
  6) verify_index
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

const CONVERT_LONG_ABOUT: &str = "\
[DEPRECATED — JSON pipeline] Convert OpenAlex snapshot JSON.GZ files into parquet files.
OpenAlex now publishes parquet natively; use `download` (+ auto `enrich`) instead.

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
        help = "Optional path to a profiles YAML defining custom stratified profiles (auto-discovers ./openalex-snapshot.performance.yaml if omitted; built-in profiles `safe` and `stratified-36` are always available)"
    )]
    performance_config: Option<PathBuf>,

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
    #[command(about = "[DEPRECATED] Convert legacy snapshot .json.gz files to parquet", long_about = CONVERT_LONG_ABOUT)]
    Convert(ConvertArgs),
    #[command(
        about = "[DEPRECATED] Verify parity between a legacy JSON snapshot and parquet",
        long_about = VERIFY_LONG_ABOUT,
        name = "verify_convert"
    )]
    Verify(VerifyArgs),
    #[command(
        about = "[DEPRECATED] Inspect schema from a legacy JSON source/cache/parquet",
        long_about = SCHEMA_LONG_ABOUT
    )]
    Schema(SchemaArgs),
    #[command(about = "[DEPRECATED] Verify schema parity across legacy sources.", long_about = VERIFY_SCHEMA_LONG_ABOUT, name = "verify_schema")]
    VerifySchema(VerifySchemaArgs),
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

    #[arg(long, default_value_t = 1)]
    #[arg(
        help = "Max number of extra `convert` retries when verify_convert reports failures (convert auto-repairs flagged parquets on each retry)"
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
    #[arg(
        help = "Write the PROFILES config YAML (default ./openalex-snapshot.performance.yaml). Auto-derives a `stratified-<RAM_GB>` profile from the host's detected RAM. This is a SEPARATE file from --create; `--performance-config` points at it. Custom profiles defined here override the built-in `safe` / `stratified-36` and are selected with `convert --profile <name>`."
    )]
    create_profiles: bool,

    #[arg(long, default_value_t = false)]
    #[arg(
        help = "List all profiles visible to the binary: built-ins (`safe`, `stratified-36`) plus any loaded from --performance-config. Shows each profile's strata table."
    )]
    list_profiles: bool,

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
// and optionally merged with a user-supplied `performance.yaml`.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
// Profile types (Stratum, ProfileKind, ProfileDef, ProfilesYaml, ProfileRegistry,
// FilePair, StratumPlan, ConvertPlan) and all related functions are imported from
// openalex_core::profile at the top of this file.
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
        help = "Performance/memory profile (default: safe). Built-in: safe, stratified-36 (fixed 36 GB baseline). For other RAM sizes run `config --create-profiles` to scaffold a tuned performance.yaml."
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

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    #[arg(
        help = "Auto-repair: at startup, read the latest verify_convert report and re-do any parquet it flagged (delete + reconvert). Default true. Disable with --auto-repair=false; ignored when --input-file is given."
    )]
    auto_repair: bool,
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

    #[arg(long)]
    #[arg(help = "Path to duckdb executable (unused; DuckDB is statically linked)")]
    duckdb_bin: Option<PathBuf>,

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
    enable_convert: Option<bool>,
    enable_verify_convert: Option<bool>,
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
    auto_repair: Option<bool>,
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
struct EnrichConfig {
    root_dir: Option<PathBuf>,
    dataset: Option<String>,
    workers: Option<usize>,
    max_memory_mb: Option<usize>,
    duckdb_bin: Option<PathBuf>,
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

// FilePair is imported from openalex_core::profile at the top of this file.

#[derive(Debug, Clone)]
struct RepairTarget {
    dataset: String,
    /// Resolved snapshot source path (unused by auto-repair but kept for the
    /// `test_collect_repair_targets_filters_and_dedups` test and as useful
    /// context if the caller wants to log/inspect targets).
    #[allow(dead_code)]
    source_path: PathBuf,
    output_path: PathBuf,
    /// Relative path under the dataset; same use as `source_path`.
    #[allow(dead_code)]
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
        return run_config(args, cli.performance_config.as_deref());
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
            run_all(args, &all_cfg, cli.performance_config.as_deref())
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
            run_convert(args, cli.performance_config.as_deref())
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
        if !cli_explicit(matches, "auto_repair") {
            if let Some(v) = c.auto_repair {
                args.auto_repair = v;
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
        if !cli_explicit(matches, "duckdb_bin") {
            if let Some(v) = &c.duckdb_bin {
                args.duckdb_bin = Some(v.clone());
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
  # allowed values: any valid executable path
  # duckdb_bin: /usr/local/bin/duckdb
  # Profile controls DuckDB memory budget and worker count.
  # safe (default) — single-pass, single worker, generous per-worker memory
  #   (45% of usable RAM, clamped 8-24 GiB).  Reliable on any host; uses DuckDB
  #   spill-to-disk for files larger than the memory budget.
  # stratified-36 — multi-pass; partitions files by gz size and runs one rayon
  #   pass per non-empty stratum (4-/3-/2-/1-workers on <400/400-600/600-800/800+ MB).
  #   Empirically tuned for ~36 GB RAM hosts.
  # Custom profiles for other RAM tiers go in a sibling `openalex-snapshot.performance.yaml`
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

  # Max number of extra `convert` retries when verify_convert reports
  # failures.  Each retry uses convert's auto-repair to delete and re-convert
  # the parquets flagged by the latest verify_convert report.  0 means: run
  # verify_convert once and fail immediately on errors.
  # allowed values: integer >= 0
  retry: 1

  # Stage toggles (default pipeline shown below).
  # Disable stages you do not want in `all` (e.g., skip download for local runs).
  # allowed values: true | false
  enable_download: true
  # allowed values: true | false
  enable_verify_download: true
  # [DEPRECATED legacy JSON pipeline] default false in the parquet-native era
  # allowed values: true | false
  enable_convert: false
  # allowed values: true | false
  enable_verify_convert: false
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

  # Auto-repair: at startup, read the latest verify_convert report under
  # <root>/openalex-snapshot_metadata/reports/ and delete any output parquet
  # it flagged so the normal skip-if-exists filter re-includes it.  Effectively
  # "run convert twice fixes things" after a verify failure.  Ignored when
  # --input-file is given (so named-file runs stay predictable).
  # allowed values: true | false
  # auto_repair: true

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

fn run_config(args: ConfigArgs, performance_config: Option<&Path>) -> Result<()> {
    let modes = (args.create.is_some() as u8)
        + (args.verify as u8)
        + (args.create_profiles as u8)
        + (args.list_profiles as u8);
    if modes != 1 {
        bail!(
            "config requires exactly one mode: use --create <complete|safe>, --verify, --create-profiles, or --list-profiles"
        );
    }
    if args.explain {
        let mode = if args.create.is_some() {
            "create"
        } else if args.verify {
            "verify"
        } else if args.create_profiles {
            "create_profiles"
        } else {
            "list_profiles"
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
    if args.list_profiles {
        return run_config_list_profiles(performance_config);
    }
    if args.create_profiles {
        return run_config_create_profiles(performance_config, args.stdout, args.overwrite);
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

/// Print all known profiles (built-ins + any from --performance-config) as a table.
fn run_config_list_profiles(performance_config: Option<&Path>) -> Result<()> {
    let registry =
        ProfileRegistry::load(discover_performance_config(performance_config).as_deref())?;
    let total_mb = detect_total_memory_mb();
    println!("Available profiles:");
    println!();
    let mut names: Vec<&str> = registry.names();
    names.sort();
    for name in names {
        let def = registry.get(name).expect("name from registry");
        let kind = match def.kind {
            ProfileKind::Safe => "safe",
            ProfileKind::Stratified => "stratified",
        };
        print!("  {name}  ({kind}");
        if let Some(min) = def.min_ram_gb {
            print!(", min_ram_gb={min}");
        }
        println!(")");
        if let Some(desc) = &def.description {
            println!("    {desc}");
        }
        if let Some(strata) = &def.strata {
            println!("    strata:");
            println!("      max_file_mb  workers  per_worker_mb");
            for s in strata {
                let mfm = s
                    .max_file_mb
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "<catch-all>".to_string());
                println!(
                    "      {:<11}  {:>7}  {:>13}",
                    mfm, s.workers, s.per_worker_mb
                );
            }
        }
        if def.kind == ProfileKind::Safe {
            println!("    resolves to: workers=1, per_worker_mb={} (single-worker safe boost on workers=1)",
                auto_profile_single_worker_safe_memory_mb(total_mb));
        }
        println!();
    }
    Ok(())
}

/// Scaffold a profiles YAML auto-derived from the host's detected RAM.
/// Writes to `--performance-config <path>` (or `./openalex-snapshot.performance.yaml`).
fn run_config_create_profiles(
    performance_config: Option<&Path>,
    stdout: bool,
    overwrite: bool,
) -> Result<()> {
    let total_mb = detect_total_memory_mb();
    let ram_gb = total_mb.map(|mb| (mb + 512) / 1024).unwrap_or(0);
    let profile_name = if ram_gb > 0 {
        format!("stratified-{ram_gb}")
    } else {
        "stratified-host".to_string()
    };
    let derived = derive_stratified_profile_for_ram(total_mb);
    let yaml = render_stratified_profiles_yaml(&profile_name, ram_gb, &derived);

    if stdout {
        print!("{yaml}");
        return Ok(());
    }

    let dest = performance_config
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("openalex-snapshot.performance.yaml"));
    if dest.exists() && !overwrite {
        bail!(
            "profiles config already exists: {} (use --overwrite)",
            dest.display()
        );
    }
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(&dest, yaml).with_context(|| format!("failed to write {}", dest.display()))?;
    println!(
        "[config] created {} (profile: {})",
        dest.display(),
        profile_name
    );
    Ok(())
}

/// Render a `performance.yaml` template containing one auto-derived profile plus
/// commented examples for half-RAM and double-RAM tiers so users have reference
/// points to copy and tune.
fn render_stratified_profiles_yaml(name: &str, ram_gb: usize, def: &ProfileDef) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let _ = writeln!(s, "# openalex-snapshot.performance.yaml");
    let _ = writeln!(
        s,
        "# Generated by `openalex-snapshot config --create-profiles`. Detected RAM: {} GB.",
        if ram_gb > 0 {
            ram_gb.to_string()
        } else {
            "<unknown>".to_string()
        }
    );
    let _ = writeln!(s, "#");
    let _ = writeln!(
        s,
        "# File-size cutoffs (max_file_mb) reflect the works dataset's data shape and rarely need tuning."
    );
    let _ = writeln!(
        s,
        "# workers + per_worker_mb were auto-derived from your RAM; edit if you observe swap/OOM."
    );
    let _ = writeln!(
        s,
        "# Share this file across machines with similar RAM by copying it next to openalex-snapshot.yaml."
    );
    let _ = writeln!(s, "#");
    let _ = writeln!(s, "# Reference the profile by name from `convert`:");
    let _ = writeln!(s, "#   openalex-snapshot convert --profile {name} ...");
    let _ = writeln!(s);
    let _ = writeln!(s, "profiles:");
    let _ = writeln!(s, "  {name}:");
    if let Some(desc) = &def.description {
        let _ = writeln!(s, "    description: {:?}", desc);
    }
    if let Some(min) = def.min_ram_gb {
        let _ = writeln!(s, "    min_ram_gb: {min}");
    }
    let _ = writeln!(s, "    kind: stratified");
    let _ = writeln!(s, "    strata:");
    if let Some(strata) = &def.strata {
        for st in strata {
            match st.max_file_mb {
                Some(mb) => {
                    let _ = writeln!(s, "      - max_file_mb: {mb}");
                    let _ = writeln!(s, "        workers: {}", st.workers);
                    let _ = writeln!(s, "        per_worker_mb: {}", st.per_worker_mb);
                }
                None => {
                    let _ = writeln!(s, "      - workers: {}        # max_file_mb omitted = catch-all (no upper bound)", st.workers);
                    let _ = writeln!(s, "        per_worker_mb: {}", st.per_worker_mb);
                }
            }
        }
    }
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "# --- Examples for other RAM tiers (commented out — copy + edit if useful) ---"
    );
    let half = ram_gb.max(2) / 2;
    let dbl = ram_gb.saturating_mul(2).max(4);
    let example = |target_gb: usize| -> String {
        let target_mb = target_gb.saturating_mul(1024);
        let other = derive_stratified_profile_for_ram(Some(target_mb));
        let mut out = String::new();
        let _ = writeln!(out, "#   stratified-{target_gb}:");
        let _ = writeln!(out, "#     description: \"Tuned for ~{target_gb} GB RAM\"");
        let _ = writeln!(
            out,
            "#     min_ram_gb: {}",
            target_gb.saturating_mul(85) / 100
        );
        let _ = writeln!(out, "#     kind: stratified");
        let _ = writeln!(out, "#     strata:");
        if let Some(strata) = &other.strata {
            for st in strata {
                match st.max_file_mb {
                    Some(mb) => {
                        let _ = writeln!(out, "#       - max_file_mb: {mb}");
                        let _ = writeln!(out, "#         workers: {}", st.workers);
                        let _ = writeln!(out, "#         per_worker_mb: {}", st.per_worker_mb);
                    }
                    None => {
                        let _ = writeln!(out, "#       - workers: {}", st.workers);
                        let _ = writeln!(out, "#         per_worker_mb: {}", st.per_worker_mb);
                    }
                }
            }
        }
        out
    };
    if half >= 2 && half != ram_gb {
        s.push_str(&example(half));
    }
    if dbl != ram_gb {
        s.push_str(&example(dbl));
    }
    s
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
    retry: usize,
    enable_download: bool,
    enable_verify_download: bool,
    enable_convert: bool,
    enable_verify_convert: bool,
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
        // Legacy JSON pipeline: off by default in the parquet-native era.
        enable_convert: c.enable_convert.unwrap_or(false),
        enable_verify_convert: c.enable_verify_convert.unwrap_or(false),
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

fn run_all(args: AllArgs, cfg: &AppConfig, performance_config: Option<&Path>) -> Result<()> {
    let resolved = resolve_all_settings(&args, cfg);
    let snapshot_dir = resolved.root_dir.join("snapshot");
    let parquet_dir = resolved.root_dir.join("parquet");
    fs::create_dir_all(&parquet_dir)?;

    if args.explain {
        println!("--explain: all");
        println!("root_dir: {}", resolved.root_dir.display());
        println!("retry: {}", resolved.retry);
        println!(
            "steps: download={} verify_download={} convert={} verify_convert={} index={} verify_index={}",
            resolved.enable_download,
            resolved.enable_verify_download,
            resolved.enable_convert,
            resolved.enable_verify_convert,
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

    if resolved.enable_convert {
        // New flow (replaces the standalone repair_convert subcommand):
        //   loop:
        //     run convert  — auto-repair from latest verify_convert report (no-op on iter 1)
        //     run verify   — if no failures, break
        //     if attempts >= retry: break
        //     attempts += 1
        // The convert command reads the latest verify_convert report at startup
        // and deletes any flagged parquets so the normal skip-if-exists filter
        // re-includes them.  Each subsequent verify produces a fresh report that
        // the next convert iteration sees.  `--retry N` caps the *additional*
        // convert attempts after the first failed verify (default 1).
        let mut verify_ok = false;
        let mut attempts = 0usize;
        loop {
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
                auto_repair: true,
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
                run_convert(ca, performance_config),
                if attempts == 0 {
                    None
                } else {
                    Some(format!("attempt={}", attempts + 1))
                },
            );
            if step_failed {
                report_finalize(&mut report);
                let _ = write_run_reports(&parquet_dir, &report);
                bail!("[all] aborting after convert failure");
            }

            if !resolved.enable_verify_convert {
                verify_ok = true; // verify is disabled, nothing to retry against
                break;
            }

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
            if attempts >= resolved.retry {
                break;
            }
            attempts += 1;
        }
        if resolved.enable_verify_convert && !verify_ok {
            report.failures.push(FailureEntry {
                dataset: "all".to_string(),
                phase: "all_verify_repair_loop".to_string(),
                rel_path: None,
                source_path: None,
                output_path: None,
                error_message: format!(
                    "verify_convert did not pass after {} convert retry attempt(s)",
                    resolved.retry
                ),
                suggested_recovery: Some(
                    "investigate the latest verify_convert report and rerun `convert` (auto-repair) with a more generous --profile or split-size"
                        .to_string(),
                ),
            });
            report_finalize(&mut report);
            let _ = write_run_reports(&parquet_dir, &report);
            bail!("[all] verify/retry loop exhausted");
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

fn run_convert(args: ConvertArgs, performance_config: Option<&Path>) -> Result<()> {
    fs::create_dir_all(&args.shared.parquet_dir)?;
    let datasets = resolve_datasets(&args.shared.snapshot_dir, &args.shared.dataset)?;
    let duckdb_bin = duckdb_bin(&args.shared);

    let total_mb = detect_total_memory_mb();
    let profile_registry =
        ProfileRegistry::load(discover_performance_config(performance_config).as_deref())?;
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

    // Capture auto-repair targets BEFORE `archive_completed_run` moves the
    // latest verify_convert report out of `reports/` into archived/.  The
    // per-dataset loop below consumes these to delete the flagged parquets.
    let auto_repair_targets_by_dataset: std::collections::HashMap<String, Vec<RepairTarget>> =
        if args.auto_repair && args.input_files.is_empty() {
            let all_allowed: BTreeSet<String> = datasets.iter().cloned().collect();
            let all_targets = verify_failures_for_repair(
                &args.shared.snapshot_dir,
                &args.shared.parquet_dir,
                &all_allowed,
            );
            let mut by_ds: std::collections::HashMap<String, Vec<RepairTarget>> =
                std::collections::HashMap::new();
            for t in all_targets {
                by_ds.entry(t.dataset.clone()).or_default().push(t);
            }
            by_ds
        } else {
            std::collections::HashMap::new()
        };

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

        // Auto-repair: use the cached verify_convert targets (captured BEFORE
        // archive_completed_run moved the report) to delete any output parquet
        // flagged for this dataset.  The normal skip-if-exists filter below
        // then re-includes those files.  Disabled by `--auto-repair=false` or
        // when the user named specific files via `--input-file`.
        if args.auto_repair && args.input_files.is_empty() {
            let force_outputs: std::collections::HashSet<PathBuf> = auto_repair_targets_by_dataset
                .get(dataset.as_str())
                .map(|ts| ts.iter().map(|t| t.output_path.clone()).collect())
                .unwrap_or_default();
            let mut auto_repaired = 0usize;
            for p in &pairs {
                if force_outputs.contains(&p.output_parquet) && p.output_parquet.exists() {
                    if let Err(e) = fs::remove_file(&p.output_parquet) {
                        eprintln!(
                            "[convert] auto-repair: could not delete {} ({e}); leaving it for the next run",
                            p.output_parquet.display()
                        );
                    } else {
                        auto_repaired += 1;
                    }
                }
            }
            if auto_repaired > 0 {
                eprintln!(
                    "[convert] dataset={dataset} auto-repair: re-doing {} file(s) flagged by latest verify_convert report",
                    auto_repaired
                );
                try_log_dataset(
                    &args.shared.parquet_dir,
                    dataset,
                    "convert",
                    &format!(
                        "auto-repair from latest verify_convert report: deleted {} parquet(s) for reconversion",
                        auto_repaired
                    ),
                );
            }
        }

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
        // For works, append two derived columns to the COPY SELECT:
        //   abstract  — reconstructed from abstract_inverted_index
        //   citation  — "Author (year)" / "A & B (year)" / "A et al. (year)"
        // abstract_inverted_index itself is kept in the output for callers that
        // want the original form.  Guarded by presence in the inferred schema
        // so synthetic test fixtures without these columns convert cleanly.
        let select_extras = if dataset == "works" {
            let field_names: std::collections::HashSet<&str> =
                schema.fields.iter().map(|f| f.name.as_str()).collect();
            works_enrichment_select_extras_if_supported(&field_names)
        } else {
            String::new()
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
                                &select_extras,
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

fn explain_enrich(args: &EnrichArgs, src_dir: &Path, out_dir: &Path, tuning: &Tuning) {
    println!("--explain: enrich");
    println!("dataset: {}", args.dataset);
    println!("source: {}", src_dir.display());
    println!("output: {}", out_dir.display());
    println!("workers: {}", tuning.workers);
    println!("memory_mb: {:?}", tuning.memory_mb);
    println!("overwrite: {}", args.overwrite);
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
    let bin = duckdb_bin_from_option(&args.duckdb_bin);
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
    let cols = describe_parquet_glob(&bin, &files[0].to_string_lossy(), tuning.memory_mb)?;
    let mut select_extras = String::new();
    if cols.contains_key("abstract_inverted_index") {
        select_extras.push_str(&format!(
            ", {} AS abstract",
            works_abstract_expr_from_json()
        ));
    }
    if cols.contains_key("authorships") && cols.contains_key("publication_year") {
        select_extras.push_str(&format!(", {} AS citation", works_citation_expr()));
    }
    if select_extras.is_empty() {
        bail!("[enrich] source works parquet has neither abstract_inverted_index nor authorships+publication_year");
    }

    let start = Instant::now();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(tuning.workers)
        .build()
        .context("failed to build rayon thread pool")?;
    let pb = make_progress_bar(args.progress, files.len() as u64, "enrich");
    let extras = select_extras.as_str();
    let mem = tuning.memory_mb;
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
                let mut sql = String::new();
                sql.push_str("SET preserve_insertion_order = false;");
                sql.push_str("SET threads = 1;");
                if let Some(mb) = mem {
                    sql.push_str(&format!("SET memory_limit='{}MB';", mb));
                }
                sql.push_str(&format!(
                    "COPY (SELECT *{} FROM read_parquet({})) TO {} (FORMAT PARQUET, COMPRESSION SNAPPY);",
                    extras,
                    sql_quote(&pf.to_string_lossy()),
                    sql_quote(&out_file.to_string_lossy())
                ));
                if let Err(e) = run_duckdb_sql(&bin, &sql) {
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
            duckdb_bin: None,
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
        let mem = tuning.memory_mb;
        let results: Vec<Option<FailureEntry>> = pool.install(|| {
            present_files
                .par_iter()
                .map(|(ds, local, expected_rc)| {
                    let got = if full {
                        duckdb_count_parquet(Path::new("duckdb"), local, mem)
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
/// directly.  Otherwise auto-discover `./openalex-snapshot.performance.yaml` if it
/// exists.  Returns `None` when no profiles config is in use (built-ins only).
fn discover_performance_config(cli_arg: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = cli_arg {
        return Some(p.to_path_buf());
    }
    let default = PathBuf::from("openalex-snapshot.performance.yaml");
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
// Convert plan — StratumPlan, ConvertPlan, build_convert_plan,
// auto_profile_safe_memory_mb, auto_profile_single_worker_safe_memory_mb,
// parse_size_str, detect_total_memory_mb are all imported from
// openalex_core::profile / openalex_core::sql at the top of this file.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Tuning {
    workers: usize,
    memory_mb: Option<usize>,
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

/// Auto-repair entry point used by `run_convert`.  Best-effort: returns empty
/// when no verify_convert report exists, when it can't be parsed, or when no
/// failures in it match the datasets being converted.  Never errors — failures
/// here just mean the normal skip-if-exists logic runs unchanged.
///
/// Used to replace the standalone `repair_convert` subcommand: convert now
/// reads the latest verify report at startup and includes flagged parquets in
/// its `todo` list.
fn verify_failures_for_repair(
    snapshot_dir: &Path,
    parquet_dir: &Path,
    allowed_datasets: &BTreeSet<String>,
) -> Vec<RepairTarget> {
    let Some(report_path) = latest_report_for_command(parquet_dir, "verify_convert") else {
        return Vec::new();
    };
    let Ok(txt) = fs::read_to_string(&report_path) else {
        eprintln!(
            "[convert] auto-repair: could not read latest verify_convert report at {} — skipping",
            report_path.display()
        );
        return Vec::new();
    };
    let report: RunReport = match serde_json::from_str(&txt) {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "[convert] auto-repair: latest verify_convert report at {} failed to parse ({e:#}) — skipping",
                report_path.display()
            );
            return Vec::new();
        }
    };
    collect_repair_targets(&report, allowed_datasets, snapshot_dir, parquet_dir)
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
fn parquet_rowcount_meta(path: &Path) -> Result<u64> {
    let sql = with_session_settings(
        &format!(
            "SELECT CAST(SUM(num_rows) AS BIGINT) AS n FROM parquet_file_metadata({})",
            sql_quote(&path.to_string_lossy())
        ),
        None,
        Some(1),
    );
    let row = query_one_row(Path::new("duckdb"), &sql)?;
    parse_u64(row.get("n"))
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

fn archive_completed_run(parquet_dir: &Path, _snapshot_dir: &Path) -> Result<()> {
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
2. `convert` (with built-in auto-repair from verify report) / `verify_convert`
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

# Repair failed files: just run convert again — it auto-repairs anything flagged
# by the latest verify_convert report.
openalex-snapshot convert --root-dir <root> --dataset works

# Scaffold a custom performance.yaml auto-derived from this host's RAM
openalex-snapshot config --create-profiles

# Show all known profiles (built-ins + user-defined)
openalex-snapshot config --list-profiles
```

## Failure Handling
- On non-zero exit, inspect latest report: `report --latest --full`.
- Datasets with failures are marked `!` in the default report view.
- For verify-driven reconversion: just re-run `convert` (auto-repair is on by default).

## Decision rules
- Default profile is `safe` for `convert`: single worker, generous per-worker memory (~45% of usable RAM, clamped 8–24 GiB on single-worker mode).  Works on any host; the most reliable choice for the worst-case files.
- On a 32+ GB host, use `--profile stratified-36` for a faster run: it partitions the file list by gz size (4-/3-/2-/1-worker buckets) and runs one rayon pass per bucket.
- Custom RAM tiers (e.g. 16 GB, 64 GB) need a user-supplied `openalex-snapshot.performance.yaml` — see [`docs/commands/convert.md`](../../docs/commands/convert.md#custom-profiles-via-profilesyaml).
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
4. `convert`            ──┐ looped by `all` up to --retry times:
5. `verify_convert`  ──┘ convert auto-repairs anything verify flagged
6. `index`
7. `verify_index`
8. `extract`

## Auto orchestration (recommended)
```bash
openalex-snapshot all --config <path> --retry 2
```
Runs all enabled stages in order with a bounded convert/verify loop.
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
- For other RAM tiers, supply a user-defined profile in `openalex-snapshot.performance.yaml` (sibling to the main config or via `--performance-config`).
- Use `--dataset <name>` to rerun a single dataset without touching others.
- Check `report --latest` after each stage to confirm success before proceeding.
- Keep reports: they drive convert's auto-repair and provide audit trails.
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
5. Rerun the failed dataset with `convert` — auto-repair (default) re-does whatever the latest verify flagged

## Common Traps
- Wrong `root-dir` (snapshot/parquet/metadata dirs won't be found)
- Missing `aws` binary (only needed for download steps)
- Low disk space (`check --root-dir <root>` reports estimates)
- Using `--config` after the subcommand instead of before it

## Failure phase hints
- `check_dependency`: missing `aws` binary (duckdb is bundled — not an external dep)
- `check_download_disk` / `check_convert_disk`: insufficient free space
- `verify_metrics`: file-level parity mismatch — just re-run `convert` (auto-repair handles it)
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
- `convert` resolves `--profile <name>` against a `ProfileRegistry`
  populated from `builtin_profiles()` plus an optional user `openalex-snapshot.performance.yaml`.
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
- `run_convert` iterates `plan.strata`, reconfiguring DuckDB + rayon per stratum.

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

#[allow(clippy::too_many_arguments)]
fn convert_one(
    duckdb_bin: &Path,
    pair: &FilePair,
    columns_clause: &str,
    compression: &str,
    row_group_rows: usize,
    memory_mb: Option<usize>,
    extra_json_options: &str,
    select_extras: &str,
) -> Result<()> {
    if let Some(parent) = pair.output_parquet.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = pair.output_parquet.with_extension("parquet.tmp");
    let in_q = sql_quote(&pair.input_gz.to_string_lossy());
    let out_q = sql_quote(&tmp.to_string_lossy());

    // `select_extras` is a comma-prefixed list of additional projected columns
    // appended after `SELECT *` (e.g. derived `abstract`, `citation` for works).
    // Empty for datasets without enrichment.
    let mut sql = String::new();
    sql.push_str("SET preserve_insertion_order = false;");
    sql.push_str(&format!(
        "COPY (SELECT *{select_extras} FROM read_json({in_q}, columns = {columns_clause}, union_by_name = true, ignore_errors = true{extra_json_options})) TO {out_q} (FORMAT PARQUET, COMPRESSION {compression}, ROW_GROUP_SIZE {row_group_rows});",
        compression = compression.to_uppercase(),
    ));
    let _ = memory_mb; // set globally via set_duckdb_memory_limit before the parallel pass

    run_duckdb_sql(duckdb_bin, &sql)?;
    fs::rename(&tmp, &pair.output_parquet)?;
    Ok(())
}

/// Returns the works-enrichment SELECT-extras string if and only if all the
/// source columns the expressions reference are present in the inferred schema.
/// Used by run_convert to skip enrichment for synthetic / minimal test data
/// that lacks `abstract_inverted_index`, `authorships`, or `publication_year`.
fn works_enrichment_select_extras_if_supported(
    field_names: &std::collections::HashSet<&str>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if field_names.contains("abstract_inverted_index") {
        parts.push(format!(", {} AS abstract", works_abstract_expr()));
    }
    if field_names.contains("authorships") && field_names.contains("publication_year") {
        parts.push(format!(", {} AS citation", works_citation_expr()));
    }
    parts.concat()
}

// works_abstract_expr and works_citation_expr are re-exported from openalex_core.
// See openalex-core/src/lib.rs for the implementations.

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

// normalize_duckdb_type and sql_quote are imported from openalex_core::sql
// at the top of this file.

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
    use openalex_core::profile::{STRATIFIED_MAX_WORKERS, STRATIFIED_MIN_PER_WORKER_MB};

    // -----------------------------------------------------------------------
    // Stratified profile machinery
    // -----------------------------------------------------------------------

    #[test]
    fn profile_registry_loads_builtins_and_user_overrides() {
        let reg = ProfileRegistry::builtins_only();
        // built-ins always present
        assert!(reg.get("safe").is_some());
        assert!(reg.get("stratified-36").is_some());
        assert!(reg.get("nonexistent").is_none());

        // user YAML overrides a built-in + adds a custom name
        let td = tempfile::tempdir().unwrap();
        let path = td.path().join("performance.yaml");
        std::fs::write(
            &path,
            r#"
profiles:
  stratified-36:
    description: "user override of built-in"
    kind: stratified
    strata:
      - max_file_mb: 100
        workers: 8
        per_worker_mb: 2048
      - workers: 1
        per_worker_mb: 4096
  stratified-64:
    description: "custom for big hosts"
    kind: stratified
    strata:
      - max_file_mb: 500
        workers: 6
        per_worker_mb: 8192
      - workers: 2
        per_worker_mb: 24576
"#,
        )
        .unwrap();
        let reg = ProfileRegistry::load(Some(&path)).expect("load ok");
        // user override wins
        let s36 = reg.get("stratified-36").unwrap();
        assert_eq!(
            s36.description.as_deref(),
            Some("user override of built-in")
        );
        assert_eq!(s36.strata.as_ref().unwrap().len(), 2);
        // user-only profile available
        assert!(reg.get("stratified-64").is_some());
        // built-in not overridden still present
        assert!(reg.get("safe").is_some());

        // invalid profile (no catch-all) is rejected with a clear error
        let bad = td.path().join("bad.yaml");
        std::fs::write(
            &bad,
            r#"
profiles:
  bad:
    kind: stratified
    strata:
      - max_file_mb: 100
        workers: 1
        per_worker_mb: 2048
"#,
        )
        .unwrap();
        let err = ProfileRegistry::load(Some(&bad)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("catch-all"), "got: {msg}");
    }

    fn fp(rel: &str, gz_bytes: u64) -> FilePair {
        FilePair {
            input_gz: PathBuf::from(rel),
            output_parquet: PathBuf::from(format!("{rel}.parquet")),
            rel: PathBuf::from(rel),
            gz_size_bytes: gz_bytes,
        }
    }

    #[test]
    fn build_convert_plan_strata_partitioning_by_size_and_largest_first_order() {
        let reg = ProfileRegistry::builtins_only();
        // files spanning all 4 strata of stratified-36
        let todo = vec![
            fp("a/small.gz", 350 * 1024 * 1024),  // <400 stratum
            fp("a/med.gz", 500 * 1024 * 1024),    // 400-600 stratum
            fp("a/large.gz", 700 * 1024 * 1024),  // 600-800 stratum
            fp("a/huge.gz", 1024 * 1024 * 1024),  // 800+ catch-all
            fp("a/small2.gz", 100 * 1024 * 1024), // <400 stratum
        ];
        let plan = build_convert_plan("stratified-36", None, None, Some(36 * 1024), todo, &reg)
            .expect("plan");
        assert!(!plan.flat);
        assert_eq!(plan.strata.len(), 4);
        // largest-files-first: catch-all stratum (1 worker / 13000 MB) comes first
        assert_eq!(plan.strata[0].workers, 1);
        assert_eq!(plan.strata[0].memory_mb, 13_000);
        assert_eq!(plan.strata[0].files.len(), 1);
        assert!(plan.strata[0].files[0].rel.ends_with("huge.gz"));
        // then 2-worker
        assert_eq!(plan.strata[1].workers, 2);
        assert_eq!(plan.strata[1].files.len(), 1);
        assert!(plan.strata[1].files[0].rel.ends_with("large.gz"));
        // then 3-worker
        assert_eq!(plan.strata[2].workers, 3);
        assert_eq!(plan.strata[2].files.len(), 1);
        assert!(plan.strata[2].files[0].rel.ends_with("med.gz"));
        // then 4-worker, with both small files
        assert_eq!(plan.strata[3].workers, 4);
        assert_eq!(plan.strata[3].files.len(), 2);
    }

    #[test]
    fn cli_workers_override_with_stratified_collapses_to_flat_plan() {
        let reg = ProfileRegistry::builtins_only();
        let todo = vec![
            fp("a/small.gz", 350 * 1024 * 1024),
            fp("a/large.gz", 900 * 1024 * 1024),
            fp("a/med.gz", 500 * 1024 * 1024),
        ];
        // --workers 2 collapses stratified plan into 1 flat stratum
        let plan = build_convert_plan("stratified-36", Some(2), None, Some(36 * 1024), todo, &reg)
            .expect("plan");
        assert!(plan.flat);
        assert_eq!(plan.strata.len(), 1);
        assert_eq!(plan.strata[0].workers, 2);
        assert_eq!(plan.strata[0].files.len(), 3);
        // files sorted largest-first
        let sizes: Vec<u64> = plan.strata[0]
            .files
            .iter()
            .map(|f| f.gz_size_bytes)
            .collect();
        assert!(sizes[0] >= sizes[1] && sizes[1] >= sizes[2]);
        // memory_mb defaults to the LARGEST stratum's per_worker (13000 for stratified-36)
        assert_eq!(plan.strata[0].memory_mb, 13_000);
    }

    #[test]
    fn derive_stratified_profile_scales_with_ram() {
        // Baseline: 36 GB matches the empirical schedule.  Workers on the smallest-files
        // strata can be capped below the baseline by the CPU count of the machine
        // running the test (see `STRATIFIED_MAX_WORKERS` and `available_parallelism`).
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(STRATIFIED_MAX_WORKERS)
            .min(STRATIFIED_MAX_WORKERS);
        let p36 = derive_stratified_profile_for_ram(Some(36 * 1024));
        let s36 = p36.strata.unwrap();
        assert_eq!(s36.len(), 4);
        assert_eq!(s36[0].workers, 4_usize.min(cpus));
        assert_eq!(s36[0].per_worker_mb, 4_800);
        // Catch-all is always workers=1 (RAM ratio = 1.0 × 1 worker = 1)
        assert_eq!(s36[3].workers, 1);
        assert_eq!(s36[3].per_worker_mb, 13_000);

        // 16 GB: workers and memory scaled down, system-RAM safety caps applied
        let p16 = derive_stratified_profile_for_ram(Some(16 * 1024));
        let s16 = p16.strata.unwrap();
        // workers can't exceed CPU cap; on 16 GB they're 2/1/1/1
        assert!(s16[0].workers >= 1 && s16[0].workers <= 4);
        // total stratum allocation should never exceed ~55 % of system RAM
        for s in &s16 {
            let cap_mb = if s.workers == 1 {
                (16 * 1024) * 40 / 100
            } else {
                (16 * 1024) * 55 / 100
            };
            assert!(
                s.workers * s.per_worker_mb <= cap_mb,
                "16GB stratum overcommit: workers={} per_worker_mb={} > cap {}",
                s.workers,
                s.per_worker_mb,
                cap_mb
            );
            // floor honoured
            assert!(s.per_worker_mb >= STRATIFIED_MIN_PER_WORKER_MB);
        }

        // 128 GB: workers cap at STRATIFIED_MAX_WORKERS (or CPU count whichever is smaller)
        let p128 = derive_stratified_profile_for_ram(Some(128 * 1024));
        let s128 = p128.strata.unwrap();
        for s in &s128 {
            assert!(s.workers <= STRATIFIED_MAX_WORKERS);
            // 55 % cap still honoured
            let cap_mb = if s.workers == 1 {
                (128 * 1024) * 40 / 100
            } else {
                (128 * 1024) * 55 / 100
            };
            assert!(
                s.workers * s.per_worker_mb <= cap_mb,
                "128GB stratum overcommit"
            );
        }

        // No RAM info: single conservative stratum
        let p_unk = derive_stratified_profile_for_ram(None);
        let s_unk = p_unk.strata.unwrap();
        assert_eq!(s_unk.len(), 1);
        assert_eq!(s_unk[0].workers, 1);
        assert!(s_unk[0].max_file_mb.is_none());
    }

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
