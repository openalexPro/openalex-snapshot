//! Performance-profile types, registry, and convert-plan builder.
//!
//! This module is the self-contained "what should we run and how?" layer.
//! It answers: given a profile name, a list of (input, output) file pairs,
//! and optional overrides, produce a `ConvertPlan` describing how many rayon
//! workers to use, how much DuckDB memory to allocate, and which files belong
//! to each parallel pass (stratum).
//!
//! Nothing here does I/O on the files themselves or calls DuckDB — that lives
//! in the CLI's `run_convert`.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum per-worker DuckDB memory (MB) when scaling a stratified profile.
/// Prevents degenerate 100-MB-per-worker configurations on small hosts.
pub const STRATIFIED_MIN_PER_WORKER_MB: usize = 1280;

/// Hard cap on rayon workers regardless of derived value.  Empirically,
/// workers > 4 showed diminishing returns on I/O-bound workloads; cap at 8
/// to leave headroom for unusually large boxes.
pub const STRATIFIED_MAX_WORKERS: usize = 8;

// ---------------------------------------------------------------------------
// Profile definition types  (user-facing YAML shape)
// ---------------------------------------------------------------------------

/// One "tier" in a stratified profile: files up to `max_file_mb` (or all
/// remaining files when `max_file_mb` is `None`) are run with this many
/// workers and this per-worker memory budget.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Stratum {
    /// Upper bound for this stratum in MB (inclusive).  `None` = catch-all
    /// (must be the last entry in the strata list).
    pub max_file_mb: Option<u64>,
    /// Rayon worker threads for this stratum's parallel pass.  Must be ≥ 1.
    pub workers: usize,
    /// DuckDB `memory_limit` per worker, in MB.  Must be ≥ 256.
    /// The global DuckDB memory limit per stratum is `workers × per_worker_mb`.
    pub per_worker_mb: usize,
}

/// Which execution mode a profile uses.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProfileKind {
    /// Conservative single-pass mode.  Workers and memory derived from system
    /// RAM at runtime.  `strata` must be `None` in the profile definition.
    Safe,
    /// Multi-pass mode partitioned by gz-file size.  `strata` must be present
    /// and non-empty, with exactly one catch-all (last entry, `max_file_mb`
    /// omitted).
    Stratified,
}

/// A complete named profile definition, as found in `performance.yaml` or as
/// a built-in.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfileDef {
    pub kind: ProfileKind,
    pub description: Option<String>,
    /// Recommended minimum system RAM in GB.  A warning is emitted at runtime
    /// if the host has less.  `None` means no guidance.
    pub min_ram_gb: Option<usize>,
    /// Required for `Stratified` profiles; must be `None` for `Safe`.
    pub strata: Option<Vec<Stratum>>,
}

/// The top-level YAML shape of a user-supplied `openalex-snapshot.performance.yaml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfilesYaml {
    pub profiles: BTreeMap<String, ProfileDef>,
}

// ---------------------------------------------------------------------------
// Profile registry
// ---------------------------------------------------------------------------

/// Resolved set of profile definitions visible to the binary.  Built-in
/// profiles (see [`builtin_profiles`]) are always present; entries from a
/// user-supplied `performance.yaml` are merged on top (user wins on name
/// collision).
#[derive(Clone, Debug)]
pub struct ProfileRegistry {
    pub profiles: BTreeMap<String, ProfileDef>,
}

impl ProfileRegistry {
    /// Built-ins only; no YAML loaded.
    pub fn builtins_only() -> Self {
        let profiles = builtin_profiles().into_iter().collect();
        Self { profiles }
    }

    /// Built-ins plus optional user YAML.  A missing path is fine (yields
    /// built-ins only).  An unreadable or invalid YAML returns `Err`.
    pub fn load(performance_config_path: Option<&Path>) -> Result<Self> {
        let mut registry = Self::builtins_only();
        let Some(path) = performance_config_path else {
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

    pub fn get(&self, name: &str) -> Option<&ProfileDef> {
        self.profiles.get(name)
    }

    pub fn names(&self) -> Vec<&str> {
        self.profiles.keys().map(String::as_str).collect()
    }

    /// Build a clear "unknown profile" error that lists what IS available.
    pub fn unknown_profile_error(&self, requested: &str) -> anyhow::Error {
        let mut names: Vec<&str> = self.names();
        names.sort();
        anyhow::anyhow!(
            "unknown profile {:?}. Available: {}",
            requested,
            names.join(", ")
        )
    }
}

// ---------------------------------------------------------------------------
// Built-in profile definitions
// ---------------------------------------------------------------------------

/// The 36 GB empirical baseline strata used by `stratified-36` and as the
/// seed for [`derive_stratified_profile_for_ram`].
pub fn stratified_baseline_36gb_strata() -> Vec<Stratum> {
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

/// All built-in profiles shipped with the binary.
pub fn builtin_profiles() -> Vec<(String, ProfileDef)> {
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

/// Derive a stratified profile scaled linearly from the 36 GB baseline to
/// match the system's actual RAM.
///
/// File-size cutoffs (`max_file_mb`) stay fixed — they reflect the works
/// dataset's compression shape (10–15× expansion).  `workers` and
/// `per_worker_mb` scale with the RAM ratio, floored at
/// [`STRATIFIED_MIN_PER_WORKER_MB`] and capped at [`STRATIFIED_MAX_WORKERS`].
///
/// Used by `config --create-profiles` to emit a starter `performance.yaml`
/// calibrated for the host.  This function is **not** registered as a runtime
/// profile — the user reviews/tunes the YAML and then references it by name.
pub fn derive_stratified_profile_for_ram(total_ram_mb: Option<usize>) -> ProfileDef {
    let cpu_cap = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(STRATIFIED_MAX_WORKERS)
        .min(STRATIFIED_MAX_WORKERS);

    let strata = match total_ram_mb {
        Some(mb) if mb > 0 => {
            let ratio = (mb as f64) / 36_864.0; // 36 GB baseline
                                                // System-RAM safety caps: workers × per_worker_mb ≤ 55% of RAM
                                                // (parallel) or 40% (single-worker catch-all).
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

// ---------------------------------------------------------------------------
// Profile validation
// ---------------------------------------------------------------------------

/// Validate a single profile definition.  Returns a clear error when the
/// shape violates invariants required by [`build_convert_plan`].
pub fn validate_profile_def(name: &str, def: &ProfileDef) -> Result<()> {
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
                    "profile '{name}': must have exactly one stratum with max_file_mb omitted \
                     (catch-all); found {catch_all_count}"
                );
            }
            if strata.last().is_some_and(|s| s.max_file_mb.is_some()) {
                anyhow::bail!(
                    "profile '{name}': the catch-all stratum (max_file_mb omitted) must be the \
                     LAST entry"
                );
            }
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
                                "profile '{name}' stratum {i}: max_file_mb must be strictly \
                                 ascending; got {curr} MB after {p} MB"
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

// ---------------------------------------------------------------------------
// File pair — the unit of work passed to the planner and executor
// ---------------------------------------------------------------------------

/// One (input gz, output parquet) pair, with its size for stratum routing.
#[derive(Debug, Clone)]
pub struct FilePair {
    pub input_gz: PathBuf,
    pub output_parquet: PathBuf,
    /// Path relative to the dataset root (used for logging and resume-skip).
    pub rel: PathBuf,
    /// Size of the source gz in bytes.  Used by [`build_convert_plan`] to
    /// route each file into the right size bucket.
    pub gz_size_bytes: u64,
}

// ---------------------------------------------------------------------------
// Convert plan — the planner output
// ---------------------------------------------------------------------------

/// One stratum in the execution plan: a set of files to convert with a fixed
/// worker count and DuckDB memory budget.
#[derive(Debug, Clone)]
pub struct StratumPlan {
    pub workers: usize,
    /// DuckDB memory limit per worker in MB.  The CLI sets the **global**
    /// DuckDB memory limit to `workers × memory_mb` before starting the
    /// rayon parallel pass.
    pub memory_mb: usize,
    pub files: Vec<FilePair>,
}

/// Full execution plan produced by [`build_convert_plan`].
#[derive(Debug, Clone)]
pub struct ConvertPlan {
    /// Strata in execution order (largest files first under stratified mode).
    pub strata: Vec<StratumPlan>,
    /// `true` when the plan is a single flat parallel pass — either because
    /// the profile is `Safe` or because `--workers N` collapsed a stratified
    /// profile.
    pub flat: bool,
    /// Resolved profile name, used for logging.
    pub profile_name: String,
}

// ---------------------------------------------------------------------------
// Memory-budget helpers  (used by the planner)
// ---------------------------------------------------------------------------

/// Memory budget (MB) for the single-worker `safe` profile: 45% of 80% of
/// total RAM, clamped to [8192, 24576].  The generous cap avoids OOM on large
/// nested-struct records.
pub fn auto_profile_single_worker_safe_memory_mb(total_mb: Option<usize>) -> usize {
    let t = match total_mb {
        Some(v) if v > 0 => v,
        _ => return 8192,
    };
    let usable = (t as f64 * 0.80).floor() as usize;
    let mb = ((usable as f64) * 0.45).floor() as usize;
    mb.clamp(8192, 24_576)
}

/// Memory budget (MB) for the multi-worker `safe` profile path: 15% of 80%
/// of total RAM, clamped to [1024, 8192].
pub fn auto_profile_safe_memory_mb(total_mb: Option<usize>) -> usize {
    let t = match total_mb {
        Some(v) if v > 0 => v,
        _ => return 2048,
    };
    let usable = (t as f64 * 0.80).floor() as usize;
    let mb = ((usable as f64) * 0.15).floor() as usize;
    mb.clamp(1024, 8192)
}

// ---------------------------------------------------------------------------
// System RAM detection
// ---------------------------------------------------------------------------

/// Detect total system RAM in MB.  Returns `None` when the OS query fails or
/// when running on an unsupported platform.
pub fn detect_total_memory_mb() -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("sysctl")
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

// ---------------------------------------------------------------------------
// The planner
// ---------------------------------------------------------------------------

/// Build the execution plan for `run_convert`.
///
/// | Profile kind | `workers_override` | Behaviour |
/// |---|---|---|
/// | `Safe` | any | One stratum; workers clamped to [1, 2]; memory from RAM. |
/// | `Stratified` | `Some(w)` | Collapses to one flat pass with `w` workers. |
/// | `Stratified` | `None` | One stratum per profile stratum (empty ones dropped); files routed by gz size; largest-files-first execution order. |
pub fn build_convert_plan(
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

            // --workers override: collapse into a single flat pass.
            if let Some(w) = workers_override {
                let workers = w.max(1);
                let memory_mb = max_memory_mb_override.unwrap_or(largest_stratum_mb);
                let mut files = todo;
                files.sort_by_key(|p| Reverse(p.gz_size_bytes));
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

            // Partition files by size into per-stratum buckets.
            // Strata are in ascending max_file_mb order; the catch-all is last.
            let mut sorted = todo;
            sorted.sort_by_key(|p| Reverse(p.gz_size_bytes));
            let n_strata = strata_defs.len();
            let mut buckets: Vec<Vec<FilePair>> = (0..n_strata).map(|_| Vec::new()).collect();
            for pair in sorted {
                let size_mb = pair.gz_size_bytes / (1024 * 1024);
                let idx = strata_defs
                    .iter()
                    .position(|s| match s.max_file_mb {
                        Some(cap_mb) => size_mb <= cap_mb,
                        None => true, // catch-all always matches
                    })
                    .expect("catch-all stratum must always match");
                buckets[idx].push(pair);
            }

            // Emit StratumPlan(s) largest-files-first: reverse iteration so the
            // catch-all (biggest files) runs first, surfacing failures early.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(rel: &str, gz_bytes: u64) -> FilePair {
        FilePair {
            input_gz: PathBuf::from(format!("/snap/{rel}.gz")),
            output_parquet: PathBuf::from(format!("/out/{rel}.parquet")),
            rel: PathBuf::from(rel),
            gz_size_bytes: gz_bytes,
        }
    }

    #[test]
    fn safe_profile_single_worker() {
        let registry = ProfileRegistry::builtins_only();
        let files = vec![fp("a", 100_000_000), fp("b", 200_000_000)];
        let plan =
            build_convert_plan("safe", None, Some(8192), None, files.clone(), &registry).unwrap();
        assert!(plan.flat);
        assert_eq!(plan.strata.len(), 1);
        assert_eq!(plan.strata[0].workers, 1);
        assert_eq!(plan.strata[0].memory_mb, 8192);
        assert_eq!(plan.strata[0].files.len(), 2);
    }

    #[test]
    fn stratified_routes_files_into_buckets() {
        let registry = ProfileRegistry::builtins_only();
        // 450 MB file → second stratum (400–600), 100 MB → first stratum (<400)
        let big = fp("big", 450 * 1024 * 1024);
        let small = fp("small", 100 * 1024 * 1024);
        let plan = build_convert_plan(
            "stratified-36",
            None,
            None,
            None,
            vec![big, small],
            &registry,
        )
        .unwrap();
        assert!(!plan.flat);
        // Both non-empty strata should appear; big file in one, small in another.
        let total_files: usize = plan.strata.iter().map(|s| s.files.len()).sum();
        assert_eq!(total_files, 2);
    }

    #[test]
    fn stratified_workers_override_collapses_to_flat() {
        let registry = ProfileRegistry::builtins_only();
        let files = vec![fp("a", 1_000_000_000)];
        let plan =
            build_convert_plan("stratified-36", Some(2), None, None, files, &registry).unwrap();
        assert!(plan.flat);
        assert_eq!(plan.strata.len(), 1);
        assert_eq!(plan.strata[0].workers, 2);
    }

    #[test]
    fn unknown_profile_returns_error() {
        let registry = ProfileRegistry::builtins_only();
        let err =
            build_convert_plan("nonexistent", None, None, None, vec![], &registry).unwrap_err();
        assert!(err.to_string().contains("unknown profile"));
        assert!(err.to_string().contains("safe"));
    }

    #[test]
    fn derive_stratified_profile_scales_with_ram() {
        let profile = derive_stratified_profile_for_ram(Some(16 * 1024));
        assert_eq!(profile.kind, ProfileKind::Stratified);
        let strata = profile.strata.unwrap();
        assert!(!strata.is_empty());
        // Workers should be ≤ STRATIFIED_MAX_WORKERS and ≥ 1.
        for s in &strata {
            assert!(s.workers >= 1);
            assert!(s.workers <= STRATIFIED_MAX_WORKERS);
        }
    }

    #[test]
    fn validate_rejects_stratified_without_catch_all() {
        let def = ProfileDef {
            kind: ProfileKind::Stratified,
            description: None,
            min_ram_gb: None,
            strata: Some(vec![Stratum {
                max_file_mb: Some(400),
                workers: 2,
                per_worker_mb: 4096,
            }]),
        };
        let err = validate_profile_def("bad", &def).unwrap_err();
        assert!(err.to_string().contains("catch-all"));
    }
}
