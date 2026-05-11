use std::process::Command;

fn main() {
    // Git commit hash (short), falling back gracefully if git is unavailable.
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Dirty flag: append '+dirty' when the working tree has uncommitted changes.
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);

    let git_ref = if dirty {
        format!("{git_hash}+dirty")
    } else {
        git_hash
    };

    // Build date (UTC, YYYY-MM-DD).
    let build_date = {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let days = now / 86400;
        let (y, m, d) = days_to_ymd(days);
        format!("{y:04}-{m:02}-{d:02}")
    };

    println!("cargo:rustc-env=BUILD_GIT_HASH={git_ref}");
    println!("cargo:rustc-env=BUILD_DATE={build_date}");

    // Re-run if HEAD or index changes.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}

/// Convert days-since-epoch to (year, month, day).
fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut year = 1970u64;
    loop {
        let leap = is_leap(year);
        let yd = if leap { 366 } else { 365 };
        if days < yd {
            break;
        }
        days -= yd;
        year += 1;
    }
    let months = [31u64, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 1u64;
    for &ml in &months {
        let ml = if month == 2 && is_leap(year) { 29 } else { ml };
        if days < ml {
            break;
        }
        days -= ml;
        month += 1;
    }
    (year, month, days + 1)
}

fn is_leap(y: u64) -> bool {
    #[allow(clippy::manual_is_multiple_of)]
    let div4 = y % 4 == 0;
    #[allow(clippy::manual_is_multiple_of)]
    let div100 = y % 100 == 0;
    #[allow(clippy::manual_is_multiple_of)]
    let div400 = y % 400 == 0;
    (div4 && !div100) || div400
}
