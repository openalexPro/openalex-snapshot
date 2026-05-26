//! SQL string utilities used by both the CLI and future R-package bindings.

use anyhow::{bail, Result};

/// Canonicalise a DuckDB type string: uppercase known SQL type keywords while
/// preserving the original case of identifier tokens (struct field names, etc.).
///
/// DuckDB's `read_json(columns = …)` matches JSON keys to STRUCT field names
/// **case-sensitively**, so naively uppercasing the entire type string turns
/// `STRUCT(author STRUCT(display_name VARCHAR))` into something that silently
/// fills every nested field with NULL when reading lowercase JSON keys.
///
/// This function walks the string character-by-character, collecting
/// alphanumeric/underscore runs into candidate words and uppercasing them only
/// when the uppercase form matches a known type keyword.  Non-word characters
/// (parens, commas, spaces) are passed through unchanged.
pub fn normalize_duckdb_type(t: &str) -> String {
    static KEYWORDS: &[&str] = &[
        "BOOLEAN",
        "BOOL",
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "INT",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DECIMAL",
        "VARCHAR",
        "TEXT",
        "CHAR",
        "STRING",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "INTERVAL",
        "BLOB",
        "BYTEA",
        "UUID",
        "STRUCT",
        "LIST",
        "MAP",
        "ARRAY",
        "BIT",
        "JSON",
        "NULL",
        "ENUM",
    ];
    let trimmed = t.trim();
    let mut out = String::with_capacity(trimmed.len());
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String| {
        if !word.is_empty() {
            let upper = word.to_ascii_uppercase();
            if KEYWORDS.contains(&upper.as_str()) {
                out.push_str(&upper);
            } else {
                out.push_str(word);
            }
            word.clear();
        }
    };
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
        } else {
            flush(&mut word, &mut out);
            out.push(ch);
        }
    }
    flush(&mut word, &mut out);
    out
}

/// Wrap a string in SQL single-quotes, escaping backslashes and embedded quotes.
pub fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// Parse a human-readable size string to bytes.
///
/// - `"0"` → 0 (sentinel for "auto/disabled")
/// - Supports `kb`/`mb`/`gb` (SI, powers of 1000) and
///   `kib`/`mib`/`gib` (binary, powers of 1024), case-insensitive.
/// - Plain integer strings are interpreted as bytes.
pub fn parse_size_str(s: &str) -> Result<usize> {
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
    let n: f64 = num_s
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid size: {:?}", s))?;
    if n < 0.0 {
        bail!("size must be non-negative: {:?}", s);
    }
    Ok((n * mult as f64).round() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_preserves_field_name_case() {
        let t = "STRUCT(author STRUCT(display_name VARCHAR))";
        let result = normalize_duckdb_type(t);
        assert!(
            result.contains("author"),
            "field name 'author' should be preserved"
        );
        assert!(
            result.contains("display_name"),
            "field name 'display_name' should be preserved"
        );
        assert!(
            result.contains("VARCHAR"),
            "type keyword should be uppercased"
        );
    }

    #[test]
    fn normalize_uppercases_keywords() {
        assert_eq!(normalize_duckdb_type("bigint"), "BIGINT");
        assert_eq!(normalize_duckdb_type("varchar"), "VARCHAR");
        assert_eq!(normalize_duckdb_type("struct"), "STRUCT");
    }

    #[test]
    fn sql_quote_escapes_quotes_and_backslashes() {
        assert_eq!(sql_quote("it's"), "'it''s'");
        assert_eq!(sql_quote(r"C:\path"), r"'C:\\path'");
        assert_eq!(sql_quote("simple"), "'simple'");
    }

    #[test]
    fn parse_size_str_units() {
        assert_eq!(parse_size_str("0").unwrap(), 0);
        assert_eq!(parse_size_str("512mb").unwrap(), 512_000_000);
        assert_eq!(parse_size_str("1gib").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size_str("2048").unwrap(), 2048);
    }
}
