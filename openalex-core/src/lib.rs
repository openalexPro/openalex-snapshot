//! Shared core for openalex-snapshot.
//!
//! This crate is the home of logic that needs to be callable from both the
//! `openalex-snapshot` CLI and the `openalexPro` R package (via `extendr`).
//!
//! # Modules
//!
//! - [`profile`] — performance-profile types, registry, and convert-plan builder
//! - [`sql`] — SQL string utilities (`normalize_duckdb_type`, `sql_quote`, `parse_size_str`)
//!
//! # Re-exports
//!
//! The two works-enrichment SQL helpers are re-exported at the crate root for
//! backwards compatibility and convenience.

pub mod profile;
pub mod sql;

#[cfg(feature = "conversion")]
pub mod conversion;

// Convenience re-exports — callers can `use openalex_core::{works_abstract_expr, …}`.
pub use sql::{normalize_duckdb_type, parse_size_str, sql_quote};

/// SQL expression that reconstructs a plain-text abstract from a
/// `MAP(VARCHAR, BIGINT[])` named `abstract_inverted_index`.  Walks the map,
/// emits one (pos, word) per position, sorts ascending by position, joins
/// words with single spaces.  Returns NULL when the source is NULL.
///
/// `list_sort` on a STRUCT sorts by field order (pos:BIGINT first, ascending)
/// which gives the correct word ordering.
pub fn works_abstract_expr() -> &'static str {
    "CASE WHEN abstract_inverted_index IS NULL THEN NULL \
     ELSE array_to_string( \
         list_transform( \
             list_sort( \
                 flatten( \
                     apply( \
                         map_entries(abstract_inverted_index), \
                         x -> apply(x.value, p -> {pos: p, word: x.key}) \
                     ) \
                 ) \
             ), \
             e -> e.word \
         ), \
         ' ' \
     ) END"
}

/// SQL expression that builds a `"Author (year)"` / `"A & B (year)"` /
/// `"A et al. (year)"` citation from `authorships` and `publication_year`.
/// Null year ⇒ `"(n.d.)"`.  Null or empty `authorships` ⇒ NULL.
///
/// Mirrors the jq filter in `openalexPro/R/jq_execute.R`.
pub fn works_citation_expr() -> String {
    let year_expr = "COALESCE(publication_year::VARCHAR, 'n.d.')";
    format!(
        "CASE \
            WHEN authorships IS NULL OR len(authorships) = 0 THEN NULL \
            WHEN len(authorships) = 1 THEN \
                authorships[1].author.display_name || ' (' || {year_expr} || ')' \
            WHEN len(authorships) = 2 THEN \
                authorships[1].author.display_name || ' & ' || authorships[2].author.display_name \
                || ' (' || {year_expr} || ')' \
            ELSE \
                authorships[1].author.display_name || ' et al. (' || {year_expr} || ')' \
        END"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abstract_expr_is_non_empty() {
        let expr = works_abstract_expr();
        assert!(expr.contains("abstract_inverted_index"));
        assert!(expr.contains("array_to_string"));
    }

    #[test]
    fn citation_expr_is_non_empty() {
        let expr = works_citation_expr();
        assert!(expr.contains("authorships"));
        assert!(expr.contains("display_name"));
        assert!(expr.contains("n.d."));
    }
}
