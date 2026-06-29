//! Deriving the `abstract` and `citation` columns for works.
//!
//! This is the heart of what `openalexPro` needs from the corpus: a plain-text
//! abstract reconstructed from `abstract_inverted_index` (a JSON string in the
//! official release) and a `"Author (year)"` citation built from the nested
//! `authorships` struct. [`enrich_one`] copies every source column through and
//! appends these two, so the CLI and R produce byte-identical enriched works.

use crate::parquetio::{snappy_writer_props, string_col_values};
use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Parse an `abstract_inverted_index` JSON object into `(word, [positions])`
/// pairs, **preserving duplicate keys** (OpenAlex emits a word more than once
/// when it recurs). A `HashMap` would silently collapse them, so this walks the
/// JSON map directly.
pub fn parse_inverted_index(json: &str) -> Option<Vec<(String, Vec<i64>)>> {
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

/// Reconstruct the plain-text abstract from an `abstract_inverted_index` JSON
/// string. Words are ordered by `(position, word)` — matching the reference
/// `list_sort` over `{pos, word}` structs. Returns `None` if the JSON does not
/// parse; an empty index yields `Some("")`.
pub fn reconstruct_abstract(json: &str) -> Option<String> {
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
    // Sort by (pos, word) to match list_sort over {pos, word} structs.
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
pub fn author_display_name(authors_struct: &StructArray, idx: usize) -> Option<String> {
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
/// and `publication_year` (Int32): "A (yr)" / "A & B (yr)" / "A et al. (yr)"; null year →
/// "n.d."; null/empty authorships or null first author → null.
pub fn build_citation_array(
    authorships: &ArrayRef,
    years: &ArrayRef,
    n: usize,
) -> Result<StringArray> {
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
pub fn enrich_one(src: &Path, out: &Path, add_abstract: bool, add_citation: bool) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstruct_orders_by_position() {
        let json = r#"{"world":[1],"Hello":[0]}"#;
        assert_eq!(reconstruct_abstract(json).as_deref(), Some("Hello world"));
    }

    #[test]
    fn reconstruct_preserves_duplicate_words() {
        // "the" appears at positions 0 and 2 — both must survive.
        let json = r#"{"the":[0,2],"big":[1],"dog":[3]}"#;
        assert_eq!(
            reconstruct_abstract(json).as_deref(),
            Some("the big the dog")
        );
    }

    #[test]
    fn reconstruct_empty_index_is_empty_string() {
        assert_eq!(reconstruct_abstract("{}").as_deref(), Some(""));
    }

    #[test]
    fn reconstruct_handles_escaped_key() {
        // An escaped quote in a key must not drop the abstract (owned-String keys).
        assert_eq!(
            reconstruct_abstract(r#"{"a\"b":[0]}"#).as_deref(),
            Some("a\"b")
        );
    }

    #[test]
    fn reconstruct_bad_json_is_none() {
        assert_eq!(reconstruct_abstract("not json"), None);
    }

    #[test]
    fn parse_keeps_repeated_keys() {
        // A genuinely repeated key in the JSON text must not collapse.
        let pairs = parse_inverted_index(r#"{"a":[0],"a":[5]}"#).unwrap();
        assert_eq!(pairs.len(), 2);
    }
}
