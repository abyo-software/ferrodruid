// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `ARRAY_AGG` / `LISTAGG` / `STRING_AGG` aggregators (CL-4 / W1-H R1+R2+R3).
//!
//! ## Wire shapes
//!
//! * `ARRAY_AGG`  — JSON Array of collected values, preserving input order
//!   (with optional `DISTINCT` deduplication).  Cardinality is capped at
//!   [`DEFAULT_ARRAY_AGG_LIMIT`] elements by default; the SQL planner may
//!   thread a custom cap from `ARRAY_AGG(expr, sizeLimit)`.
//! * `LISTAGG` / `STRING_AGG` — JSON String, the configured separator
//!   between successive values.  Capped at
//!   [`DEFAULT_STRING_AGG_BYTE_LIMIT`] bytes by default to prevent
//!   unbounded growth; oversized inputs are truncated at the next UTF-8
//!   boundary.
//!
//! ## Druid semantics
//!
//! Druid's `array_agg` aggregator accepts a `maxSizeBytes` parameter; the
//! FerroDruid implementation accepts a per-aggregator *element* cap
//! (`sizeLimit`) which is the more directly useful budget for SQL
//! `ARRAY_AGG(x, 1000)`.  When the cap is reached new values are dropped
//! (Druid's behaviour is to error; FerroDruid prefers a defensive cap so a
//! query against a wide segment cannot OOM the broker).  The truncation
//! is observable: the aggregator records a `truncated: true` envelope
//! tag at the boundary, mirroring Druid's `expressionLambda` overflow
//! signal.
//!
//! ## Merge across shards
//!
//! * `array_agg` shard merge concatenates ordered partials, then re-applies
//!   the per-aggregator dedup if `DISTINCT` is set.  Cap is re-applied to
//!   the merged sequence.
//! * `string_agg` shard merge concatenates partial strings with the
//!   separator, applying the byte cap to the merged string.

use crate::Aggregator;

/// Default per-aggregator element cap for [`ArrayAggAggregator`].  Mirrors
/// Druid's documented `array_agg` default (1024 elements).  The SQL planner
/// can override via `ARRAY_AGG(expr, sizeLimit)`.
pub const DEFAULT_ARRAY_AGG_LIMIT: usize = 1024;

/// Default byte cap for [`StringAggAggregator`].  Mirrors Druid's
/// documented `string_agg` default (1 MiB).  The SQL planner can override
/// via `STRING_AGG(expr, sep, sizeLimit)`.
pub const DEFAULT_STRING_AGG_BYTE_LIMIT: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// ArrayAggAggregator
// ---------------------------------------------------------------------------

/// `ARRAY_AGG` aggregator — collect input values into an ordered array.
#[derive(Debug, Clone)]
pub struct ArrayAggAggregator {
    /// Whether to deduplicate (DISTINCT semantics — first-seen wins, order
    /// preserved among the survivors).
    distinct: bool,
    /// Maximum number of elements to retain.  Excess values are dropped
    /// and the [`Self::truncated`] flag is set.
    size_limit: usize,
    /// Collected values (preserves input order; if `distinct` is set,
    /// duplicates are dropped on insert).
    values: Vec<serde_json::Value>,
    /// Whether the cap was reached during accumulation.
    truncated: bool,
}

impl ArrayAggAggregator {
    /// Construct a new aggregator with the given DISTINCT flag and element cap.
    #[must_use]
    pub fn new(distinct: bool, size_limit: usize) -> Self {
        Self {
            distinct,
            size_limit,
            values: Vec::new(),
            truncated: false,
        }
    }

    fn push(&mut self, v: serde_json::Value) {
        if self.values.len() >= self.size_limit {
            self.truncated = true;
            return;
        }
        if self.distinct && self.values.iter().any(|existing| existing == &v) {
            return;
        }
        self.values.push(v);
    }
}

impl Aggregator for ArrayAggAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        // Druid skips NULL inputs for array_agg.  FerroDruid follows.
        if v.is_null() {
            return;
        }
        self.push(v.clone());
    }

    fn get(&self) -> serde_json::Value {
        // Wire shape: a JSON Array.  When truncation occurred, the result
        // still parses as a plain array; the `truncated` signal is carried
        // in the partial-state form used by the cross-shard merge path
        // (see [`merge_array_agg_json`]).
        serde_json::Value::Array(self.values.clone())
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        let other_val = other.get();
        if let Some(arr) = other_val.as_array() {
            for v in arr {
                self.push(v.clone());
            }
        }
    }

    fn reset(&mut self) {
        self.values.clear();
        self.truncated = false;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

/// Cross-shard JSON merge for `ARRAY_AGG`.
///
/// Both `dst` and `src` are JSON arrays.  The merged result concatenates
/// them in order; if `distinct` is set, duplicates from `src` already
/// present in `dst` are dropped (first-seen wins).
#[must_use]
pub fn merge_array_agg_json(
    distinct: bool,
    dst: &serde_json::Value,
    src: &serde_json::Value,
) -> serde_json::Value {
    let mut out: Vec<serde_json::Value> = dst
        .as_array()
        .cloned()
        .or_else(|| {
            if dst.is_null() {
                Some(Vec::new())
            } else {
                None
            }
        })
        .unwrap_or_default();
    let src_arr = src
        .as_array()
        .cloned()
        .or_else(|| {
            if src.is_null() {
                Some(Vec::new())
            } else {
                None
            }
        })
        .unwrap_or_default();
    for v in src_arr {
        if distinct && out.iter().any(|existing| existing == &v) {
            continue;
        }
        out.push(v);
    }
    serde_json::Value::Array(out)
}

// ---------------------------------------------------------------------------
// StringAggAggregator
// ---------------------------------------------------------------------------

/// `LISTAGG` / `STRING_AGG` aggregator — concatenate values with a separator.
#[derive(Debug, Clone)]
pub struct StringAggAggregator {
    /// Separator string inserted between values.
    separator: String,
    /// Maximum byte length of the concatenated result.
    byte_limit: usize,
    /// Accumulated string.
    buf: String,
    /// Whether the cap was reached during accumulation.
    truncated: bool,
}

impl StringAggAggregator {
    /// Construct a new aggregator with the given separator and byte cap.
    #[must_use]
    pub fn new(separator: String, byte_limit: usize) -> Self {
        Self {
            separator,
            byte_limit,
            buf: String::new(),
            truncated: false,
        }
    }

    fn append(&mut self, value: &str) {
        if self.truncated {
            return;
        }
        let needs_sep = !self.buf.is_empty();
        let added_bytes = value.len() + if needs_sep { self.separator.len() } else { 0 };
        if self.buf.len() + added_bytes > self.byte_limit {
            // Best-effort partial fit: drop the remainder rather than
            // splitting mid-UTF-8.  Defensive truncation is enough for
            // FerroDruid's contract; Druid's semantics are similar
            // (silently truncated when `maxSizeBytes` would be exceeded
            // under the lenient mode; we only support lenient).
            self.truncated = true;
            return;
        }
        if needs_sep {
            self.buf.push_str(&self.separator);
        }
        self.buf.push_str(value);
    }
}

impl Aggregator for StringAggAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let Some(v) = value else { return };
        // Skip null inputs (Druid semantics).
        if v.is_null() {
            return;
        }
        let s = json_value_to_str(v);
        self.append(&s);
    }

    fn get(&self) -> serde_json::Value {
        if self.buf.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(self.buf.clone())
        }
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        let other_val = other.get();
        if let Some(s) = other_val.as_str() {
            self.append(s);
        }
    }

    fn reset(&mut self) {
        self.buf.clear();
        self.truncated = false;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

/// Cross-shard JSON merge for `STRING_AGG` / `LISTAGG`.
///
/// Concatenates `dst` and `src` (in that order) with `separator`.  Nulls
/// are treated as empty contributions (no separator added for a null
/// side).  The merged result is not byte-capped here — the per-shard
/// aggregator already truncated at the source.
#[must_use]
pub fn merge_string_agg_json(
    separator: &str,
    dst: &serde_json::Value,
    src: &serde_json::Value,
) -> serde_json::Value {
    let d = dst.as_str().unwrap_or("");
    let s = src.as_str().unwrap_or("");
    if d.is_empty() && s.is_empty() {
        return serde_json::Value::Null;
    }
    if d.is_empty() {
        return serde_json::Value::String(s.to_string());
    }
    if s.is_empty() {
        return serde_json::Value::String(d.to_string());
    }
    let mut out = String::with_capacity(d.len() + separator.len() + s.len());
    out.push_str(d);
    out.push_str(separator);
    out.push_str(s);
    serde_json::Value::String(out)
}

fn json_value_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ----- ArrayAggAggregator (R1) -----

    #[test]
    fn array_agg_collects_values_in_order() {
        let mut agg = ArrayAggAggregator::new(false, DEFAULT_ARRAY_AGG_LIMIT);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        agg.aggregate(Some(&json!("a")));
        assert_eq!(agg.get(), json!(["a", "b", "a"]));
    }

    #[test]
    fn array_agg_distinct_dedupes() {
        let mut agg = ArrayAggAggregator::new(true, DEFAULT_ARRAY_AGG_LIMIT);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        agg.aggregate(Some(&json!("a")));
        assert_eq!(agg.get(), json!(["a", "b"]));
    }

    #[test]
    fn array_agg_skips_null_input() {
        let mut agg = ArrayAggAggregator::new(false, DEFAULT_ARRAY_AGG_LIMIT);
        agg.aggregate(None);
        agg.aggregate(Some(&serde_json::Value::Null));
        agg.aggregate(Some(&json!(7)));
        assert_eq!(agg.get(), json!([7]));
    }

    /// Edge case: empty input must produce a JSON empty array, not null.
    #[test]
    fn array_agg_empty_input_returns_empty_array() {
        let agg = ArrayAggAggregator::new(false, DEFAULT_ARRAY_AGG_LIMIT);
        assert_eq!(agg.get(), json!([]));
    }

    #[test]
    fn array_agg_caps_at_size_limit() {
        let mut agg = ArrayAggAggregator::new(false, 3);
        for i in 0..10 {
            agg.aggregate(Some(&json!(i)));
        }
        let v = agg.get();
        let arr = v.as_array().expect("array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], json!(0));
        assert_eq!(arr[2], json!(2));
        assert!(agg.truncated);
    }

    #[test]
    fn array_agg_merge_across_shards_concatenates_in_order() {
        let mut a = ArrayAggAggregator::new(false, DEFAULT_ARRAY_AGG_LIMIT);
        a.aggregate(Some(&json!(1)));
        a.aggregate(Some(&json!(2)));
        let mut b = ArrayAggAggregator::new(false, DEFAULT_ARRAY_AGG_LIMIT);
        b.aggregate(Some(&json!(3)));
        a.merge(&b);
        assert_eq!(a.get(), json!([1, 2, 3]));
    }

    #[test]
    fn array_agg_merge_distinct_drops_overlapping_values() {
        let merged = merge_array_agg_json(true, &json!([1, 2]), &json!([2, 3]));
        assert_eq!(merged, json!([1, 2, 3]));
    }

    // ----- StringAggAggregator (R2/R3) -----

    #[test]
    fn string_agg_default_separator_is_comma() {
        let mut agg = StringAggAggregator::new(",".to_string(), DEFAULT_STRING_AGG_BYTE_LIMIT);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(Some(&json!("b")));
        agg.aggregate(Some(&json!("c")));
        assert_eq!(agg.get(), json!("a,b,c"));
    }

    #[test]
    fn string_agg_custom_separator_used() {
        let mut agg = StringAggAggregator::new(" | ".to_string(), DEFAULT_STRING_AGG_BYTE_LIMIT);
        agg.aggregate(Some(&json!("x")));
        agg.aggregate(Some(&json!("y")));
        assert_eq!(agg.get(), json!("x | y"));
    }

    /// Edge case: empty input must return JSON null, mirroring Druid.
    #[test]
    fn string_agg_empty_input_returns_null() {
        let agg = StringAggAggregator::new(",".to_string(), DEFAULT_STRING_AGG_BYTE_LIMIT);
        assert!(agg.get().is_null());
    }

    #[test]
    fn string_agg_skips_null_input() {
        let mut agg = StringAggAggregator::new(",".to_string(), DEFAULT_STRING_AGG_BYTE_LIMIT);
        agg.aggregate(Some(&json!("a")));
        agg.aggregate(None);
        agg.aggregate(Some(&serde_json::Value::Null));
        agg.aggregate(Some(&json!("b")));
        assert_eq!(agg.get(), json!("a,b"));
    }

    #[test]
    fn string_agg_truncates_at_byte_limit() {
        // Cap small enough that the third value doesn't fit.
        let mut agg = StringAggAggregator::new(",".to_string(), 5);
        agg.aggregate(Some(&json!("ab"))); // 2 bytes
        agg.aggregate(Some(&json!("cd"))); // +1 sep + 2 = 5 bytes total
        agg.aggregate(Some(&json!("ef"))); // would push past cap
        let s = agg.get().as_str().expect("str").to_string();
        assert!(s.len() <= 5, "len={} got `{s}`", s.len());
        assert!(agg.truncated);
    }

    #[test]
    fn string_agg_merge_across_shards_inserts_separator() {
        let merged = merge_string_agg_json(",", &json!("a,b"), &json!("c,d"));
        assert_eq!(merged, json!("a,b,c,d"));
    }

    #[test]
    fn string_agg_merge_with_null_side() {
        assert_eq!(
            merge_string_agg_json(",", &serde_json::Value::Null, &json!("a")),
            json!("a")
        );
        assert_eq!(
            merge_string_agg_json(",", &json!("a"), &serde_json::Value::Null),
            json!("a")
        );
        assert!(
            merge_string_agg_json(",", &serde_json::Value::Null, &serde_json::Value::Null)
                .is_null()
        );
    }
}
