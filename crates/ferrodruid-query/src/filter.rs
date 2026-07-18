// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Filter specification parsing and evaluation for Druid Native Queries.
//!
//! Filters narrow the set of rows that a query operates on.  The [`FilterSpec`]
//! enum covers all filter types defined by the Druid Native Query API.

use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::SearchQuerySpec;
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;

use crate::helpers::deserialize_intervals;

// ---------------------------------------------------------------------------
// FilterSpec
// ---------------------------------------------------------------------------

/// A Druid filter specification.
///
/// Filters are evaluated per-row against a map of column name to JSON value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum FilterSpec {
    /// Matches rows where a dimension equals the given value.
    #[serde(rename = "selector")]
    Selector {
        /// The dimension to match against.
        dimension: String,
        /// The value to match (null means match null/missing).
        value: Option<serde_json::Value>,
    },
    /// Matches rows where a dimension is one of the given values.
    #[serde(rename = "in")]
    In {
        /// The dimension to match against.
        dimension: String,
        /// The set of acceptable values.
        values: Vec<serde_json::Value>,
    },
    /// Matches rows where a dimension falls within a bound.
    #[serde(rename = "bound")]
    Bound {
        /// The dimension to match against.
        dimension: String,
        /// Lower bound value (inclusive by default).
        #[serde(default)]
        lower: Option<String>,
        /// Upper bound value (inclusive by default).
        #[serde(default)]
        upper: Option<String>,
        /// If true, the lower bound is exclusive.
        #[serde(default, rename = "lowerStrict")]
        lower_strict: Option<bool>,
        /// If true, the upper bound is exclusive.
        #[serde(default, rename = "upperStrict")]
        upper_strict: Option<bool>,
        /// Ordering type: "lexicographic", "alphanumeric", "numeric", or "strlen".
        #[serde(default)]
        ordering: Option<String>,
    },
    /// Matches rows where a column falls within a typed range.
    #[serde(rename = "range")]
    Range {
        /// The column to match against.
        column: String,
        /// The value type for comparison (e.g. "LONG", "DOUBLE", "STRING").
        #[serde(rename = "matchValueType")]
        match_value_type: String,
        /// Lower bound value.
        #[serde(default)]
        lower: Option<serde_json::Value>,
        /// Upper bound value.
        #[serde(default)]
        upper: Option<serde_json::Value>,
        /// If true, the lower bound is exclusive.
        #[serde(default, rename = "lowerOpen")]
        lower_open: Option<bool>,
        /// If true, the upper bound is exclusive.
        #[serde(default, rename = "upperOpen")]
        upper_open: Option<bool>,
    },
    /// Matches rows where a dimension matches a SQL LIKE pattern.
    #[serde(rename = "like")]
    Like {
        /// The dimension to match against.
        dimension: String,
        /// The LIKE pattern (% and _ are wildcards).
        pattern: String,
        /// Optional escape character for literal % and _.
        #[serde(default)]
        escape: Option<String>,
    },
    /// Matches rows where a dimension matches a regular expression.
    #[serde(rename = "regex")]
    Regex {
        /// The dimension to match against.
        dimension: String,
        /// The regular expression pattern.
        pattern: String,
    },
    /// Matches rows where a dimension matches a search query.
    #[serde(rename = "search")]
    Search {
        /// The dimension to match against.
        dimension: String,
        /// The search query specification.
        query: SearchQuerySpec,
    },
    /// Logical AND of multiple filters.
    #[serde(rename = "and")]
    And {
        /// The child filters (all must match).
        fields: Vec<FilterSpec>,
    },
    /// Logical OR of multiple filters.
    #[serde(rename = "or")]
    Or {
        /// The child filters (at least one must match).
        fields: Vec<FilterSpec>,
    },
    /// Logical NOT of a filter.
    #[serde(rename = "not")]
    Not {
        /// The child filter to negate.
        field: Box<FilterSpec>,
    },
    /// Matches rows where a dimension's timestamp falls within one of the given intervals.
    #[serde(rename = "interval")]
    Interval {
        /// The dimension to match against (typically `__time`).
        dimension: String,
        /// ISO-8601 intervals (e.g. `"2024-01-01/2024-02-01"`).
        ///
        /// Accepts both a single ISO `"start/end"` string and an array of
        /// such strings (TG-4-finding-001, W2-D pydruid/druid-go compat).
        #[serde(deserialize_with = "deserialize_intervals")]
        intervals: Vec<String>,
    },
    /// Matches rows using an expression (not yet evaluated — always matches).
    #[serde(rename = "expression")]
    Expression {
        /// The expression string.
        expression: String,
    },
    /// Matches rows where two (or more) named columns hold equal values.
    ///
    /// Druid's `columnComparison` filter compares the values of the listed
    /// `dimensions` against one another (using the same numeric/string
    /// coercion as [`Self::Selector`]) and matches when every listed column
    /// holds the same value in the row.  An empty or single-element list
    /// trivially matches (there is nothing to disagree).
    #[serde(rename = "columnComparison")]
    ColumnComparison {
        /// The columns whose values must all be equal for the row to match.
        dimensions: Vec<String>,
    },
    /// Always matches.
    #[serde(rename = "true")]
    True,
    /// Never matches.
    #[serde(rename = "false")]
    False,
    /// Matches rows where a column is null.
    #[serde(rename = "null")]
    Null {
        /// The column to check for null.
        column: String,
    },
    /// `BLOOM_FILTER_TEST(col, base64)` — probe `dimension` against a
    /// serialized bloom filter (CL-4 / W1-H R4).  Matches when the
    /// dimension value is reported as (probably) present by the decoded
    /// FerroDruid bloom filter envelope.  Strict byte-eq with Apache
    /// Hive `BloomKFilter` is a documented residual; FerroDruid
    /// round-trip with `BLOOM_FILTER` aggregator is guaranteed.
    #[serde(rename = "bloomFilter")]
    BloomFilter {
        /// The dimension to probe.
        dimension: String,
        /// Base64-encoded FerroDruid bloom filter envelope.
        #[serde(rename = "base64Filter")]
        base64_filter: String,
    },
    /// `MV_FILTER_ONLY(col, ARRAY[v1, v2, ...])` — keep only rows where
    /// the multi-value column has at least one value listed in `values`
    /// (CL-4 / W1-H R5).  Null / empty multi-values do not match.
    #[serde(rename = "mvFilterOnly")]
    MvFilterOnly {
        /// The multi-value column.
        dimension: String,
        /// Values to keep.
        values: Vec<serde_json::Value>,
    },
    /// `MV_FILTER_NONE(col, ARRAY[v1, v2, ...])` — keep only rows where
    /// the multi-value column has NO value listed in `values`
    /// (CL-4 / W1-H R5).  Null / empty multi-values *do* match (no
    /// banned value present).
    #[serde(rename = "mvFilterNone")]
    MvFilterNone {
        /// The multi-value column.
        dimension: String,
        /// Values to drop.
        values: Vec<serde_json::Value>,
    },
}

impl FilterSpec {
    /// Evaluate this filter against a single row represented as a column-name-to-value map.
    ///
    /// Returns `true` if the row matches the filter.
    pub fn matches(&self, row: &HashMap<String, serde_json::Value>) -> bool {
        match self {
            Self::Selector { dimension, value } => {
                let row_val = row.get(dimension);
                match (row_val, value) {
                    (None, None) | (Some(serde_json::Value::Null), None) => true,
                    (None, Some(_)) | (Some(serde_json::Value::Null), Some(_)) => {
                        value.as_ref().is_some_and(|v| v.is_null())
                    }
                    // A multi-value row (JSON array) with elements is never
                    // null; an EMPTY MV row matches the null selector
                    // (Druid `[]`/null equivalence — defensive: empty MV
                    // rows normally materialise as JSON null already).
                    (Some(serde_json::Value::Array(a)), None) => a.is_empty(),
                    (Some(rv), None) => rv.is_null(),
                    // compat-11: an MV row matches when ANY element equals
                    // the filter value (Druid selector-on-MV semantics);
                    // scalar rows keep the unchanged comparison.
                    (Some(rv), Some(fv)) => row_value_equals(rv, fv),
                }
            }

            Self::In { dimension, values } => {
                let row_val = row.get(dimension);
                match row_val {
                    None | Some(serde_json::Value::Null) => values.iter().any(|v| v.is_null()),
                    // Defensive `[]`-as-null (see the Selector arm).
                    Some(serde_json::Value::Array(a)) if a.is_empty() => {
                        values.iter().any(|v| v.is_null())
                    }
                    // compat-11: an MV row matches when ANY element is in
                    // the set (Druid IN-on-MV semantics).
                    Some(rv) => values.iter().any(|v| row_value_equals(rv, v)),
                }
            }

            Self::Bound {
                dimension,
                lower,
                upper,
                lower_strict,
                upper_strict,
                ordering,
            } => {
                // compat-11: an MV row (JSON array) matches when ANY element
                // satisfies the bound; scalar rows contribute exactly one
                // candidate, preserving the unchanged single-value logic.
                let candidates = row_value_strings(row.get(dimension));
                if candidates.is_empty() {
                    return false;
                }
                let is_numeric = ordering.as_deref().is_some_and(|o| o == "numeric");
                let matches_one = |row_str: &str| -> bool {
                    if is_numeric {
                        let rv: f64 = match row_str.parse() {
                            Ok(v) => v,
                            Err(_) => return false,
                        };
                        if let Some(lo) = lower {
                            let lo_f: f64 = match lo.parse() {
                                Ok(v) => v,
                                Err(_) => return false,
                            };
                            if lower_strict.unwrap_or(false) {
                                if rv <= lo_f {
                                    return false;
                                }
                            } else if rv < lo_f {
                                return false;
                            }
                        }
                        if let Some(hi) = upper {
                            let hi_f: f64 = match hi.parse() {
                                Ok(v) => v,
                                Err(_) => return false,
                            };
                            if upper_strict.unwrap_or(false) {
                                if rv >= hi_f {
                                    return false;
                                }
                            } else if rv > hi_f {
                                return false;
                            }
                        }
                        true
                    } else {
                        // lexicographic ordering
                        if let Some(lo) = lower {
                            if lower_strict.unwrap_or(false) {
                                if row_str <= lo.as_str() {
                                    return false;
                                }
                            } else if row_str < lo.as_str() {
                                return false;
                            }
                        }
                        if let Some(hi) = upper {
                            if upper_strict.unwrap_or(false) {
                                if row_str >= hi.as_str() {
                                    return false;
                                }
                            } else if row_str > hi.as_str() {
                                return false;
                            }
                        }
                        true
                    }
                };
                candidates.iter().any(|s| matches_one(s))
            }

            Self::Range {
                column,
                match_value_type,
                lower,
                upper,
                lower_open,
                upper_open,
            } => {
                let row_val = row.get(column);
                // compat-11: explode an MV row into per-element candidates
                // (any element in range matches); scalars pass through as
                // the single candidate, preserving single-value behaviour.
                let candidates: Vec<serde_json::Value> = match row_val {
                    None | Some(serde_json::Value::Null) => Vec::new(),
                    Some(serde_json::Value::Array(elems)) => {
                        elems.iter().filter(|e| !e.is_null()).cloned().collect()
                    }
                    Some(v) => vec![v.clone()],
                };
                if candidates.is_empty() {
                    return false;
                }
                let is_numeric = matches!(match_value_type.as_str(), "LONG" | "DOUBLE" | "FLOAT");
                let matches_one = |rv: &serde_json::Value| -> bool {
                    if is_numeric {
                        let rv_f = match value_to_f64(rv) {
                            Some(v) => v,
                            None => return false,
                        };
                        if let Some(lo) = lower {
                            let lo_f = match value_to_f64(lo) {
                                Some(v) => v,
                                None => return false,
                            };
                            if lower_open.unwrap_or(false) {
                                if rv_f <= lo_f {
                                    return false;
                                }
                            } else if rv_f < lo_f {
                                return false;
                            }
                        }
                        if let Some(hi) = upper {
                            let hi_f = match value_to_f64(hi) {
                                Some(v) => v,
                                None => return false,
                            };
                            if upper_open.unwrap_or(false) {
                                if rv_f >= hi_f {
                                    return false;
                                }
                            } else if rv_f > hi_f {
                                return false;
                            }
                        }
                        true
                    } else {
                        let rv_s = value_to_string(rv);
                        if let Some(lo) = lower {
                            let lo_s = value_to_string(lo);
                            if lower_open.unwrap_or(false) {
                                if rv_s.as_str() <= lo_s.as_str() {
                                    return false;
                                }
                            } else if rv_s.as_str() < lo_s.as_str() {
                                return false;
                            }
                        }
                        if let Some(hi) = upper {
                            let hi_s = value_to_string(hi);
                            if upper_open.unwrap_or(false) {
                                if rv_s.as_str() >= hi_s.as_str() {
                                    return false;
                                }
                            } else if rv_s.as_str() > hi_s.as_str() {
                                return false;
                            }
                        }
                        true
                    }
                };
                candidates.iter().any(matches_one)
            }

            Self::Like {
                dimension,
                pattern,
                escape,
            } => {
                // compat-11: any MV element matching the pattern matches.
                let candidates = row_value_strings(row.get(dimension));
                if candidates.is_empty() {
                    return false;
                }
                let regex_pat = like_to_regex(pattern, escape.as_deref());
                Regex::new(&regex_pat)
                    .map(|re| candidates.iter().any(|s| re.is_match(s)))
                    .unwrap_or(false)
            }

            Self::Regex { dimension, pattern } => {
                // compat-11: any MV element matching the pattern matches.
                let candidates = row_value_strings(row.get(dimension));
                if candidates.is_empty() {
                    return false;
                }
                Regex::new(pattern)
                    .map(|re| candidates.iter().any(|s| re.is_match(s)))
                    .unwrap_or(false)
            }

            Self::Search { dimension, query } => {
                // compat-11: any MV element matching the search matches.
                let candidates = row_value_strings(row.get(dimension));
                candidates.iter().any(|s| search_query_matches(query, s))
            }

            Self::And { fields } => fields.iter().all(|f| f.matches(row)),
            Self::Or { fields } => fields.iter().any(|f| f.matches(row)),
            Self::Not { field } => !field.matches(row),

            Self::Interval {
                dimension,
                intervals,
            } => {
                let row_val = row.get(dimension);
                let ts_millis = match row_val {
                    Some(serde_json::Value::Number(n)) => n.as_i64(),
                    _ => None,
                };
                let ts_millis = match ts_millis {
                    Some(v) => v,
                    None => return false,
                };
                intervals
                    .iter()
                    .any(|iv_str| interval_contains(iv_str, ts_millis))
            }

            Self::Expression { expression } => evaluate_expression(expression, row),

            Self::ColumnComparison { dimensions } => {
                // A row matches when every listed column holds an equal value.
                // Null is handled explicitly (like the Selector arm): all-null
                // columns match, but a null compared against a non-null does
                // NOT match. Nulls must not be routed through string coercion,
                // which would coerce null -> "" and falsely equate null with the
                // empty string. Fewer than two columns trivially match.
                let is_null =
                    |dim: &String| matches!(row.get(dim), None | Some(serde_json::Value::Null));
                let mut iter = dimensions.iter();
                let Some(first) = iter.next() else {
                    return true;
                };
                let first_null = is_null(first);
                let first_val = row.get(first).cloned().unwrap_or(serde_json::Value::Null);
                iter.all(|dim| {
                    let dim_null = is_null(dim);
                    // Both null -> equal; exactly one null -> not equal; neither
                    // null -> compare values (with the usual number/string
                    // coercion).
                    match (first_null, dim_null) {
                        (true, true) => true,
                        (true, false) | (false, true) => false,
                        (false, false) => {
                            let v = row.get(dim).cloned().unwrap_or(serde_json::Value::Null);
                            values_equal(&first_val, &v)
                        }
                    }
                })
            }

            Self::True => true,
            Self::False => false,

            Self::Null { column } => {
                let row_val = row.get(column);
                matches!(row_val, None | Some(serde_json::Value::Null))
            }

            Self::BloomFilter {
                dimension,
                base64_filter,
            } => {
                let Ok(filter) = ferrodruid_aggregator::decode_bloom_filter(base64_filter) else {
                    // A malformed base64 filter is a query bug, not a
                    // per-row data issue: surface it via `validate()` rather
                    // than silently letting it match (handled here as
                    // defence-in-depth: fail CLOSED on per-row eval).
                    return false;
                };
                // compat-11 R2: ELEMENT-AWARE probing (mirrors the R1 IN
                // fix) — an MV row matches when ANY non-null element is
                // (probably) in the bloom; pre-fix the row's JSON-array
                // TEXT was probed, so a bloom containing "a" rejected
                // `["a","b"]`.  Scalars contribute exactly one candidate
                // (unchanged).  A null / missing / empty-MV row yields no
                // candidates and never matches — the FerroDruid bloom
                // envelope has no null representation (the BLOOM_FILTER
                // aggregator skips nulls), so no bloom is null-inclusive;
                // this is the selector/IN null rule with an always-null-
                // free value set.
                let candidates = row_value_strings(row.get(dimension));
                candidates.iter().any(|s| filter.test(s))
            }

            Self::MvFilterOnly { dimension, values } => {
                mv_filter_matches(row, dimension, values, true)
            }
            Self::MvFilterNone { dimension, values } => {
                mv_filter_matches(row, dimension, values, false)
            }
        }
    }

    /// Validate that this filter is well-formed *before* row iteration.
    ///
    /// DD R40: an `expression` filter whose body failed to tokenize/parse (or
    /// used an unsupported construct, e.g. `"tenant == 'acme' &&"` or a function
    /// call) used to fall through to `true` in [`evaluate_expression`], so the
    /// filter silently matched EVERY row — dropping the intended constraint
    /// entirely (a data-exposure / wrong-aggregation risk).  Filters are now
    /// validated up front so a malformed expression is *rejected* with
    /// [`DruidError::Query`] rather than matching everything.  Validation
    /// recurses through the boolean combinators (`and` / `or` / `not`) so a
    /// malformed expression nested inside them is also caught.  As defence in
    /// depth, [`evaluate_expression`] additionally fails CLOSED (returns
    /// `false`) on any residual per-row parse/eval failure.
    ///
    /// An empty expression is treated as "no constraint" (match all) and is
    /// therefore valid, preserving the long-standing behaviour exercised by
    /// `expression_empty_string_defaults_to_true`.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] when an `expression` filter (possibly
    /// nested under `and`/`or`/`not`) cannot be parsed.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Expression { expression } => validate_expression(expression),
            Self::And { fields } | Self::Or { fields } => {
                for f in fields {
                    f.validate()?;
                }
                Ok(())
            }
            Self::Not { field } => field.validate(),
            // CL-4 / W1-H R4: reject a malformed `base64Filter` up front
            // so a query bug surfaces as a hard error rather than as a
            // silently-matching-nothing filter on every row.
            Self::BloomFilter { base64_filter, .. } => {
                ferrodruid_aggregator::decode_bloom_filter(base64_filter)
                    .map(|_| ())
                    .map_err(|reason| {
                        DruidError::Query(format!(
                            "bloomFilter filter rejected: {reason}; \
                             produce the base64 envelope from BLOOM_FILTER(expr, n)"
                        ))
                    })
            }
            _ => Ok(()),
        }
    }

    /// **W3-SL1-B step 3 (Task #31)** — typed fast-path matcher that
    /// bypasses the `HashMap<String, Value>` row map by reading
    /// [`ColumnData`] cells directly from the segment.
    ///
    /// Returns `Some(true)` / `Some(false)` when this filter tree is
    /// fully typed-supported for the given segment; returns `None`
    /// when any variant in the tree needs the row-map path (Regex,
    /// Search, Interval, Expression, ColumnComparison, Like,
    /// BloomFilter, MvFilter*, string-dictionary comparisons, etc.).
    ///
    /// Callers use the return value to decide whether to skip the
    /// per-row `build_row_update_only` call:
    ///
    /// ```ignore
    /// if let Some(filter) = &self.filter {
    ///     match filter.matches_typed(segment, row_idx) {
    ///         Some(true) => { /* keep — populate row map next */ }
    ///         Some(false) => continue,               // skip row (fast reject)
    ///         None => {
    ///             build_row_update_only(segment, row_idx, &mut row);
    ///             virtual_columns.augment_row(&mut row);
    ///             if !filter.matches(&row) { continue; }
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// For the W2-C TPC-H shape: Q1 has no filter so the caller never
    /// calls this; Q3 has an `interval` filter that the caller's own
    /// `intervals`-check loop handles; Q6 filter is `And { fields:
    /// [Bound(l_quantity, numeric), Bound(l_discount, numeric)] }`
    /// which this fast path covers, eliminating one
    /// `build_row_update_only` call for every rejected row (~99 %
    /// of the 60 M-row scan on Q6).
    #[must_use]
    pub fn matches_typed(&self, segment: &SegmentData, row_idx: usize) -> Option<bool> {
        match self {
            Self::And { fields } => {
                for f in fields {
                    let ok = f.matches_typed(segment, row_idx)?;
                    if !ok {
                        return Some(false);
                    }
                }
                Some(true)
            }
            Self::Or { fields } => {
                for f in fields {
                    let ok = f.matches_typed(segment, row_idx)?;
                    if ok {
                        return Some(true);
                    }
                }
                Some(false)
            }
            Self::Not { field } => Some(!field.matches_typed(segment, row_idx)?),
            Self::True => Some(true),
            Self::False => Some(false),
            Self::Bound {
                dimension,
                lower,
                upper,
                lower_strict,
                upper_strict,
                ordering,
            } => {
                let is_numeric = ordering.as_deref().is_some_and(|o| o == "numeric");
                if !is_numeric {
                    return None; // lexicographic path stays on row map
                }
                let col = segment.columns.get(dimension)?;
                let rv = match col {
                    ColumnData::Long(v) => *v.get(row_idx)? as f64,
                    ColumnData::Double(v) => *v.get(row_idx)?,
                    ColumnData::Float(v) => *v.get(row_idx)? as f64,
                    _ => return None,
                };
                // NaN is the in-band SQL-NULL marker for double/float
                // columns.  Null never matches a bound — and without this
                // check NaN would *pass* (every NaN comparison is false, so
                // neither reject branch fires), diverging from the row-map
                // path where NaN renders as JSON null and the Bound arm
                // rejects it.
                if rv.is_nan() {
                    return Some(false);
                }
                if let Some(lo) = lower {
                    let lo_f: f64 = lo.parse().ok()?;
                    if lower_strict.unwrap_or(false) {
                        if rv <= lo_f {
                            return Some(false);
                        }
                    } else if rv < lo_f {
                        return Some(false);
                    }
                }
                if let Some(hi) = upper {
                    let hi_f: f64 = hi.parse().ok()?;
                    if upper_strict.unwrap_or(false) {
                        if rv >= hi_f {
                            return Some(false);
                        }
                    } else if rv > hi_f {
                        return Some(false);
                    }
                }
                Some(true)
            }
            _ => None,
        }
    }

    /// Compile this filter into a [`CompiledFilter`] that borrows the segment's
    /// column slices directly and pre-parses all numeric bounds, so per-row
    /// evaluation is branch-light with no `HashMap` lookup or string parse.
    ///
    /// Returns `None` for any filter shape [`Self::matches_typed`] cannot decide
    /// from typed columns (lexicographic bounds, string selectors, etc.), so
    /// callers fall back to the row-map path with identical semantics. The set
    /// of accepted shapes matches `matches_typed` exactly: `And`/`Or`/`Not`,
    /// `True`/`False`, and numeric `Bound`.
    #[must_use]
    pub fn compile_typed<'a>(&self, segment: &'a SegmentData) -> Option<CompiledFilter<'a>> {
        match self {
            Self::And { fields } => Some(CompiledFilter::And(
                fields
                    .iter()
                    .map(|f| f.compile_typed(segment))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Self::Or { fields } => Some(CompiledFilter::Or(
                fields
                    .iter()
                    .map(|f| f.compile_typed(segment))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Self::Not { field } => {
                Some(CompiledFilter::Not(Box::new(field.compile_typed(segment)?)))
            }
            Self::True => Some(CompiledFilter::Const(true)),
            Self::False => Some(CompiledFilter::Const(false)),
            Self::Bound {
                dimension,
                lower,
                upper,
                lower_strict,
                upper_strict,
                ordering,
            } => {
                if !ordering.as_deref().is_some_and(|o| o == "numeric") {
                    return None;
                }
                let col = match segment.columns.get(dimension)? {
                    ColumnData::Long(v) => NumCol::Long(v.as_slice()),
                    ColumnData::Double(v) => NumCol::Double(v.as_slice()),
                    ColumnData::Float(v) => NumCol::Float(v.as_slice()),
                    _ => return None,
                };
                // Pre-parse the bounds once (was `lo.parse()` per row).
                let lo = match lower {
                    Some(s) => Some(s.parse::<f64>().ok()?),
                    None => None,
                };
                let hi = match upper {
                    Some(s) => Some(s.parse::<f64>().ok()?),
                    None => None,
                };
                Some(CompiledFilter::Bound {
                    col,
                    lo,
                    lo_strict: lower_strict.unwrap_or(false),
                    hi,
                    hi_strict: upper_strict.unwrap_or(false),
                })
            }
            _ => None,
        }
    }

    /// compat-11 R2: comprehensive PLAN-TIME multi-value (MV) filter guard.
    ///
    /// Walks this filter tree once per query (the filter twin of
    /// [`crate::helpers::ensure_aggregations_not_multi_value`]) and rejects
    /// any filter whose target column is a genuine `StringMulti` segment
    /// column but whose variant has NO element-aware implementation.
    /// Pre-fix, those variants silently compared the row's stringified
    /// JSON-array text (`["a","b"]` → `"[\"a\",\"b\"]"`) — wrong matches,
    /// never an error.
    ///
    /// The element-aware allowlist (verified any-element semantics —
    /// selector, in, bound, range, like, regex, search, bloomFilter — plus
    /// the MV-native mvFilterOnly / mvFilterNone, null (an MV row's
    /// null-ness is well-defined: empty row == null), and the column-free
    /// true / false) is enumerated EXPLICITLY below, and the match is
    /// exhaustive on purpose: a future filter variant will not compile
    /// until a deliberate MV decision is made here, so it can never
    /// silently stringify.  Rejected on MV today: `columnComparison`
    /// (Druid overlap semantics pending), `expression` (element-wise
    /// scalar application pending), and `interval` (element-wise timestamp
    /// parse pending).
    ///
    /// A dimension shadowed by a virtual column is exempt — the row map
    /// resolves the virtual column's (scalar) output, never the raw MV
    /// array, and virtual columns *referencing* MV columns are already
    /// rejected by
    /// [`crate::virtual_columns::VirtualColumns::ensure_no_multi_value_refs`].
    /// Single-value columns are entirely unaffected.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] naming the filter type and the first
    /// multi-value column it targets.
    pub(crate) fn ensure_multi_value_supported(
        &self,
        segment: &SegmentData,
        virtual_columns: &crate::virtual_columns::VirtualColumns,
    ) -> Result<()> {
        let is_mv = |name: &str| -> bool {
            !virtual_columns.names().any(|n| n == name)
                && matches!(segment.columns.get(name), Some(ColumnData::StringMulti(_)))
        };
        self.ensure_multi_value_supported_inner(&is_mv)
    }

    /// Recursive walk for [`Self::ensure_multi_value_supported`]; `is_mv`
    /// reports whether a name resolves to an unshadowed `StringMulti`
    /// segment column.
    fn ensure_multi_value_supported_inner(&self, is_mv: &dyn Fn(&str) -> bool) -> Result<()> {
        match self {
            // ELEMENT-AWARE (any-element) — the R1/R2-verified set.  Keep
            // this list in sync with the doc above.
            Self::Selector { .. }
            | Self::In { .. }
            | Self::Bound { .. }
            | Self::Range { .. }
            | Self::Like { .. }
            | Self::Regex { .. }
            | Self::Search { .. }
            | Self::BloomFilter { .. }
            // MV-native by definition.
            | Self::MvFilterOnly { .. }
            | Self::MvFilterNone { .. }
            // An MV row's null-ness is well-defined (empty row == null);
            // no stringification is possible.
            | Self::Null { .. }
            // No column access at all.
            | Self::True
            | Self::False => Ok(()),

            Self::And { fields } | Self::Or { fields } => {
                for f in fields {
                    f.ensure_multi_value_supported_inner(is_mv)?;
                }
                Ok(())
            }
            Self::Not { field } => field.ensure_multi_value_supported_inner(is_mv),

            // NOT element-aware — fail loud when targeting an MV column.
            Self::Interval { dimension, .. } => reject_mv_filter("interval", dimension, is_mv),
            Self::ColumnComparison { dimensions } => {
                for dim in dimensions {
                    reject_mv_filter("columnComparison", dim, is_mv)?;
                }
                Ok(())
            }
            Self::Expression { expression } => {
                for col in expression_column_refs(expression) {
                    reject_mv_filter("expression", &col, is_mv)?;
                }
                Ok(())
            }
        }
    }
}

/// Error when `name` is an unshadowed multi-value column targeted by a
/// filter variant with no element-aware MV implementation.
fn reject_mv_filter(filter_type: &str, name: &str, is_mv: &dyn Fn(&str) -> bool) -> Result<()> {
    if is_mv(name) {
        return Err(DruidError::Query(format!(
            "{filter_type} filter over a multi-value dimension `{name}` is not supported yet \
             (element-wise MV support for this filter is a follow-on)"
        )));
    }
    Ok(())
}

/// The column identifiers referenced by an expression filter body.
///
/// Uses the same tokenizer as evaluation, so the guard sees exactly the
/// identifiers [`evaluate_expression`] would resolve against the row map
/// (`null` / `true` / `false` are literals, not columns).  A body that
/// fails to tokenize contributes no references — [`FilterSpec::validate`]
/// rejects it loudly on its own.
fn expression_column_refs(expr: &str) -> Vec<String> {
    let Some(tokens) = tokenize_expr(expr) else {
        return Vec::new();
    };
    tokens
        .into_iter()
        .filter_map(|t| match t {
            ExprToken::Ident(id) if !matches!(id.as_str(), "null" | "true" | "false") => Some(id),
            _ => None,
        })
        .collect()
}

/// A numeric column slice borrowed from a segment, tagged with its physical
/// type so bound comparisons coerce to `f64` without a per-row enum lookup by
/// name.
#[derive(Debug, Clone, Copy)]
pub enum NumCol<'a> {
    /// 64-bit integer column.
    Long(&'a [i64]),
    /// 64-bit float column.
    Double(&'a [f64]),
    /// 32-bit float column.
    Float(&'a [f32]),
}

impl NumCol<'_> {
    /// Read row `row` as `f64`, or `None` if out of bounds.
    #[inline]
    fn get(&self, row: usize) -> Option<f64> {
        match self {
            #[allow(clippy::cast_precision_loss)]
            NumCol::Long(v) => v.get(row).map(|&x| x as f64),
            NumCol::Double(v) => v.get(row).copied(),
            NumCol::Float(v) => v.get(row).map(|&x| f64::from(x)),
        }
    }
}

/// A [`FilterSpec`] pre-resolved against a segment: column slices are borrowed
/// directly and numeric bounds are already parsed, so [`Self::eval`] is a tight
/// per-row predicate with no `HashMap` lookup or string parsing. Built by
/// [`FilterSpec::compile_typed`]; semantics are identical to
/// [`FilterSpec::matches_typed`] over the shapes it accepts.
#[derive(Debug)]
pub enum CompiledFilter<'a> {
    /// Conjunction: all children must pass.
    And(Vec<CompiledFilter<'a>>),
    /// Disjunction: any child passes.
    Or(Vec<CompiledFilter<'a>>),
    /// Negation.
    Not(Box<CompiledFilter<'a>>),
    /// Constant truth value (`True`/`False` filters).
    Const(bool),
    /// Numeric range bound over a resolved column slice.
    Bound {
        /// The resolved column slice.
        col: NumCol<'a>,
        /// Pre-parsed lower bound.
        lo: Option<f64>,
        /// Whether the lower bound is exclusive.
        lo_strict: bool,
        /// Pre-parsed upper bound.
        hi: Option<f64>,
        /// Whether the upper bound is exclusive.
        hi_strict: bool,
    },
}

impl CompiledFilter<'_> {
    /// Evaluate the predicate for row `row`. Out-of-bounds numeric reads count
    /// as non-matches (matching `matches_typed`'s `?`-on-`None` behaviour).
    #[inline]
    #[must_use]
    pub fn eval(&self, row: usize) -> bool {
        match self {
            CompiledFilter::And(fs) => fs.iter().all(|f| f.eval(row)),
            CompiledFilter::Or(fs) => fs.iter().any(|f| f.eval(row)),
            CompiledFilter::Not(f) => !f.eval(row),
            CompiledFilter::Const(b) => *b,
            CompiledFilter::Bound {
                col,
                lo,
                lo_strict,
                hi,
                hi_strict,
            } => {
                let Some(rv) = col.get(row) else {
                    return false;
                };
                // NaN = SQL NULL: never matches a bound (mirrors
                // `matches_typed` and the row-map path — see the note there).
                if rv.is_nan() {
                    return false;
                }
                if let Some(lo) = lo {
                    if *lo_strict {
                        if rv <= *lo {
                            return false;
                        }
                    } else if rv < *lo {
                        return false;
                    }
                }
                if let Some(hi) = hi {
                    if *hi_strict {
                        if rv >= *hi {
                            return false;
                        }
                    } else if rv > *hi {
                        return false;
                    }
                }
                true
            }
        }
    }
}

/// Helper for `MV_FILTER_ONLY` / `MV_FILTER_NONE` filter evaluation.
///
/// `keep_listed = true` is `MV_FILTER_ONLY` (match when *any* row value
/// is in `values`).  `keep_listed = false` is `MV_FILTER_NONE` (match
/// when *none* of the row values are in `values`).  Multi-value columns
/// are represented as JSON arrays; a single non-array value is treated
/// as a one-element array.  Null / missing values:
///
/// * MV_FILTER_ONLY: a null / missing / empty MV column does NOT match
///   (no value to keep).
/// * MV_FILTER_NONE: a null / missing / empty MV column DOES match
///   (vacuously: no banned values present).
fn mv_filter_matches(
    row: &HashMap<String, serde_json::Value>,
    dimension: &str,
    values: &[serde_json::Value],
    keep_listed: bool,
) -> bool {
    let row_val = row.get(dimension);
    let row_arr: Vec<serde_json::Value> = match row_val {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(a)) => a.clone(),
        Some(other) => vec![other.clone()],
    };
    if row_arr.is_empty() {
        // MV_FILTER_ONLY: no value to keep -> not a match.
        // MV_FILTER_NONE: no banned value present -> vacuous match.
        return !keep_listed;
    }
    let any_in_list = row_arr
        .iter()
        .any(|rv| values.iter().any(|v| values_equal(rv, v)));
    if keep_listed {
        any_in_list
    } else {
        !any_in_list
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Multi-value-aware row-value equality (compat-11): an MV row (JSON
/// array) matches when ANY element equals the filter value — a null
/// element only matches a null filter value; scalar rows use the plain
/// [`values_equal`] coercion unchanged.
fn row_value_equals(rv: &serde_json::Value, fv: &serde_json::Value) -> bool {
    match rv {
        serde_json::Value::Array(elems) => elems.iter().any(|e| {
            if e.is_null() {
                fv.is_null()
            } else {
                values_equal(e, fv)
            }
        }),
        _ => values_equal(rv, fv),
    }
}

/// The candidate strings a row value contributes to a string-domain
/// predicate (bound / like / regex / search), compat-11: an MV row (JSON
/// array) contributes each non-null element (Druid any-element matching);
/// a scalar contributes itself; null/missing contributes nothing (never
/// matches).
fn row_value_strings(row_val: Option<&serde_json::Value>) -> Vec<String> {
    match row_val {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(elems)) => elems
            .iter()
            .filter(|e| !e.is_null())
            .map(value_to_string)
            .collect(),
        Some(v) => vec![value_to_string(v)],
    }
}

/// Compare two JSON values for equality, coercing numbers and strings.
fn values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if a == b {
        return true;
    }
    // Coerce: compare string representations for cross-type comparison.
    let sa = value_to_string(a);
    let sb = value_to_string(b);
    sa == sb
}

/// Convert a JSON value to a string representation.
fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        _ => v.to_string(),
    }
}

/// Extract an f64 from a JSON value.
fn value_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Convert a SQL LIKE pattern to a regex pattern string.
fn like_to_regex(pattern: &str, escape: Option<&str>) -> String {
    let esc_char = escape.and_then(|e| e.chars().next());
    let mut re = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if Some(c) == esc_char {
            if let Some(next) = chars.next() {
                re.push_str(&regex::escape(&next.to_string()));
            }
        } else if c == '%' {
            re.push_str(".*");
        } else if c == '_' {
            re.push('.');
        } else {
            re.push_str(&regex::escape(&c.to_string()));
        }
    }
    re.push('$');
    re
}

/// Check if a search query matches a string value.
fn search_query_matches(query: &SearchQuerySpec, val: &str) -> bool {
    match query {
        SearchQuerySpec::Contains { value } => val.contains(value.as_str()),
        SearchQuerySpec::InsensitiveContains { value } => {
            val.to_lowercase().contains(&value.to_lowercase())
        }
        SearchQuerySpec::Fragment {
            values,
            case_sensitive,
        } => {
            if *case_sensitive {
                values.iter().all(|frag| val.contains(frag.as_str()))
            } else {
                let lower = val.to_lowercase();
                values
                    .iter()
                    .all(|frag| lower.contains(&frag.to_lowercase()))
            }
        }
        SearchQuerySpec::Regex { pattern } => Regex::new(pattern)
            .map(|re| re.is_match(val))
            .unwrap_or(false),
    }
}

/// Check if a timestamp in epoch-millis falls within an ISO-8601 interval string.
fn interval_contains(interval_str: &str, ts_millis: i64) -> bool {
    let parts: Vec<&str> = interval_str.splitn(2, '/').collect();
    if parts.len() != 2 {
        return false;
    }
    // DD R48: reuse the shared bound parser so the interval FILTER accepts the
    // same forms as top-level query intervals — including the bare `YYYY-MM-DD`
    // (UTC-midnight) date form, which this path previously rejected, causing a
    // valid `{"type":"interval","intervals":["2024-01-01/2024-01-02"]}` to match
    // no rows.
    match (
        crate::helpers::parse_iso_millis(parts[0].trim()),
        crate::helpers::parse_iso_millis(parts[1].trim()),
    ) {
        (Some(s_millis), Some(e_millis)) => ts_millis >= s_millis && ts_millis < e_millis,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Expression evaluator
// ---------------------------------------------------------------------------

/// Evaluate a Druid expression filter string against a row.
///
/// Supports a simplified expression mini-language:
/// - Comparisons: `==`, `!=`, `>`, `<`, `>=`, `<=`
/// - Logical: `&&`, `||`, `!`
/// - Column references (bare identifiers)
/// - String literals (`'...'`), numeric literals
/// - Parentheses for grouping
/// - Null checks: `col == null`, `col != null`
fn evaluate_expression(expr: &str, row: &HashMap<String, serde_json::Value>) -> bool {
    // An empty expression carries no constraint — match all (preserves the
    // long-standing `expression_empty_string_defaults_to_true` behaviour).
    if expr.trim().is_empty() {
        return true;
    }
    let tokens = match tokenize_expr(expr) {
        Some(t) => t,
        // DD R40: a tokenize failure used to default to `true`, so a malformed
        // expression matched EVERY row and silently dropped the filter. Fail
        // CLOSED instead. (Well-formed filters are also rejected up front by
        // `FilterSpec::validate`; this is the per-row defence in depth.)
        None => return false,
    };
    let mut pos = 0;
    match parse_or(&tokens, &mut pos, row) {
        Some(v) if pos == tokens.len() => v,
        // DD R40: parse failure, leftover tokens, or eval failure now fail
        // CLOSED (was: default to match).
        _ => false,
    }
}

/// Validate that an expression filter body parses to completion.
///
/// DD R40: runs the same tokenizer/parser as [`evaluate_expression`] against an
/// empty row (column references are irrelevant to *structural* validity — an
/// unknown column resolves to `Null` either way).  A failure to tokenize, a
/// parse that returns `None`, or leftover tokens means the expression is
/// malformed or uses an unsupported construct, so we fail CLOSED with a
/// [`DruidError::Query`] rather than letting it silently match every row.
///
/// An empty expression is "no constraint" and is therefore accepted.
///
/// # Errors
///
/// Returns [`DruidError::Query`] when `expr` is non-empty and cannot be parsed.
fn validate_expression(expr: &str) -> Result<()> {
    if expr.trim().is_empty() {
        return Ok(());
    }
    let tokens = tokenize_expr(expr)
        .ok_or_else(|| DruidError::Query(format!("malformed expression filter: `{expr}`")))?;
    let empty: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pos = 0;
    match parse_or(&tokens, &mut pos, &empty) {
        Some(_) if pos == tokens.len() => Ok(()),
        _ => Err(DruidError::Query(format!(
            "malformed or unsupported expression filter: `{expr}`"
        ))),
    }
}

/// Token types for the expression mini-language.
#[derive(Debug, Clone, PartialEq)]
enum ExprToken {
    /// An identifier (column name or `null` / `true` / `false`).
    Ident(String),
    /// A numeric literal.
    Number(f64),
    /// A string literal (content without quotes).
    StringLit(String),
    /// Operator token.
    Op(String),
    /// Left parenthesis.
    LParen,
    /// Right parenthesis.
    RParen,
}

/// Tokenize an expression string.
fn tokenize_expr(input: &str) -> Option<Vec<ExprToken>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Skip whitespace.
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Parentheses.
        if c == '(' {
            tokens.push(ExprToken::LParen);
            i += 1;
            continue;
        }
        if c == ')' {
            tokens.push(ExprToken::RParen);
            i += 1;
            continue;
        }

        // String literal.
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != '\'' {
                s.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                i += 1; // skip closing quote
            }
            tokens.push(ExprToken::StringLit(s));
            continue;
        }

        // Two-char operators.
        if i + 1 < chars.len() {
            let two: String = chars[i..=i + 1].iter().collect();
            match two.as_str() {
                "==" | "!=" | ">=" | "<=" | "&&" | "||" => {
                    tokens.push(ExprToken::Op(two));
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char operators.
        if matches!(c, '>' | '<' | '!') {
            tokens.push(ExprToken::Op(c.to_string()));
            i += 1;
            continue;
        }

        // Numeric literal (including negative).
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let start = i;
            if c == '-' {
                i += 1;
            }
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let num_str: String = chars[start..i].iter().collect();
            match num_str.parse::<f64>() {
                Ok(n) => tokens.push(ExprToken::Number(n)),
                Err(_) => return None,
            }
            continue;
        }

        // Identifier.
        if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let ident: String = chars[start..i].iter().collect();
            tokens.push(ExprToken::Ident(ident));
            continue;
        }

        // Unknown character — skip.
        i += 1;
    }

    Some(tokens)
}

/// Parse an OR expression: `and_expr (|| and_expr)*`
fn parse_or(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &HashMap<String, serde_json::Value>,
) -> Option<bool> {
    let mut result = parse_and(tokens, pos, row)?;
    while *pos < tokens.len() {
        if tokens[*pos] == ExprToken::Op("||".to_string()) {
            *pos += 1;
            let rhs = parse_and(tokens, pos, row)?;
            result = result || rhs;
        } else {
            break;
        }
    }
    Some(result)
}

/// Parse an AND expression: `not_expr (&& not_expr)*`
fn parse_and(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &HashMap<String, serde_json::Value>,
) -> Option<bool> {
    let mut result = parse_not(tokens, pos, row)?;
    while *pos < tokens.len() {
        if tokens[*pos] == ExprToken::Op("&&".to_string()) {
            *pos += 1;
            let rhs = parse_not(tokens, pos, row)?;
            result = result && rhs;
        } else {
            break;
        }
    }
    Some(result)
}

/// Parse a NOT expression: `!primary | primary`
fn parse_not(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &HashMap<String, serde_json::Value>,
) -> Option<bool> {
    if *pos < tokens.len() && tokens[*pos] == ExprToken::Op("!".to_string()) {
        *pos += 1;
        let val = parse_primary(tokens, pos, row)?;
        Some(!val)
    } else {
        parse_primary(tokens, pos, row)
    }
}

/// Resolved value for expression evaluation.
#[derive(Debug, Clone)]
enum ExprValue {
    /// A numeric value.
    Num(f64),
    /// A string value.
    Str(String),
    /// A boolean value.
    Bool(bool),
    /// A null value.
    Null,
}

/// Parse a primary expression: parenthesized or comparison.
fn parse_primary(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &HashMap<String, serde_json::Value>,
) -> Option<bool> {
    if *pos >= tokens.len() {
        return None;
    }

    // Parenthesized expression.
    if tokens[*pos] == ExprToken::LParen {
        *pos += 1;
        let result = parse_or(tokens, pos, row)?;
        if *pos < tokens.len() && tokens[*pos] == ExprToken::RParen {
            *pos += 1;
        }
        return Some(result);
    }

    // Resolve the left-hand side.
    let lhs = resolve_value(tokens, pos, row)?;

    // Check for a comparison operator.
    if let Some(ExprToken::Op(op)) = tokens.get(*pos) {
        let op_str = op.clone();
        if matches!(op_str.as_str(), "==" | "!=" | ">" | "<" | ">=" | "<=") {
            *pos += 1;
            let rhs = resolve_value(tokens, pos, row)?;
            return Some(compare_values(&lhs, &op_str, &rhs));
        }
    }

    // No comparison — treat as a boolean value.
    Some(value_is_truthy(&lhs))
}

/// Resolve a token into an [`ExprValue`].
fn resolve_value(
    tokens: &[ExprToken],
    pos: &mut usize,
    row: &HashMap<String, serde_json::Value>,
) -> Option<ExprValue> {
    if *pos >= tokens.len() {
        return None;
    }

    let token = &tokens[*pos];
    *pos += 1;

    match token {
        ExprToken::Number(n) => Some(ExprValue::Num(*n)),
        ExprToken::StringLit(s) => Some(ExprValue::Str(s.clone())),
        ExprToken::Ident(ident) => {
            match ident.as_str() {
                "null" => Some(ExprValue::Null),
                "true" => Some(ExprValue::Bool(true)),
                "false" => Some(ExprValue::Bool(false)),
                _ => {
                    // Column reference.
                    match row.get(ident) {
                        None | Some(serde_json::Value::Null) => Some(ExprValue::Null),
                        Some(serde_json::Value::Number(n)) => {
                            Some(ExprValue::Num(n.as_f64().unwrap_or(0.0)))
                        }
                        Some(serde_json::Value::String(s)) => Some(ExprValue::Str(s.clone())),
                        Some(serde_json::Value::Bool(b)) => Some(ExprValue::Bool(*b)),
                        Some(other) => Some(ExprValue::Str(other.to_string())),
                    }
                }
            }
        }
        _ => None,
    }
}

/// Compare two expression values with the given operator.
fn compare_values(lhs: &ExprValue, op: &str, rhs: &ExprValue) -> bool {
    // Null comparisons.
    if matches!(lhs, ExprValue::Null) || matches!(rhs, ExprValue::Null) {
        return match op {
            "==" => matches!(lhs, ExprValue::Null) && matches!(rhs, ExprValue::Null),
            "!=" => !(matches!(lhs, ExprValue::Null) && matches!(rhs, ExprValue::Null)),
            _ => false, // ordering with null is always false
        };
    }

    // Try numeric comparison first.
    if let (Some(l), Some(r)) = (expr_value_to_f64(lhs), expr_value_to_f64(rhs)) {
        return match op {
            "==" => (l - r).abs() < f64::EPSILON,
            "!=" => (l - r).abs() >= f64::EPSILON,
            ">" => l > r,
            "<" => l < r,
            ">=" => l >= r,
            "<=" => l <= r,
            _ => false,
        };
    }

    // Fall back to string comparison.
    let l_str = expr_value_to_string(lhs);
    let r_str = expr_value_to_string(rhs);
    match op {
        "==" => l_str == r_str,
        "!=" => l_str != r_str,
        ">" => l_str > r_str,
        "<" => l_str < r_str,
        ">=" => l_str >= r_str,
        "<=" => l_str <= r_str,
        _ => false,
    }
}

/// Convert an [`ExprValue`] to f64 if possible.
fn expr_value_to_f64(v: &ExprValue) -> Option<f64> {
    match v {
        ExprValue::Num(n) => Some(*n),
        ExprValue::Str(s) => s.parse().ok(),
        ExprValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        ExprValue::Null => None,
    }
}

/// Convert an [`ExprValue`] to a string.
fn expr_value_to_string(v: &ExprValue) -> String {
    match v {
        ExprValue::Num(n) => n.to_string(),
        ExprValue::Str(s) => s.clone(),
        ExprValue::Bool(b) => b.to_string(),
        ExprValue::Null => String::new(),
    }
}

/// Check if an expression value is truthy.
fn value_is_truthy(v: &ExprValue) -> bool {
    match v {
        ExprValue::Bool(b) => *b,
        ExprValue::Num(n) => *n != 0.0,
        ExprValue::Str(s) => !s.is_empty(),
        ExprValue::Null => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn interval_contains_accepts_date_only_bounds() {
        // DD R48: a bare YYYY-MM-DD interval filter must match a ts within the
        // range (previously rejected -> matched nothing -> empty/wrong results).
        // 2024-01-01T12:00:00Z = 1_704_110_400_000 ms is inside 2024-01-01/2024-01-02.
        let mid = 1_704_110_400_000;
        assert!(interval_contains("2024-01-01/2024-01-02", mid));
        // A ts outside the date-only range does not match.
        assert!(!interval_contains(
            "2024-01-01/2024-01-02",
            1_700_000_000_000
        ));
        // RFC3339 form still works.
        assert!(interval_contains(
            "2024-01-01T00:00:00Z/2024-01-02T00:00:00Z",
            mid
        ));
    }

    fn row(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn selector_match() {
        let f = FilterSpec::Selector {
            dimension: "city".into(),
            value: Some(json!("Tokyo")),
        };
        assert!(f.matches(&row(&[("city", json!("Tokyo"))])));
    }

    #[test]
    fn column_comparison_equal_columns_match() {
        let f: FilterSpec = serde_json::from_value(json!({
            "type": "columnComparison",
            "dimensions": ["a", "b"]
        }))
        .expect("deser columnComparison");
        // Equal values (string vs string) match.
        assert!(f.matches(&row(&[("a", json!("x")), ("b", json!("x"))])));
        // Unequal values do not match.
        assert!(!f.matches(&row(&[("a", json!("x")), ("b", json!("y"))])));
        // Numeric/string coercion: 1 == "1".
        assert!(f.matches(&row(&[("a", json!(1)), ("b", json!("1"))])));
        // Two absent columns are both null and compare equal.
        assert!(f.matches(&row(&[("c", json!("x"))])));
        // Three-column comparison requires all equal.
        let f3: FilterSpec = serde_json::from_value(json!({
            "type": "columnComparison",
            "dimensions": ["a", "b", "c"]
        }))
        .expect("deser");
        assert!(f3.matches(&row(&[("a", json!(2)), ("b", json!(2)), ("c", json!(2))])));
        assert!(!f3.matches(&row(&[("a", json!(2)), ("b", json!(2)), ("c", json!(3))])));
    }

    /// DD R10 A#5: columnComparison must not equate null with the empty string.
    /// Routing nulls through string coercion mapped null -> "", so `null` vs ""
    /// falsely matched. Null is now handled explicitly: all-null matches, any
    /// null-vs-non-null does not.
    #[test]
    fn column_comparison_null_not_equal_to_empty_string() {
        let f: FilterSpec = serde_json::from_value(json!({
            "type": "columnComparison",
            "dimensions": ["colA", "colB"]
        }))
        .expect("deser columnComparison");

        // colA = null, colB = "" must NOT match (the regression).
        assert!(
            !f.matches(&row(&[("colA", json!(null)), ("colB", json!(""))])),
            "null must not equal empty string"
        );
        // A missing column (also null) vs "" must NOT match either.
        assert!(!f.matches(&row(&[("colB", json!(""))])));

        // colA = null, colB = null must match.
        assert!(f.matches(&row(&[("colA", json!(null)), ("colB", json!(null))])));

        // colA = "x", colB = "x" must match.
        assert!(f.matches(&row(&[("colA", json!("x")), ("colB", json!("x"))])));

        // null vs a non-empty, non-null value must not match.
        assert!(!f.matches(&row(&[("colA", json!(null)), ("colB", json!("x"))])));
    }

    #[test]
    fn selector_no_match() {
        let f = FilterSpec::Selector {
            dimension: "city".into(),
            value: Some(json!("Tokyo")),
        };
        assert!(!f.matches(&row(&[("city", json!("Osaka"))])));
    }

    #[test]
    fn selector_null() {
        let f = FilterSpec::Selector {
            dimension: "city".into(),
            value: None,
        };
        assert!(f.matches(&row(&[])));
        assert!(f.matches(&row(&[("city", json!(null))])));
        assert!(!f.matches(&row(&[("city", json!("Tokyo"))])));
    }

    #[test]
    fn in_match() {
        let f = FilterSpec::In {
            dimension: "country".into(),
            values: vec![json!("US"), json!("JP")],
        };
        assert!(f.matches(&row(&[("country", json!("JP"))])));
    }

    #[test]
    fn in_no_match() {
        let f = FilterSpec::In {
            dimension: "country".into(),
            values: vec![json!("US"), json!("JP")],
        };
        assert!(!f.matches(&row(&[("country", json!("DE"))])));
    }

    #[test]
    fn bound_numeric() {
        let f = FilterSpec::Bound {
            dimension: "age".into(),
            lower: Some("18".into()),
            upper: Some("65".into()),
            lower_strict: Some(false),
            upper_strict: Some(true),
            ordering: Some("numeric".into()),
        };
        assert!(f.matches(&row(&[("age", json!(18))])));
        assert!(f.matches(&row(&[("age", json!(30))])));
        assert!(!f.matches(&row(&[("age", json!(65))])));
        assert!(!f.matches(&row(&[("age", json!(10))])));
    }

    #[test]
    fn bound_lexicographic() {
        let f = FilterSpec::Bound {
            dimension: "name".into(),
            lower: Some("b".into()),
            upper: Some("d".into()),
            lower_strict: None,
            upper_strict: None,
            ordering: None,
        };
        assert!(f.matches(&row(&[("name", json!("c"))])));
        assert!(f.matches(&row(&[("name", json!("b"))])));
        assert!(f.matches(&row(&[("name", json!("d"))])));
        assert!(!f.matches(&row(&[("name", json!("a"))])));
        assert!(!f.matches(&row(&[("name", json!("e"))])));
    }

    #[test]
    fn like_percent() {
        let f = FilterSpec::Like {
            dimension: "page".into(),
            pattern: "Main%".into(),
            escape: None,
        };
        assert!(f.matches(&row(&[("page", json!("MainPage"))])));
        assert!(f.matches(&row(&[("page", json!("Main"))])));
        assert!(!f.matches(&row(&[("page", json!("Other"))])));
    }

    #[test]
    fn regex_filter() {
        let f = FilterSpec::Regex {
            dimension: "page".into(),
            pattern: "^Main.*$".into(),
        };
        assert!(f.matches(&row(&[("page", json!("MainPage"))])));
        assert!(!f.matches(&row(&[("page", json!("Other"))])));
    }

    #[test]
    fn and_filter() {
        let f = FilterSpec::And {
            fields: vec![
                FilterSpec::Selector {
                    dimension: "a".into(),
                    value: Some(json!("1")),
                },
                FilterSpec::Selector {
                    dimension: "b".into(),
                    value: Some(json!("2")),
                },
            ],
        };
        assert!(f.matches(&row(&[("a", json!("1")), ("b", json!("2"))])));
        assert!(!f.matches(&row(&[("a", json!("1")), ("b", json!("3"))])));
    }

    #[test]
    fn or_filter() {
        let f = FilterSpec::Or {
            fields: vec![
                FilterSpec::Selector {
                    dimension: "a".into(),
                    value: Some(json!("1")),
                },
                FilterSpec::Selector {
                    dimension: "a".into(),
                    value: Some(json!("2")),
                },
            ],
        };
        assert!(f.matches(&row(&[("a", json!("1"))])));
        assert!(f.matches(&row(&[("a", json!("2"))])));
        assert!(!f.matches(&row(&[("a", json!("3"))])));
    }

    #[test]
    fn not_filter() {
        let f = FilterSpec::Not {
            field: Box::new(FilterSpec::Selector {
                dimension: "a".into(),
                value: Some(json!("1")),
            }),
        };
        assert!(!f.matches(&row(&[("a", json!("1"))])));
        assert!(f.matches(&row(&[("a", json!("2"))])));
    }

    #[test]
    fn true_false_filters() {
        let t = FilterSpec::True;
        let f = FilterSpec::False;
        assert!(t.matches(&row(&[])));
        assert!(!f.matches(&row(&[])));
    }

    #[test]
    fn null_filter() {
        let f = FilterSpec::Null { column: "x".into() };
        assert!(f.matches(&row(&[])));
        assert!(f.matches(&row(&[("x", json!(null))])));
        assert!(!f.matches(&row(&[("x", json!(1))])));
    }

    #[test]
    fn filter_json_round_trip() {
        let f = FilterSpec::And {
            fields: vec![
                FilterSpec::Selector {
                    dimension: "city".into(),
                    value: Some(json!("Tokyo")),
                },
                FilterSpec::Not {
                    field: Box::new(FilterSpec::Regex {
                        dimension: "page".into(),
                        pattern: "^test".into(),
                    }),
                },
            ],
        };
        let json = serde_json::to_string(&f).expect("serialize");
        let back: FilterSpec = serde_json::from_str(&json).expect("deserialize");
        // Verify it still works
        assert!(back.matches(&row(&[
            ("city", json!("Tokyo")),
            ("page", json!("MainPage")),
        ])));
    }

    #[test]
    fn range_numeric() {
        let f = FilterSpec::Range {
            column: "price".into(),
            match_value_type: "DOUBLE".into(),
            lower: Some(json!(10.0)),
            upper: Some(json!(100.0)),
            lower_open: Some(false),
            upper_open: Some(true),
        };
        assert!(f.matches(&row(&[("price", json!(10.0))])));
        assert!(f.matches(&row(&[("price", json!(50.5))])));
        assert!(!f.matches(&row(&[("price", json!(100.0))])));
        assert!(!f.matches(&row(&[("price", json!(5.0))])));
    }

    #[test]
    fn search_filter_contains() {
        let f = FilterSpec::Search {
            dimension: "page".into(),
            query: SearchQuerySpec::Contains {
                value: "Main".into(),
            },
        };
        assert!(f.matches(&row(&[("page", json!("MainPage"))])));
        assert!(!f.matches(&row(&[("page", json!("Other"))])));
    }

    // -----------------------------------------------------------------------
    // Expression filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn expression_simple_gt() {
        let f = FilterSpec::Expression {
            expression: "revenue > 100".into(),
        };
        assert!(f.matches(&row(&[("revenue", json!(200))])));
        assert!(!f.matches(&row(&[("revenue", json!(50))])));
        assert!(!f.matches(&row(&[("revenue", json!(100))])));
    }

    #[test]
    fn expression_string_equality() {
        let f = FilterSpec::Expression {
            expression: "city == 'tokyo'".into(),
        };
        assert!(f.matches(&row(&[("city", json!("tokyo"))])));
        assert!(!f.matches(&row(&[("city", json!("london"))])));
    }

    #[test]
    fn expression_and() {
        let f = FilterSpec::Expression {
            expression: "revenue > 50 && revenue < 200".into(),
        };
        assert!(f.matches(&row(&[("revenue", json!(100))])));
        assert!(!f.matches(&row(&[("revenue", json!(30))])));
        assert!(!f.matches(&row(&[("revenue", json!(250))])));
    }

    #[test]
    fn expression_or() {
        let f = FilterSpec::Expression {
            expression: "city == 'tokyo' || city == 'london'".into(),
        };
        assert!(f.matches(&row(&[("city", json!("tokyo"))])));
        assert!(f.matches(&row(&[("city", json!("london"))])));
        assert!(!f.matches(&row(&[("city", json!("paris"))])));
    }

    #[test]
    fn expression_not() {
        let f = FilterSpec::Expression {
            expression: "!(city == 'unknown')".into(),
        };
        assert!(f.matches(&row(&[("city", json!("tokyo"))])));
        assert!(!f.matches(&row(&[("city", json!("unknown"))])));
    }

    #[test]
    fn expression_null_check() {
        let f = FilterSpec::Expression {
            expression: "city != null".into(),
        };
        assert!(f.matches(&row(&[("city", json!("tokyo"))])));
        assert!(!f.matches(&row(&[])));
        assert!(!f.matches(&row(&[("city", json!(null))])));
    }

    #[test]
    fn expression_null_equals() {
        let f = FilterSpec::Expression {
            expression: "city == null".into(),
        };
        assert!(!f.matches(&row(&[("city", json!("tokyo"))])));
        assert!(f.matches(&row(&[])));
    }

    #[test]
    fn expression_parentheses() {
        let f = FilterSpec::Expression {
            expression: "(revenue > 100) && (city == 'tokyo')".into(),
        };
        assert!(f.matches(&row(&[("revenue", json!(200)), ("city", json!("tokyo")),])));
        assert!(!f.matches(&row(&[("revenue", json!(200)), ("city", json!("london")),])));
        assert!(!f.matches(&row(&[("revenue", json!(50)), ("city", json!("tokyo")),])));
    }

    #[test]
    fn expression_nested_logic() {
        let f = FilterSpec::Expression {
            expression: "(a > 1 && b < 2) || c == 'x'".into(),
        };
        // a > 1 && b < 2 => true
        assert!(f.matches(&row(
            &[("a", json!(5)), ("b", json!(1)), ("c", json!("y")),]
        )));
        // c == 'x' => true
        assert!(f.matches(&row(&[
            ("a", json!(0)),
            ("b", json!(10)),
            ("c", json!("x")),
        ])));
        // neither
        assert!(!f.matches(&row(&[
            ("a", json!(0)),
            ("b", json!(10)),
            ("c", json!("y")),
        ])));
    }

    #[test]
    fn expression_gte_lte() {
        let f = FilterSpec::Expression {
            expression: "x >= 10 && x <= 20".into(),
        };
        assert!(f.matches(&row(&[("x", json!(10))])));
        assert!(f.matches(&row(&[("x", json!(15))])));
        assert!(f.matches(&row(&[("x", json!(20))])));
        assert!(!f.matches(&row(&[("x", json!(9))])));
        assert!(!f.matches(&row(&[("x", json!(21))])));
    }

    #[test]
    fn expression_not_equals_string() {
        let f = FilterSpec::Expression {
            expression: "status != 'error'".into(),
        };
        assert!(f.matches(&row(&[("status", json!("ok"))])));
        assert!(!f.matches(&row(&[("status", json!("error"))])));
    }

    #[test]
    fn expression_empty_string_defaults_to_true() {
        let f = FilterSpec::Expression {
            expression: String::new(),
        };
        // Empty expression should default to matching (like the old behavior).
        assert!(f.matches(&row(&[])));
        // An empty expression carries no constraint, so it is valid.
        f.validate().expect("empty expression is valid");
    }

    #[test]
    fn malformed_expression_fails_closed_not_open() {
        // DD R40: a malformed / unsupported expression used to default to `true`
        // in `evaluate_expression`, so it matched EVERY row and silently dropped
        // the intended filter (data-exposure / wrong-aggregation risk). It must
        // now (a) be rejected by `validate()` and (b) NOT match all rows.
        for expr in [
            "tenant == 'acme' &&", // dangling operator
            "tenant == ",          // missing rhs
            "lower(tenant)",       // unsupported function call -> leftover tokens
            "&& tenant == 'acme'", // leading operator
        ] {
            let f = FilterSpec::Expression {
                expression: expr.to_owned(),
            };
            assert!(
                f.validate().is_err(),
                "malformed expression `{expr}` must be rejected by validate()"
            );
            // Per-row evaluation must fail CLOSED (match none), never open.
            assert!(
                !f.matches(&row(&[("tenant", json!("acme"))])),
                "malformed expression `{expr}` must NOT match (fail open)"
            );
            assert!(
                !f.matches(&row(&[("tenant", json!("other"))])),
                "malformed expression `{expr}` must NOT match (fail open)"
            );
        }

        // A malformed expression nested under `and`/`or`/`not` is also caught.
        let nested = FilterSpec::And {
            fields: vec![
                FilterSpec::True,
                FilterSpec::Expression {
                    expression: "x ==".to_owned(),
                },
            ],
        };
        assert!(
            nested.validate().is_err(),
            "malformed expression nested under `and` must be rejected"
        );

        // A well-formed expression still validates and behaves as before.
        let ok = FilterSpec::Expression {
            expression: "revenue > 100".to_owned(),
        };
        ok.validate().expect("well-formed expression is valid");
        assert!(ok.matches(&row(&[("revenue", json!(200))])));
        assert!(!ok.matches(&row(&[("revenue", json!(50))])));
    }

    // ----- CL-4 / W1-H R4: BLOOM_FILTER_TEST -----

    fn build_bloom(values: &[&str], num_entries: u64) -> String {
        use ferrodruid_aggregator::Aggregator as _;
        let mut agg = ferrodruid_aggregator::BloomFilterAggregator::new(num_entries);
        for v in values {
            agg.aggregate(Some(&json!(*v)));
        }
        let env = agg.get();
        env.get("bytes")
            .and_then(serde_json::Value::as_str)
            .expect("bloom envelope bytes")
            .to_owned()
    }

    #[test]
    fn bloom_filter_matches_inserted_value() {
        let b64 = build_bloom(&["alpha", "bravo", "charlie"], 1000);
        let f = FilterSpec::BloomFilter {
            dimension: "u".into(),
            base64_filter: b64,
        };
        assert!(f.matches(&row(&[("u", json!("alpha"))])));
        assert!(f.matches(&row(&[("u", json!("bravo"))])));
    }

    #[test]
    fn bloom_filter_rejects_absent_value() {
        let b64 = build_bloom(&["alpha"], 1000);
        let f = FilterSpec::BloomFilter {
            dimension: "u".into(),
            base64_filter: b64,
        };
        assert!(!f.matches(&row(&[("u", json!("absent-xyz-zzz-1234"))])));
    }

    /// Edge case: null / missing dimension fails closed (never matches).
    #[test]
    fn bloom_filter_null_dimension_is_no_match() {
        let b64 = build_bloom(&["alpha"], 100);
        let f = FilterSpec::BloomFilter {
            dimension: "u".into(),
            base64_filter: b64,
        };
        assert!(!f.matches(&row(&[("u", serde_json::Value::Null)])));
        assert!(!f.matches(&row(&[])));
    }

    #[test]
    fn bloom_filter_validate_rejects_malformed_base64() {
        let f = FilterSpec::BloomFilter {
            dimension: "u".into(),
            base64_filter: "not-base64!".into(),
        };
        f.validate().expect_err("bad base64 must reject");
        // Per-row evaluation also fails closed (defence in depth).
        assert!(!f.matches(&row(&[("u", json!("anything"))])));
    }

    // ----- CL-4 / W1-H R5: MV_FILTER_ONLY / MV_FILTER_NONE -----

    #[test]
    fn mv_filter_only_matches_when_any_value_in_set() {
        let f = FilterSpec::MvFilterOnly {
            dimension: "tags".into(),
            values: vec![json!("a"), json!("b")],
        };
        assert!(f.matches(&row(&[("tags", json!(["a", "x"]))])));
        assert!(f.matches(&row(&[("tags", json!(["b"]))])));
        assert!(!f.matches(&row(&[("tags", json!(["x", "y"]))])));
    }

    #[test]
    fn mv_filter_only_null_dim_is_no_match() {
        let f = FilterSpec::MvFilterOnly {
            dimension: "tags".into(),
            values: vec![json!("a")],
        };
        assert!(!f.matches(&row(&[("tags", serde_json::Value::Null)])));
        assert!(!f.matches(&row(&[("tags", json!([]))])));
        assert!(!f.matches(&row(&[])));
    }

    #[test]
    fn mv_filter_only_single_string_treated_as_one_element() {
        let f = FilterSpec::MvFilterOnly {
            dimension: "tags".into(),
            values: vec![json!("solo")],
        };
        assert!(f.matches(&row(&[("tags", json!("solo"))])));
        assert!(!f.matches(&row(&[("tags", json!("other"))])));
    }

    #[test]
    fn mv_filter_none_matches_when_no_value_in_set() {
        let f = FilterSpec::MvFilterNone {
            dimension: "tags".into(),
            values: vec![json!("spam"), json!("banned")],
        };
        assert!(f.matches(&row(&[("tags", json!(["clean"]))])));
        assert!(!f.matches(&row(&[("tags", json!(["spam"]))])));
        assert!(!f.matches(&row(&[("tags", json!(["clean", "banned"]))])));
    }

    /// Edge case: null / empty MV column DOES match MV_FILTER_NONE
    /// (vacuously: no banned value present).
    #[test]
    fn mv_filter_none_null_dim_is_vacuous_match() {
        let f = FilterSpec::MvFilterNone {
            dimension: "tags".into(),
            values: vec![json!("spam")],
        };
        assert!(f.matches(&row(&[("tags", serde_json::Value::Null)])));
        assert!(f.matches(&row(&[("tags", json!([]))])));
        assert!(f.matches(&row(&[])));
    }

    // -----------------------------------------------------------------------
    // compat-11: multi-value dimensions — plain selector / in / bound /
    // like / regex match ANY element (Druid MV semantics)
    // -----------------------------------------------------------------------

    /// Selector `tag = "a"` over the fixture rows
    /// `[["a","b"], ["a"], null, ["c","a"]]` matches rows 0, 1 and 3
    /// (any-element), and `tag = "b"` matches only row 0.
    #[test]
    fn selector_matches_any_mv_element() {
        let f = FilterSpec::Selector {
            dimension: "tag".into(),
            value: Some(json!("a")),
        };
        assert!(f.matches(&row(&[("tag", json!(["a", "b"]))])), "row 0");
        assert!(f.matches(&row(&[("tag", json!("a"))])), "row 1 (scalar)");
        assert!(!f.matches(&row(&[("tag", json!(null))])), "row 2 (null)");
        assert!(f.matches(&row(&[("tag", json!(["c", "a"]))])), "row 3");

        let f_b = FilterSpec::Selector {
            dimension: "tag".into(),
            value: Some(json!("b")),
        };
        assert!(f_b.matches(&row(&[("tag", json!(["a", "b"]))])));
        assert!(!f_b.matches(&row(&[("tag", json!(["c", "a"]))])));
        assert!(!f_b.matches(&row(&[("tag", json!("a"))])));

        // Null selector: an MV row WITH values never matches; a null row
        // (how an empty MV row materialises) does.
        let f_null = FilterSpec::Selector {
            dimension: "tag".into(),
            value: None,
        };
        assert!(!f_null.matches(&row(&[("tag", json!(["a", "b"]))])));
        assert!(f_null.matches(&row(&[("tag", json!(null))])));
        // Defensive: a literal empty array is the `[]`/null equivalence.
        assert!(f_null.matches(&row(&[("tag", json!([]))])));
    }

    /// IN `{"b","c"}` over the fixture rows matches rows 0 (has b) and
    /// 3 (has c) but not the pure-"a" row.
    #[test]
    fn in_filter_matches_any_mv_element() {
        let f = FilterSpec::In {
            dimension: "tag".into(),
            values: vec![json!("b"), json!("c")],
        };
        assert!(f.matches(&row(&[("tag", json!(["a", "b"]))])), "row 0");
        assert!(!f.matches(&row(&[("tag", json!("a"))])), "row 1");
        assert!(!f.matches(&row(&[("tag", json!(null))])), "row 2");
        assert!(f.matches(&row(&[("tag", json!(["c", "a"]))])), "row 3");
    }

    /// Bound / Like / Regex over an MV row match when ANY element
    /// satisfies the predicate (never the JSON-serialized array text).
    #[test]
    fn string_predicates_match_any_mv_element() {
        let bound = FilterSpec::Bound {
            dimension: "tag".into(),
            lower: Some("b".into()),
            upper: Some("c".into()),
            lower_strict: None,
            upper_strict: None,
            ordering: None,
        };
        assert!(bound.matches(&row(&[("tag", json!(["a", "b"]))])));
        assert!(!bound.matches(&row(&[("tag", json!(["a", "x"]))])));

        let like = FilterSpec::Like {
            dimension: "tag".into(),
            pattern: "b%".into(),
            escape: None,
        };
        assert!(like.matches(&row(&[("tag", json!(["a", "bear"]))])));
        assert!(!like.matches(&row(&[("tag", json!(["a", "cat"]))])));

        let re = FilterSpec::Regex {
            dimension: "tag".into(),
            pattern: "^c".into(),
        };
        assert!(re.matches(&row(&[("tag", json!(["c", "a"]))])));
        assert!(!re.matches(&row(&[("tag", json!(["a", "b"]))])));
        // The regex must NOT be probed against the array's JSON text.
        let re_bracket = FilterSpec::Regex {
            dimension: "tag".into(),
            pattern: r"^\[".into(),
        };
        assert!(!re_bracket.matches(&row(&[("tag", json!(["a", "b"]))])));
    }
}
