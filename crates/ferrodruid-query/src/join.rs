// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid-style JOIN data source and broadcast hash-join executor.
//!
//! Apache Druid models a join as a `join` data source that combines a *left*
//! data source with a *right* data source via an equi-join `condition`, a
//! `joinType` (Druid mainly supports `INNER` and `LEFT`), and a `rightPrefix`
//! applied to every column contributed by the right side.  The right side is
//! typically *broadcast* — a small relation (an inline table, a lookup, or a
//! sub-query result) that fits in memory and is built into a hash table keyed
//! on the join key.  For each left row the executor probes the hash table and
//! emits one joined row per match (INNER) or, when no match exists, keeps the
//! left row with `NULL` right columns (LEFT).
//!
//! This module is deliberately self-contained: the join is represented by
//! [`JoinDataSource`] rather than as a new variant of the shared
//! [`ferrodruid_common::types::DataSource`] enum (which other crates match on
//! exhaustively), so adding joins does not ripple a breaking change across the
//! workspace.  The [`execute_join`] entry point takes the already-materialised
//! left rows plus a [`JoinDataSource`] and produces joined rows that downstream
//! scan / groupBy code can consume directly.
//!
//! ## Scope
//!
//! Supported: equi-join on a single key expression (`left.k = right.k`),
//! `INNER` and `LEFT` join types, right side as inline table / lookup /
//! pre-materialised sub-query rows, multiple matches per key, the right-side
//! `rightPrefix`, and chaining (a join whose left side is itself a join — the
//! caller materialises the inner join first).
//!
//! Out of scope (documented, returns an error rather than silently mis-joining):
//! non-equi conditions (`<`, `>`, range, function-of-both-sides), `RIGHT` and
//! `FULL OUTER` joins, and a shuffle / partitioned join for a large
//! (non-broadcast) right side.  The right side is always materialised in full.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_lookup::LookupManager;
use ferrodruid_segment::SegmentData;

use crate::helpers::build_row;
use crate::scan::{ScanQuery, ScanResult};

/// A single materialised row: column name to JSON value.
pub type Row = HashMap<String, serde_json::Value>;

// ---------------------------------------------------------------------------
// Join type
// ---------------------------------------------------------------------------

/// The join type.  Druid's broadcast hash join supports `INNER` and `LEFT`;
/// `RIGHT` and `FULL` are out of scope for the broadcast-right executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum JoinType {
    /// Keep only left rows that have at least one matching right row.
    Inner,
    /// Keep every left row; right columns are `NULL` when there is no match.
    Left,
}

impl JoinType {
    /// Parse a Druid `joinType` string (`"INNER"` / `"LEFT"`).
    ///
    /// `RIGHT` and `FULL` are recognised but rejected with a clear error so a
    /// caller never silently downgrades an unsupported join to INNER.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "INNER" => Ok(JoinType::Inner),
            "LEFT" => Ok(JoinType::Left),
            "RIGHT" => Err(DruidError::Query(
                "RIGHT OUTER join is not supported (broadcast-right executor handles INNER/LEFT)"
                    .to_string(),
            )),
            "FULL" => Err(DruidError::Query(
                "FULL OUTER join is not supported (broadcast-right executor handles INNER/LEFT)"
                    .to_string(),
            )),
            other => Err(DruidError::Query(format!("unknown joinType \"{other}\""))),
        }
    }
}

// ---------------------------------------------------------------------------
// Join condition
// ---------------------------------------------------------------------------

/// An equi-join condition: `left_key = prefixed_right_key`.
///
/// `left_key` names a column of the (already materialised) left rows.
/// `right_key` names a column of the right relation *before* the
/// [`JoinDataSource::right_prefix`] is applied (i.e. the raw right column name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinCondition {
    /// The left-side join column name.
    pub left_key: String,
    /// The right-side join column name (before the right prefix is applied).
    pub right_key: String,
}

// ---------------------------------------------------------------------------
// Right side
// ---------------------------------------------------------------------------

/// The right side of a join — always a small, broadcastable relation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JoinRight {
    /// An inline table embedded in the query.
    #[serde(rename = "inline")]
    Inline {
        /// Column names, one per value in each row.
        column_names: Vec<String>,
        /// Row values (each inner vec has one value per column).
        rows: Vec<Vec<serde_json::Value>>,
    },
    /// A registered lookup, exposed as a two-column relation `(k, v)`.
    ///
    /// The lookup's keys are surfaced under `key_column` and its values under
    /// `value_column`, so a join `ON left.x = j.k` followed by selecting `j.v`
    /// realises the classic dimension-enrichment lookup join.
    #[serde(rename = "lookup")]
    Lookup {
        /// Name of the registered lookup.
        lookup: String,
        /// Output column name for the lookup keys (default `"k"`).
        #[serde(default = "default_lookup_key_column")]
        key_column: String,
        /// Output column name for the lookup values (default `"v"`).
        #[serde(default = "default_lookup_value_column")]
        value_column: String,
    },
    /// A sub-query right side whose rows have already been materialised.
    ///
    /// The planner / executor runs the right sub-query first and feeds the
    /// resulting rows here; this keeps the join executor independent of the
    /// recursive query machinery.
    #[serde(rename = "rows")]
    Rows {
        /// Column names present across the materialised rows.
        column_names: Vec<String>,
        /// The materialised right rows.
        rows: Vec<Row>,
    },
}

fn default_lookup_key_column() -> String {
    "k".to_string()
}

fn default_lookup_value_column() -> String {
    "v".to_string()
}

// ---------------------------------------------------------------------------
// Join data source
// ---------------------------------------------------------------------------

/// A Druid-style `join` data source.
///
/// The left side is supplied to [`execute_join`] as already-materialised rows
/// (a scan over the left table / segment, or the output of an inner join when
/// chaining).  The right side is described by [`JoinRight`] and is materialised
/// in full into a hash table keyed on the join key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinDataSource {
    /// The right side of the join.
    pub right: JoinRight,
    /// Prefix applied to every right-side column in the joined output.
    pub right_prefix: String,
    /// The equi-join condition.
    pub condition: JoinCondition,
    /// The join type (INNER / LEFT).
    pub join_type: JoinType,
}

// ---------------------------------------------------------------------------
// Right-relation materialisation
// ---------------------------------------------------------------------------

/// A materialised right relation: prefixed column names plus a hash index of
/// the join key (as its canonical string form) to the matching right rows.
struct RightRelation {
    /// Prefixed column names (in stable order), used to null-fill LEFT misses.
    prefixed_columns: Vec<String>,
    /// Hash index: join-key string -> list of prefixed right rows.
    index: HashMap<String, Vec<Row>>,
}

/// Canonicalise a JSON value into the string form used as the hash-join key.
///
/// Equi-join keys compare by value; `10` (integer) and `"10"` (string) are
/// treated as equal keys so a numeric left column joins a stringly-typed
/// lookup / inline right column (matching Druid's stringly join semantics).
/// `NULL` keys never match (SQL semantics) and are represented as `None`.
fn join_key(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        // Arrays / objects are not valid scalar join keys.
        other => Some(other.to_string()),
    }
}

impl RightRelation {
    /// Build the right relation, applying `right_prefix` to every column and
    /// indexing on the (raw) `right_key` column.
    fn build(
        right: &JoinRight,
        right_prefix: &str,
        right_key: &str,
        lookups: &LookupManager,
    ) -> Result<Self> {
        let (column_names, rows): (Vec<String>, Vec<Row>) = match right {
            JoinRight::Inline { column_names, rows } => {
                let mut materialised = Vec::with_capacity(rows.len());
                for raw in rows {
                    if raw.len() != column_names.len() {
                        return Err(DruidError::Query(format!(
                            "join: inline right row has {} values but {} columns",
                            raw.len(),
                            column_names.len()
                        )));
                    }
                    let mut row: Row = HashMap::with_capacity(column_names.len());
                    for (name, val) in column_names.iter().zip(raw.iter()) {
                        row.insert(name.clone(), val.clone());
                    }
                    materialised.push(row);
                }
                (column_names.clone(), materialised)
            }
            JoinRight::Lookup {
                lookup,
                key_column,
                value_column,
            } => {
                let table = lookups.get(lookup).ok_or_else(|| {
                    DruidError::Query(format!("join: lookup \"{lookup}\" not found"))
                })?;
                let mut materialised = Vec::with_capacity(table.len());
                for key in table.keys() {
                    let value = table.get(&key).unwrap_or_default();
                    let mut row: Row = HashMap::with_capacity(2);
                    row.insert(key_column.clone(), serde_json::Value::String(key));
                    row.insert(value_column.clone(), serde_json::Value::String(value));
                    materialised.push(row);
                }
                (vec![key_column.clone(), value_column.clone()], materialised)
            }
            JoinRight::Rows { column_names, rows } => (column_names.clone(), rows.clone()),
        };

        // The join key must exist among the right columns.
        if !column_names.iter().any(|c| c == right_key) {
            return Err(DruidError::Query(format!(
                "join: right key \"{right_key}\" is not a column of the right relation"
            )));
        }

        let prefixed_columns: Vec<String> = column_names
            .iter()
            .map(|c| format!("{right_prefix}{c}"))
            .collect();

        let mut index: HashMap<String, Vec<Row>> = HashMap::new();
        for row in &rows {
            let key = match row.get(right_key).and_then(join_key) {
                Some(k) => k,
                // A right row with a NULL (or absent) join key can never match.
                None => continue,
            };
            let prefixed: Row = row
                .iter()
                .map(|(name, value)| (format!("{right_prefix}{name}"), value.clone()))
                .collect();
            index.entry(key).or_default().push(prefixed);
        }

        Ok(RightRelation {
            prefixed_columns,
            index,
        })
    }
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute a broadcast hash join.
///
/// `left_rows` is the already-materialised left side (column-name to value
/// maps).  The right relation described by `join` is materialised in full and
/// indexed on the join key; each left row probes the index and emits one joined
/// row per matching right row.  For a `LEFT` join, a left row with no match is
/// kept with every (prefixed) right column set to `NULL`.
///
/// Right columns are exposed under [`JoinDataSource::right_prefix`].  If a
/// prefixed right column name collides with a left column name, the left value
/// is preserved (the right value remains available only under its prefix, which
/// by construction differs — a collision can only occur if the caller chose an
/// empty/duplicating prefix, in which case left wins, matching Druid's
/// left-precedence).
pub fn execute_join(
    join: &JoinDataSource,
    left_rows: &[Row],
    lookups: &LookupManager,
) -> Result<Vec<Row>> {
    let right = RightRelation::build(
        &join.right,
        &join.right_prefix,
        &join.condition.right_key,
        lookups,
    )?;

    let mut out: Vec<Row> = Vec::new();
    for left in left_rows {
        let key = left.get(&join.condition.left_key).and_then(join_key);
        let matches = key.as_ref().and_then(|k| right.index.get(k));

        match matches {
            Some(right_rows) if !right_rows.is_empty() => {
                for r in right_rows {
                    let mut joined = left.clone();
                    for (name, value) in r {
                        // Left precedence on collision (see doc above).
                        joined.entry(name.clone()).or_insert_with(|| value.clone());
                    }
                    out.push(joined);
                }
            }
            _ => {
                if join.join_type == JoinType::Left {
                    let mut joined = left.clone();
                    for col in &right.prefixed_columns {
                        joined.entry(col.clone()).or_insert(serde_json::Value::Null);
                    }
                    out.push(joined);
                }
                // INNER: drop the unmatched left row.
            }
        }
    }

    Ok(out)
}

/// The full set of output column names produced by a join, in a stable order:
/// the left columns (in the order given) followed by the prefixed right
/// columns.  Useful for building a `ScanResult`-style header over a join.
pub fn join_output_columns(join: &JoinDataSource, left_columns: &[String]) -> Vec<String> {
    let mut cols: Vec<String> = left_columns.to_vec();
    let right_cols: Vec<String> = match &join.right {
        JoinRight::Inline { column_names, .. } => column_names.clone(),
        JoinRight::Lookup {
            key_column,
            value_column,
            ..
        } => vec![key_column.clone(), value_column.clone()],
        JoinRight::Rows { column_names, .. } => column_names.clone(),
    };
    for c in right_cols {
        let prefixed = format!("{}{}", join.right_prefix, c);
        if !cols.contains(&prefixed) {
            cols.push(prefixed);
        }
    }
    cols
}

// ---------------------------------------------------------------------------
// Segment-backed entry points
// ---------------------------------------------------------------------------

/// Materialise every row of `segment` as a column-name to value map.
///
/// `__time` is always included alongside the dimension and metric columns so a
/// join condition or downstream projection can reference it.
pub fn materialise_segment_rows(segment: &SegmentData) -> Vec<Row> {
    (0..segment.num_rows())
        .map(|row_idx| build_row(segment, row_idx))
        .collect()
}

/// Run a join whose left side is a scan over `left_segment`, returning a
/// [`ScanResult`] over the joined rows.
///
/// The left scan (filter, interval, column selection, ordering, limit/offset)
/// is applied first; its result rows form the join's left input.  The joined
/// rows are then projected to `columns` when supplied, otherwise to the full
/// left-then-prefixed-right column set.  This is the executor path that makes a
/// join feed a `scan` downstream.
pub fn execute_join_scan(
    join: &JoinDataSource,
    left_scan: &ScanQuery,
    left_segment: &SegmentData,
    lookups: &LookupManager,
    columns: Option<&[String]>,
) -> Result<ScanResult> {
    let left_result = left_scan.execute(left_segment)?;
    let left_rows: Vec<Row> = left_result.events;
    let joined = execute_join(join, &left_rows, lookups)?;

    let output_columns: Vec<String> = match columns {
        Some(cols) if !cols.is_empty() => cols.to_vec(),
        _ => join_output_columns(join, &left_result.columns),
    };

    let events: Vec<Row> = joined
        .into_iter()
        .map(|row| {
            let mut event: Row = HashMap::with_capacity(output_columns.len());
            for col in &output_columns {
                if let Some(val) = row.get(col) {
                    event.insert(col.clone(), val.clone());
                }
            }
            event
        })
        .collect();

    Ok(ScanResult {
        segment_id: None,
        columns: output_columns,
        events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn left_rows() -> Vec<Row> {
        // Three left rows; key column "cc".
        vec![
            HashMap::from([
                ("id".to_string(), json!(1)),
                ("cc".to_string(), json!("US")),
            ]),
            HashMap::from([
                ("id".to_string(), json!(2)),
                ("cc".to_string(), json!("JP")),
            ]),
            HashMap::from([
                ("id".to_string(), json!(3)),
                ("cc".to_string(), json!("XX")),
            ]),
        ]
    }

    fn inline_right() -> JoinRight {
        JoinRight::Inline {
            column_names: vec!["code".to_string(), "country".to_string()],
            rows: vec![
                vec![json!("US"), json!("United States")],
                vec![json!("JP"), json!("Japan")],
            ],
        }
    }

    #[test]
    fn inner_join_inline_basic() {
        let join = JoinDataSource {
            right: inline_right(),
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let out = execute_join(&join, &left_rows(), &lookups).expect("join");
        // US and JP match; XX is dropped (INNER).
        assert_eq!(out.len(), 2);
        let us = out
            .iter()
            .find(|r| r.get("cc") == Some(&json!("US")))
            .expect("us row");
        assert_eq!(us.get("r.country"), Some(&json!("United States")));
        assert_eq!(us.get("r.code"), Some(&json!("US")));
        assert!(out.iter().all(|r| r.get("cc") != Some(&json!("XX"))));
    }

    #[test]
    fn left_join_keeps_unmatched_with_null_right() {
        let join = JoinDataSource {
            right: inline_right(),
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Left,
        };
        let lookups = LookupManager::new();
        let out = execute_join(&join, &left_rows(), &lookups).expect("join");
        // All three left rows kept.
        assert_eq!(out.len(), 3);
        let xx = out
            .iter()
            .find(|r| r.get("cc") == Some(&json!("XX")))
            .expect("xx row");
        // Right columns are present but NULL.
        assert_eq!(xx.get("r.country"), Some(&serde_json::Value::Null));
        assert_eq!(xx.get("r.code"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn inner_join_multiple_matches_fans_out() {
        // Right side has two rows for key "US".
        let right = JoinRight::Inline {
            column_names: vec!["code".to_string(), "city".to_string()],
            rows: vec![
                vec![json!("US"), json!("NYC")],
                vec![json!("US"), json!("LA")],
                vec![json!("JP"), json!("Tokyo")],
            ],
        };
        let join = JoinDataSource {
            right,
            right_prefix: "c_".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let out = execute_join(&join, &left_rows(), &lookups).expect("join");
        // US fans out to 2 rows, JP to 1, XX dropped => 3 total.
        assert_eq!(out.len(), 3);
        let us_cities: Vec<&serde_json::Value> = out
            .iter()
            .filter(|r| r.get("cc") == Some(&json!("US")))
            .filter_map(|r| r.get("c_city"))
            .collect();
        assert_eq!(us_cities.len(), 2);
        assert!(us_cities.contains(&&json!("NYC")));
        assert!(us_cities.contains(&&json!("LA")));
    }

    #[test]
    fn lookup_join_enriches_dimension() {
        let lookups = LookupManager::new();
        let table = ferrodruid_lookup::LookupTable::new("cc_name".to_string(), "v1".to_string());
        table.put("US".to_string(), "United States".to_string());
        table.put("JP".to_string(), "Japan".to_string());
        lookups.register(table);

        let join = JoinDataSource {
            right: JoinRight::Lookup {
                lookup: "cc_name".to_string(),
                key_column: "k".to_string(),
                value_column: "v".to_string(),
            },
            right_prefix: "j.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "k".to_string(),
            },
            join_type: JoinType::Left,
        };
        let out = execute_join(&join, &left_rows(), &lookups).expect("join");
        assert_eq!(out.len(), 3);
        let us = out
            .iter()
            .find(|r| r.get("cc") == Some(&json!("US")))
            .expect("us");
        assert_eq!(us.get("j.v"), Some(&json!("United States")));
        let xx = out
            .iter()
            .find(|r| r.get("cc") == Some(&json!("XX")))
            .expect("xx");
        assert_eq!(us.get("j.k"), Some(&json!("US")));
        assert_eq!(xx.get("j.v"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn numeric_left_key_joins_string_right_key() {
        // Left key is numeric; right key is a string spelling of the same code.
        let left = vec![HashMap::from([("code".to_string(), json!(10))])];
        let join = JoinDataSource {
            right: JoinRight::Inline {
                column_names: vec!["rc".to_string(), "label".to_string()],
                rows: vec![vec![json!("10"), json!("ten")]],
            },
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "code".to_string(),
                right_key: "rc".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let out = execute_join(&join, &left, &lookups).expect("join");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("r.label"), Some(&json!("ten")));
    }

    #[test]
    fn rows_right_side_from_subquery() {
        let right = JoinRight::Rows {
            column_names: vec!["code".to_string(), "n".to_string()],
            rows: vec![HashMap::from([
                ("code".to_string(), json!("US")),
                ("n".to_string(), json!(42)),
            ])],
        };
        let join = JoinDataSource {
            right,
            right_prefix: "s.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let out = execute_join(&join, &left_rows(), &lookups).expect("join");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("s.n"), Some(&json!(42)));
    }

    #[test]
    fn null_left_key_never_matches() {
        let left = vec![HashMap::from([("cc".to_string(), serde_json::Value::Null)])];
        let join = JoinDataSource {
            right: inline_right(),
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let out = execute_join(&join, &left, &lookups).expect("join");
        assert!(out.is_empty());
    }

    #[test]
    fn missing_right_key_column_errors() {
        let join = JoinDataSource {
            right: inline_right(),
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "nonexistent".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let err = execute_join(&join, &left_rows(), &lookups).unwrap_err();
        assert!(err.to_string().contains("right key"));
    }

    #[test]
    fn join_type_parse_rejects_right_and_full() {
        assert_eq!(JoinType::parse("inner").expect("inner"), JoinType::Inner);
        assert_eq!(JoinType::parse("LEFT").expect("left"), JoinType::Left);
        assert!(JoinType::parse("RIGHT").is_err());
        assert!(JoinType::parse("FULL").is_err());
        assert!(JoinType::parse("bogus").is_err());
    }

    #[test]
    fn output_columns_left_then_prefixed_right() {
        let join = JoinDataSource {
            right: inline_right(),
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let cols = join_output_columns(&join, &["id".to_string(), "cc".to_string()]);
        assert_eq!(
            cols,
            vec![
                "id".to_string(),
                "cc".to_string(),
                "r.code".to_string(),
                "r.country".to_string(),
            ]
        );
    }

    // -----------------------------------------------------------------------
    // Segment-backed join tests (left side = a scan over a segment).
    // -----------------------------------------------------------------------

    /// Build a small segment with a string dimension `cc` and a metric `value`.
    ///
    /// Rows: (cc=US, value=10), (cc=JP, value=20), (cc=US, value=30),
    /// (cc=XX, value=40).
    fn build_left_segment() -> SegmentData {
        use ferrodruid_bitmap::DruidBitmap;
        use ferrodruid_dict::FrontCodedDictionary;
        use ferrodruid_segment::Interval;
        use ferrodruid_segment::column::{ColumnData, StringColumnData};

        let timestamps = vec![1000i64, 1000, 1000, 1000];
        // dictionary sorted: JP=0, US=1, XX=2
        let dict = FrontCodedDictionary::from_sorted(vec![
            "JP".to_string(),
            "US".to_string(),
            "XX".to_string(),
        ]);
        let encoded_values = vec![1u32, 0, 1, 2]; // US, JP, US, XX
        let mut bm_jp = DruidBitmap::new();
        bm_jp.insert(1);
        let mut bm_us = DruidBitmap::new();
        bm_us.insert(0);
        bm_us.insert(2);
        let mut bm_xx = DruidBitmap::new();
        bm_xx.insert(3);
        let cc_col = ColumnData::String(StringColumnData {
            dictionary: dict,
            encoded_values,
            bitmap_indexes: vec![bm_jp, bm_us, bm_xx],
        });
        let value_col = ColumnData::Double(vec![10.0, 20.0, 30.0, 40.0]);

        let mut columns = HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("cc".to_string(), cc_col);
        columns.insert("value".to_string(), value_col);

        SegmentData {
            version: 9,
            num_rows: 4,
            interval: Interval {
                start_millis: 0,
                end_millis: 2000,
            },
            dimensions: vec!["cc".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: false,
        }
    }

    fn scan_all() -> ScanQuery {
        ScanQuery {
            data_source: ferrodruid_common::types::DataSource::Table {
                name: "left".to_string(),
            },
            intervals: vec!["1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z".to_string()],
            filter: None,
            virtual_columns: None,
            columns: None,
            limit: None,
            offset: None,
            order: Some("none".to_string()),
            result_format: None,
            context: None,
        }
    }

    #[test]
    fn execute_join_scan_left_join_over_segment() {
        let segment = build_left_segment();
        let join = JoinDataSource {
            right: JoinRight::Inline {
                column_names: vec!["code".to_string(), "country".to_string()],
                rows: vec![
                    vec![json!("US"), json!("United States")],
                    vec![json!("JP"), json!("Japan")],
                ],
            },
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Left,
        };
        let lookups = LookupManager::new();
        let result =
            execute_join_scan(&join, &scan_all(), &segment, &lookups, None).expect("join scan");
        // 4 left rows, all kept (LEFT). XX has NULL right columns.
        assert_eq!(result.events.len(), 4);
        let xx = result
            .events
            .iter()
            .find(|e| e.get("cc") == Some(&json!("XX")))
            .expect("xx row");
        assert_eq!(xx.get("r.country"), Some(&serde_json::Value::Null));
        let us_rows: Vec<&Row> = result
            .events
            .iter()
            .filter(|e| e.get("cc") == Some(&json!("US")))
            .collect();
        assert_eq!(us_rows.len(), 2);
        assert!(
            us_rows
                .iter()
                .all(|r| r.get("r.country") == Some(&json!("United States")))
        );
    }

    #[test]
    fn join_result_feeds_groupby() {
        use crate::groupby::GroupByQuery;
        use ferrodruid_bitmap::DruidBitmap;
        use ferrodruid_dict::FrontCodedDictionary;
        use ferrodruid_segment::Interval;
        use ferrodruid_segment::column::{ColumnData, StringColumnData};

        // Inner-join the segment to a country lookup, then group the joined
        // rows by the enriched country name and sum `value`.
        let segment = build_left_segment();
        let join = JoinDataSource {
            right: JoinRight::Inline {
                column_names: vec!["code".to_string(), "country".to_string()],
                rows: vec![
                    vec![json!("US"), json!("United States")],
                    vec![json!("JP"), json!("Japan")],
                ],
            },
            right_prefix: "r.".to_string(),
            condition: JoinCondition {
                left_key: "cc".to_string(),
                right_key: "code".to_string(),
            },
            join_type: JoinType::Inner,
        };
        let lookups = LookupManager::new();
        let joined = execute_join_scan(&join, &scan_all(), &segment, &lookups, None)
            .expect("join scan")
            .events;
        // 3 matched rows: 2x US + 1x JP (XX dropped by INNER).
        assert_eq!(joined.len(), 3);

        // Rebuild a segment from the joined rows so a real GroupBy can run over
        // the enriched `r.country` dimension — this proves the join output is
        // usable downstream by the existing groupBy executor.
        let countries: Vec<String> = joined
            .iter()
            .map(|r| {
                r.get("r.country")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        let values: Vec<f64> = joined
            .iter()
            .map(|r| r.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0))
            .collect();

        // dictionary sorted: Japan=0, United States=1
        let dict = FrontCodedDictionary::from_sorted(vec![
            "Japan".to_string(),
            "United States".to_string(),
        ]);
        let encoded: Vec<u32> = countries
            .iter()
            .map(|c| if c == "Japan" { 0 } else { 1 })
            .collect();
        let mut bm_jp = DruidBitmap::new();
        let mut bm_us = DruidBitmap::new();
        for (i, c) in countries.iter().enumerate() {
            if c == "Japan" {
                bm_jp.insert(i as u32);
            } else {
                bm_us.insert(i as u32);
            }
        }
        let mut columns = HashMap::new();
        columns.insert(
            "__time".to_string(),
            ColumnData::Long(vec![1000i64; countries.len()]),
        );
        columns.insert(
            "r.country".to_string(),
            ColumnData::String(StringColumnData {
                dictionary: dict,
                encoded_values: encoded,
                bitmap_indexes: vec![bm_jp, bm_us],
            }),
        );
        columns.insert("value".to_string(), ColumnData::Double(values));
        let joined_segment = SegmentData {
            version: 9,
            num_rows: countries.len(),
            interval: Interval {
                start_millis: 0,
                end_millis: 2000,
            },
            dimensions: vec!["r.country".to_string()],
            metrics: vec!["value".to_string()],
            columns,
            time_sorted: false,
        };

        let gb_json = r#"{
            "queryType": "groupBy",
            "dataSource": {"type":"table","name":"joined"},
            "intervals": ["1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
            "granularity": "all",
            "dimensions": [{"type":"default","dimension":"r.country","output_name":"country","output_type":"STRING"}],
            "aggregations": [{"type":"doubleSum","name":"total","fieldName":"value"}]
        }"#;
        let gb: GroupByQuery = serde_json::from_str(gb_json).expect("parse groupBy");
        let results = gb.execute(&joined_segment).expect("groupBy");
        // United States: 10 + 30 = 40; Japan: 20.
        let us = results
            .iter()
            .find(|r| r.event.get("country") == Some(&json!("United States")))
            .expect("us group");
        assert_eq!(us.event.get("total"), Some(&json!(40.0)));
        let jp = results
            .iter()
            .find(|r| r.event.get("country") == Some(&json!("Japan")))
            .expect("jp group");
        assert_eq!(jp.event.get("total"), Some(&json!(20.0)));
    }
}
