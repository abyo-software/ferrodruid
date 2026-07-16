// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Scan query type — raw row scan with optional filter and column selection.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::DataSource;
use ferrodruid_segment::SegmentData;

use crate::context::QueryContext;
use crate::filter::FilterSpec;
use crate::helpers::{
    build_row_prealloc, build_row_update_only, deserialize_intervals, parse_intervals,
};
use crate::virtual_columns::{VirtualColumnSpec, VirtualColumns};

// ---------------------------------------------------------------------------
// Query spec
// ---------------------------------------------------------------------------

/// A Druid Scan query.
///
/// Scan queries return raw rows (optionally filtered and column-selected).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanQuery {
    /// The data source to query.
    pub data_source: DataSource,
    /// Time intervals to query over.
    ///
    /// Accepts both a single ISO `"start/end"` string and an array of
    /// such strings (TG-4-finding-001, W2-D pydruid/druid-go compat).
    #[serde(deserialize_with = "deserialize_intervals")]
    pub intervals: Vec<String>,
    /// Optional filter.
    #[serde(default)]
    pub filter: Option<FilterSpec>,
    /// Optional virtual columns (derived columns computed by expression).
    #[serde(default)]
    pub virtual_columns: Option<Vec<VirtualColumnSpec>>,
    /// Optional list of columns to return.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Maximum number of rows to return.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Number of rows to skip from the beginning.
    #[serde(default)]
    pub offset: Option<usize>,
    /// Row ordering: `"ascending"`, `"descending"`, or `"none"`.
    #[serde(default)]
    pub order: Option<String>,
    /// Result format: `"list"` or `"compactedList"`.
    #[serde(default)]
    pub result_format: Option<String>,
    /// Optional query context.
    #[serde(default)]
    pub context: Option<QueryContext>,
}

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

/// The result of a Scan query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanResult {
    /// Segment identifier.
    #[serde(default)]
    pub segment_id: Option<String>,
    /// Column names in the result.
    pub columns: Vec<String>,
    /// Result rows (each row is a map of column name to value).
    pub events: Vec<HashMap<String, serde_json::Value>>,
}

/// Positionally align a `UNION ALL` branch's scan result to the first
/// branch's column names.
///
/// Apache Druid names a `UNION ALL`'s output columns from the FIRST branch
/// and maps every later branch's columns into them by POSITION. Because
/// FerroDruid's scan rows are keyed by native column name, aligning a branch
/// means renaming its `columns[i]` (and each event's key `columns[i]`) to
/// `target_columns[i]`. After alignment every branch shares the first
/// branch's column keys, so branch concatenation is correct and the wire
/// projection (named from the first branch) emits every branch's values.
///
/// A branch whose column count differs from the first is a genuine
/// `UNION ALL` error (fail-closed). The common same-schema case (identical
/// columns, including `SELECT *` over the same shape) is a no-op.
///
/// # Errors
/// Returns [`DruidError::Query`] if the branch has a different number of
/// columns than `target_columns`.
pub fn align_union_branch(target_columns: &[String], branch: &mut ScanResult) -> Result<()> {
    if branch.columns.len() != target_columns.len() {
        return Err(DruidError::Query(format!(
            "UNION ALL branch has {} column(s) {:?} but the first branch has {} column(s) {:?}; \
             every branch must project the same number of columns",
            branch.columns.len(),
            branch.columns,
            target_columns.len(),
            target_columns,
        )));
    }
    // Fast path: already aligned (same names in the same order).
    if branch
        .columns
        .iter()
        .zip(target_columns)
        .all(|(have, want)| have == want)
    {
        return Ok(());
    }
    // Rebuild each row by MOVING the branch's i-th native column value under
    // the first branch's i-th column name. Removing from the taken original
    // row (rather than renaming in place) is correct even when the mapping is
    // a permutation of overlapping names, and moves the values instead of
    // cloning them (union scans are unbounded, so rows can be large).
    let source_columns = std::mem::take(&mut branch.columns);
    for event in &mut branch.events {
        let mut source_row = std::mem::take(event);
        let mut remapped = HashMap::with_capacity(target_columns.len());
        for (source, target) in source_columns.iter().zip(target_columns) {
            if let Some(value) = source_row.remove(source) {
                remapped.insert(target.clone(), value);
            }
        }
        *event = remapped;
    }
    branch.columns = target_columns.to_vec();
    Ok(())
}

impl ScanResult {
    /// Project this result into the Druid `compactedList` wire shape.
    ///
    /// Druid's `compactedList` result format emits, per segment, an object
    /// of the form:
    ///
    /// ```json
    /// {"columns": ["__time", "a", "b"], "events": [[t0, a0, b0], [t1, a1, b1]]}
    /// ```
    ///
    /// — a single `columns` header followed by each row as a JSON array of
    /// values *in column order* (rather than the `list` format's per-row
    /// object).  A column missing from a given event is emitted as JSON
    /// `null` so every inner array has the same arity as `columns`.
    ///
    /// The in-memory [`ScanResult`] always stores map-shaped `events` (so
    /// the broker merge and the existing `list` serialization are
    /// unaffected); this method performs the column-ordered projection on
    /// demand.
    #[must_use]
    pub fn compacted_value(&self) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = self
            .events
            .iter()
            .map(|event| {
                let values: Vec<serde_json::Value> = self
                    .columns
                    .iter()
                    .map(|col| event.get(col).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                serde_json::Value::Array(values)
            })
            .collect();
        let mut obj = serde_json::Map::new();
        if let Some(segment_id) = &self.segment_id {
            obj.insert(
                "segmentId".to_string(),
                serde_json::Value::String(segment_id.clone()),
            );
        }
        obj.insert(
            "columns".to_string(),
            serde_json::Value::Array(
                self.columns
                    .iter()
                    .map(|c| serde_json::Value::String(c.clone()))
                    .collect(),
            ),
        );
        obj.insert("events".to_string(), serde_json::Value::Array(rows));
        serde_json::Value::Object(obj)
    }
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl ScanQuery {
    /// Execute this Scan query against a segment.
    ///
    /// Wave 45-A (Wave 37B query Medium #3 + Medium #4):
    ///
    /// * `result_format` is now validated.  Druid recognises `"list"` and
    ///   `"compactedList"`; anything else (including misspellings such as
    ///   `"compactList"`) is rejected with [`DruidError::Query`] rather
    ///   than silently downgrading to `"list"`.  The returned [`ScanResult`]
    ///   always carries the same `columns` + map-shaped `events`; callers
    ///   wanting the `compactedList` wire shape (a `columns` header plus
    ///   rows as column-ordered value arrays) call
    ///   [`ScanResult::compacted_value`] to project it.
    /// * When `order` is `"none"` (or any non-time-ordered value), the
    ///   executor short-circuits as soon as `offset + limit` matching
    ///   rows have been collected, so a small `limit` no longer forces
    ///   the segment-wide row buffer to fill up before truncation.  The
    ///   ascending/descending paths still buffer (sort must see all
    ///   matches) but trim before materialising events.
    pub fn execute(&self, segment: &SegmentData) -> Result<ScanResult> {
        // Wave 45-A: reject unsupported `resultFormat`.
        if let Some(fmt) = self.result_format.as_deref()
            && !matches!(fmt, "list" | "compactedList")
        {
            return Err(DruidError::Query(format!(
                "Scan: unsupported resultFormat \"{fmt}\"; expected \"list\" or \"compactedList\""
            )));
        }

        // DD R43 (Finding 6): an unrecognised `order` value (e.g. "sideways")
        // was silently treated as insertion order, so the rows came back in an
        // arbitrary (segment-storage) order instead of failing. Reject any
        // order that is not one of Druid's documented values up front.
        if let Some(order) = self.order.as_deref()
            && !matches!(order, "ascending" | "descending" | "none")
        {
            return Err(DruidError::Query(format!(
                "Scan: unsupported order \"{order}\"; expected \"ascending\", \"descending\", \
                 or \"none\""
            )));
        }

        // DD R40: reject a malformed expression filter up front instead of
        // letting it silently match every row (fail-open data exposure).
        if let Some(ref filter) = self.filter {
            filter.validate()?;
        }

        let virtual_columns = VirtualColumns::compile(&self.virtual_columns)?;

        let intervals = parse_intervals(&self.intervals)?;
        let timestamps = segment.timestamp_column()?;

        // Determine which columns to include.  Virtual columns are appended
        // to the default column set so an unqualified scan surfaces them.
        let all_columns: Vec<String> = {
            let mut cols = vec!["__time".to_string()];
            cols.extend(segment.dimensions.iter().cloned());
            cols.extend(segment.metrics.iter().cloned());
            for name in virtual_columns.names() {
                if !cols.iter().any(|c| c == name) {
                    cols.push(name.to_string());
                }
            }
            cols
        };
        let output_columns: Vec<String> = match &self.columns {
            Some(requested) if !requested.is_empty() => requested.clone(),
            _ => all_columns.clone(),
        };

        // Wave 45-A: when `order` is "none" (or any non-time-ordered
        // value) we can short-circuit collection at `offset + limit` so
        // an oversized segment doesn't dictate `Vec` allocation.  For
        // ascending/descending we still need every match to sort
        // correctly.
        let order_is_time_sorted = matches!(self.order.as_deref(), Some("descending") | None)
            || self.order.as_deref() == Some("ascending");
        let early_cutoff = if order_is_time_sorted {
            None
        } else {
            // The "none" branch — and any unrecognised value (today the
            // executor falls through and "keeps insertion order").  Bound
            // collection to `offset + limit` rows.
            self.limit
                .map(|lim| self.offset.unwrap_or(0).saturating_add(lim))
        };

        // Collect matching row indices.
        // W3-SL1-B step 1: hoist row-map allocation out of the loop.
        let mut row_indices: Vec<usize> = Vec::new();
        let mut row = build_row_prealloc(segment);
        for (row_idx, &ts) in timestamps.iter().enumerate().take(segment.num_rows()) {
            if !intervals.is_empty()
                && !intervals
                    .iter()
                    .any(|(start, end)| ts >= *start && ts < *end)
            {
                continue;
            }

            if let Some(ref filter) = self.filter {
                build_row_update_only(segment, row_idx, &mut row);
                virtual_columns.augment_row(&mut row);
                if !filter.matches(&row) {
                    continue;
                }
            }

            row_indices.push(row_idx);
            if let Some(cap) = early_cutoff
                && row_indices.len() >= cap
            {
                break;
            }
        }

        // Apply ordering.
        match self.order.as_deref() {
            Some("descending") => row_indices.sort_by(|a, b| timestamps[*b].cmp(&timestamps[*a])),
            Some("ascending") | None => {
                row_indices.sort_by(|a, b| timestamps[*a].cmp(&timestamps[*b]))
            }
            _ => {} // "none" — keep insertion order
        }

        // Apply offset.
        let offset = self.offset.unwrap_or(0);
        if offset > 0 && offset < row_indices.len() {
            row_indices = row_indices[offset..].to_vec();
        } else if offset >= row_indices.len() {
            row_indices.clear();
        }

        // Apply limit.
        if let Some(limit) = self.limit {
            row_indices.truncate(limit);
        }

        // Build events.
        // W3-SL1-B step 1: reuse `full_row` allocation across rows.
        let mut events = Vec::with_capacity(row_indices.len());
        let mut full_row = build_row_prealloc(segment);
        for row_idx in row_indices {
            build_row_update_only(segment, row_idx, &mut full_row);
            virtual_columns.augment_row(&mut full_row);
            let mut event = HashMap::new();
            for col in &output_columns {
                if let Some(val) = full_row.get(col) {
                    event.insert(col.clone(), val.clone());
                }
            }
            events.push(event);
        }

        Ok(ScanResult {
            segment_id: None,
            columns: output_columns,
            events,
        })
    }

    /// Execute this scan and project the result into the Druid
    /// `compactedList` wire shape (see [`ScanResult::compacted_value`]).
    ///
    /// This is a convenience for callers that requested
    /// `resultFormat == "compactedList"`; it runs the same execution path
    /// as [`Self::execute`] (so filter / virtual-column / ordering /
    /// limit semantics are identical) and projects column-ordered value
    /// arrays on the resulting rows.
    pub fn execute_compacted(&self, segment: &SegmentData) -> Result<serde_json::Value> {
        Ok(self.execute(segment)?.compacted_value())
    }
}

// ---------------------------------------------------------------------------
// Wave 45-A regression tests (Wave 37B query Medium #3 + Medium #4)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_segment::Interval;
    use ferrodruid_segment::column::ColumnData;

    fn make_segment(num_rows: usize) -> SegmentData {
        let timestamps: Vec<i64> = (0..num_rows as i64).map(|i| (i + 1) * 100).collect();
        let codes: Vec<i64> = (0..num_rows as i64).collect();
        let mut columns = std::collections::HashMap::new();
        columns.insert("__time".to_string(), ColumnData::Long(timestamps));
        columns.insert("code".to_string(), ColumnData::Long(codes));
        SegmentData {
            version: 9,
            num_rows,
            interval: Interval {
                start_millis: 0,
                end_millis: ((num_rows as i64) + 1) * 100,
            },
            dimensions: vec!["code".to_string()],
            metrics: vec![],
            columns,
            time_sorted: false,
        }
    }

    /// Wave 45-A regression for Wave 37B query Medium #3: `resultFormat`
    /// must be validated; an unrecognised value (e.g. `"compactList"`)
    /// must error rather than silently returning `"list"` shape.
    #[test]
    fn scan_rejects_unknown_result_format() {
        let segment = make_segment(3);
        let q = ScanQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: None,
            limit: None,
            offset: None,
            order: Some("none".into()),
            result_format: Some("compactList".into()), // typo
            context: None,
        };
        let err = q.execute(&segment).expect_err("must reject typo");
        match err {
            DruidError::Query(msg) => {
                assert!(msg.contains("unsupported resultFormat"), "msg = {msg}")
            }
            other => panic!("expected DruidError::Query, got {other:?}"),
        }
    }

    /// DD R43 (Finding 6): an unknown scan `order` value (e.g. "sideways")
    /// was silently treated as insertion order, returning rows in an
    /// arbitrary storage order. It must now be rejected.
    #[test]
    fn scan_rejects_unknown_order() {
        let segment = make_segment(3);
        let q = ScanQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: None,
            limit: None,
            offset: None,
            order: Some("sideways".into()),
            result_format: None,
            context: None,
        };
        let err = q.execute(&segment).expect_err("must reject unknown order");
        match err {
            DruidError::Query(msg) => assert!(msg.contains("unsupported order"), "msg = {msg}"),
            other => panic!("expected DruidError::Query, got {other:?}"),
        }
        // The documented values still execute.
        for order in ["ascending", "descending", "none"] {
            let mut ok = q.clone();
            ok.order = Some(order.into());
            assert!(ok.execute(&segment).is_ok(), "order {order} must execute");
        }
    }

    /// Wave 45-A: `compactedList` is recognised as a *valid* Druid
    /// resultFormat name but is not yet implemented; rejecting it
    /// explicitly is safer than silently returning the `"list"` shape.
    /// `compactedList` is now implemented: it must be accepted by
    /// `execute` (returning the list-shape result) and projectable to the
    /// `columns` header + column-ordered value-array shape via
    /// `compacted_value` / `execute_compacted`.
    #[test]
    fn scan_compacted_list_shape_and_values() {
        let segment = make_segment(3);
        let q = ScanQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: Some(vec!["__time".into(), "code".into()]),
            limit: None,
            offset: None,
            order: Some("ascending".into()),
            result_format: Some("compactedList".into()),
            context: None,
        };
        // execute() must accept compactedList (no longer reject).
        let list = q.execute(&segment).expect("execute compactedList");
        assert_eq!(list.events.len(), 3);

        // The compacted projection: a `columns` header + rows as arrays.
        let compacted = q.execute_compacted(&segment).expect("execute_compacted");
        let obj = compacted.as_object().expect("object");
        assert_eq!(
            obj.get("columns"),
            Some(&serde_json::json!(["__time", "code"]))
        );
        let events = obj
            .get("events")
            .and_then(|v| v.as_array())
            .expect("events array");
        assert_eq!(events.len(), 3);
        // Each event is a value array in column order: [__time, code].
        // make_segment: __time = (i+1)*100, code = i.
        assert_eq!(events[0], serde_json::json!([100, 0]));
        assert_eq!(events[1], serde_json::json!([200, 1]));
        assert_eq!(events[2], serde_json::json!([300, 2]));
        // Every inner array has the same arity as `columns`.
        for ev in events {
            assert_eq!(ev.as_array().map(Vec::len), Some(2));
        }
    }

    /// A virtual column must be computable per-row, surfaced in an
    /// unqualified scan's columns, and usable in a scan filter.
    #[test]
    fn scan_virtual_column_computed_and_filterable() {
        use ferrodruid_segment::SegmentDataBuilder;
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![100, 200, 300])
            .add_double_column("val", true, vec![5.0, 15.0, 25.0])
            .build()
            .expect("build segment");

        // Filter on the virtual column `big = val > 10`; expect 2 rows
        // (15, 25), each carrying the derived `val2 = val * 2`.
        let q: ScanQuery = serde_json::from_str(
            r#"{
                "dataSource": {"type":"table","name":"t"},
                "intervals": [],
                "order": "ascending",
                "virtualColumns": [
                    {"type":"expression","name":"big","expression":"val > 10"},
                    {"type":"expression","name":"val2","expression":"val * 2"}
                ],
                "filter": {"type":"selector","dimension":"big","value":true}
            }"#,
        )
        .expect("parse scan");
        let r = q.execute(&segment).expect("execute");
        assert_eq!(r.events.len(), 2);
        // Unqualified scan surfaces the virtual columns.
        assert!(r.columns.iter().any(|c| c == "val2"));
        assert!(r.columns.iter().any(|c| c == "big"));
        let val2s: Vec<f64> = r
            .events
            .iter()
            .filter_map(|e| e.get("val2").and_then(serde_json::Value::as_f64))
            .collect();
        assert_eq!(val2s, vec![30.0, 50.0]);
    }

    /// Wave 45-A: `result_format == "list"` is the only currently-supported
    /// shape and must NOT be rejected.  Sanity test so the validation
    /// closure does not regress the happy path.
    #[test]
    fn scan_accepts_explicit_list_result_format() {
        let segment = make_segment(3);
        let q = ScanQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: None,
            limit: None,
            offset: None,
            order: Some("none".into()),
            result_format: Some("list".into()),
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        assert_eq!(r.events.len(), 3);
    }

    /// Wave 45-A regression for Wave 37B query Medium #4: when `order`
    /// is `"none"` the executor must short-circuit collection at
    /// `offset + limit` rows so a small client limit doesn't
    /// materialise the full segment.
    ///
    /// We can't directly observe the early break from outside, but we
    /// can prove that the behaviour is stable when offset+limit is much
    /// smaller than the segment row count.
    #[test]
    fn scan_order_none_with_small_limit_returns_only_requested_rows() {
        // 1000 rows; ask for offset=2, limit=3 → 3 events.
        let segment = make_segment(1000);
        let q = ScanQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: Some(vec!["code".into()]),
            limit: Some(3),
            offset: Some(2),
            order: Some("none".into()),
            result_format: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        assert_eq!(r.events.len(), 3, "limit=3 must be honoured");
        // First three matches in segment order are codes 0,1,2; with
        // offset=2 the visible codes must be 2,3,4.
        let codes: Vec<i64> = r
            .events
            .iter()
            .filter_map(|m| m.get("code").and_then(|v| v.as_i64()))
            .collect();
        assert_eq!(codes, vec![2_i64, 3_i64, 4_i64]);
    }

    /// Wave 45-A: ascending order still buffers all matches because the
    /// sort cannot be early-terminated; this test guards against a
    /// regression that would short-circuit and break sorted output.
    #[test]
    fn scan_order_ascending_ignores_early_cutoff() {
        let segment = make_segment(5);
        let q = ScanQuery {
            data_source: DataSource::Table {
                name: "wiki".into(),
            },
            intervals: vec![],
            filter: None,
            virtual_columns: None,
            columns: Some(vec!["code".into()]),
            limit: Some(2),
            offset: Some(0),
            order: Some("ascending".into()),
            result_format: None,
            context: None,
        };
        let r = q.execute(&segment).expect("execute");
        // 5 rows with __time = 100, 200, 300, 400, 500 — ascending sort
        // is the same as insertion order, so the first two codes are 0
        // and 1.
        let codes: Vec<i64> = r
            .events
            .iter()
            .filter_map(|m| m.get("code").and_then(|v| v.as_i64()))
            .collect();
        assert_eq!(codes, vec![0_i64, 1_i64]);
    }

    fn scan_result(columns: &[&str], rows: Vec<Vec<serde_json::Value>>) -> ScanResult {
        let columns: Vec<String> = columns.iter().map(|c| (*c).to_string()).collect();
        let events = rows
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .cloned()
                    .zip(row)
                    .collect::<HashMap<String, serde_json::Value>>()
            })
            .collect();
        ScanResult {
            segment_id: None,
            columns,
            events,
        }
    }

    #[test]
    fn align_union_branch_renames_by_position() {
        // Branch reads `revenue`; align it to the first branch's `city`.
        let mut branch = scan_result(&["revenue"], vec![vec![serde_json::json!(42)]]);
        align_union_branch(&["city".to_string()], &mut branch).expect("align");
        assert_eq!(branch.columns, vec!["city".to_string()]);
        assert_eq!(branch.events[0].get("city"), Some(&serde_json::json!(42)));
        assert!(!branch.events[0].contains_key("revenue"));
    }

    #[test]
    fn align_union_branch_handles_column_permutation() {
        // Positional swap: branch [b, a] -> target [a, b]; values follow
        // position, not name.
        let mut branch = scan_result(
            &["b", "a"],
            vec![vec![serde_json::json!("B0"), serde_json::json!("A0")]],
        );
        align_union_branch(&["a".to_string(), "b".to_string()], &mut branch).expect("align");
        assert_eq!(branch.events[0].get("a"), Some(&serde_json::json!("B0")));
        assert_eq!(branch.events[0].get("b"), Some(&serde_json::json!("A0")));
    }

    #[test]
    fn align_union_branch_same_columns_is_noop() {
        let mut branch = scan_result(&["city"], vec![vec![serde_json::json!("x")]]);
        align_union_branch(&["city".to_string()], &mut branch).expect("align");
        assert_eq!(branch.columns, vec!["city".to_string()]);
        assert_eq!(branch.events[0].get("city"), Some(&serde_json::json!("x")));
    }

    #[test]
    fn align_union_branch_rejects_arity_mismatch() {
        let mut branch = scan_result(&["a", "b"], vec![]);
        let err = align_union_branch(&["a".to_string()], &mut branch)
            .expect_err("different column count must fail closed");
        assert!(format!("{err}").contains("UNION ALL"));
    }
}
