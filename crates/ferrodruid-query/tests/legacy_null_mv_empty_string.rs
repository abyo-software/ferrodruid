// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — multi-value `''` element canonicalization (H2).
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! Under legacy semantics `''` IS the null string, and the scalar
//! string read path already canonicalizes a `''` cell to JSON null
//! (`column_value_at`).  A multi-value (`StringMulti`) cell must
//! canonicalize its `''` ELEMENTS the same way — otherwise a legacy
//! `selector(mv, '')` / IS NULL silently misses `[""]` rows and native
//! scan leaks `""` where a legacy Druid renders null:
//!
//! * a singleton `[""]` row reads as canonical JSON null (identical to
//!   a scalar `''` cell);
//! * a multi-element row's `''` element reads as a null ELEMENT, so the
//!   merged `''`/null selector / IN entry / null filter match it by the
//!   usual any-element rule, and native scan / groupBy explosion render
//!   the element as null;
//! * MV explosion and any-element semantics are otherwise unchanged.
//!
//! Documented residual (consistent scalar-vs-MV, not a silent
//! inconsistency): the string-domain predicates (bound / like / regex /
//! search / bloom) treat a canonicalized `''` element exactly like the
//! scalar path treats a canonical-null cell — it contributes no
//! candidate string.  Likewise `MV_FILTER_ONLY` / `MV_FILTER_NONE`
//! (modern-SQL-era functions) keep their ANSI element comparison.

use std::collections::HashMap;

use ferrodruid_query::{FilterSpec, GroupByQuery, ScanQuery, column_value_at};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};
use serde_json::{Value, json};

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

const INTERVAL: &str = "1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z";

/// 4 rows, `__time` 0..=3 identifies them:
///
/// | row (`__time`) | tags        | reads as (legacy)   |
/// |----------------|-------------|---------------------|
/// | 0              | `[""]`      | null (canonical)    |
/// | 1              | `["a", ""]` | `["a", null]`       |
/// | 2              | `["b"]`     | `"b"`               |
/// | 3              | `[]`        | null (empty MV row) |
fn segment() -> SegmentData {
    SegmentDataBuilder::new()
        .add_timestamp_column(vec![0, 1, 2, 3])
        .add_string_multi_column(
            "tags",
            vec![
                vec![String::new()],
                vec!["a".to_string(), String::new()],
                vec!["b".to_string()],
                vec![],
            ],
        )
        .build()
        .expect("segment builds")
}

fn scan_with_filter(filter: Option<Value>) -> ScanQuery {
    let mut q = json!({
        "queryType": "scan",
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "columns": ["__time", "tags"],
        "resultFormat": "list",
    });
    if let Some(f) = filter {
        q["filter"] = f;
    }
    serde_json::from_value(q).expect("scan query parses")
}

/// Run a filtered scan and return the matched rows' `__time` ids.
fn matched_times(filter: Value) -> Vec<i64> {
    let res = scan_with_filter(Some(filter))
        .execute(&segment())
        .expect("scan executes");
    let mut times: Vec<i64> = res
        .events
        .iter()
        .map(|e| {
            e.get("__time")
                .and_then(Value::as_i64)
                .expect("__time present")
        })
        .collect();
    times.sort_unstable();
    times
}

/// The read-side canonicalization itself: a `''` MV element reads as
/// null through `column_value_at`, exactly like a scalar `''` cell.
#[test]
fn legacy_mv_empty_string_cells_canonicalize_to_null() {
    latch_legacy();
    let seg = segment();
    let col = seg.columns.get("tags").expect("tags column");
    assert_eq!(
        column_value_at(col, 0),
        Value::Null,
        "singleton [\"\"] must read as canonical null (scalar-'' parity)"
    );
    assert_eq!(
        column_value_at(col, 1),
        json!(["a", null]),
        "the '' element of a multi-element row must read as a null element"
    );
    assert_eq!(column_value_at(col, 2), json!("b"));
    assert_eq!(column_value_at(col, 3), Value::Null, "empty MV row is null");
}

/// Native scan renders the canonical nulls (Druid 27 renders `''` as
/// null in every native surface; the SQL layer restores `""`).
#[test]
fn legacy_scan_renders_empty_elements_as_null() {
    latch_legacy();
    let res = scan_with_filter(None)
        .execute(&segment())
        .expect("scan executes");
    assert_eq!(res.events.len(), 4);
    let tags: Vec<&Value> = res
        .events
        .iter()
        .map(|e| e.get("tags").expect("tags key"))
        .collect();
    assert_eq!(*tags[0], Value::Null, "[\"\"] scans as null: {tags:?}");
    assert_eq!(
        *tags[1],
        json!(["a", null]),
        "[\"a\",\"\"] scans as [\"a\",null]: {tags:?}"
    );
    assert_eq!(*tags[2], json!("b"));
    assert_eq!(*tags[3], Value::Null);
}

/// `selector(tags, '')` — the legacy null selector — matches the `[""]`
/// singleton, the row CONTAINING a `''` element (any-element rule), and
/// the empty MV row; `selector(tags, null)` is the same filter.
#[test]
fn legacy_empty_selector_matches_mv_rows() {
    latch_legacy();
    assert_eq!(
        matched_times(json!({"type": "selector", "dimension": "tags", "value": ""})),
        vec![0, 1, 3],
        "selector '' must match [\"\"] (0), [\"a\",\"\"] (1), and [] (3)"
    );
    assert_eq!(
        matched_times(json!({"type": "selector", "dimension": "tags", "value": null})),
        vec![0, 1, 3],
        "selector null is the SAME legacy filter"
    );
    // Any-element semantics intact: a non-null selector still matches
    // only rows containing that element.
    assert_eq!(
        matched_times(json!({"type": "selector", "dimension": "tags", "value": "a"})),
        vec![1]
    );
    assert_eq!(
        matched_times(json!({"type": "selector", "dimension": "tags", "value": "b"})),
        vec![2]
    );
}

/// The native `null` filter (SQL IS NULL) agrees with the null selector
/// on every MV shape.
#[test]
fn legacy_null_filter_matches_mv_rows() {
    latch_legacy();
    assert_eq!(
        matched_times(json!({"type": "null", "column": "tags"})),
        vec![0, 1, 3],
        "IS NULL must match [\"\"] (0), [\"a\",\"\"] (1), and [] (3)"
    );
}

/// An IN list containing the `''` entry matches the merged `''`/null MV
/// rows (any-element rule); non-null entries keep exact-element matching.
#[test]
fn legacy_in_filter_empty_entry_matches_mv_rows() {
    latch_legacy();
    assert_eq!(
        matched_times(json!({"type": "in", "dimension": "tags", "values": [""]})),
        vec![0, 1, 3],
        "IN ('') must match the ''/null rows"
    );
    assert_eq!(
        matched_times(json!({"type": "in", "dimension": "tags", "values": ["b"]})),
        vec![2],
        "a null element must NOT match a non-null IN entry"
    );
}

/// Row-map level (the exact seam H2 lived on): a selector/null filter
/// over an already-materialised MV row containing a null element.
#[test]
fn legacy_filter_matches_on_materialised_rows() {
    latch_legacy();
    let row: HashMap<String, Value> = HashMap::from([("tags".to_string(), json!(["a", null]))]);
    let empty_sel = FilterSpec::Selector {
        dimension: "tags".to_string(),
        value: Some(json!("")),
    };
    assert!(
        empty_sel.matches(&row),
        "selector '' must match an MV row with a canonicalized null element"
    );
    let null_filter: FilterSpec =
        serde_json::from_value(json!({"type": "null", "column": "tags"})).expect("parses");
    assert!(null_filter.matches(&row));
    let other_sel = FilterSpec::Selector {
        dimension: "tags".to_string(),
        value: Some(json!("zzz")),
    };
    assert!(!other_sel.matches(&row));
}

/// groupBy explosion is intact and the `''` element lands in the null
/// group (Druid legacy renders the `''` group as null): null gets rows
/// 0 and 3 (whole-null rows) plus row 1's `''` element.
#[test]
fn legacy_groupby_explodes_empty_element_into_null_group() {
    latch_legacy();
    let q: GroupByQuery = serde_json::from_value(json!({
        "dataSource": "ds",
        "intervals": [INTERVAL],
        "granularity": "all",
        "dimensions": [{
            "type": "default",
            "dimension": "tags",
            "outputName": "tags",
            "outputType": "STRING"
        }],
        "aggregations": [{"type": "count", "name": "cnt"}],
    }))
    .expect("groupBy query parses");
    let results = q.execute(&segment()).expect("groupBy executes");
    let groups: HashMap<Option<String>, i64> = results
        .iter()
        .map(|r| {
            let key = match r.event.get("tags").expect("tags key") {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                other => panic!("unexpected group key shape: {other:?}"),
            };
            let cnt = r.event.get("cnt").and_then(Value::as_i64).expect("cnt");
            (key, cnt)
        })
        .collect();
    assert_eq!(
        groups,
        HashMap::from([
            (None, 3),
            (Some("a".to_string()), 1),
            (Some("b".to_string()), 1),
        ]),
        "null group = rows 0+3 (whole-null) + row 1's '' element"
    );
}
