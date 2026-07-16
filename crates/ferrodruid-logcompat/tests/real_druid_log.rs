// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! End-to-end classification of a REAL Apache Druid 35.0.1 request log.
//!
//! The fixture (`tests/fixtures/druid35-request-log-2026-07-11.log`) is the
//! verbatim `druid.request.logging.type=file` output of a Druid 35.0.1
//! micro-quickstart (docker `druid35logcap`, 2026-07-11) after ingesting
//! the standard `wikipedia_compat` fixture and firing the diff-harness
//! query battery (base SQL / window functions / native timeseries / native
//! topN / scan / search / metadata), Superset-style SQL (SELECT 1,
//! INFORMATION_SCHEMA introspection, DATE_TRUNC chart queries), and a set
//! of intentionally-incompatible probes (FULL OUTER JOIN, WITH RECURSIVE,
//! a JavaScript aggregator, the Druid-26+ `equals` filter). All data in
//! the log is the synthetic wikipedia fixture — nothing sensitive.

use ferrodruid_logcompat::classify::Bucket;
use ferrodruid_logcompat::report::{Analyzer, QueryKind, Report, render_json, render_markdown};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/druid35-request-log-2026-07-11.log"
);

fn analyze(keep_exemplars: bool) -> Report {
    let log = std::fs::read_to_string(FIXTURE).expect("fixture readable");
    let mut analyzer = Analyzer::new(keep_exemplars);
    for line in log.lines() {
        analyzer.add_line(line);
    }
    analyzer.finish()
}

#[test]
fn real_druid35_log_headline_numbers() {
    let r = analyze(false);
    // Every line of the real log is recognized: no malformed / non-query
    // lines and no emitter-format lines.
    assert_eq!(r.input.lines, 208);
    assert_eq!(r.input.non_query_lines, 0);
    assert_eq!(r.input.emitter_lines, 0);
    // The micro-quickstart logs each query at several tiers; client
    // workload vs cluster machinery must be separated.
    assert_eq!(r.input.query_records, 115);
    assert_eq!(r.input.internal_records, 56, "segment-pinned fan-outs");
    assert_eq!(
        r.input.sql_lowered_records, 37,
        "Calcite lowerings of SQL lines"
    );

    // Classification split over the 51 distinct client shapes.
    assert_eq!(r.supported.shapes, 47);
    assert_eq!(r.fail_closed.shapes, 3);
    assert_eq!(r.unsupported.shapes, 1);
    assert_eq!(r.supported.records, 105);
    assert_eq!(r.fail_closed.records, 6);
    assert_eq!(r.unsupported.records, 4);

    let pct_shapes = r.compatible_pct_shapes.expect("has queries");
    let pct_records = r.compatible_pct_records.expect("has queries");
    assert!((pct_shapes - 92.156_862).abs() < 1e-3, "{pct_shapes}");
    assert!((pct_records - 91.304_347).abs() < 1e-3, "{pct_records}");
}

/// Work-order probe: intentionally-incompatible queries must be detected
/// with correct reasons — a JOIN-execution query (`FULL OUTER JOIN`; plain
/// INNER/LEFT equi-joins are supported and classified as such), a
/// JavaScript aggregator, and a CTE (`WITH RECURSIVE`; plain CTEs are
/// inlined and supported).
#[test]
fn real_druid35_log_detects_intentionally_incompatible_queries() {
    let r = analyze(false);
    let find = |needle: &str| {
        r.shapes
            .iter()
            .find(|s| {
                s.classification
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains(needle))
            })
            .unwrap_or_else(|| panic!("no shape classified with reason containing {needle:?}"))
    };

    // 1. JOIN-execution probe: FULL OUTER JOIN — fail-closed at SQL parse.
    let join = find("FULL OUTER JOIN is not supported");
    assert_eq!(join.classification.bucket, Bucket::FailClosed);
    assert_eq!(join.kind, QueryKind::Sql);
    assert_eq!(join.count, 2);

    // 2. CTE probe: WITH RECURSIVE — fail-closed at SQL parse.
    let cte = find("Recursive CTEs (WITH RECURSIVE) are not supported");
    assert_eq!(cte.classification.bucket, Bucket::FailClosed);
    assert_eq!(cte.kind, QueryKind::Sql);

    // 3. JavaScript aggregator — fail-closed with the JS-engine reason.
    let js = find("JavaScript-based constructs");
    assert_eq!(js.classification.bucket, Bucket::FailClosed);
    assert_eq!(js.kind, QueryKind::Native);
    assert!(js.shape.contains("\"type\":\"javascript\""));

    // 4. Genuine wire gap surfaced by the same log: the Druid 26+
    //    `equals` filter has no FerroDruid native deserialization yet.
    let equals = find("unknown variant `equals`");
    assert_eq!(equals.classification.bucket, Bucket::Unsupported);
    assert_eq!(equals.kind, QueryKind::Native);
    assert_eq!(equals.count, 4, "2 probes x router+broker log tiers");
}

/// The supported bucket must cover the bread-and-butter workload: plain
/// aggregates, GROUP BY/topN SQL, window functions, Superset-style chart
/// SQL, INFORMATION_SCHEMA introspection, and hand-written native
/// timeseries/topN/scan/search — including forms Druid re-serializes into
/// log-only spec objects (LegacyDimensionSpec / LegacyTopNMetricSpec).
#[test]
fn real_druid35_log_supported_covers_expected_workload() {
    let r = analyze(false);
    let supported_shape = |needle: &str| {
        r.shapes
            .iter()
            .find(|s| s.shape.contains(needle))
            .unwrap_or_else(|| panic!("no shape containing {needle:?}"))
            .classification
            .bucket
    };
    // Superset chart SQL (DATE_TRUNC + schema-prefixed datasource).
    assert_eq!(
        supported_shape("DATE_TRUNC(?, CAST(__time AS TIMESTAMP))"),
        Bucket::Supported
    );
    // INFORMATION_SCHEMA introspection (broker virtual-table path).
    assert_eq!(
        supported_shape("INFORMATION_SCHEMA.COLUMNS"),
        Bucket::Supported
    );
    // Window function SQL.
    assert_eq!(supported_shape("ROW_NUMBER() OVER"), Bucket::Supported);
    // Hand-written native topN whose dimension/metric Druid logged as
    // Legacy*Spec objects (de-normalized back to wire form).
    assert_eq!(supported_shape("\"queryType\":\"topN\""), Bucket::Supported);
    // Native search (sort + searchDimensions logged in log-only form).
    assert_eq!(
        supported_shape("\"queryType\":\"search\""),
        Bucket::Supported
    );
}

/// The default (redacted) report must not leak literals that are known to
/// be in the raw log: filter values, interval bounds, TIMESTAMP literals.
#[test]
fn real_druid35_log_redacted_report_has_no_literals() {
    let r = analyze(false);
    let md = render_markdown(&r, 50, true);
    let json = render_json(&r, true).expect("report serializes");
    for leaked in [
        "'en'",                                   // SQL filter literal
        "matchValue\":\"en",                      // native equals filter literal
        "2024-01-01",                             // interval bound / TIMESTAMP literal
        "insensitive_contains\",\"value\":\"foo", // search literal
    ] {
        assert!(!md.contains(leaked), "markdown leaked {leaked:?}");
        assert!(!json.contains(leaked), "json leaked {leaked:?}");
    }
}
