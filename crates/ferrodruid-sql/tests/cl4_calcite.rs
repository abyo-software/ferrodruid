// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CL-4 / W1-D + W1-H — Calcite + Druid-specific SQL surface integration tests.
//!
//! Bar (W1-D + W1-H prompts § Validation):
//!   • ≥ 3 SQL-compat integration tests per function / clause.
//!   • At least one query per function shows up in the Druid 35 / 36
//!     diff harness (added by a separate commit under
//!     `crates/ferrodruid-rest/tests/druid_diff_test.rs`).
//!
//! Unit-level coverage for individual AST node shapes lives in
//! `crates/ferrodruid-sql/src/{parser,planner,functions}.rs::tests`;
//! this file exercises end-to-end SQL surface — parse → plan → either
//! `Ok(_)` (functions with backing native primitives — the window
//! family, plus W1-H R1-R7 closures) or `Err(_)` with a precise
//! message (no longer used for CL-4 R1-R7 since W1-H landed; kept for
//! GROUPING SETS dimension validation, etc).
//!
//! Apache Druid 35/36 deep-match for these queries is gated on the
//! W1-G docker harness run (see
//! `tests/druid-compat/RESULTS_wave_v1_sql_2026-06-30_run_v1d.md`).

use ferrodruid_common::types::ColumnType;
use ferrodruid_sql::parser::parse_druid_sql;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema, plan_sql};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

fn cl4_schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "cl4".to_string(),
        dimensions: vec![
            ColumnSchema {
                name: "city".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "country".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "page".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "tags".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "user_id".to_string(),
                column_type: ColumnType::String,
            },
        ],
        metrics: vec![
            ColumnSchema {
                name: "added".to_string(),
                column_type: ColumnType::Long,
            },
            ColumnSchema {
                name: "deleted".to_string(),
                column_type: ColumnType::Long,
            },
            ColumnSchema {
                name: "price".to_string(),
                column_type: ColumnType::Double,
            },
            ColumnSchema {
                name: "revenue".to_string(),
                column_type: ColumnType::Double,
            },
            ColumnSchema {
                name: "evt_time".to_string(),
                column_type: ColumnType::Long,
            },
        ],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn must_parse(sql: &str) {
    parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse failed for [{sql}]: {e}"));
}

fn must_parse_and_plan(sql: &str) {
    let stmt = parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse failed for [{sql}]: {e}"));
    plan_sql(&stmt, &cl4_schema()).unwrap_or_else(|e| panic!("plan failed for [{sql}]: {e}"));
}

// NOTE: W1-D's `must_fail_closed` helper is intentionally removed in
// W1-H — every CL-4 R1-R7 family now real-lowers, and the WITH
// RECURSIVE rejection test (below) needs only a parser-level check
// rather than a planner-level fail-closed assertion.

// ===========================================================================
// JOIN (broadcast / lookup / inline) — already implemented; CL-4 closure bar
// asks for ≥3 integration cases per surface, including JOIN.
// ===========================================================================

#[test]
fn cl4_join_inline_values() {
    must_parse_and_plan(
        "SELECT cl4.city, c.name AS country_name FROM cl4 \
         INNER JOIN (VALUES ('tokyo', 'Tokyo')) AS c(code, name) ON cl4.country = c.code",
    );
}

#[test]
fn cl4_join_lookup_function() {
    must_parse_and_plan(
        "SELECT cl4.city, l.v AS country_name FROM cl4 \
         JOIN LOOKUP('country_codes') AS l ON cl4.country = l.k",
    );
}

#[test]
fn cl4_join_two_way_chained() {
    must_parse_and_plan(
        "SELECT cl4.city, a.v AS code_name, b.v AS country_name FROM cl4 \
         JOIN (VALUES ('tokyo','T')) AS a(k, v) ON cl4.city = a.k \
         JOIN LOOKUP('cc') AS b ON cl4.country = b.k",
    );
}

// ===========================================================================
// CTE (WITH ...)
// ===========================================================================

#[test]
fn cl4_cte_single_level() {
    must_parse_and_plan(
        "WITH per_city AS (SELECT city, COUNT(*) AS cnt FROM cl4 GROUP BY city) \
         SELECT city, cnt FROM per_city",
    );
}

#[test]
fn cl4_cte_chained() {
    must_parse_and_plan(
        "WITH a AS (SELECT city FROM cl4), \
              b AS (SELECT city FROM a) \
         SELECT city FROM b",
    );
}

#[test]
fn cl4_cte_recursive_rejected() {
    let err = parse_druid_sql(
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r) \
         SELECT * FROM r",
    )
    .expect_err("WITH RECURSIVE must reject");
    assert!(
        format!("{err}").contains("Recursive"),
        "expected Recursive in error, got: {err}"
    );
}

// ===========================================================================
// GROUPING SETS / CUBE / ROLLUP
// ===========================================================================

#[test]
fn cl4_grouping_sets_basic() {
    must_parse_and_plan(
        "SELECT city, country, COUNT(*) AS cnt FROM cl4 \
         GROUP BY GROUPING SETS ((city, country), (city), ())",
    );
}

#[test]
fn cl4_cube_two_dim() {
    must_parse_and_plan(
        "SELECT city, country, COUNT(*) AS cnt FROM cl4 GROUP BY CUBE(city, country)",
    );
}

#[test]
fn cl4_rollup_two_dim() {
    must_parse_and_plan(
        "SELECT city, country, COUNT(*) AS cnt FROM cl4 GROUP BY ROLLUP(city, country)",
    );
}

// ===========================================================================
// CL-4 / W1-H R1 — ARRAY_AGG (now lowers to a real native aggregator)
// ===========================================================================

#[test]
fn cl4_array_agg_lowers_end_to_end() {
    must_parse_and_plan("SELECT ARRAY_AGG(city) FROM cl4");
    must_parse_and_plan("SELECT ARRAY_AGG(DISTINCT city, 500) FROM cl4");
    must_parse_and_plan("SELECT city, ARRAY_AGG(country) AS gs FROM cl4 GROUP BY city");
}

// ===========================================================================
// CL-4 / W1-H R2 — LISTAGG (now lowers to native string-concat aggregator)
// ===========================================================================

#[test]
fn cl4_listagg_lowers_end_to_end() {
    must_parse_and_plan("SELECT LISTAGG(city) FROM cl4");
    must_parse_and_plan("SELECT LISTAGG(city, '|') FROM cl4");
    must_parse_and_plan("SELECT city, LISTAGG(country, ',', 4096) AS list FROM cl4 GROUP BY city");
}

// ===========================================================================
// CL-4 / W1-H R3 — STRING_AGG (alias of LISTAGG / Druid 33+ alignment)
// ===========================================================================

#[test]
fn cl4_string_agg_lowers_end_to_end() {
    must_parse_and_plan("SELECT STRING_AGG(city, ',') FROM cl4");
    must_parse_and_plan("SELECT STRING_AGG(city, '|', 2048) FROM cl4");
    must_parse_and_plan("SELECT country, STRING_AGG(city, ',') AS cs FROM cl4 GROUP BY country");
}

// ===========================================================================
// Window function additions — NTH_VALUE / NTILE / CUME_DIST / PERCENT_RANK
// (these are FULLY supported end-to-end including the executor)
// ===========================================================================

#[test]
fn cl4_window_nth_value() {
    must_parse_and_plan(
        "SELECT page, NTH_VALUE(price, 2) OVER (PARTITION BY city ORDER BY evt_time) AS p2 \
         FROM cl4",
    );
    must_parse_and_plan(
        "SELECT page, NTH_VALUE(added, 1) OVER (ORDER BY evt_time) AS first_added FROM cl4",
    );
    must_parse_and_plan(
        "SELECT city, NTH_VALUE(country, 3) OVER (PARTITION BY city ORDER BY evt_time) AS c3 \
         FROM cl4",
    );
}

#[test]
fn cl4_window_ntile() {
    must_parse_and_plan(
        "SELECT page, NTILE(4) OVER (PARTITION BY city ORDER BY price) AS quartile FROM cl4",
    );
    must_parse_and_plan("SELECT page, NTILE(10) OVER (ORDER BY price) AS decile FROM cl4");
    must_parse_and_plan(
        "SELECT page, NTILE(2) OVER (PARTITION BY country ORDER BY price) AS half FROM cl4",
    );
}

#[test]
fn cl4_window_cume_dist() {
    must_parse_and_plan(
        "SELECT page, CUME_DIST() OVER (PARTITION BY city ORDER BY price) AS cd FROM cl4",
    );
    must_parse_and_plan("SELECT page, CUME_DIST() OVER (ORDER BY price) AS cd FROM cl4");
    must_parse_and_plan(
        "SELECT page, CUME_DIST() OVER (PARTITION BY country ORDER BY evt_time) AS cd FROM cl4",
    );
}

#[test]
fn cl4_window_percent_rank() {
    must_parse_and_plan(
        "SELECT page, PERCENT_RANK() OVER (PARTITION BY city ORDER BY price) AS pr FROM cl4",
    );
    must_parse_and_plan("SELECT page, PERCENT_RANK() OVER (ORDER BY price) AS pr FROM cl4");
    must_parse_and_plan(
        "SELECT page, PERCENT_RANK() OVER (PARTITION BY country ORDER BY evt_time) AS pr \
         FROM cl4",
    );
}

// ===========================================================================
// CL-4 / W1-H R4 — BLOOM_FILTER aggregate + BLOOM_FILTER_TEST filter
// ===========================================================================

#[test]
fn cl4_bloom_filter_aggregate_lowers_end_to_end() {
    must_parse_and_plan("SELECT BLOOM_FILTER(user_id, 10000) FROM cl4");
    must_parse_and_plan("SELECT city, BLOOM_FILTER(user_id, 50000) AS bf FROM cl4 GROUP BY city");
    must_parse_and_plan("SELECT BLOOM_FILTER(page, 100000) AS bf FROM cl4");
}

/// BLOOM_FILTER_TEST in WHERE needs a real (or at least decodable) base64
/// envelope to pass `FilterSpec::validate()`.  We build one via the
/// aggregator API so the test exercises the round-trip path the planner
/// commits to.
#[test]
fn cl4_bloom_filter_test_in_where_lowers_end_to_end() {
    use ferrodruid_aggregator::{Aggregator as _, BloomFilterAggregator};
    let mut agg = BloomFilterAggregator::new(1000);
    agg.aggregate(Some(&serde_json::json!("alpha")));
    agg.aggregate(Some(&serde_json::json!("bravo")));
    let envelope = agg.get();
    let b64 = envelope
        .get("bytes")
        .and_then(serde_json::Value::as_str)
        .expect("bytes");

    let sql_single = format!("SELECT user_id FROM cl4 WHERE BLOOM_FILTER_TEST(user_id, '{b64}')");
    must_parse_and_plan(&sql_single);

    let sql_count = format!("SELECT COUNT(*) FROM cl4 WHERE BLOOM_FILTER_TEST(city, '{b64}')");
    must_parse_and_plan(&sql_count);

    let sql_and =
        format!("SELECT city FROM cl4 WHERE city = 'tokyo' AND BLOOM_FILTER_TEST(city, '{b64}')");
    must_parse_and_plan(&sql_and);
}

// ===========================================================================
// CL-4 / W1-H R5 — MV_FILTER_ONLY / MV_FILTER_NONE
// ===========================================================================

#[test]
fn cl4_mv_filter_only_lowers_end_to_end() {
    must_parse_and_plan("SELECT tags FROM cl4 WHERE MV_FILTER_ONLY(tags, ARRAY['a','b'])");
    must_parse_and_plan("SELECT COUNT(*) FROM cl4 WHERE MV_FILTER_ONLY(tags, ARRAY['premium'])");
    must_parse_and_plan(
        "SELECT tags FROM cl4 WHERE MV_FILTER_ONLY(tags, ARRAY['x','y','z']) AND price > 100",
    );
}

#[test]
fn cl4_mv_filter_none_lowers_end_to_end() {
    must_parse_and_plan("SELECT tags FROM cl4 WHERE MV_FILTER_NONE(tags, ARRAY['spam'])");
    must_parse_and_plan("SELECT COUNT(*) FROM cl4 WHERE MV_FILTER_NONE(tags, ARRAY['banned'])");
    must_parse_and_plan(
        "SELECT tags FROM cl4 WHERE price > 0 AND MV_FILTER_NONE(tags, ARRAY['x'])",
    );
}

// ===========================================================================
// CL-4 / W1-H R6 — EARLIEST / LATEST by non-`__time` column (2-arg form)
// ===========================================================================

#[test]
fn cl4_earliest_by_non_time_lowers_end_to_end() {
    must_parse_and_plan("SELECT EARLIEST(price, evt_time) FROM cl4");
    must_parse_and_plan(
        "SELECT city, EARLIEST(price, evt_time) AS first_price FROM cl4 GROUP BY city",
    );
    must_parse_and_plan("SELECT EARLIEST(page, evt_time) FROM cl4");
}

#[test]
fn cl4_latest_by_non_time_lowers_end_to_end() {
    must_parse_and_plan("SELECT LATEST(price, evt_time) FROM cl4");
    must_parse_and_plan(
        "SELECT city, LATEST(price, evt_time) AS last_price FROM cl4 GROUP BY city",
    );
    must_parse_and_plan("SELECT LATEST(page, evt_time) FROM cl4");
}

// ===========================================================================
// CL-4 / W1-H R7 — GROUPING() indicator
// ===========================================================================

#[test]
fn cl4_grouping_indicator_lowers_end_to_end() {
    must_parse_and_plan("SELECT city, GROUPING(city) FROM cl4 GROUP BY city");
    must_parse_and_plan(
        "SELECT city, country, GROUPING(city, country) AS g FROM cl4 \
         GROUP BY GROUPING SETS ((city, country), (city), ())",
    );
    must_parse_and_plan("SELECT GROUPING(city) AS g FROM cl4 GROUP BY city");
}
