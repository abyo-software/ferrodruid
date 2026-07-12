// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real Apache Druid ⇄ FerroDruid wire/JSON diff test.
//!
//! These tests are `#[ignore]`d by default — they require a running
//! Druid container on a known port, started via the per-version
//! docker-compose files under `tests/druid-compat/`:
//!
//! | Test fn                        | Druid version | Compose file                    | Druid host port |
//! |--------------------------------|---------------|----------------------------------|-----------------|
//! | `druid_30_vs_ferrodruid_diff`  | 30.0.1        | `docker-compose.yml`             | 8888            |
//! | `druid_31_vs_ferrodruid_diff`  | 31.0.2        | `docker-compose.druid31.yml`     | 31888           |
//! | `druid_32_vs_ferrodruid_diff`  | 32.0.1        | `docker-compose.druid32.yml`     | 18888           |
//! | `druid_33_vs_ferrodruid_diff`  | 33.0.0        | `docker-compose.druid33.yml`     | 33888           |
//! | `druid_34_vs_ferrodruid_diff`  | 34.0.0        | `docker-compose.druid34.yml`     | 34888           |
//! | `druid_35_vs_ferrodruid_diff`  | 35.0.1        | `docker-compose.druid35.yml`     | 28888           |
//! | `druid_36_vs_ferrodruid_diff`  | 36.0.0        | `docker-compose.druid36.yml`     | 36888           |
//!
//! Each test:
//!   1. Verifies its target Druid is reachable; SKIPs if not.
//!   2. Ingests the shared `wikipedia_compat` sample dataset (idempotent).
//!   3. Spawns a fresh `target/release/ferrodruid serve` subprocess on
//!      a per-version port (38888 / 38889 / 38890).
//!   4. Submits the same ingestion spec to FerroDruid.
//!   5. Runs nine query sections on both engines, JSON-normalises, and
//!      writes a per-version `RESULTS_*_run.md` log:
//!      - 5 base SQL queries (count/min-max/groupBy/where/sum)
//!      - 15 SQL window functions (Wave 47-D + Wave 10:
//!        ROW_NUMBER/RANK/LAG/FIRST_VALUE/LAST_VALUE/MIN/MAX/COUNT,
//!        plus running and sliding frame variations)
//!      - 4 native TIMESERIES queries (Wave 47-D)
//!      - 4 native TopN queries (Wave 47-D)
//!      - 12 CL-4 / W1-D Calcite + Druid-specific SQL queries
//!      - 6 Apache Superset queries (S-2: INFORMATION_SCHEMA introspection,
//!        TIME_FLOOR / DATE_TRUNC time-series, SELECT * preview, EXPLAIN)
//!      - 6 null-semantics queries over the dual-ingested `nulltest`
//!        dataset (T8)
//!      - 18 Superset time-grain queries (T9)
//!      - 3 ingestion-time rollup queries over the dual-ingested
//!        `rolluptest` dataset (2026-07-11 rollup bug pin — these must
//!        deep-match and are not allow-listed)
//!
//! Run all three with:
//! ```text
//! cargo test -p ferrodruid-rest --test druid_diff_test \
//!     -- --ignored --nocapture
//! ```
//!
//! The tests are intentionally tolerant: they assert response shape
//! (top-level JSON type and key set on the first row) is consistent
//! with Druid, and report all per-query diffs to stdout.  They only
//! fail hard if FerroDruid returns a non-2xx HTTP status or crashes —
//! the goal of Wave 30/47-B is to *measure* compatibility honestly,
//! not to claim parity FerroDruid does not yet have.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};

const DATASOURCE: &str = "wikipedia_compat";

/// One Druid version under test.  The triple `(label, druid_base,
/// ferro_port)` is enough to drive the parametrized harness.
struct DruidTarget {
    /// Short label, e.g. "30.0.1".  Used in the results filename.
    label: &'static str,
    /// Base URL of the Druid router, e.g. `http://localhost:8888`.
    druid_base: &'static str,
    /// Port for the FerroDruid subprocess.  Different per version so
    /// concurrent test runs don't collide.
    ferro_port: u16,
}

impl DruidTarget {
    fn ferro_base(&self) -> String {
        format!("http://127.0.0.1:{}", self.ferro_port)
    }
}

/// Test harness output collected for the final assertion.
#[derive(Debug, Default)]
struct DiffReport {
    queries_run: usize,
    matched: usize,
    shape_only_match: usize,
    mismatched: Vec<(String, String)>, // (query name, diff summary)
    transport_failures: Vec<(String, String)>, // (query name, failure reason)
    ferrodruid_only_failures: Vec<(String, String)>,
}

/// The base 5 SQL queries — Wave 30/47-B/47-C surface tests.  Kept
/// simple so they exercise the SELECT / aggregate / GROUP BY / WHERE /
/// ORDER BY paths without pulling in features that FerroDruid is known
/// to not yet support.
fn sql_queries() -> Vec<(&'static str, String)> {
    vec![
        (
            "count_star",
            format!("SELECT COUNT(*) AS cnt FROM {DATASOURCE}"),
        ),
        (
            "min_max_added",
            format!("SELECT MIN(\"added\") AS mn, MAX(\"added\") AS mx FROM {DATASOURCE}"),
        ),
        (
            "groupby_page_topn",
            format!(
                "SELECT \"page\", COUNT(*) AS cnt FROM {DATASOURCE} \
                 GROUP BY \"page\" ORDER BY cnt DESC LIMIT 10"
            ),
        ),
        (
            "filter_lang_en",
            format!(
                "SELECT COUNT(*) AS cnt FROM {DATASOURCE} \
                 WHERE \"language\" = 'en'"
            ),
        ),
        (
            "sum_delta",
            format!("SELECT SUM(\"delta\") AS total_delta FROM {DATASOURCE}"),
        ),
    ]
}

/// Wave 47-D + Wave 10 — SQL window function coverage (15 queries).
///
/// Modern Druid (25.0+) supports SQL window functions natively.
/// FerroDruid lowers `OVER (...)` to a native [`WindowQuery`] (see
/// `crates/ferrodruid-query/src/window.rs`).
///
/// Wave 47-D §1: ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD / SUM /
/// AVG over a partition.
///
/// Wave 10 (47-D §1 extension): adds FIRST_VALUE / LAST_VALUE / MIN /
/// MAX / COUNT(*) plus two `ROWS BETWEEN ...` frame variations
/// (cumulative `UNBOUNDED PRECEDING AND CURRENT ROW`, sliding
/// `2 PRECEDING AND CURRENT ROW`).
///
/// Every query has a deterministic ORDER BY tail (including the
/// dimension column itself) so ties between equal metric values do
/// not flip row order between runs.
fn sql_window_queries() -> Vec<(&'static str, String)> {
    vec![
        (
            "window_row_number_global",
            format!(
                "SELECT \"page\", \"added\", \
                 ROW_NUMBER() OVER (ORDER BY \"added\" DESC, \"page\" ASC) AS rn \
                 FROM {DATASOURCE} \
                 ORDER BY rn"
            ),
        ),
        (
            "window_row_number_partition",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 ROW_NUMBER() OVER (PARTITION BY \"language\" ORDER BY \"added\" DESC, \"page\" ASC) AS rn \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", rn"
            ),
        ),
        (
            "window_rank",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 RANK() OVER (PARTITION BY \"language\" ORDER BY \"added\" DESC) AS rk \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", rk, \"page\""
            ),
        ),
        (
            "window_dense_rank",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 DENSE_RANK() OVER (PARTITION BY \"language\" ORDER BY \"added\" DESC) AS dr \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", dr, \"page\""
            ),
        ),
        (
            "window_lag",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 LAG(\"added\", 1) OVER (PARTITION BY \"language\" ORDER BY \"added\" DESC, \"page\" ASC) AS prev_added \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_lead",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 LEAD(\"added\", 1) OVER (PARTITION BY \"language\" ORDER BY \"added\" DESC, \"page\" ASC) AS next_added \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_sum",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 SUM(\"added\") OVER (PARTITION BY \"language\") AS lang_total \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_avg",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 AVG(\"added\") OVER (PARTITION BY \"language\") AS lang_avg \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        // ------------------------------------------------------------------
        // Wave 10 — extension: FIRST_VALUE / LAST_VALUE / MIN / MAX /
        // COUNT(*) / running and sliding SUM frames.
        // ------------------------------------------------------------------
        (
            "window_first_value",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 FIRST_VALUE(\"added\") OVER (PARTITION BY \"language\" ORDER BY \"added\" DESC, \"page\" ASC) AS top_added \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_last_value_full_partition",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 LAST_VALUE(\"added\") OVER (\
                     PARTITION BY \"language\" \
                     ORDER BY \"added\" ASC, \"page\" ASC \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS bottom_added \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
        (
            "window_min",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 MIN(\"added\") OVER (PARTITION BY \"language\") AS lang_min \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_max",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 MAX(\"added\") OVER (PARTITION BY \"language\") AS lang_max \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_count_star",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 COUNT(*) OVER (PARTITION BY \"language\") AS lang_cnt \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" DESC, \"page\""
            ),
        ),
        (
            "window_sum_running_frame",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 SUM(\"added\") OVER (\
                     PARTITION BY \"language\" \
                     ORDER BY \"added\" ASC, \"page\" ASC \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
        (
            "window_sum_sliding_frame",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 SUM(\"added\") OVER (\
                     PARTITION BY \"language\" \
                     ORDER BY \"added\" ASC, \"page\" ASC \
                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sliding3_total \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
    ]
}

/// Wave 47-D — Native TIMESERIES API coverage (4 queries).
///
/// All queries use the verbose `dataSource: {type:"table", name:...}`
/// form because FerroDruid's native parser does not accept the
/// `dataSource: "name"` string shorthand that Druid permits.  This
/// keeps the harness focused on result-shape semantics rather than
/// JSON sugar.
fn native_timeseries_queries() -> Vec<(&'static str, Value)> {
    vec![
        (
            "ts_count_day",
            json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "day",
                "aggregations": [{"type": "count", "name": "cnt"}],
            }),
        ),
        (
            "ts_count_filter_en_day",
            json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "day",
                "filter": {"type": "selector", "dimension": "language", "value": "en"},
                "aggregations": [{"type": "count", "name": "cnt"}],
            }),
        ),
        (
            "ts_multi_agg_day",
            json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "day",
                "aggregations": [
                    {"type": "count", "name": "cnt"},
                    // doubleSum (not longSum) — see known-limitations divergence #4
                    {"type": "doubleSum", "name": "total_added", "fieldName": "added"},
                ],
            }),
        ),
        (
            "ts_doublesum_filter_main_hour",
            json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "hour",
                "filter": {"type": "selector", "dimension": "namespace", "value": "Main"},
                "aggregations": [{"type": "doubleSum", "name": "total_added", "fieldName": "added"}],
            }),
        ),
        // Wave 47-D §7 verification: `longSum` must accept the floating-
        // point storage form of the `added` metric column and produce the
        // same per-bucket totals as the parallel `doubleSum` query above.
        // Pre-fix this returned `total_added=0` for every bucket.
        (
            "ts_longsum_day",
            json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "day",
                "aggregations": [{"type": "longSum", "name": "total_added", "fieldName": "added"}],
            }),
        ),
    ]
}

/// Wave 47-D — Native TopN API coverage (4 queries).
///
/// All queries use the verbose `dataSource`, `dimension` and `metric`
/// forms because FerroDruid's native parser does not accept the
/// string shorthand that Druid permits for any of the three.
fn native_topn_queries() -> Vec<(&'static str, Value)> {
    vec![
        (
            "topn_page_count",
            json!({
                "queryType": "topN",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "dimension": {"type": "default", "dimension": "page", "outputName": "page", "outputType": "STRING"},
                "metric": {"type": "numeric", "metric": "cnt"},
                "threshold": 5,
                "aggregations": [{"type": "count", "name": "cnt"}],
            }),
        ),
        (
            "topn_user_sum_added",
            json!({
                "queryType": "topN",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "dimension": {"type": "default", "dimension": "user", "outputName": "user", "outputType": "STRING"},
                "metric": {"type": "numeric", "metric": "total_added"},
                "threshold": 5,
                "aggregations": [{"type": "doubleSum", "name": "total_added", "fieldName": "added"}],
            }),
        ),
        (
            "topn_page_count_filter_en",
            json!({
                "queryType": "topN",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "filter": {"type": "selector", "dimension": "language", "value": "en"},
                "dimension": {"type": "default", "dimension": "page", "outputName": "page", "outputType": "STRING"},
                "metric": {"type": "numeric", "metric": "cnt"},
                "threshold": 5,
                "aggregations": [{"type": "count", "name": "cnt"}],
            }),
        ),
        (
            "topn_page_min_added_asc",
            json!({
                "queryType": "topN",
                "dataSource": {"type": "table", "name": DATASOURCE},
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "dimension": {"type": "default", "dimension": "page", "outputName": "page", "outputType": "STRING"},
                "metric": {"type": "inverted", "metric": {"type": "numeric", "metric": "min_added"}},
                "threshold": 5,
                "aggregations": [{"type": "doubleMin", "name": "min_added", "fieldName": "added"}],
            }),
        ),
        // Wave 47-D §2-4 verification: same TopN as `topn_page_count` but
        // submitted with Druid's bare-string shorthand for `dataSource`,
        // `dimension`, and `metric`.  Pre-fix the FerroDruid native parser
        // rejected each shape with a `400 Bad Request`; post-fix the
        // engine treats the shorthand as equivalent to the verbose form.
        (
            "topn_shorthand_page_count",
            json!({
                "queryType": "topN",
                "dataSource": DATASOURCE,
                "intervals": ["2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z"],
                "granularity": "all",
                "dimension": "page",
                "metric": "cnt",
                "threshold": 5,
                "aggregations": [{"type": "count", "name": "cnt"}],
            }),
        ),
    ]
}

/// CL-4 / W1-D — Calcite + Druid-specific SQL surface coverage (12
/// queries, one per function / clause family).  Each query is a
/// concrete SQL statement that Apache Druid 35 / 36 executes; the
/// FerroDruid response is captured in `RESULTS_*_run_v1d_*.md` and
/// classified by the existing diff harness as:
///
///   • deep / shape-only — both engines produced comparable rows
///   • mismatched        — both engines returned something but they
///     disagreed
///   • ferro-fail        — FerroDruid returned an HTTP error (this is
///     the *expected* outcome for the surface-only families — see the
///     W1-D RESULTS file for the residual list R1–R6)
///
/// The harness intentionally does not assert deep-match for these
/// queries; they are added to surface honest divergence rather than to
/// gate a release.  CL-4 closure flips when every query returns deep
/// (which requires the corresponding native primitives to land —
/// tracked as residuals).
fn cl4_sql_queries() -> Vec<(&'static str, String)> {
    vec![
        // ----- JOIN: inline VALUES right side -----
        (
            "cl4_join_inline_values",
            format!(
                "SELECT w.\"language\", c.label AS lang_label, COUNT(*) AS cnt \
                 FROM {DATASOURCE} w \
                 INNER JOIN (VALUES ('en','English'),('fr','French')) AS c(code, label) \
                   ON w.\"language\" = c.code \
                 GROUP BY w.\"language\", c.label \
                 ORDER BY cnt DESC, w.\"language\""
            ),
        ),
        // ----- CTE (WITH ...) -----
        (
            "cl4_cte_single_level",
            format!(
                "WITH per_lang AS (\
                   SELECT \"language\", COUNT(*) AS cnt FROM {DATASOURCE} GROUP BY \"language\"\
                 ) \
                 SELECT \"language\", cnt FROM per_lang ORDER BY cnt DESC, \"language\""
            ),
        ),
        // ----- GROUPING SETS -----
        (
            "cl4_grouping_sets",
            format!(
                "SELECT \"language\", \"page\", COUNT(*) AS cnt FROM {DATASOURCE} \
                 GROUP BY GROUPING SETS ((\"language\", \"page\"), (\"language\"), ()) \
                 ORDER BY \"language\", \"page\", cnt"
            ),
        ),
        // ----- CUBE -----
        (
            "cl4_cube_two_dim",
            format!(
                "SELECT \"language\", \"page\", COUNT(*) AS cnt FROM {DATASOURCE} \
                 GROUP BY CUBE(\"language\", \"page\") \
                 ORDER BY \"language\", \"page\", cnt"
            ),
        ),
        // ----- ROLLUP -----
        (
            "cl4_rollup_two_dim",
            format!(
                "SELECT \"language\", \"page\", COUNT(*) AS cnt FROM {DATASOURCE} \
                 GROUP BY ROLLUP(\"language\", \"page\") \
                 ORDER BY \"language\", \"page\", cnt"
            ),
        ),
        // ----- ARRAY_AGG -----
        (
            "cl4_array_agg",
            format!("SELECT ARRAY_AGG(\"page\") AS pages FROM {DATASOURCE}"),
        ),
        // ----- LISTAGG -----
        (
            "cl4_listagg",
            format!("SELECT LISTAGG(\"page\", '|') AS pages FROM {DATASOURCE}"),
        ),
        // ----- STRING_AGG -----
        (
            "cl4_string_agg",
            format!("SELECT STRING_AGG(\"page\", ',') AS pages FROM {DATASOURCE}"),
        ),
        // ----- NTH_VALUE window -----
        (
            "cl4_window_nth_value",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 NTH_VALUE(\"added\", 2) OVER (\
                     PARTITION BY \"language\" \
                     ORDER BY \"added\" ASC, \"page\" ASC \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_added \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
        // ----- NTILE window -----
        (
            "cl4_window_ntile",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 NTILE(4) OVER (PARTITION BY \"language\" ORDER BY \"added\" ASC, \"page\" ASC) \
                   AS quartile \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
        // ----- CUME_DIST window -----
        (
            "cl4_window_cume_dist",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 CUME_DIST() OVER (PARTITION BY \"language\" ORDER BY \"added\" ASC, \"page\" ASC) \
                   AS cd \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
        // ----- PERCENT_RANK window -----
        (
            "cl4_window_percent_rank",
            format!(
                "SELECT \"language\", \"page\", \"added\", \
                 PERCENT_RANK() OVER (PARTITION BY \"language\" ORDER BY \"added\" ASC, \"page\" ASC) \
                   AS pr \
                 FROM {DATASOURCE} \
                 ORDER BY \"language\", \"added\" ASC, \"page\""
            ),
        ),
        // ----- BLOOM_FILTER_TEST (filter form) -----
        //
        // Druid accepts a base64-encoded bloom filter literal here.
        // The empty filter `AAAAAAAAA` is a valid placeholder that
        // matches nothing on Druid's side and lets FerroDruid surface
        // its fail-closed error without depending on a per-cluster
        // pre-built filter.
        (
            "cl4_bloom_filter_test_where",
            format!(
                "SELECT COUNT(*) AS matched FROM {DATASOURCE} \
                 WHERE BLOOM_FILTER_TEST(\"page\", 'AAAAAAAA')"
            ),
        ),
        // ----- MV_FILTER_ONLY (projection form) -----
        //
        // Druid 35/36 require a multi-value column; `page` is
        // single-valued in the wikipedia_compat fixture so Druid will
        // execute the function as a no-op pass-through.  We capture
        // the responses honestly — the goal is wire-shape parity, not
        // multi-value semantics (those are FG-5).
        (
            "cl4_mv_filter_only_projection",
            format!(
                "SELECT MV_FILTER_ONLY(\"page\", ARRAY['Foo','Bar']) AS kept FROM {DATASOURCE} \
                 LIMIT 1"
            ),
        ),
        // ----- MV_FILTER_NONE (projection form) -----
        (
            "cl4_mv_filter_none_projection",
            format!(
                "SELECT MV_FILTER_NONE(\"page\", ARRAY['Spam']) AS clean FROM {DATASOURCE} \
                 LIMIT 1"
            ),
        ),
        // ----- EARLIEST(expr, timeCol, maxBytesPerString) by non-`__time` column -----
        //
        // W1-J finding-B: Druid 35/36 require the VARCHAR / COMPLEX
        // 3-arg form `EARLIEST(expr, timeCol, maxBytesPerString)` —
        // the planner rejects the 2-arg shape with "Argument to
        // function 'EARLIEST_BY' must be a literal" because Druid
        // needs the literal int to size its off-heap accumulator.
        // The literal goes LAST, not in the middle — verified by
        // directly probing Druid 35.0.1 on 2026-06-30 (the docs are
        // ambiguous on argument order).  FerroDruid's heap-string
        // executor honours the same wire shape (W1-J parser
        // extension) but ignores the literal.
        (
            "cl4_earliest_by_non_time",
            format!("SELECT EARLIEST(\"page\", \"added\", 1024) AS first_page FROM {DATASOURCE}"),
        ),
        // ----- LATEST(expr, timeCol, maxBytesPerString) by non-`__time` column -----
        (
            "cl4_latest_by_non_time",
            format!("SELECT LATEST(\"page\", \"added\", 1024) AS last_page FROM {DATASOURCE}"),
        ),
        // ----- GROUPING() indicator -----
        (
            "cl4_grouping_indicator",
            format!(
                "SELECT \"language\", \"page\", GROUPING(\"language\") AS g_lang, \
                        GROUPING(\"language\", \"page\") AS g_lp, COUNT(*) AS cnt \
                 FROM {DATASOURCE} \
                 GROUP BY GROUPING SETS ((\"language\", \"page\"), (\"language\"), ()) \
                 ORDER BY \"language\", \"page\", cnt"
            ),
        ),
    ]
}

/// Section 6 — Apache Superset query surface (S-2).
///
/// The exact query shapes Superset's pydruid SQLAlchemy dialect + chart engine
/// emit against a Druid datasource:
///   * metadata introspection (dataset picker + column sync),
///   * `TIME_FLOOR` / `DATE_TRUNC` time-bucketed group-by (time-series charts),
///   * `SELECT *` preview,
///   * `EXPLAIN PLAN FOR` (SQL Lab validation).
///
/// The time-bucket queries are the meaningful deep-match cases (identical bucket
/// counts vs Druid). Metadata and `EXPLAIN` are expected to diverge in wire
/// detail across engines (Druid exposes more INFORMATION_SCHEMA columns; the
/// EXPLAIN body is each engine's own native plan) — the harness records the
/// per-query outcome rather than asserting deep-match, matching the Section-5
/// convention.
fn superset_sql_queries() -> Vec<(&'static str, String)> {
    vec![
        (
            "superset_infoschema_tables",
            // Filter to the fixture datasource: the Druid containers RETAIN
            // datasources across harness runs (Section 7's `nulltest`
            // persists), while the per-run FerroDruid instance starts empty —
            // an unfiltered TABLES listing spuriously mismatches on every
            // re-run against a warm container.
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'wikipedia_compat' \
             ORDER BY TABLE_NAME"
                .to_string(),
        ),
        (
            "superset_infoschema_columns",
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = 'wikipedia_compat' ORDER BY COLUMN_NAME"
                .to_string(),
        ),
        (
            "superset_time_floor_hour",
            "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS c \
             FROM wikipedia_compat GROUP BY 1 ORDER BY 1"
                .to_string(),
        ),
        (
            "superset_date_trunc_hour",
            "SELECT DATE_TRUNC('hour', __time) AS t, COUNT(*) AS c \
             FROM wikipedia_compat GROUP BY 1 ORDER BY 1"
                .to_string(),
        ),
        (
            "superset_preview_limit",
            "SELECT * FROM wikipedia_compat ORDER BY __time LIMIT 100".to_string(),
        ),
        (
            "superset_agg_before_dim_alias",
            // codex QA r5: an aggregate projected BEFORE an aliased dimension.
            // Druid's SQL layer emits wire columns exactly matching the SELECT
            // list ({c, s} in that order, alias applied); FerroDruid previously
            // emitted {language, c} — the alias vanished and positional
            // clients (pydruid / Superset) saw the columns swapped. No LIMIT,
            // so it exercises the GroupBy (not TopN) lowering; the two-key
            // ORDER BY keeps the row order deterministic for deep-match.
            // `language` must be quoted (Calcite reserved word).
            "SELECT COUNT(*) AS c, \"language\" AS s FROM wikipedia_compat \
             GROUP BY \"language\" ORDER BY c DESC, s ASC"
                .to_string(),
        ),
        (
            "superset_explain_plan_for",
            // `language` must be quoted: LANGUAGE is a Calcite reserved word,
            // so real Druid rejects the bare identifier at parse time
            // (verified live against Druid 36.0.0, 2026-07-11). The PLAN
            // payloads legitimately differ between engines (different native
            // query JSON), so this query deep-diffs shape-only.
            "EXPLAIN PLAN FOR SELECT \"language\", COUNT(*) AS c \
             FROM wikipedia_compat GROUP BY 1"
                .to_string(),
        ),
    ]
}

/// Section 7 — null semantics (T8). The 7-row `nulltest` dataset, measured
/// live against apache/druid 35.0.1 + 36.0.0 (2026-07-11):
///   site_a values 10,20,NULL; site_b 30,NULL,NULL; site_c NULL;
///   device_id d1,d2,d2,d1,NULL,d3,d3.
const NULLTEST_DATASOURCE: &str = "nulltest";

/// Inline index task for the nulltest dataset — same shape as the
/// wikipedia_compat spec but with `value` a typed double dimension and
/// rollup disabled so nulls survive verbatim.
fn nulltest_ingestion_spec() -> Value {
    let rows = [
        json!({"timestamp":"2024-01-01T00:00:00Z","site_id":"site_a","device_id":"d1","value":10.0}),
        json!({"timestamp":"2024-01-01T01:00:00Z","site_id":"site_a","device_id":"d2","value":20.0}),
        json!({"timestamp":"2024-01-01T02:00:00Z","site_id":"site_a","device_id":"d2","value":null}),
        json!({"timestamp":"2024-01-01T03:00:00Z","site_id":"site_b","device_id":"d1","value":30.0}),
        json!({"timestamp":"2024-01-01T04:00:00Z","site_id":"site_b","device_id":null,"value":null}),
        json!({"timestamp":"2024-01-01T05:00:00Z","site_id":"site_b","device_id":"d3","value":null}),
        json!({"timestamp":"2024-01-01T06:00:00Z","site_id":"site_c","device_id":"d3","value":null}),
    ];
    let data = rows
        .iter()
        .map(|r| serde_json::to_string(r).expect("serialise row"))
        .collect::<Vec<_>>()
        .join("\n");
    json!({
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": NULLTEST_DATASOURCE,
                "timestampSpec": {"column": "timestamp", "format": "iso"},
                "dimensionsSpec": {
                    "dimensions": [
                        "site_id",
                        "device_id",
                        {"type": "double", "name": "value"}
                    ]
                },
                "metricsSpec": [],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "rollup": false
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": data},
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": 5000000,
                "maxRowsInMemory": 25000
            }
        }
    })
}

/// Section 7 queries — ground truth per query (Druid 35.0.1/36.0.0, live
/// 2026-07-11, default config):
///   1. AVG("value")            → 15.0 / 30.0 / null (null-skipping denominator)
///   2. COUNT("value")          → 2 / 1 / 0 (non-null count)
///   3. COUNT(DISTINCT device)  → 3 (BIGINT — integer on the wire) and
///      APPROX_COUNT_DISTINCT   → 3.
///      E16 `null_exact_count_distinct_device`: the same COUNT(DISTINCT)
///      submitted with the SQL context `{"useApproximateCountDistinct":
///      false}` to BOTH engines — Druid switches to its exact
///      (grouping-based) distinct count and returns the exact 3;
///      FerroDruid lowers to the not-null-filtered exact `cardinality`
///      aggregation and must deep-match. NOT allow-listed. Live-verified
///      2026-07-11: `[{"dc":3}]` ⇄ `[{"dc":3}]` deep=true on BOTH ends of
///      the range — Druid 30.0.1 and Druid 36.0.0 (the NULL device_id row
///      is skipped; a bare cardinality would report 4). Evidence:
///      tests/druid-compat/RESULTS_e16_exact_countdistinct_v3{0,6}_*.txt.
///   4. ROUND(AVG("value"), 1)  → 15.0 / 30.0 / null
///   5. SUM("value")/COUNT(*)   → 10.0 / 10.0 / null: SUM of an all-null
///      group is null in BOTH engines (FerroDruid's sum accumulators track
///      a `seen` flag since the a14 null-semantics fix), and arithmetic
///      over the null aggregate propagates null. Expected to deep-match.
///
/// Note `"value"` must be quoted — VALUE is a Calcite reserved word.
/// On a worktree where the parallel null-preserving-ingest task has not
/// merged yet these queries may not deep-match (FerroDruid's ingest may
/// still destroy nulls); Section 7's contract is to RUN and REPORT — the
/// live re-run happens after both tasks merge.
fn null_semantics_sql_queries() -> Vec<(&'static str, String)> {
    vec![
        (
            "null_avg_by_site",
            format!(
                "SELECT site_id, AVG(\"value\") AS avg_v FROM {NULLTEST_DATASOURCE} \
                 GROUP BY site_id ORDER BY site_id"
            ),
        ),
        (
            "null_count_col_by_site",
            format!(
                "SELECT site_id, COUNT(\"value\") AS c FROM {NULLTEST_DATASOURCE} \
                 GROUP BY site_id ORDER BY site_id"
            ),
        ),
        (
            "null_count_distinct_device",
            format!("SELECT COUNT(DISTINCT device_id) AS dc FROM {NULLTEST_DATASOURCE}"),
        ),
        (
            "null_approx_count_distinct_device",
            format!("SELECT APPROX_COUNT_DISTINCT(device_id) AS adc FROM {NULLTEST_DATASOURCE}"),
        ),
        // E16: same COUNT(DISTINCT) but submitted with the exact-mode SQL
        // context (`useApproximateCountDistinct: false` — see the Section 7
        // dispatch loop, which attaches the context for this name only).
        // Druid answers with its exact distinct count (3); FerroDruid must
        // deep-match via the not-null-filtered `cardinality` lowering.
        (
            "null_exact_count_distinct_device",
            format!("SELECT COUNT(DISTINCT device_id) AS dc FROM {NULLTEST_DATASOURCE}"),
        ),
        (
            "null_round_avg_by_site",
            format!(
                "SELECT site_id, ROUND(AVG(\"value\"), 1) AS r FROM {NULLTEST_DATASOURCE} \
                 GROUP BY site_id ORDER BY site_id"
            ),
        ),
        // Formerly a KNOWN divergence (Druid null vs FerroDruid 0 for the
        // all-null group's SUM); closed by the a14 null-semantics fix —
        // both engines now return null, so this should deep-match.
        (
            "null_sum_div_count_star_by_site",
            format!(
                "SELECT site_id, SUM(\"value\") / COUNT(*) AS r FROM {NULLTEST_DATASOURCE} \
                 GROUP BY site_id ORDER BY site_id"
            ),
        ),
    ]
}

/// Section 8 — the full Apache Superset time-grain surface (T9).
///
/// These are the EXACT GROUP BY expressions Superset 4.1.4's
/// `DruidEngineSpec._time_grain_expressions` emits per user-selectable
/// grain (extracted from the running Superset container's
/// `superset/db_engine_specs/druid.py` — Superset is a separate Apache
/// project, clean-room-safe). Every grain wraps the column in a CAST:
/// `TIME_FLOOR(CAST(__time AS TIMESTAMP), '<P>')`, and charts alias the
/// bucket exactly `__timestamp`.
///
/// Expectations are managed honestly and none of these are allow-listed:
///
/// - PT5S / PT30S now PLAN (they lower to an epoch-anchored fixed-period
///   `duration` granularity — `resolve_time_floor_granularity` /
///   `period_to_fixed_millis`) and are expected to deep-match live Druid
///   on the next harness session.
/// - `week_starting_sunday` now PLANS (the nested TIME_SHIFT/TIME_FLOOR
///   expression lowers to a 7-day `duration` granularity anchored on the
///   Sunday before the epoch) and is expected to deep-match likewise.
/// - `week_ending_saturday` REMAINS fail-closed (honest
///   ferro-transport-fail): its bucket label is the Saturday ENDING the
///   `[Sunday, Sunday+7d)` bucket — 6 days after the bucket start — and a
///   floor-to-origin granularity can only label a bucket by its start.
///   Lowering it would take an executor-side post-bucket label offset on
///   the timeseries/groupBy result path (beyond a granularity `origin`).
///   The harness assertion tolerates ferro-side failures (only DRUID-side
///   failures assert), so it stays pinned here unfixed rather than hidden.
fn superset_time_grain_queries() -> Vec<(String, String)> {
    let periods = [
        "PT1S", "PT5S", "PT30S", "PT1M", "PT5M", "PT10M", "PT15M", "PT30M", "PT1H", "PT6H", "P1D",
        "P1W", "P1M", "P3M", "P1Y",
    ];
    let mut out: Vec<(String, String)> = periods
        .iter()
        .map(|p| {
            (
                format!("grain_{}", p.to_lowercase()),
                format!(
                    "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), '{p}') AS __timestamp, \
                     COUNT(*) AS \"count\" FROM {DATASOURCE} \
                     WHERE __time >= TIME_PARSE('2024-01-01T00:00:00') \
                     GROUP BY 1 ORDER BY 1"
                ),
            )
        })
        .collect();
    // Week-variant grains (nested TIME_SHIFT/TIME_FLOOR) — Superset's
    // week_ending_saturday / week_starting_sunday.
    out.push((
        "grain_week_ending_saturday".to_string(),
        format!(
            "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
             'P1W'), 'P1D', 5) AS __timestamp, COUNT(*) AS \"count\" FROM {DATASOURCE} \
             WHERE __time >= TIME_PARSE('2024-01-01T00:00:00') GROUP BY 1 ORDER BY 1"
        ),
    ));
    out.push((
        "grain_week_starting_sunday".to_string(),
        format!(
            "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
             'P1W'), 'P1D', -1) AS __timestamp, COUNT(*) AS \"count\" FROM {DATASOURCE} \
             WHERE __time >= TIME_PARSE('2024-01-01T00:00:00') GROUP BY 1 ORDER BY 1"
        ),
    ));
    // Temporal WHERE literal via CAST(TIME_PARSE(...) AS DATE) — the other
    // filter shape Superset emits.
    out.push((
        "grain_where_cast_time_parse_date".to_string(),
        format!(
            "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), 'P1D') AS __timestamp, \
             COUNT(*) AS \"count\" FROM {DATASOURCE} \
             WHERE __time >= CAST(TIME_PARSE('2024-01-01') AS DATE) \
             GROUP BY 1 ORDER BY 1"
        ),
    ));
    out
}

/// Section 9 — ingestion-time rollup (2026-07-11 bug pin).
///
/// The self-discovered v1.1.0 bug: FerroDruid's `/druid/indexer/v1/task`
/// path never read `granularitySpec.rollup` / `queryGranularity`, so every
/// spec silently ingested raw un-rolled rows.  The `wikipedia_compat`
/// fixture could not catch it — its 10 rows are all distinct at
/// (hour, dims), so raw and rolled ingestion produce identical segments.
/// This dataset is built to MERGE: 6 raw rows where two pairs share an
/// (hour, site_id) key, so rollup at `queryGranularity: "hour"` stores 4
/// rows with summed metrics and `cnt` = merged raw-row count.  The
/// Section-9 queries must DEEP-match Druid (Druid rolls; FerroDruid now
/// rolls too) — they are intentionally NOT allow-listed.
const ROLLUPTEST_DATASOURCE: &str = "rolluptest";

/// Inline `index_parallel` task for the rolluptest dataset — rollup
/// enabled at hour grain, one string dimension, a `count` metric and a
/// renamed `longSum` metric (`value_sum` reads raw field `value`, which
/// also keeps the reserved word VALUE out of the SQL surface).
fn rolluptest_ingestion_spec() -> Value {
    let rows = [
        // hour 00, site_a — MERGES with the next row (cnt 2, sum 15).
        json!({"timestamp":"2024-01-01T00:05:00Z","site_id":"site_a","value":10}),
        json!({"timestamp":"2024-01-01T00:40:00Z","site_id":"site_a","value":5}),
        // hour 00, site_b — alone (cnt 1, sum 7).
        json!({"timestamp":"2024-01-01T00:10:00Z","site_id":"site_b","value":7}),
        // hour 01, site_a — alone (cnt 1, sum 3).
        json!({"timestamp":"2024-01-01T01:20:00Z","site_id":"site_a","value":3}),
        // hour 01, site_b — MERGES with the next row (cnt 2, sum 10).
        json!({"timestamp":"2024-01-01T01:30:00Z","site_id":"site_b","value":4}),
        json!({"timestamp":"2024-01-01T01:45:00Z","site_id":"site_b","value":6}),
    ];
    let data = rows
        .iter()
        .map(|r| serde_json::to_string(r).expect("serialise row"))
        .collect::<Vec<_>>()
        .join("\n");
    json!({
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": ROLLUPTEST_DATASOURCE,
                "timestampSpec": {"column": "timestamp", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["site_id"]},
                "metricsSpec": [
                    {"type": "count", "name": "cnt"},
                    {"type": "longSum", "name": "value_sum", "fieldName": "value"}
                ],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "hour",
                    "rollup": true
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": data},
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": 5000000,
                "maxRowsInMemory": 25000
            }
        }
    })
}

/// Section 9 queries — expected results (both engines, after rollup):
///   1. COUNT(*)               → 4 (6 raw rows rolled to 4)
///   2. GROUP BY site_id       → site_a: total 18, raw_rows 3; site_b:
///      total 17, raw_rows 3 (`SUM("cnt")` re-aggregates the rollup count
///      metric back to the raw-row count, the standard Druid rollup idiom)
///   3. SELECT * preview       → the 4 rolled rows with hour-truncated
///      `__time` (00:00 ×2, 01:00 ×2), deterministic two-key ORDER BY
///
/// Metric identifiers are quoted throughout (harness convention for
/// reserved words; `cnt` / `value_sum` are safe but quoting is uniform).
fn rollup_sql_queries() -> Vec<(&'static str, String)> {
    vec![
        (
            "rollup_count_star",
            format!("SELECT COUNT(*) AS cnt FROM {ROLLUPTEST_DATASOURCE}"),
        ),
        (
            "rollup_groupby_site_sums",
            format!(
                "SELECT site_id, SUM(\"value_sum\") AS total_value, SUM(\"cnt\") AS raw_rows \
                 FROM {ROLLUPTEST_DATASOURCE} GROUP BY site_id ORDER BY site_id"
            ),
        ),
        (
            "rollup_preview_all",
            // ORDER BY the TIME column only: both Druid and FerroDruid reject
            // a table-scan ordered by a non-time column (`site_id`) with a
            // 400 — that is the documented scan-ORDER-BY-fail-closed contract,
            // so a `, site_id` secondary key made this a (both-engines) Druid-
            // side transport fail. Time-only ordering is a valid scan on both;
            // the two rows sharing an hour bucket make full row order
            // engine-dependent, so this compares shape-only (like
            // `superset_preview_limit`), still proving the rolled row set.
            format!("SELECT * FROM {ROLLUPTEST_DATASOURCE} ORDER BY __time"),
        ),
    ]
}

/// Locate the workspace root by walking up from CARGO_MANIFEST_DIR
/// until we find the workspace-level `Cargo.toml`.
fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let candidate = p.join("Cargo.toml");
        if candidate.exists() {
            // Read it and look for [workspace].
            if let Ok(s) = std::fs::read_to_string(&candidate)
                && s.contains("[workspace]")
            {
                return p;
            }
        }
        if !p.pop() {
            panic!(
                "could not locate workspace root from {CARGO_MANIFEST_DIR}",
                CARGO_MANIFEST_DIR = env!("CARGO_MANIFEST_DIR")
            );
        }
    }
}

/// Path to the prebuilt `ferrodruid` release binary.
fn ferrodruid_binary() -> PathBuf {
    let root = workspace_root();
    let p = root.join("target").join("release").join("ferrodruid");
    if !p.exists() {
        panic!(
            "FerroDruid release binary not found at {p:?}. \
             Build with `cargo build --release -p ferrodruid-cli-lib` first.",
        );
    }
    p
}

/// Wait for an HTTP endpoint to return 200 OK, polling every 500 ms,
/// up to `timeout`.
async fn wait_for_url(client: &reqwest::Client, url: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(url).send().await
            && resp.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

/// Verify Druid is up; if not, skip the test gracefully.
async fn require_druid(client: &reqwest::Client, druid_base: &str) -> bool {
    wait_for_url(
        client,
        &format!("{druid_base}/status/health"),
        Duration::from_secs(10),
    )
    .await
}

/// Spawn a fresh FerroDruid `serve` subprocess on the given port
/// and wait for it to become reachable.
async fn spawn_ferrodruid(
    client: &reqwest::Client,
    ferro_port: u16,
) -> (tokio::process::Child, tempfile::TempDir) {
    let bin = ferrodruid_binary();
    let data_dir = tempfile::tempdir().expect("create temp data dir");
    // `--no-auth` is required because the Wave 36-A change made auth
    // on-by-default; the diff harness has no use for credentialed
    // round-trips, and it binds to loopback (`127.0.0.1`) so the
    // `--no-auth` guard accepts it without `--allow-insecure-public-bind`.
    let child = tokio::process::Command::new(&bin)
        .arg("serve")
        .arg("--mode")
        .arg("single-binary")
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(ferro_port.to_string())
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--no-auth")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ferrodruid");

    // Wait up to 30s for the health endpoint.
    let ok = wait_for_url(
        client,
        &format!("http://127.0.0.1:{ferro_port}/status/health"),
        Duration::from_secs(30),
    )
    .await;
    assert!(
        ok,
        "FerroDruid did not come up on port {ferro_port}; binary at {bin:?}"
    );

    (child, data_dir)
}

/// Submit the inline-data ingestion spec to either Druid or FerroDruid.
/// Returns `Ok(task_id)` if the submit endpoint accepted the spec.
async fn submit_ingestion(
    client: &reqwest::Client,
    base: &str,
    spec: &Value,
) -> Result<String, String> {
    let url = format!("{base}/druid/indexer/v1/task");
    let resp = client
        .post(&url)
        .json(spec)
        .send()
        .await
        .map_err(|e| format!("submit POST failed: {e}"))?;
    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .unwrap_or_else(|_| Value::String("<non-json body>".to_string()));
    if !status.is_success() {
        return Err(format!("submit returned {status}: {body}"));
    }
    body.get("task")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| format!("missing 'task' field in submit response: {body}"))
}

/// Wait up to `timeout` for the Druid task to reach SUCCESS or FAILED.
/// Returns the final status string.
async fn wait_for_task(
    client: &reqwest::Client,
    base: &str,
    task_id: &str,
    timeout: Duration,
) -> String {
    let deadline = std::time::Instant::now() + timeout;
    let url = format!("{base}/druid/indexer/v1/task/{task_id}/status");
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).send().await
            && let Ok(body) = resp.json::<Value>().await
        {
            let s = body
                .pointer("/status/status")
                .and_then(|v| v.as_str())
                .unwrap_or("UNKNOWN")
                .to_string();
            if s == "SUCCESS" || s == "FAILED" {
                return s;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    "TIMEOUT".to_string()
}

/// Execute a SQL query against the given base URL and return the
/// parsed JSON body or an error string.
async fn run_sql(client: &reqwest::Client, base: &str, query: &str) -> Result<Value, String> {
    run_sql_with_context(client, base, query, None).await
}

/// Execute a SQL query with an optional `context` map (e.g. for
/// `enableWindowing: true`).  The base 5 queries use no context;
/// window-function queries pass `{"enableWindowing": true}` so they
/// run on the older Druid releases that gate the feature.
async fn run_sql_with_context(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    context: Option<&Value>,
) -> Result<Value, String> {
    let url = format!("{base}/druid/v2/sql");
    let mut body = json!({"query": query, "resultFormat": "object"});
    if let Some(ctx) = context
        && let Some(obj) = body.as_object_mut()
    {
        obj.insert("context".to_string(), ctx.clone());
    }
    let resp = client
        .post(&url)
        .json(&body)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("SQL POST failed: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("SQL body read failed: {e}"))?;
    if !status.is_success() {
        return Err(format!("SQL returned {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|e| format!("SQL response not JSON: {e}: {text}"))
}

/// Execute a native (`POST /druid/v2`) JSON query.
async fn run_native(client: &reqwest::Client, base: &str, body: &Value) -> Result<Value, String> {
    let url = format!("{base}/druid/v2");
    let resp = client
        .post(&url)
        .json(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("native POST failed: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("native body read failed: {e}"))?;
    if !status.is_success() {
        return Err(format!("native returned {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|e| format!("native response not JSON: {e}: {text}"))
}

/// Round any `f64` numbers in a JSON value to 6 decimal places to
/// absorb float imprecision in the ±1e-9 range.
fn normalize(v: &Value) -> Value {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64()
                && !n.is_i64()
                && !n.is_u64()
            {
                let rounded = (f * 1e6).round() / 1e6;
                serde_json::Number::from_f64(rounded)
                    .map(Value::Number)
                    .unwrap_or_else(|| v.clone())
            } else {
                v.clone()
            }
        }
        Value::Array(arr) => Value::Array(arr.iter().map(normalize).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                out.insert(k.clone(), normalize(val));
            }
            Value::Object(out)
        }
        _ => v.clone(),
    }
}

/// Compare two JSON values.  Returns `(deep_match, shape_match,
/// summary)` where `summary` is a short human-readable diff.
fn compare(druid: &Value, ferro: &Value) -> (bool, bool, String) {
    let d = normalize(druid);
    let f = normalize(ferro);
    if d == f {
        return (true, true, "exact match".to_string());
    }
    // Shape match: both arrays of the same length OR both objects with the same keys.
    let shape = match (&d, &f) {
        (Value::Array(da), Value::Array(fa)) => {
            if da.len() == fa.len() {
                if da.is_empty() {
                    true
                } else {
                    // Compare top-level keyset of first row.
                    match (&da[0], &fa[0]) {
                        (Value::Object(do_map), Value::Object(fo_map)) => {
                            do_map.keys().collect::<std::collections::BTreeSet<_>>()
                                == fo_map.keys().collect::<std::collections::BTreeSet<_>>()
                        }
                        _ => da[0].is_array() == fa[0].is_array(),
                    }
                }
            } else {
                false
            }
        }
        _ => std::mem::discriminant(&d) == std::mem::discriminant(&f),
    };
    let summary = format!(
        "Druid len={} type={} | FerroDruid len={} type={}",
        json_len_hint(&d),
        json_type(&d),
        json_len_hint(&f),
        json_type(&f),
    );
    (false, shape, summary)
}

fn json_len_hint(v: &Value) -> String {
    match v {
        Value::Array(a) => format!("array[{}]", a.len()),
        Value::Object(o) => format!("object[{} keys]", o.len()),
        _ => "scalar".to_string(),
    }
}

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Run a single SQL query against both engines, normalise + diff the
/// JSON, and append the result to the running `DiffReport` and detail
/// log.  Used by `run_diff_harness` for both the base 5 queries and
/// the Wave 47-D window-function expansion.
#[allow(clippy::too_many_arguments)]
async fn run_one_sql_query(
    client: &reqwest::Client,
    druid_base: &str,
    ferro_base: &str,
    name: &str,
    sql: &str,
    context: Option<&Value>,
    report: &mut DiffReport,
    detail_log: &mut Vec<String>,
) {
    report.queries_run += 1;
    eprintln!("\n--- Query: {name} ---");
    eprintln!("    SQL: {sql}");

    let druid_resp = match run_sql_with_context(client, druid_base, sql, context).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("    Druid: TRANSPORT FAIL: {e}");
            report.transport_failures.push((name.to_string(), e));
            detail_log.push(format!(
                "### {name}\n\n```sql\n{sql}\n```\n\n- Druid    : TRANSPORT FAIL\n- FerroDruid: (skipped)\n",
            ));
            return;
        }
    };

    let ferro_resp = match run_sql_with_context(client, ferro_base, sql, context).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("    FerroDruid: TRANSPORT FAIL: {e}");
            report
                .ferrodruid_only_failures
                .push((name.to_string(), e.clone()));
            detail_log.push(format!(
                "### {name}\n\n```sql\n{sql}\n```\n\n- Druid    : `{druid_resp}`\n- FerroDruid: TRANSPORT FAIL: {e}\n",
            ));
            return;
        }
    };

    let (deep, shape, summary) = compare(&druid_resp, &ferro_resp);
    eprintln!("    Druid    : {druid_resp}");
    eprintln!("    FerroDruid: {ferro_resp}");
    eprintln!("    Compare  : deep={deep} shape={shape} | {summary}");
    detail_log.push(format!(
        "### {name}\n\n```sql\n{sql}\n```\n\n- Druid    : `{druid_resp}`\n- FerroDruid: `{ferro_resp}`\n- Compare  : deep={deep} shape={shape} ({summary})\n",
    ));

    if deep {
        report.matched += 1;
    } else if shape {
        report.shape_only_match += 1;
        report
            .mismatched
            .push((name.to_string(), format!("shape only: {summary}")));
    } else {
        report
            .mismatched
            .push((name.to_string(), format!("full diff: {summary}")));
    }
}

/// Run a single native (`POST /druid/v2`) JSON query against both
/// engines and diff the response.  Used by Wave 47-D's TIMESERIES and
/// TopN expansion.
async fn run_one_native_query(
    client: &reqwest::Client,
    druid_base: &str,
    ferro_base: &str,
    name: &str,
    query: &Value,
    report: &mut DiffReport,
    detail_log: &mut Vec<String>,
) {
    report.queries_run += 1;
    eprintln!("\n--- Query: {name} ---");
    eprintln!(
        "    Native: {}",
        serde_json::to_string(query).unwrap_or_default()
    );

    let druid_resp = match run_native(client, druid_base, query).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("    Druid: TRANSPORT FAIL: {e}");
            report.transport_failures.push((name.to_string(), e));
            detail_log.push(format!(
                "### {name}\n\n```json\n{}\n```\n\n- Druid    : TRANSPORT FAIL\n- FerroDruid: (skipped)\n",
                serde_json::to_string_pretty(query).unwrap_or_default()
            ));
            return;
        }
    };

    let ferro_resp = match run_native(client, ferro_base, query).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("    FerroDruid: TRANSPORT FAIL: {e}");
            report
                .ferrodruid_only_failures
                .push((name.to_string(), e.clone()));
            detail_log.push(format!(
                "### {name}\n\n```json\n{}\n```\n\n- Druid    : `{druid_resp}`\n- FerroDruid: TRANSPORT FAIL: {e}\n",
                serde_json::to_string_pretty(query).unwrap_or_default()
            ));
            return;
        }
    };

    let (deep, shape, summary) = compare(&druid_resp, &ferro_resp);
    eprintln!("    Druid    : {druid_resp}");
    eprintln!("    FerroDruid: {ferro_resp}");
    eprintln!("    Compare  : deep={deep} shape={shape} | {summary}");
    detail_log.push(format!(
        "### {name}\n\n```json\n{}\n```\n\n- Druid    : `{druid_resp}`\n- FerroDruid: `{ferro_resp}`\n- Compare  : deep={deep} shape={shape} ({summary})\n",
        serde_json::to_string_pretty(query).unwrap_or_default()
    ));

    if deep {
        report.matched += 1;
    } else if shape {
        report.shape_only_match += 1;
        report
            .mismatched
            .push((name.to_string(), format!("shape only: {summary}")));
    } else {
        report
            .mismatched
            .push((name.to_string(), format!("full diff: {summary}")));
    }
}

/// Run the full diff harness against one Druid target.  Returns the
/// final `DiffReport`.
///
/// This is the parametrized core of the `druid_NN_vs_ferrodruid_diff`
/// tests below.
async fn run_diff_harness(target: &DruidTarget, results_filename: &str) -> DiffReport {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("build reqwest client");
    let druid_base = target.druid_base;
    let ferro_base = target.ferro_base();
    let label = target.label;

    // ------------------------------------------------------------------
    // 1. Sanity: real Druid is reachable.
    // ------------------------------------------------------------------
    if !require_druid(&client, druid_base).await {
        eprintln!("=== SKIP: Druid {label} not reachable on {druid_base} ===");
        eprintln!(
            "Start it with: cd tests/druid-compat && \
             docker compose -f <compose-file-for-{label}> up -d"
        );
        return DiffReport::default();
    }
    eprintln!("=== Real Druid {label} detected at {druid_base} ===");

    // ------------------------------------------------------------------
    // 2. Ingest the sample dataset into Druid (idempotent — if the
    //    datasource already exists with rows we skip submission).
    // ------------------------------------------------------------------
    let spec_path = workspace_root()
        .join("tests")
        .join("druid-compat")
        .join("sample_ingestion_spec.json");
    let spec_text = std::fs::read_to_string(&spec_path).expect("read ingestion spec");
    let spec: Value = serde_json::from_str(&spec_text).expect("parse ingestion spec");

    let count_check = run_sql(
        &client,
        druid_base,
        &format!("SELECT COUNT(*) AS cnt FROM {DATASOURCE}"),
    )
    .await;
    let need_ingest = match count_check {
        Ok(v) => {
            // If the row count is 0 or the datasource doesn't exist (Druid returns
            // an error in that case for unknown tables) we (re)submit the spec.
            v.get(0)
                .and_then(|r| r.get("cnt"))
                .and_then(|n| n.as_i64())
                .map(|n| n == 0)
                .unwrap_or(true)
        }
        Err(_) => true,
    };

    if need_ingest {
        eprintln!("=== Submitting Druid ingestion task ===");
        let task_id = submit_ingestion(&client, druid_base, &spec)
            .await
            .expect("submit Druid ingestion");
        eprintln!("Druid task: {task_id}");
        let final_status =
            wait_for_task(&client, druid_base, &task_id, Duration::from_secs(180)).await;
        assert_eq!(
            final_status, "SUCCESS",
            "Druid {label} ingestion did not succeed: {final_status}"
        );
        // Druid takes a few seconds to publish the segment after task SUCCESS.
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if let Ok(v) = run_sql(
                &client,
                druid_base,
                &format!("SELECT COUNT(*) AS cnt FROM {DATASOURCE}"),
            )
            .await
                && v.get(0)
                    .and_then(|r| r.get("cnt"))
                    .and_then(|n| n.as_i64())
                    .unwrap_or(0)
                    > 0
            {
                break;
            }
        }
    } else {
        eprintln!("=== Druid datasource '{DATASOURCE}' already populated ===");
    }

    // ------------------------------------------------------------------
    // 3. Spawn a fresh FerroDruid subprocess on the per-version port.
    // ------------------------------------------------------------------
    eprintln!("=== Spawning FerroDruid on port {} ===", target.ferro_port);
    let (mut ferro, _data_dir) = spawn_ferrodruid(&client, target.ferro_port).await;

    // ------------------------------------------------------------------
    // 4. Submit the same ingestion spec to FerroDruid.
    //    NOTE (honest limitation): FerroDruid's `submit_task` is currently
    //    a stub — it accepts the spec, generates an ID, and stores the
    //    task as Pending without actually running the ingestion.  We
    //    submit anyway to verify the wire shape of the submit endpoint
    //    and then proceed to query both engines.  The query-result diff
    //    will therefore necessarily show FerroDruid returning empty
    //    results for everything that depends on actual segment data.
    // ------------------------------------------------------------------
    eprintln!("=== Submitting FerroDruid ingestion (stub) ===");
    match submit_ingestion(&client, &ferro_base, &spec).await {
        Ok(id) => eprintln!("FerroDruid task: {id}"),
        Err(e) => eprintln!("FerroDruid submit failed (recorded): {e}"),
    }

    // ------------------------------------------------------------------
    // 5. Run each query on both engines and diff the JSON.  Three
    //    categories: base SQL surface (Wave 30/47), SQL window
    //    functions (Wave 47-D), and native TIMESERIES + TopN APIs
    //    (Wave 47-D).
    // ------------------------------------------------------------------
    let mut report = DiffReport::default();
    let mut detail_log: Vec<String> = Vec::new();

    eprintln!("\n=== Section 1: base SQL surface (5 queries) ===");
    detail_log.push("## Section 1 — base SQL surface\n".to_string());
    for (name, sql) in &sql_queries() {
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            None,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    eprintln!("\n=== Section 2: SQL window functions (15 queries) ===");
    detail_log.push("## Section 2 — SQL window functions\n".to_string());
    // `enableWindowing: true` clears the gating on Druid 25-29; v35+
    // enables windowing by default and ignores the context key
    // harmlessly. Druid 30.0.1 specifically still returns `[]` for
    // ROW_NUMBER / RANK / LAG / SUM-OVER even with this context — see
    // TG-1-finding-W2A-v30-A in
    // `tests/druid-compat/RESULTS_wave_v2_tg1_2026-06-30_v30.md` for
    // the silent-empty divergence (not a Ferro defect; Ferro returns
    // correct rows in all 15 cases). The Section-5 NTILE/CUME_DIST/
    // PERCENT_RANK queries also need this context — see the
    // dispatch loop further down for the per-query gating shim.
    let window_ctx = json!({"enableWindowing": true});
    for (name, sql) in &sql_window_queries() {
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            Some(&window_ctx),
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    eprintln!("\n=== Section 3: native TIMESERIES API (4 queries) ===");
    detail_log.push("## Section 3 — native TIMESERIES API\n".to_string());
    for (name, query) in &native_timeseries_queries() {
        run_one_native_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            query,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    eprintln!("\n=== Section 4: native TopN API (4 queries) ===");
    detail_log.push("## Section 4 — native TopN API\n".to_string());
    for (name, query) in &native_topn_queries() {
        run_one_native_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            query,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // Section 5 — CL-4 / W1-D Calcite + Druid-specific SQL surface
    // (12 queries: JOIN, CTE, GROUPING SETS/CUBE/ROLLUP, ARRAY_AGG,
    // LISTAGG, STRING_AGG, NTH_VALUE, NTILE, CUME_DIST, PERCENT_RANK,
    // BLOOM_FILTER_TEST, MV_FILTER_*, EARLIEST/LATEST by non-`__time`,
    // GROUPING()).  Several queries are *expected* to fail closed on
    // the FerroDruid side until the native primitives land — this is
    // the explicit contract; the harness records the per-query
    // outcome in `RESULTS_*_run_v1d_*.md` and the closure bar flips
    // only when every query reaches deep / shape match.
    // ------------------------------------------------------------------
    eprintln!(
        "\n=== Section 5: CL-4 / W1-D SQL surface ({} queries) ===",
        cl4_sql_queries().len()
    );
    detail_log.push("## Section 5 — CL-4 / W1-D Calcite + Druid-specific surface\n".to_string());
    // TG-1-finding-W2A-v30-A (W2-A 2026-06-30): Druid 30 gates
    // NTILE / CUME_DIST / PERCENT_RANK behind `enableWindowing: true`
    // (v35+ enables by default). The Section-5 dispatch previously
    // passed `None` as the context for every CL-4 query, which made
    // the three `cl4_window_*` queries `druid-fail` on v30 with an
    // explicit "enableWindowing required" error. Pass the windowing
    // context for the `cl4_window_*` family only — every other CL-4
    // query keeps its `None` context so the existing 14/18-W1-J
    // classification on v35 / v36 stays byte-identical.
    let window_ctx = json!({"enableWindowing": true});
    for (name, sql) in &cl4_sql_queries() {
        let ctx = name.starts_with("cl4_window_").then_some(&window_ctx);
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            ctx,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // Section 6 — Apache Superset query surface (S-2).  Metadata + EXPLAIN
    // are expected to diverge in wire detail; the TIME_FLOOR / DATE_TRUNC
    // time-bucket queries are the meaningful deep-match cases.
    // ------------------------------------------------------------------
    eprintln!(
        "\n=== Section 6: Apache Superset query surface ({} queries) ===",
        superset_sql_queries().len()
    );
    detail_log.push("## Section 6 — Apache Superset query surface\n".to_string());
    for (name, sql) in &superset_sql_queries() {
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            None,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // Section 7 — null semantics (T8). Ingest the 7-row nulltest dataset
    // into BOTH engines (idempotent on the Druid side), then diff the
    // measured ground-truth queries. `null_sum_div_count_star_by_site`
    // formerly carried a KNOWN divergence (Druid: SUM of an all-null
    // group is null; FerroDruid: 0); the a14 null-semantics fix closed
    // it — both engines return null, so it is expected to deep-match.
    // ------------------------------------------------------------------
    eprintln!(
        "\n=== Section 7: null semantics ({} queries) ===",
        null_semantics_sql_queries().len()
    );
    detail_log.push("## Section 7 — null semantics (nulltest dataset)\n".to_string());
    let null_spec = nulltest_ingestion_spec();
    let need_null_ingest = match run_sql(
        &client,
        druid_base,
        &format!("SELECT COUNT(*) AS cnt FROM {NULLTEST_DATASOURCE}"),
    )
    .await
    {
        Ok(v) => v
            .get(0)
            .and_then(|r| r.get("cnt"))
            .and_then(|n| n.as_i64())
            .map(|n| n == 0)
            .unwrap_or(true),
        Err(_) => true,
    };
    if need_null_ingest {
        eprintln!("=== Submitting Druid nulltest ingestion task ===");
        match submit_ingestion(&client, druid_base, &null_spec).await {
            Ok(task_id) => {
                let final_status =
                    wait_for_task(&client, druid_base, &task_id, Duration::from_secs(180)).await;
                eprintln!("Druid nulltest task {task_id}: {final_status}");
                // Wait for segment publication (same pattern as wikipedia).
                for _ in 0..30 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if let Ok(v) = run_sql(
                        &client,
                        druid_base,
                        &format!("SELECT COUNT(*) AS cnt FROM {NULLTEST_DATASOURCE}"),
                    )
                    .await
                        && v.get(0)
                            .and_then(|r| r.get("cnt"))
                            .and_then(|n| n.as_i64())
                            .unwrap_or(0)
                            > 0
                    {
                        break;
                    }
                }
            }
            Err(e) => eprintln!("Druid nulltest submit failed (recorded): {e}"),
        }
    } else {
        eprintln!("=== Druid datasource '{NULLTEST_DATASOURCE}' already populated ===");
    }
    eprintln!("=== Submitting FerroDruid nulltest ingestion ===");
    match submit_ingestion(&client, &ferro_base, &null_spec).await {
        Ok(id) => eprintln!("FerroDruid nulltest task: {id}"),
        Err(e) => eprintln!("FerroDruid nulltest submit failed (recorded): {e}"),
    }
    // E16: `null_exact_count_distinct_device` submits with the exact-mode
    // SQL context so BOTH engines run their exact COUNT(DISTINCT) path
    // (Druid: useApproximateCountDistinct=false → exact grouping-based
    // count; FerroDruid: not-null-filtered `cardinality` aggregation).
    // Every other Section-7 query keeps its `None` context so the
    // existing v30-36 deep-match classification stays byte-identical.
    let exact_count_distinct_ctx = json!({"useApproximateCountDistinct": false});
    for (name, sql) in &null_semantics_sql_queries() {
        let ctx =
            (*name == "null_exact_count_distinct_device").then_some(&exact_count_distinct_ctx);
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            ctx,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // Section 8 — the full Superset time-grain surface (T9), over the
    // existing wikipedia_compat dataset. PT5S / PT30S and
    // week_starting_sunday now plan (duration-granularity lowering) and
    // must deep-match live Druid; the only EXPECTED FerroDruid-side
    // failure left is week_ending_saturday (no floor-based lowering —
    // see `superset_time_grain_queries`), surfacing honestly as a
    // ferro-transport-fail.
    // ------------------------------------------------------------------
    eprintln!(
        "\n=== Section 8: Superset time grains ({} queries) ===",
        superset_time_grain_queries().len()
    );
    detail_log.push("## Section 8 — Superset time-grain surface\n".to_string());
    for (name, sql) in &superset_time_grain_queries() {
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            None,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // Section 9 — ingestion-time rollup (2026-07-11 bug pin). Ingest the
    // 6-row rolluptest dataset into BOTH engines (idempotent on the Druid
    // side, same pattern as Section 7) and diff queries whose results
    // depend on rows actually MERGING at ingest.  Unlike wikipedia_compat
    // (1 row per input either way), a FerroDruid that ignores
    // `granularitySpec.rollup` returns COUNT(*)=6 here vs Druid's 4 —
    // these queries deep-match only when rollup is honoured, and they are
    // NOT allow-listed.
    // ------------------------------------------------------------------
    eprintln!(
        "\n=== Section 9: ingestion-time rollup ({} queries) ===",
        rollup_sql_queries().len()
    );
    detail_log.push("## Section 9 — ingestion-time rollup (rolluptest dataset)\n".to_string());
    let rollup_spec = rolluptest_ingestion_spec();
    let need_rollup_ingest = match run_sql(
        &client,
        druid_base,
        &format!("SELECT COUNT(*) AS cnt FROM {ROLLUPTEST_DATASOURCE}"),
    )
    .await
    {
        Ok(v) => v
            .get(0)
            .and_then(|r| r.get("cnt"))
            .and_then(|n| n.as_i64())
            .map(|n| n == 0)
            .unwrap_or(true),
        Err(_) => true,
    };
    if need_rollup_ingest {
        eprintln!("=== Submitting Druid rolluptest ingestion task ===");
        match submit_ingestion(&client, druid_base, &rollup_spec).await {
            Ok(task_id) => {
                let final_status =
                    wait_for_task(&client, druid_base, &task_id, Duration::from_secs(180)).await;
                eprintln!("Druid rolluptest task {task_id}: {final_status}");
                // Wait for segment publication (same pattern as wikipedia).
                for _ in 0..30 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if let Ok(v) = run_sql(
                        &client,
                        druid_base,
                        &format!("SELECT COUNT(*) AS cnt FROM {ROLLUPTEST_DATASOURCE}"),
                    )
                    .await
                        && v.get(0)
                            .and_then(|r| r.get("cnt"))
                            .and_then(|n| n.as_i64())
                            .unwrap_or(0)
                            > 0
                    {
                        break;
                    }
                }
            }
            Err(e) => eprintln!("Druid rolluptest submit failed (recorded): {e}"),
        }
    } else {
        eprintln!("=== Druid datasource '{ROLLUPTEST_DATASOURCE}' already populated ===");
    }
    eprintln!("=== Submitting FerroDruid rolluptest ingestion ===");
    match submit_ingestion(&client, &ferro_base, &rollup_spec).await {
        Ok(id) => eprintln!("FerroDruid rolluptest task: {id}"),
        Err(e) => eprintln!("FerroDruid rolluptest submit failed (recorded): {e}"),
    }
    for (name, sql) in &rollup_sql_queries() {
        run_one_sql_query(
            &client,
            druid_base,
            &ferro_base,
            name,
            sql,
            None,
            &mut report,
            &mut detail_log,
        )
        .await;
    }

    // ------------------------------------------------------------------
    // 6. Print final report.
    // ------------------------------------------------------------------
    eprintln!("\n========== Druid {label} Diff Report ==========");
    eprintln!("Queries run     : {}", report.queries_run);
    eprintln!("Deep match      : {}", report.matched);
    eprintln!("Shape-only match: {}", report.shape_only_match);
    eprintln!("Mismatched      : {}", report.mismatched.len());
    eprintln!("Druid transport fails: {}", report.transport_failures.len());
    eprintln!(
        "FerroDruid transport fails: {}",
        report.ferrodruid_only_failures.len()
    );
    for (q, why) in &report.mismatched {
        eprintln!("  - {q}: {why}");
    }
    for (q, why) in &report.transport_failures {
        eprintln!("  - druid-fail {q}: {why}");
    }
    for (q, why) in &report.ferrodruid_only_failures {
        eprintln!("  - ferro-fail {q}: {why}");
    }
    eprintln!("===============================================\n");

    // ------------------------------------------------------------------
    // 7. Write the per-query detail to a results file.
    // ------------------------------------------------------------------
    let results_path = workspace_root()
        .join("tests")
        .join("druid-compat")
        .join(results_filename);
    let mut out = String::new();
    out.push_str(&format!("# Druid {label} — live diff log\n\n"));
    out.push_str("Auto-generated by `druid_diff_test::run_diff_harness`.\n\n");
    out.push_str(&format!(
        "Summary: {}/{} deep, {} shape-only, {} mismatched, \
         {} druid-transport-fails, {} ferro-transport-fails.\n\n",
        report.matched,
        report.queries_run,
        report.shape_only_match,
        report.mismatched.len(),
        report.transport_failures.len(),
        report.ferrodruid_only_failures.len(),
    ));
    for line in &detail_log {
        out.push_str(line);
        out.push('\n');
    }
    let _ = std::fs::write(&results_path, out);
    eprintln!("Wrote per-query log to: {}", results_path.display());

    // ------------------------------------------------------------------
    // 8. Tear down FerroDruid.
    // ------------------------------------------------------------------
    let _ = ferro.kill().await;
    let _ = ferro.wait().await;

    report
}

/// Assert post-conditions on the report — every query reached Druid
/// and at least one query reached FerroDruid.  This is the same gate
/// used by all version-specific test fns.
///
/// `allowed_druid_absent`: query names whose Druid-side transport
/// failure is a documented v30-specific upstream absentee (e.g.
/// `NTH_VALUE` never landed in v30; `BLOOM_FILTER_TEST` extension not
/// enabled in the micro-quickstart config). Passing them here lets
/// the assertion succeed on genuine v30 runs without watering down
/// the invariant for v35 / v36 (where the same functions land) —
/// TG-1-finding-W2A-v30-B closure (Task #24).
fn assert_report_invariants(report: &DiffReport, allowed_druid_absent: &[&str]) {
    if report.queries_run == 0 {
        // SKIP path (Druid not running) — nothing to assert.
        return;
    }
    let unexpected: Vec<_> = report
        .transport_failures
        .iter()
        .filter(|(name, _)| !allowed_druid_absent.contains(&name.as_str()))
        .collect();
    assert!(
        unexpected.is_empty(),
        "Druid transport failures (not on allow-list {allowed_druid_absent:?}): {:?}",
        unexpected,
    );
    // FerroDruid is allowed to return errors (unknown table etc.); we
    // only require that the HTTP round-trip succeeded for at least one
    // query so we know the subprocess is alive.
    let total_completed = report.matched + report.shape_only_match + report.mismatched.len();
    assert!(
        total_completed >= 1,
        "FerroDruid did not respond to a single query: {:?}",
        report.ferrodruid_only_failures,
    );
}

/// Documented Druid-side upstream absentees that the diff harness must
/// tolerate without failing the assertion. Both are pre-existing
/// DRUID-SIDE RESIDUALs classified in the W2-A 2026-06-30 evidence.
///
/// Live-verified 2026-07-11 against ALL SEVEN containers (30.0.1–36.0.0,
/// micro-quickstart config): `NTH_VALUE` is absent from Druid's SQL
/// function set in every version (Druid's window family is FIRST_VALUE /
/// LAST_VALUE / LAG / LEAD / NTILE / RANK…, no NTH_VALUE), and
/// `BLOOM_FILTER_TEST` requires the `druid-bloom-filter` extension, which
/// the harness containers do not load. The previous comment's assumption
/// that "the same functions land in v35 / v36" was contradicted by the
/// live runs (the recorded v36 report has the same two druid-transport
/// fails), so the allow-list now applies to every version.
const ALLOWED_DRUID_ABSENT: &[&str] = &["cl4_window_nth_value", "cl4_bloom_filter_test_where"];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 30.0.1 on localhost:8888 and built FerroDruid release binary"]
async fn druid_30_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "30.0.1",
        druid_base: "http://localhost:8888",
        ferro_port: 38888,
    };
    let report = run_diff_harness(&target, "RESULTS_wave30_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 32.0.1 on localhost:18888 and built FerroDruid release binary"]
async fn druid_32_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "32.0.1",
        druid_base: "http://localhost:18888",
        ferro_port: 38889,
    };
    let report = run_diff_harness(&target, "RESULTS_wave47b_v32_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 35.0.1 on localhost:28888 and built FerroDruid release binary"]
async fn druid_35_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "35.0.1",
        druid_base: "http://localhost:28888",
        ferro_port: 38890,
    };
    let report = run_diff_harness(&target, "RESULTS_wave47b_v35_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 36.0.0 on localhost:36888 and built FerroDruid release binary"]
async fn druid_36_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "36.0.0",
        druid_base: "http://localhost:36888",
        ferro_port: 38891,
    };
    let report = run_diff_harness(&target, "RESULTS_wave47c_v36_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 33.0.0 on localhost:33888 and built FerroDruid release binary"]
async fn druid_33_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "33.0.0",
        druid_base: "http://localhost:33888",
        ferro_port: 38893,
    };
    let report = run_diff_harness(&target, "RESULTS_wave47c_v33_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 34.0.0 on localhost:34888 and built FerroDruid release binary"]
async fn druid_34_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "34.0.0",
        druid_base: "http://localhost:34888",
        ferro_port: 38894,
    };
    let report = run_diff_harness(&target, "RESULTS_wave47c_v34_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Druid 31.0.2 on localhost:31888 and built FerroDruid release binary"]
async fn druid_31_vs_ferrodruid_diff() {
    let target = DruidTarget {
        label: "31.0.2",
        druid_base: "http://localhost:31888",
        ferro_port: 38895,
    };
    let report = run_diff_harness(&target, "RESULTS_wave47c_v31_run.md").await;
    assert_report_invariants(&report, ALLOWED_DRUID_ABSENT);
}
