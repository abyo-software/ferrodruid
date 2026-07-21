// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! TPC-H benchmark query execution tests.
//!
//! Generates 10K rows of TPC-H lineitem data, builds a FerroDruid segment,
//! and executes Q1 (groupBy), Q3 (topN), and Q6 (timeseries) against it.

use ferrodruid_common::tpch::{generate_lineitem_rows, tpch_queries};
use ferrodruid_query::{DruidQuery, QueryResult, execute_query};
use ferrodruid_segment::{SegmentData, SegmentDataBuilder};

/// Build a SegmentData from TPC-H generated rows.
fn build_lineitem_segment(num_rows: usize) -> SegmentData {
    let rows = generate_lineitem_rows(num_rows);

    // Collect column vectors.
    let mut timestamps = Vec::with_capacity(num_rows);
    let mut l_orderkey = Vec::with_capacity(num_rows);
    let mut l_partkey = Vec::with_capacity(num_rows);
    let mut l_suppkey = Vec::with_capacity(num_rows);
    let mut l_linenumber = Vec::with_capacity(num_rows);
    let mut l_quantity = Vec::with_capacity(num_rows);
    let mut l_extendedprice = Vec::with_capacity(num_rows);
    let mut l_discount = Vec::with_capacity(num_rows);
    let mut l_tax = Vec::with_capacity(num_rows);
    let mut l_returnflag = Vec::with_capacity(num_rows);
    let mut l_linestatus = Vec::with_capacity(num_rows);
    let mut l_shipmode = Vec::with_capacity(num_rows);
    let mut l_shipinstruct = Vec::with_capacity(num_rows);
    let mut n_name = Vec::with_capacity(num_rows);
    let mut r_name = Vec::with_capacity(num_rows);

    for row in &rows {
        timestamps.push(row["__time"].as_i64().expect("__time"));
        l_orderkey.push(row["l_orderkey"].as_i64().expect("l_orderkey"));
        l_partkey.push(row["l_partkey"].as_i64().expect("l_partkey"));
        l_suppkey.push(row["l_suppkey"].as_i64().expect("l_suppkey"));
        l_linenumber.push(row["l_linenumber"].as_i64().expect("l_linenumber"));
        l_quantity.push(row["l_quantity"].as_i64().expect("l_quantity"));
        l_extendedprice.push(row["l_extendedprice"].as_f64().expect("l_extendedprice"));
        l_discount.push(row["l_discount"].as_f64().expect("l_discount"));
        l_tax.push(row["l_tax"].as_f64().expect("l_tax"));
        l_returnflag.push(
            row["l_returnflag"]
                .as_str()
                .expect("l_returnflag")
                .to_string(),
        );
        l_linestatus.push(
            row["l_linestatus"]
                .as_str()
                .expect("l_linestatus")
                .to_string(),
        );
        l_shipmode.push(row["l_shipmode"].as_str().expect("l_shipmode").to_string());
        l_shipinstruct.push(
            row["l_shipinstruct"]
                .as_str()
                .expect("l_shipinstruct")
                .to_string(),
        );
        n_name.push(row["n_name"].as_str().expect("n_name").to_string());
        r_name.push(row["r_name"].as_str().expect("r_name").to_string());
    }

    SegmentDataBuilder::new()
        .add_timestamp_column(timestamps)
        .add_long_column("l_orderkey", false, l_orderkey)
        .add_long_column("l_partkey", false, l_partkey)
        .add_long_column("l_suppkey", false, l_suppkey)
        .add_long_column("l_linenumber", false, l_linenumber)
        .add_long_column("l_quantity", true, l_quantity)
        .add_double_column("l_extendedprice", true, l_extendedprice)
        .add_double_column("l_discount", true, l_discount)
        .add_double_column("l_tax", true, l_tax)
        .add_string_column("l_returnflag", l_returnflag)
        .add_string_column("l_linestatus", l_linestatus)
        .add_string_column("l_shipmode", l_shipmode)
        .add_string_column("l_shipinstruct", l_shipinstruct)
        .add_string_column("n_name", n_name)
        .add_string_column("r_name", r_name)
        .build()
        .expect("build segment")
}

#[test]
fn tpch_queries_parse_as_valid_druid_query() {
    for (name, query_json) in tpch_queries() {
        let json_str = serde_json::to_string(&query_json).expect("serialize");
        let parsed: DruidQuery =
            serde_json::from_str(&json_str).unwrap_or_else(|e| panic!("{name}: parse failed: {e}"));
        // Verify round-trip.
        let _re = serde_json::to_string(&parsed).expect("re-serialize");
    }
}

#[test]
fn tpch_q1_pricing_summary_report() {
    let segment = build_lineitem_segment(10_000);
    let queries = tpch_queries();
    let (name, query_json) = &queries[0];
    assert!(name.contains("Q1"));

    let json_str = serde_json::to_string(query_json).expect("serialize");
    let query: DruidQuery = serde_json::from_str(&json_str).expect("parse");
    let result = execute_query(&query, &segment).expect("execute Q1");

    match result {
        QueryResult::GroupBy(results) => {
            // Q1 groups by (l_returnflag, l_linestatus) -> 3 flags * 2 statuses = up to 6 groups
            assert!(!results.is_empty(), "Q1 must produce at least one group");
            assert!(
                results.len() <= 6,
                "Q1 should have at most 6 groups (3 flags x 2 statuses), got {}",
                results.len()
            );
            // Each group must have sum_qty, sum_base_price, count_order.
            for r in &results {
                assert!(r.event.get("sum_qty").is_some(), "Q1 group missing sum_qty");
                assert!(
                    r.event.get("sum_base_price").is_some(),
                    "Q1 group missing sum_base_price"
                );
                assert!(
                    r.event.get("count_order").is_some(),
                    "Q1 group missing count_order"
                );
                // count_order must be > 0
                let count = r.event["count_order"].as_i64().expect("count_order");
                assert!(count > 0, "Q1 count_order must be > 0");
            }
        }
        other => panic!("Q1 expected GroupBy result, got {other:?}"),
    }
}

/// Validates the parallel vectorized timeseries (Q6) fast path against an
/// independent computation of the filtered revenue sum. Row-set-defining
/// integer/bound comparisons are exact; the summed revenue is checked within a
/// relative tolerance (parallel float summation is order-non-deterministic).
#[test]
fn tpch_q6_parallel_matches_ground_truth_at_scale() {
    const N: usize = 300_000; // above the 250k parallel threshold
    let rows = generate_lineitem_rows(N);
    // Q6 filter: l_quantity == 24 AND 0.05 <= l_discount <= 0.07; revenue =
    // SUM(l_extendedprice). Interval 1994-01-01/1995-01-01 (epoch ms).
    let (i_start, i_end) = (757_382_400_000i64, 788_918_400_000i64);
    let mut expected_rev = 0.0f64;
    for row in &rows {
        let ts = row["__time"].as_i64().expect("ts");
        let qty = row["l_quantity"].as_i64().expect("qty");
        let disc = row["l_discount"].as_f64().expect("disc");
        if ts >= i_start && ts < i_end && qty == 24 && (0.05..=0.07).contains(&disc) {
            expected_rev += row["l_extendedprice"].as_f64().expect("price");
        }
    }

    let segment = build_lineitem_segment(N);
    let queries = tpch_queries();
    let (_name, query_json) = &queries[2]; // Q6
    let query: DruidQuery =
        serde_json::from_str(&serde_json::to_string(query_json).expect("ser")).expect("parse");
    let result = execute_query(&query, &segment).expect("execute Q6 at scale");

    let QueryResult::Timeseries(results) = result else {
        panic!("Q6 expected Timeseries result");
    };
    if expected_rev == 0.0 {
        // No matching rows -> Q6 emits no bucket.
        assert!(results.is_empty(), "Q6 with no matches must be empty");
        return;
    }
    assert_eq!(results.len(), 1, "granularity all -> single bucket");
    let got = results[0].result["revenue"].as_f64().expect("revenue");
    let rel = (got - expected_rev).abs() / expected_rev.abs().max(1.0);
    assert!(
        rel < 1e-9,
        "Q6 revenue: got {got}, expected {expected_rev}, rel {rel}"
    );
}

/// Validates the parallel vectorized groupBy path (only engaged above the
/// per-query row threshold) against an independent ground-truth computation.
/// count/sum_qty (integer) must match exactly; sum_base_price (f64) is checked
/// within a relative tolerance because parallel float summation is
/// order-non-deterministic by design (matches Druid).
#[test]
fn tpch_q1_parallel_matches_ground_truth_at_scale() {
    const N: usize = 300_000; // above the 250k parallel threshold
    let rows = generate_lineitem_rows(N);

    // Ground truth per (l_returnflag, l_linestatus) group.
    use std::collections::HashMap;
    let mut expected: HashMap<(String, String), (i64, u64, f64)> = HashMap::new();
    for row in &rows {
        let rf = row["l_returnflag"].as_str().expect("rf").to_string();
        let ls = row["l_linestatus"].as_str().expect("ls").to_string();
        let qty = row["l_quantity"].as_i64().expect("qty");
        let price = row["l_extendedprice"].as_f64().expect("price");
        let e = expected.entry((rf, ls)).or_insert((0, 0, 0.0));
        e.0 += qty;
        e.1 += 1;
        e.2 += price;
    }

    let segment = build_lineitem_segment(N);
    let queries = tpch_queries();
    let (_name, query_json) = &queries[0];
    let query: DruidQuery =
        serde_json::from_str(&serde_json::to_string(query_json).expect("ser")).expect("parse");
    let result = execute_query(&query, &segment).expect("execute Q1 at scale");

    let QueryResult::GroupBy(results) = result else {
        panic!("Q1 expected GroupBy result");
    };
    assert_eq!(
        results.len(),
        expected.len(),
        "parallel Q1 group count must match ground truth"
    );
    for r in &results {
        let rf = r.event["l_returnflag"].as_str().expect("rf").to_string();
        let ls = r.event["l_linestatus"].as_str().expect("ls").to_string();
        let (exp_qty, exp_count, exp_price) = *expected
            .get(&(rf.clone(), ls.clone()))
            .expect("group present");
        let got_qty = r.event["sum_qty"].as_i64().expect("sum_qty");
        let got_count = r.event["count_order"].as_i64().expect("count_order");
        let got_price = r.event["sum_base_price"].as_f64().expect("sum_base_price");
        assert_eq!(got_qty, exp_qty, "sum_qty exact for ({rf},{ls})");
        assert_eq!(got_count as u64, exp_count, "count exact for ({rf},{ls})");
        let rel = (got_price - exp_price).abs() / exp_price.abs().max(1.0);
        assert!(
            rel < 1e-9,
            "sum_base_price for ({rf},{ls}): got {got_price}, expected {exp_price}, rel {rel}"
        );
    }
}

#[test]
fn tpch_q3_shipping_priority() {
    let segment = build_lineitem_segment(10_000);
    let queries = tpch_queries();
    let (name, query_json) = &queries[1];
    assert!(name.contains("Q3"));

    let json_str = serde_json::to_string(query_json).expect("serialize");
    let query: DruidQuery = serde_json::from_str(&json_str).expect("parse");
    let result = execute_query(&query, &segment).expect("execute Q3");

    match result {
        QueryResult::TopN(results) => {
            // The interval 1995-03-01/1995-03-31 covers ~1 month of 7 years,
            // so some rows should fall in this window.
            assert!(
                !results.is_empty(),
                "Q3 must produce at least one TopN bucket"
            );
            let entries = &results[0].result;
            assert!(
                entries.len() <= 10,
                "Q3 threshold is 10, got {}",
                entries.len()
            );
            // Each entry must have revenue.
            for e in entries {
                assert!(e.get("revenue").is_some(), "Q3 entry missing revenue");
            }
        }
        other => panic!("Q3 expected TopN result, got {other:?}"),
    }
}

#[test]
fn tpch_q6_forecasting_revenue_change() {
    let segment = build_lineitem_segment(10_000);
    let queries = tpch_queries();
    let (name, query_json) = &queries[2];
    assert!(name.contains("Q6"));

    let json_str = serde_json::to_string(query_json).expect("serialize");
    let query: DruidQuery = serde_json::from_str(&json_str).expect("parse");
    let result = execute_query(&query, &segment).expect("execute Q6");

    match result {
        QueryResult::Timeseries(results) => {
            // Q6 has a narrow filter (quantity=24, discount 0.05-0.07) so may
            // return 0 or 1 result bucket with granularity "all".
            assert!(
                !results.is_empty(),
                "Q6 must produce at least one timeseries bucket"
            );
            assert!(
                results[0].result.get("revenue").is_some(),
                "Q6 bucket missing revenue"
            );
        }
        other => panic!("Q6 expected Timeseries result, got {other:?}"),
    }
}

/// Coarse performance regression tripwire for the Q6 timeseries fast path.
///
/// Gated behind `FERRODRUID_PERF_GATE=1` so it runs only in the dedicated CI
/// perf job, never in a developer's `cargo test` (a wall-clock assertion would
/// otherwise be flaky under load). It builds a fixed 1M-row segment and times
/// Q6 (median of 7, after 2 warmups); a healthy vectorized+pruned fast path is
/// ~1 ms, whereas losing the fast path (row-oriented scan) is >10 ms, so the
/// default ceiling reliably catches that class of *silent* regression without
/// false-failing on machine variance.
///
/// The ceiling is deliberately generous rather than a strict `baseline × 1.3`,
/// because the comparison must survive across heterogeneous CI hardware. To
/// tighten it toward the +30% intent on a fixed self-hosted runner, set
/// `FERRODRUID_PERF_Q6_CEIL_MS` to `<runner baseline median> * 1.3`.
#[test]
fn q6_perf_gate() {
    if std::env::var("FERRODRUID_PERF_GATE").ok().as_deref() != Some("1") {
        eprintln!("q6_perf_gate skipped (set FERRODRUID_PERF_GATE=1 to enable)");
        return;
    }
    let ceil_ms: f64 = std::env::var("FERRODRUID_PERF_Q6_CEIL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8.0);

    let segment = build_lineitem_segment(1_000_000);
    let queries = tpch_queries();
    let (name, query_json) = &queries[2];
    assert!(name.contains("Q6"));
    let query: DruidQuery =
        serde_json::from_str(&serde_json::to_string(query_json).expect("ser")).expect("parse");

    // Warmup.
    for _ in 0..2 {
        let _ = execute_query(&query, &segment).expect("execute Q6 (warmup)");
    }
    let mut samples_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let t = std::time::Instant::now();
        let _ = execute_query(&query, &segment).expect("execute Q6 (measure)");
        samples_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    samples_ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN timing"));
    let median = samples_ms[samples_ms.len() / 2];
    eprintln!("q6_perf_gate: median {median:.3} ms over 1M rows (ceiling {ceil_ms} ms)");
    assert!(
        median < ceil_ms,
        "Q6 perf regression: median {median:.3} ms exceeds ceiling {ceil_ms} ms \
         (a lost vectorized/pruned fast path scans row-oriented at >10 ms)"
    );
}
