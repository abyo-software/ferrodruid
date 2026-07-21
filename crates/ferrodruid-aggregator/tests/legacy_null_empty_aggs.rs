// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — aggregator-level empty-set defaults.
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! Oracle basis (measured, never assumed — Druid 27.0.0 legacy,
//! `tests/segment-compat/fixtures/legacy_null_druid27/oracle/legacy27_ext/`):
//!
//! * `empty_sum_y.json` — SUM over an empty set = `0` (ANSI: null).
//! * `empty_min_y.json` — MIN = `9223372036854775807` (i64::MAX sentinel).
//! * `empty_max_y.json` — MAX = `-9223372036854775808` (i64::MIN sentinel).
//! * `ext_empty_sum_d.json` — double SUM = `0.0`.
//! * `ext_empty_min_d.json` / `ext_empty_max_d.json` — double MIN/MAX =
//!   the JSON STRINGS `"Infinity"` / `"-Infinity"` (Druid's ±∞ init
//!   sentinels on the wire; ANSI: null).
//! * `count_distinct_strcol.json` — approximate COUNT DISTINCT counts the
//!   merged ''/null as ONE distinct value (4 on the fixture; ANSI: 3),
//!   i.e. the HLL build must hash a null feed as `""` instead of
//!   skipping it.
//! * `empty_avg_y.json` / `avg_x.json` — AVG over an empty set (0/0 in
//!   the sum/count post-agg) = `0` (ANSI: null).

use std::collections::HashMap;

use ferrodruid_aggregator::{
    Aggregator, AggregatorSpec, DoubleMaxAggregator, DoubleMinAggregator, DoubleSumAggregator,
    FloatMaxAggregator, FloatMinAggregator, FloatSumAggregator, HllSketchAggregator,
    LongMaxAggregator, LongMinAggregator, LongSumAggregator, PostAggregatorSpec,
    merge_json_by_spec,
};
use serde_json::{Value, json};

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

#[test]
fn empty_sums_default_to_zero() {
    latch_legacy();
    assert_eq!(LongSumAggregator::new().get(), json!(0));
    assert_eq!(DoubleSumAggregator::new().get(), json!(0.0));
    assert_eq!(FloatSumAggregator::new().get(), json!(0.0));
}

#[test]
fn empty_long_minmax_leak_the_i64_sentinels() {
    latch_legacy();
    assert_eq!(
        LongMinAggregator::new().get(),
        json!(i64::MAX),
        "oracle empty_min_y.json: 9223372036854775807"
    );
    assert_eq!(
        LongMaxAggregator::new().get(),
        json!(i64::MIN),
        "oracle empty_max_y.json: -9223372036854775808"
    );
}

#[test]
fn empty_double_float_minmax_render_infinity_strings() {
    latch_legacy();
    assert_eq!(DoubleMinAggregator::new().get(), json!("Infinity"));
    assert_eq!(DoubleMaxAggregator::new().get(), json!("-Infinity"));
    assert_eq!(FloatMinAggregator::new().get(), json!("Infinity"));
    assert_eq!(FloatMaxAggregator::new().get(), json!("-Infinity"));
}

/// The sentinels must stay merge IDENTITIES: folding an empty partial's
/// `get()` into a non-empty accumulator must not change its value (a
/// numeric i64::MAX folds away by comparison; the ±Infinity strings
/// extract no number, so the merge is a no-op).
#[test]
fn legacy_empty_partials_stay_merge_identities() {
    latch_legacy();
    let mut min = LongMinAggregator::new();
    min.aggregate(Some(&json!(7)));
    let empty = LongMinAggregator::new();
    min.merge(&empty);
    assert_eq!(min.get(), json!(7), "min(7, i64::MAX sentinel) = 7");

    // Reverse order: sentinel-first accumulator adopts the real value.
    let mut min2 = LongMinAggregator::new();
    min2.merge(&min);
    assert_eq!(min2.get(), json!(7));

    let mut dmax = DoubleMaxAggregator::new();
    dmax.aggregate(Some(&json!(2.5)));
    dmax.merge(&DoubleMaxAggregator::new());
    assert_eq!(dmax.get(), json!(2.5), "max(2.5, \"-Infinity\") = 2.5");
}

/// H2: the broker JSON min/max merge must compare LONG partials as i64
/// (exact) — an EMPTY legacy longMin partial emits the `i64::MAX`
/// sentinel, and an f64 comparison collapses it onto a REAL
/// `i64::MAX - 1`, letting the empty shard win by SHARD ORDER.  The
/// sentinel must be a true merge identity: the real near-boundary value
/// wins regardless of order (analogous for longMax near `i64::MIN`).
#[test]
fn broker_json_minmax_merge_empty_sentinel_never_beats_real_boundary() {
    latch_legacy();
    let min_spec = AggregatorSpec::LongMin {
        name: "m".to_string(),
        field_name: "v".to_string(),
    };
    let empty_min = LongMinAggregator::new().get();
    assert_eq!(empty_min, json!(i64::MAX), "legacy empty longMin sentinel");
    let real = json!(i64::MAX - 1);
    assert_eq!(
        merge_json_by_spec(&min_spec, &empty_min, &real),
        real,
        "shard order empty→real: the real i64::MAX-1 must win"
    );
    assert_eq!(
        merge_json_by_spec(&min_spec, &real, &empty_min),
        real,
        "shard order real→empty: the real i64::MAX-1 must win"
    );
    // Empty ⊕ empty still finalizes to the oracle-measured legacy
    // default (empty_min_y.json: 9223372036854775807).
    assert_eq!(
        merge_json_by_spec(&min_spec, &empty_min, &LongMinAggregator::new().get()),
        json!(i64::MAX)
    );

    let max_spec = AggregatorSpec::LongMax {
        name: "m".to_string(),
        field_name: "v".to_string(),
    };
    let empty_max = LongMaxAggregator::new().get();
    assert_eq!(empty_max, json!(i64::MIN), "legacy empty longMax sentinel");
    let real = json!(i64::MIN + 1);
    assert_eq!(
        merge_json_by_spec(&max_spec, &empty_max, &real),
        real,
        "shard order empty→real: the real i64::MIN+1 must win"
    );
    assert_eq!(
        merge_json_by_spec(&max_spec, &real, &empty_max),
        real,
        "shard order real→empty: the real i64::MIN+1 must win"
    );
    assert_eq!(
        merge_json_by_spec(&max_spec, &empty_max, &LongMaxAggregator::new().get()),
        json!(i64::MIN)
    );
}

/// Latch-gating (H1): under the LEGACY latch the broker min/max merge
/// stays i64-EXACT for adjacent integers above 2^53 — the sentinel
/// identity above depends on it, so gating the exact comparator to the
/// latch must not have re-introduced the f64 collapse HERE (only ANSI
/// keeps the historical `as_f64` tie-keeps-dst behavior).
#[test]
fn legacy_broker_minmax_merge_is_exact_beyond_2_pow_53() {
    latch_legacy();
    const P53: i64 = 9_007_199_254_740_992; // 2^53
    let min_spec = AggregatorSpec::LongMin {
        name: "m".to_string(),
        field_name: "v".to_string(),
    };
    let max_spec = AggregatorSpec::LongMax {
        name: "m".to_string(),
        field_name: "v".to_string(),
    };
    let lo = json!(P53);
    let hi = json!(P53 + 1);
    assert_eq!(merge_json_by_spec(&min_spec, &hi, &lo), lo, "exact min");
    assert_eq!(merge_json_by_spec(&min_spec, &lo, &hi), lo, "exact min");
    assert_eq!(merge_json_by_spec(&max_spec, &lo, &hi), hi, "exact max");
    assert_eq!(merge_json_by_spec(&max_spec, &hi, &lo), hi, "exact max");
}

/// A non-empty aggregate is unchanged by the legacy latch (values, not
/// defaults): the latch only replaces the NO-INPUT `get()`.
#[test]
fn non_empty_aggregates_unchanged() {
    latch_legacy();
    let mut sum = LongSumAggregator::new();
    sum.aggregate(Some(&json!(10)));
    sum.aggregate(Some(&json!(20)));
    assert_eq!(sum.get(), json!(30));
    let mut min = LongMinAggregator::new();
    min.aggregate(Some(&json!(5)));
    assert_eq!(min.get(), json!(5));
}

/// H2 scoping fix: the GENERIC `expression` post-aggregator does
/// ORDINARY division under the legacy latch — `0/0` is NOT forced to 0.
/// Druid's expression division is plain numeric division in every null
/// mode; only AVG's LOWERING carries the divide-by-zero → 0 exception
/// (the arithmetic `/` post-agg, see
/// [`legacy_avg_arithmetic_divide_carrier_answers_zero_on_empty`]).  A
/// legacy `ROUND(SUM(x)/COUNT(*), 2)` over an empty match therefore
/// stays null, exactly as pre-W-B.
#[test]
fn legacy_generic_expression_division_is_not_forced_to_zero() {
    latch_legacy();
    let post = PostAggregatorSpec::Expression {
        name: "r".to_string(),
        expression: "round(\"s\" / \"c\", 2)".to_string(),
    };
    let mut aggs: HashMap<String, Value> = HashMap::new();
    aggs.insert("s".to_string(), json!(0.0));
    aggs.insert("c".to_string(), json!(0));
    assert_eq!(
        post.evaluate(&aggs),
        None,
        "generic 0/0 must stay null (NaN → None) under legacy — the \
         divide-by-zero → 0 exception belongs to AVG's lowering only"
    );

    // x/0 for x != 0 likewise keeps ordinary (non-finite → None) division.
    aggs.insert("s".to_string(), json!(5.0));
    let bare = PostAggregatorSpec::Expression {
        name: "d".to_string(),
        expression: "\"s\" / \"c\"".to_string(),
    };
    assert_eq!(
        bare.evaluate(&aggs),
        None,
        "generic x/0 must stay null (infinity → None) under legacy"
    );
}

/// The AVG carrier (H2 scoping fix): under the latch AVG lowers to an
/// arithmetic `/` post-agg over its hidden sum/count helpers, whose
/// divide-by-zero → 0 rule (pre-existing, Druid arithmetic post-agg
/// semantics, mode-independent) answers the oracle empty-set AVG
/// (`empty_avg_y.json`: 0/0 = 0; ANSI keeps the `expression` lowering
/// whose 0/0 → None → SQL null).
#[test]
fn legacy_avg_arithmetic_divide_carrier_answers_zero_on_empty() {
    latch_legacy();
    let post = PostAggregatorSpec::Arithmetic {
        name: "a".to_string(),
        fn_name: "/".to_string(),
        fields: vec![
            PostAggregatorSpec::FieldAccess {
                name: "$avg_sum_0".to_string(),
                field_name: "$avg_sum_0".to_string(),
            },
            PostAggregatorSpec::FieldAccess {
                name: "$avg_count_0".to_string(),
                field_name: "$avg_count_0".to_string(),
            },
        ],
    };
    let mut aggs: HashMap<String, Value> = HashMap::new();
    // A legacy EMPTY match set: the empty doubleSum reports the 0.0
    // identity and the not-null-filtered count reports 0.
    aggs.insert("$avg_sum_0".to_string(), DoubleSumAggregator::new().get());
    aggs.insert("$avg_count_0".to_string(), json!(0));
    assert_eq!(
        post.evaluate(&aggs),
        Some(0.0),
        "oracle empty_avg_y.json: legacy AVG over an empty set = 0"
    );
}

/// oracle count_distinct_strcol.json: the HLL build hashes a JSON-null
/// feed as `""` under legacy — the merged ''/null value is ONE distinct
/// value, so {null-feed, "a", "b", "c"} estimates 4 (ANSI skips the null
/// feed → 3), and a `""` feed lands in the SAME register as the null
/// feed (one merged value, not two).
#[test]
fn hll_build_hashes_null_feed_as_empty_string() {
    latch_legacy();
    // Null feed COUNTS under legacy: {null, a, b, c} = 4 (ANSI skips the
    // null feed → 3).  No literal "" is fed — this isolates the null feed.
    let mut hll = HllSketchAggregator::build(14);
    hll.aggregate(Some(&Value::Null));
    hll.aggregate(Some(&json!("a")));
    hll.aggregate(Some(&json!("b")));
    hll.aggregate(Some(&json!("c")));
    let est = hll.estimate();
    assert!(
        (est - 4.0).abs() < 0.01,
        "legacy HLL must count a null feed as a value: estimate {est} != 4"
    );

    // ...and it lands in the SAME register as a literal "" feed (one
    // merged value, not two): {null, ""} estimates 1.
    let mut merged = HllSketchAggregator::build(14);
    merged.aggregate(Some(&Value::Null));
    merged.aggregate(Some(&json!("")));
    let est = merged.estimate();
    assert!(
        (est - 1.0).abs() < 0.01,
        "legacy HLL: null and \"\" must hash identically: estimate {est} != 1"
    );
}
