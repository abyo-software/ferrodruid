// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W-B legacy null mode — numeric filter equality must stay i64-EXACT.
//!
//! **This test binary latches the process-global legacy-null mode ON** in
//! every test; no ANSI-mode test may be added here (own-process latch).
//!
//! The legacy branch of `values_equal` exists so the coerced numeric
//! default `0` matches the SQL literal `0.0` (oracle
//! `ext_count_d_eq_0.json`).  It must NOT alter equality between real
//! non-null integers: routing two i64 operands through f64 collapses
//! neighbours beyond 2^53 (the compat-10 silent-precision-loss species),
//! making `selector(x, 9007199254740992)` wrongly match a row holding
//! `9007199254740993` and letting selector/IN return extra rows.

use std::collections::HashMap;

use ferrodruid_query::FilterSpec;
use serde_json::{Value, json};

/// 2^53 — the first f64 rounding boundary for integers.
const P53: i64 = 9_007_199_254_740_992;

fn latch_legacy() {
    assert!(
        ferrodruid_common::null_mode::init_legacy_null_mode(true),
        "this test binary requires the legacy-null latch"
    );
}

fn row(col: &str, v: Value) -> HashMap<String, Value> {
    HashMap::from([(col.to_string(), v)])
}

fn selector(dim: &str, v: Value) -> FilterSpec {
    FilterSpec::Selector {
        dimension: dim.to_string(),
        value: Some(v),
    }
}

/// A legacy selector on a LONG column with 2^53 / 2^53+1 matches ONLY
/// the queried value — never its f64-collapsed neighbour.
#[test]
fn legacy_selector_is_exact_beyond_2_pow_53() {
    latch_legacy();
    let r = row("x", json!(P53 + 1));
    assert!(
        !selector("x", json!(P53)).matches(&r),
        "selector(2^53) must NOT match a row holding 2^53+1"
    );
    assert!(
        selector("x", json!(P53 + 1)).matches(&r),
        "selector(2^53+1) must match the row holding 2^53+1"
    );

    // The i64 boundary pair the empty-min/max sentinels sit on.
    let r = row("x", json!(i64::MAX));
    assert!(
        !selector("x", json!(i64::MAX - 1)).matches(&r),
        "selector(i64::MAX-1) must NOT match a row holding i64::MAX"
    );
    assert!(selector("x", json!(i64::MAX)).matches(&r));
}

/// Same exactness for the IN filter.
#[test]
fn legacy_in_filter_is_exact_beyond_2_pow_53() {
    latch_legacy();
    let r = row("x", json!(P53 + 1));
    let in_p53 = FilterSpec::In {
        dimension: "x".to_string(),
        values: vec![json!(P53)],
    };
    assert!(
        !in_p53.matches(&r),
        "IN [2^53] must NOT match a row holding 2^53+1"
    );
    let in_p53_1 = FilterSpec::In {
        dimension: "x".to_string(),
        values: vec![json!(P53 + 1)],
    };
    assert!(in_p53_1.matches(&r));
}

/// An integral FLOAT literal only equals an integer row value when it
/// represents that exact integer (2^53.0 must not match 2^53+1).
#[test]
fn legacy_float_literal_vs_i64_row_is_exact() {
    latch_legacy();
    let r = row("x", json!(P53 + 1));
    #[allow(clippy::cast_precision_loss)]
    let p53_float = json!(P53 as f64); // exactly 2^53.0
    assert!(
        !selector("x", p53_float).matches(&r),
        "float 2^53.0 must NOT match the integer 2^53+1"
    );
    let r2 = row("x", json!(P53));
    #[allow(clippy::cast_precision_loss)]
    let p53_float = json!(P53 as f64);
    assert!(
        selector("x", p53_float).matches(&r2),
        "float 2^53.0 DOES equal the integer 2^53 exactly"
    );
}

/// The behavior the legacy branch exists for (oracle
/// `ext_count_d_eq_0.json`): the coerced double default `0.0` still
/// matches the SQL literal `0` numerically.
#[test]
fn legacy_zero_int_still_matches_zero_double() {
    latch_legacy();
    let r = row("d", json!(0.0));
    assert!(
        selector("d", json!(0)).matches(&r),
        "legacy `d = 0` must match the coerced 0.0 rows (oracle-pinned)"
    );
    let r = row("d", json!(1.5));
    assert!(!selector("d", json!(0)).matches(&r));
}
