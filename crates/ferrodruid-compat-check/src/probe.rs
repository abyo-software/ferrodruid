// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Probe model and the self-contained assertion engine.
//!
//! A [`Probe`] is one Druid-SQL statement plus an [`Expectation`] that
//! encodes the KNOWN-CORRECT Apache Druid behavior for that statement,
//! taken from the live diff-harness evidence
//! (`crates/ferrodruid-rest/tests/druid_diff_test.rs` +
//! `tests/druid-compat/RESULTS_*` — Druid 30.0.1-36.0.0, 2026-07-11).
//! Because the expected values are embedded, evaluating a probe needs
//! only the FerroDruid response — no Druid cluster is required.
//!
//! The assertion engine ([`evaluate`]) is pure: it takes the probe and
//! the raw query outcome (parsed JSON rows or a transport failure) and
//! returns a [`Verdict`]. This keeps it unit-testable with canned JSON.

use serde_json::Value;

/// Battery section a probe belongs to (mirrors the diff-harness
/// sections that were live-verified against Apache Druid 30-36).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// Fixture ingestion (synthetic results emitted by the runner; not
    /// user-selectable via `--section`).
    Fixtures,
    /// Connection ping (`SELECT 1`, the Superset `do_ping` shape).
    Ping,
    /// Base aggregate surface (COUNT/MIN/MAX/GROUP BY/WHERE/SUM).
    Aggregates,
    /// Apache Superset surface (INFORMATION_SCHEMA, TIME_FLOOR /
    /// DATE_TRUNC, `SELECT *` preview, column order, EXPLAIN).
    Superset,
    /// SQL null semantics (harness Section 7).
    Null,
    /// Superset time grains (harness Section 8).
    Grains,
    /// Ingestion-time rollup (harness Section 9).
    Rollup,
}

impl Section {
    /// Parse a user-supplied `--section` value. `"all"` is handled by
    /// the caller (it means "no filter"); unknown values return `None`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ping" => Some(Self::Ping),
            "aggregates" => Some(Self::Aggregates),
            "superset" => Some(Self::Superset),
            "null" => Some(Self::Null),
            "grains" => Some(Self::Grains),
            "rollup" => Some(Self::Rollup),
            _ => None,
        }
    }

    /// Short human label used in reports.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Fixtures => "fixtures",
            Self::Ping => "ping",
            Self::Aggregates => "aggregates",
            Self::Superset => "superset",
            Self::Null => "null",
            Self::Grains => "grains",
            Self::Rollup => "rollup",
        }
    }
}

/// Whether a probe gates the process exit code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeKind {
    /// A failed assertion makes the whole check FAIL (non-zero exit).
    Assertive,
    /// Recorded only — known-divergent or engine-internal surfaces
    /// (e.g. EXPLAIN plan bodies, the `week_ending_saturday` grain).
    /// Never fails the run; the observed behavior is reported.
    Informational,
}

/// Which self-ingested fixture datasource a probe queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fixture {
    /// No fixture needed (e.g. `SELECT 1`).
    None,
    /// The 10-row wikipedia-style dataset (aggregates / grains /
    /// Superset probes).
    Wiki,
    /// The 7-row null-semantics dataset (typed double dimension with
    /// real SQL NULLs).
    Null,
    /// The 6-raw-row rollup dataset (rolls to 4 stored rows at hour
    /// grain).
    Rollup,
}

/// Self-contained expectation on the JSON rows returned by
/// `POST /druid/v2/sql` (`resultFormat: "object"`).
#[derive(Debug, Clone)]
pub enum Expectation {
    /// Rows must deep-equal the embedded known-good value after float
    /// normalization (f64 rounded to 1e-6, exactly like the diff
    /// harness). Integer-vs-float wire typing is significant:
    /// `3` does NOT match `3.0`.
    Rows(Value),
    /// [`Expectation::Rows`] plus per-row column ORDER must match the
    /// embedded value (pins the projection-order contract that
    /// positional clients such as pydruid / Superset rely on).
    RowsOrdered(Value),
    /// Rows must match as a multiset (row order ignored). Used where
    /// both engines legitimately leave intra-bucket row order
    /// unspecified (`rollup_preview_all`).
    RowsUnordered(Value),
    /// Any HTTP-2xx response whose body is a non-empty JSON array.
    /// Used by informational surface probes (EXPLAIN).
    NonEmptyArray,
}

/// One executable probe.
#[derive(Debug, Clone)]
pub struct Probe {
    /// Stable probe name (mirrors the diff-harness query name where
    /// one exists).
    pub name: String,
    /// Battery section.
    pub section: Section,
    /// Assertive or informational.
    pub kind: ProbeKind,
    /// The Druid-SQL statement submitted to `/druid/v2/sql`.
    pub sql: String,
    /// Fixture datasource the statement reads.
    pub fixture: Fixture,
    /// The known-correct-Druid expectation.
    pub expect: Expectation,
    /// Optional context note (shown for informational probes and on
    /// failure).
    pub note: Option<String>,
}

/// Raw outcome of submitting one probe's SQL to the server.
#[derive(Debug, Clone)]
pub enum QueryOutcome {
    /// HTTP 2xx with a parsed JSON body.
    Rows(Value),
    /// Transport-level failure or non-2xx HTTP status (message
    /// includes the status and response body when available).
    Error(String),
}

/// Result of evaluating one probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Response matched the known-correct Druid behavior.
    Pass,
    /// Response diverged (or the server failed) on an assertive probe.
    Fail {
        /// Compact rendering of the expected value / condition.
        expected: String,
        /// Compact rendering of what the server actually returned.
        actual: String,
        /// Extra diagnosis (e.g. integer-vs-float wire typing).
        hint: Option<String>,
    },
    /// Probe not run (e.g. its fixture failed to ingest).
    Skip {
        /// Why the probe was skipped.
        reason: String,
    },
    /// Informational probe outcome (never fails the run).
    Info {
        /// Observed behavior, recorded honestly.
        note: String,
    },
}

/// Maximum rendered length for expected/actual excerpts in reports.
const EXCERPT_MAX: usize = 400;

/// Round every non-integer f64 in a JSON tree to 6 decimal places —
/// identical to the diff harness's normalization — so float noise in
/// the ±1e-9 range never flips a verdict. Integers are preserved
/// as-is (wire typing stays significant).
#[must_use]
pub fn normalize(v: &Value) -> Value {
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

/// Compare two JSON trees treating every number as f64 — used only to
/// produce the "numerically equal but wire types differ" hint when the
/// strict comparison fails.
fn numerically_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(fx), Some(fy)) => {
                let rx = (fx * 1e6).round() / 1e6;
                let ry = (fy * 1e6).round() / 1e6;
                rx == ry
            }
            _ => false,
        },
        (Value::Array(xs), Value::Array(ys)) => {
            xs.len() == ys.len() && xs.iter().zip(ys).all(|(x, y)| numerically_equal(x, y))
        }
        (Value::Object(xm), Value::Object(ym)) => {
            xm.len() == ym.len()
                && xm
                    .iter()
                    .all(|(k, x)| ym.get(k).is_some_and(|y| numerically_equal(x, y)))
        }
        _ => a == b,
    }
}

/// `true` when both values are arrays of objects and every row's key
/// ORDER matches the expected row's key order. Relies on the
/// workspace-wide `serde_json` `preserve_order` feature (objects keep
/// insertion order), which is exactly what BI clients observe.
fn column_order_matches(expected: &Value, actual: &Value) -> bool {
    let (Value::Array(exp), Value::Array(act)) = (expected, actual) else {
        return false;
    };
    if exp.len() != act.len() {
        return false;
    }
    exp.iter().zip(act).all(|(e, a)| match (e, a) {
        (Value::Object(em), Value::Object(am)) => em.keys().eq(am.keys()),
        _ => true,
    })
}

/// Rebuild a JSON tree with all object keys sorted, so serialized
/// comparison is insensitive to key insertion order (the workspace
/// `preserve_order` feature keeps insertion order otherwise).
fn canonical(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(canonical).collect()),
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::with_capacity(m.len());
            for k in keys {
                if let Some(val) = m.get(k) {
                    out.insert(k.clone(), canonical(val));
                }
            }
            Value::Object(out)
        }
        _ => v.clone(),
    }
}

/// Canonical multiset form: each top-level row canonicalized,
/// serialized, and sorted. `None` when the value is not an array.
fn sorted_rows(v: &Value) -> Option<Vec<String>> {
    let Value::Array(rows) = v else { return None };
    let mut out: Vec<String> = rows.iter().map(|r| canonical(r).to_string()).collect();
    out.sort();
    Some(out)
}

/// Render a JSON value compactly, truncated to [`EXCERPT_MAX`] chars.
#[must_use]
pub fn excerpt(v: &Value) -> String {
    truncate(&v.to_string())
}

fn truncate(s: &str) -> String {
    if s.chars().count() <= EXCERPT_MAX {
        s.to_string()
    } else {
        let cut: String = s.chars().take(EXCERPT_MAX).collect();
        format!("{cut}… ({} chars total)", s.chars().count())
    }
}

fn describe_expectation(e: &Expectation) -> String {
    match e {
        Expectation::Rows(v) => excerpt(v),
        Expectation::RowsOrdered(v) => format!("{} [column order significant]", excerpt(v)),
        Expectation::RowsUnordered(v) => format!("{} [row order ignored]", excerpt(v)),
        Expectation::NonEmptyArray => "any 2xx response with a non-empty JSON array".to_string(),
    }
}

/// Evaluate a probe against the raw query outcome.
#[must_use]
pub fn evaluate(probe: &Probe, outcome: &QueryOutcome) -> Verdict {
    match (probe.kind, outcome) {
        (ProbeKind::Informational, QueryOutcome::Error(e)) => Verdict::Info {
            note: format!("server declined (recorded, not a failure): {}", truncate(e)),
        },
        (ProbeKind::Informational, QueryOutcome::Rows(rows)) => {
            let matched = expectation_matches(&probe.expect, rows).is_ok();
            let n = rows.as_array().map_or(0, Vec::len);
            let note = if matched {
                format!("responded 2xx ({n} rows), matches the recorded expectation")
            } else {
                format!(
                    "responded 2xx ({n} rows); body diverges from the recorded Druid output \
                     (expected for this probe — recorded, not a failure)"
                )
            };
            Verdict::Info { note }
        }
        (ProbeKind::Assertive, QueryOutcome::Error(e)) => Verdict::Fail {
            expected: describe_expectation(&probe.expect),
            actual: format!("transport/HTTP failure: {}", truncate(e)),
            hint: None,
        },
        (ProbeKind::Assertive, QueryOutcome::Rows(rows)) => {
            match expectation_matches(&probe.expect, rows) {
                Ok(()) => Verdict::Pass,
                Err(hint) => Verdict::Fail {
                    expected: describe_expectation(&probe.expect),
                    actual: excerpt(rows),
                    hint,
                },
            }
        }
    }
}

/// Core matcher. `Ok(())` on match; `Err(hint)` on mismatch, where the
/// hint (if any) narrows down WHY (wire typing, column order, ...).
fn expectation_matches(expect: &Expectation, rows: &Value) -> Result<(), Option<String>> {
    match expect {
        Expectation::Rows(exp) => {
            let (e, a) = (normalize(exp), normalize(rows));
            if e == a {
                Ok(())
            } else {
                Err(mismatch_hint(&e, &a))
            }
        }
        Expectation::RowsOrdered(exp) => {
            let (e, a) = (normalize(exp), normalize(rows));
            if e != a {
                return Err(mismatch_hint(&e, &a));
            }
            if column_order_matches(&e, &a) {
                Ok(())
            } else {
                Err(Some(
                    "values match but column ORDER differs — positional clients \
                     (pydruid / Superset) would mis-map columns"
                        .to_string(),
                ))
            }
        }
        Expectation::RowsUnordered(exp) => {
            let (e, a) = (normalize(exp), normalize(rows));
            match (sorted_rows(&e), sorted_rows(&a)) {
                (Some(se), Some(sa)) if se == sa => Ok(()),
                _ => Err(None),
            }
        }
        Expectation::NonEmptyArray => match rows {
            Value::Array(a) if !a.is_empty() => Ok(()),
            _ => Err(Some("expected a non-empty JSON array".to_string())),
        },
    }
}

fn mismatch_hint(expected: &Value, actual: &Value) -> Option<String> {
    if numerically_equal(expected, actual) {
        return Some(
            "values are numerically equal but wire types differ (integer vs float) — \
             type-sensitive Druid clients distinguish these"
                .to_string(),
        );
    }
    let (el, al) = (
        expected.as_array().map(Vec::len),
        actual.as_array().map(Vec::len),
    );
    if let (Some(e), Some(a)) = (el, al)
        && e != a
    {
        return Some(format!("row count differs: expected {e}, got {a}"));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn probe(kind: ProbeKind, expect: Expectation) -> Probe {
        Probe {
            name: "t".to_string(),
            section: Section::Aggregates,
            kind,
            sql: "SELECT 1".to_string(),
            fixture: Fixture::None,
            expect,
            note: None,
        }
    }

    #[test]
    fn rows_exact_match_passes() {
        let p = probe(
            ProbeKind::Assertive,
            Expectation::Rows(json!([{"cnt": 10}])),
        );
        let v = evaluate(&p, &QueryOutcome::Rows(json!([{"cnt": 10}])));
        assert_eq!(v, Verdict::Pass);
    }

    #[test]
    fn rows_value_mismatch_fails_with_row_count_hint() {
        let p = probe(
            ProbeKind::Assertive,
            Expectation::Rows(json!([{"cnt": 10}, {"cnt": 11}])),
        );
        let v = evaluate(&p, &QueryOutcome::Rows(json!([{"cnt": 10}])));
        match v {
            Verdict::Fail { hint, .. } => {
                assert_eq!(
                    hint.as_deref(),
                    Some("row count differs: expected 2, got 1")
                );
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn float_noise_within_tolerance_passes() {
        // 15.000000_4 rounds to 15.0 at 1e-6 — must match 15.0.
        let p = probe(
            ProbeKind::Assertive,
            Expectation::Rows(json!([{"avg_v": 15.0}])),
        );
        let v = evaluate(&p, &QueryOutcome::Rows(json!([{"avg_v": 15.000_000_4}])));
        assert_eq!(v, Verdict::Pass);
    }

    #[test]
    fn integer_vs_float_wire_typing_fails_with_hint() {
        // COUNT(DISTINCT ...) must be an integer on the wire; 3.0 is a
        // divergence even though it is numerically equal.
        let p = probe(ProbeKind::Assertive, Expectation::Rows(json!([{"dc": 3}])));
        let v = evaluate(&p, &QueryOutcome::Rows(json!([{"dc": 3.0}])));
        match v {
            Verdict::Fail { hint, .. } => {
                let h = hint.expect("wire-typing hint");
                assert!(h.contains("wire types differ"), "hint was: {h}");
            }
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn null_aggregate_expectation_distinguishes_null_from_zero() {
        // AVG over an all-null group is SQL NULL in Druid; 0 must FAIL.
        let exp = json!([{"site_id": "site_c", "avg_v": null}]);
        let p = probe(ProbeKind::Assertive, Expectation::Rows(exp));
        let pass = evaluate(
            &p,
            &QueryOutcome::Rows(json!([{"site_id": "site_c", "avg_v": null}])),
        );
        assert_eq!(pass, Verdict::Pass);
        let fail = evaluate(
            &p,
            &QueryOutcome::Rows(json!([{"site_id": "site_c", "avg_v": 0.0}])),
        );
        assert!(matches!(fail, Verdict::Fail { .. }));
    }

    #[test]
    fn column_order_significant_for_rows_ordered() {
        let exp = json!([{"c": 6, "s": "en"}]);
        let p = probe(ProbeKind::Assertive, Expectation::RowsOrdered(exp));
        // Same values, swapped key order (preserve_order keeps it).
        let swapped = json!([{"s": "en", "c": 6}]);
        let v = evaluate(&p, &QueryOutcome::Rows(swapped));
        match v {
            Verdict::Fail { hint, .. } => {
                let h = hint.expect("column-order hint");
                assert!(h.contains("column ORDER"), "hint was: {h}");
            }
            other => panic!("expected Fail, got {other:?}"),
        }
        // Correct order passes.
        let ok = evaluate(&p, &QueryOutcome::Rows(json!([{"c": 6, "s": "en"}])));
        assert_eq!(ok, Verdict::Pass);
    }

    #[test]
    fn rows_unordered_ignores_row_order_only() {
        let exp = json!([{"a": 1}, {"a": 2}]);
        let p = probe(ProbeKind::Assertive, Expectation::RowsUnordered(exp));
        let v = evaluate(&p, &QueryOutcome::Rows(json!([{"a": 2}, {"a": 1}])));
        assert_eq!(v, Verdict::Pass);
        let bad = evaluate(&p, &QueryOutcome::Rows(json!([{"a": 2}, {"a": 3}])));
        assert!(matches!(bad, Verdict::Fail { .. }));
    }

    #[test]
    fn assertive_transport_error_fails() {
        let p = probe(ProbeKind::Assertive, Expectation::Rows(json!([{"ok": 1}])));
        let v = evaluate(&p, &QueryOutcome::Error("HTTP 500: boom".to_string()));
        match v {
            Verdict::Fail { actual, .. } => assert!(actual.contains("HTTP 500")),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn informational_never_fails() {
        let p = probe(ProbeKind::Informational, Expectation::NonEmptyArray);
        let err = evaluate(
            &p,
            &QueryOutcome::Error("HTTP 501: fail-closed".to_string()),
        );
        assert!(matches!(err, Verdict::Info { .. }));
        let ok = evaluate(&p, &QueryOutcome::Rows(json!([{"PLAN": "..."}])));
        assert!(matches!(ok, Verdict::Info { .. }));
        let empty = evaluate(&p, &QueryOutcome::Rows(json!([])));
        assert!(matches!(empty, Verdict::Info { .. }));
    }

    #[test]
    fn section_parse_round_trips() {
        for s in ["ping", "aggregates", "superset", "null", "grains", "rollup"] {
            let sec = Section::parse(s).expect("known section");
            assert_eq!(sec.label(), s);
        }
        assert_eq!(Section::parse("all"), None);
        assert_eq!(Section::parse("fixtures"), None);
    }
}
