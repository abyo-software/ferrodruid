// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Internal helpers shared across query types.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_aggregator::{Aggregator, AggregatorSpec};
use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_common::types::{ColumnType, DimensionSpec};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, is_null_long_row};

// ---------------------------------------------------------------------------
// GranularitySpec — flexible deserialization
// ---------------------------------------------------------------------------

/// A flexible granularity specification that accepts both a simple string
/// (e.g. `"day"`, `"all"`) and the full object form used by
/// [`ferrodruid_common::types::Granularity`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GranularitySpec {
    /// Simple string granularity (e.g. `"all"`, `"day"`, `"hour"`).
    Simple(String),
    /// Full granularity object (e.g. `{"type":"duration","period_ms":86400000,"origin":...}`).
    Full(ferrodruid_common::types::Granularity),
}

// ---------------------------------------------------------------------------
// Interval-list deserialization — accept either a single ISO interval
// string (`"2024-01-01/2024-01-04"`) or an array of strings
// (`["2024-01-01/2024-01-04"]`). Apache Druid's native query schema
// documents both shapes; pydruid + the `druid` Python client default to
// the single-string form, and pre-fix FerroDruid rejected those with
// `invalid type: string, expected a sequence` (TG-4-finding-001, W2-D).
// Same shape as the Wave 47-D shorthand fix for `granularity`.
// ---------------------------------------------------------------------------

/// Deserialize a Druid `intervals` field that may be either a single
/// `"start/end"` string or an array of such strings. Always yields a
/// `Vec<String>` — the executors and `parse_intervals` already handle
/// the empty-vec / multi-vec cases.
///
/// # Errors
///
/// Returns the underlying deserializer error if the input is neither a
/// string nor an array of strings.
pub fn deserialize_intervals<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrVec {
        One(String),
        Many(Vec<String>),
    }
    match StringOrVec::deserialize(deserializer)? {
        StringOrVec::One(s) => Ok(vec![s]),
        StringOrVec::Many(v) => Ok(v),
    }
}

/// `Option<Vec<String>>` variant of [`deserialize_intervals`] for the
/// optional `intervals` field on `segmentMetadata` queries. An absent
/// field stays `None`; a present string or array becomes
/// `Some(vec![...])`.
///
/// # Errors
///
/// Returns the underlying deserializer error on mismatched shape.
pub fn deserialize_optional_intervals<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrVecOrNull {
        One(String),
        Many(Vec<String>),
    }
    let opt: Option<StringOrVecOrNull> = Option::deserialize(deserializer)?;
    Ok(opt.map(|v| match v {
        StringOrVecOrNull::One(s) => vec![s],
        StringOrVecOrNull::Many(v) => v,
    }))
}

// ---------------------------------------------------------------------------
// Interval parsing
// ---------------------------------------------------------------------------

/// Parse a list of ISO-8601 interval strings into `(start_millis, end_millis)` pairs.
///
/// Each string is expected to be in the form `"start/end"` where start and end
/// are ISO-8601 timestamps.
///
/// DD R40: interval parsing now fails CLOSED. The previous implementation
/// silently skipped any interval it could not parse and returned a
/// possibly-empty `Vec`.  Every executor guards time filtering with
/// `if !intervals.is_empty()`, so a query whose ONLY interval was malformed
/// (e.g. `"2024-01-01T00:00:00Z/not-a-date"`) parsed to an empty list and then
/// scanned ALL rows instead of being rejected — a fail-open time filter.  An
/// absent/empty `intervals` field still legitimately means "all time"; only a
/// provided-but-malformed interval string is an error.
///
/// # Errors
///
/// Returns [`DruidError::Query`] when any supplied interval string is not a
/// well-formed `"start/end"` pair of ISO-8601 timestamps.  Because any
/// malformed entry errors, a non-empty input that yields zero valid intervals
/// is rejected rather than collapsing to "all time".
pub fn parse_intervals(intervals: &[String]) -> Result<Vec<(i64, i64)>> {
    let mut out = Vec::with_capacity(intervals.len());
    for s in intervals {
        let parts: Vec<&str> = s.splitn(2, '/').collect();
        let parsed = match (parts.first(), parts.get(1)) {
            (Some(start), Some(end)) => parse_iso_millis(start).zip(parse_iso_millis(end)),
            _ => None,
        };
        match parsed {
            Some(pair) => out.push(pair),
            None => {
                return Err(DruidError::Query(format!(
                    "invalid interval \"{s}\"; expected an ISO-8601 \"start/end\" pair"
                )));
            }
        }
    }
    Ok(out)
}

/// Parse an ISO-8601 timestamp string into epoch milliseconds.
///
/// Accepts full RFC-3339 timestamps (`"2024-01-01T00:00:00Z"`), the trailing-`Z`
/// short form, and — DD R40 — the bare date form `"YYYY-MM-DD"` (interpreted as
/// midnight UTC).  Date-only bounds are valid ISO-8601 intervals in Druid;
/// previously `parse_intervals` silently skipped them, which combined with the
/// `if !intervals.is_empty()` executor guard to disable time filtering
/// altogether (fail-open).  Returns `None` for genuinely malformed strings so
/// the caller can reject the query.
pub fn parse_iso_millis(s: &str) -> Option<i64> {
    if let Some(dt) = chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .or_else(|| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ").ok())
        .or_else(|| {
            // Handle formats like "2024-01-01T00:00:00.000Z"
            let with_tz = if s.ends_with('Z') {
                s.to_string()
            } else {
                format!("{s}Z")
            };
            chrono::DateTime::parse_from_rfc3339(&with_tz).ok()
        })
    {
        return Some(dt.timestamp_millis());
    }

    // Bare date form "YYYY-MM-DD" -> midnight UTC.
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|ndt| ndt.and_utc().timestamp_millis())
}

/// Time-interval pruning: when the `__time` column is sorted ascending and the
/// query has a single interval, return the `[lo, hi)` row range that falls in
/// `[start, end)` so callers scan only matching rows instead of the whole
/// segment (Druid's segment/time pruning, applied at row granularity).
///
/// Returns `None` (caller scans all rows with a per-row interval check) when
/// there is not exactly one interval or the timestamps are not sorted. Real
/// ingested segments are timestamp-sorted (`ferrodruid-ingest-batch`), so this
/// engages on production data; the `is_sorted` guard keeps it correct if a
/// segment ever is not.
#[must_use]
pub fn pruned_row_range(
    timestamps: &[i64],
    intervals: &[(i64, i64)],
    time_sorted: bool,
) -> Option<(usize, usize)> {
    let &[(start, end)] = intervals else {
        return None;
    };
    if !time_sorted {
        return None;
    }
    // Defense-in-depth (compat-12): the binary range-prune below is only correct
    // when `__time` is genuinely ascending. `time_sorted` is an INVARIANT upheld
    // by every install boundary reconciling the flag against the real `__time`
    // (`SegmentEntry::resident` via `Arc::make_mut`, spill re-derive, the honest
    // `SegmentDataBuilder`) — not a local guarantee here. This assert makes a
    // lying flag that ever reaches the fast-path fail LOUD in debug/CT builds
    // (zero release cost) instead of silently binary-pruning a non-ascending
    // segment and dropping rows (the FG-7 R21 failure mode).
    debug_assert!(
        timestamps.is_sorted(),
        "pruned_row_range trusted time_sorted=true but __time is not ascending — \
         a lying flag reached the vectorized fast-path (install-boundary reconcile missed)"
    );
    let lo = timestamps.partition_point(|&t| t < start);
    let hi = timestamps.partition_point(|&t| t < end);
    Some((lo, hi))
}

/// Validate a granularity spec before execution (DD R48).
///
/// A `duration` granularity with `periodMs == 0` would divide-by-zero in
/// [`bucket_timestamp`] (a request-triggered panic/DoS), and one exceeding
/// `i64::MAX` would cast to a negative `i64` and mis-bucket. Reject both up front.
///
/// # Errors
///
/// Returns [`DruidError::Query`] for a non-positive or oversized duration period.
pub fn validate_granularity(granularity: &GranularitySpec) -> Result<()> {
    if let GranularitySpec::Full(ferrodruid_common::types::Granularity::Duration {
        period_ms,
        ..
    }) = granularity
    {
        if *period_ms == 0 {
            return Err(DruidError::Query(
                "granularity duration periodMs must be greater than 0".to_owned(),
            ));
        }
        if *period_ms > i64::MAX as u64 {
            return Err(DruidError::Query(
                "granularity duration periodMs is too large".to_owned(),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Bucketing
// ---------------------------------------------------------------------------

/// Bucket an epoch-millis timestamp according to the granularity.
pub fn bucket_timestamp(ts_millis: i64, granularity: &GranularitySpec) -> i64 {
    let period_ms = match granularity {
        GranularitySpec::Simple(s) => match s.to_lowercase().as_str() {
            "all" | "none" => return 0,
            "second" => 1_000,
            "minute" => 60_000,
            "five_minute" => 300_000,
            "ten_minute" => 600_000,
            "fifteen_minute" => 900_000,
            "thirty_minute" => 1_800_000,
            "hour" => 3_600_000,
            "six_hour" => 21_600_000,
            "day" => 86_400_000,
            "week" => return bucket_week(ts_millis),
            "month" => return bucket_month(ts_millis),
            "quarter" => return bucket_quarter(ts_millis),
            "year" => return bucket_year(ts_millis),
            _ => return 0,
        },
        GranularitySpec::Full(g) => match g {
            ferrodruid_common::types::Granularity::None => return ts_millis,
            ferrodruid_common::types::Granularity::Second => 1_000,
            ferrodruid_common::types::Granularity::Minute => 60_000,
            ferrodruid_common::types::Granularity::FiveMinute => 300_000,
            ferrodruid_common::types::Granularity::TenMinute => 600_000,
            ferrodruid_common::types::Granularity::FifteenMinute => 900_000,
            ferrodruid_common::types::Granularity::ThirtyMinute => 1_800_000,
            ferrodruid_common::types::Granularity::Hour => 3_600_000,
            ferrodruid_common::types::Granularity::SixHour => 21_600_000,
            ferrodruid_common::types::Granularity::Day => 86_400_000,
            ferrodruid_common::types::Granularity::Week => return bucket_week(ts_millis),
            ferrodruid_common::types::Granularity::Month => return bucket_month(ts_millis),
            ferrodruid_common::types::Granularity::Quarter => return bucket_quarter(ts_millis),
            ferrodruid_common::types::Granularity::Year => return bucket_year(ts_millis),
            ferrodruid_common::types::Granularity::Duration {
                period_ms, origin, ..
            } => {
                // DD R48: defensive guard mirroring `validate_granularity` —
                // never divide by zero (or by a negative cast of a huge u64).
                // Executors validate up front, so this is a belt-and-braces path.
                let period = (*period_ms).min(i64::MAX as u64) as i64;
                if period <= 0 {
                    return ts_millis;
                }
                // i128 intermediate + euclidean floor, like `bucket_week`:
                // `ts - origin` can overflow i64 for extreme inputs, and the
                // pre-fix truncating division rounded PRE-ORIGIN offsets
                // toward zero — a bucket start AFTER the timestamp — instead
                // of Druid's floor toward -inf (bucket start <= timestamp,
                // always).
                let period = i128::from(period);
                let origin_ms = i128::from(origin.timestamp_millis());
                let offset = i128::from(ts_millis) - origin_ms;
                let bucket = offset.div_euclid(period) * period + origin_ms;
                // The bucket start is always <= the input, so the only
                // unrepresentable case is a bucket just below i64::MIN:
                // saturate rather than wrap.
                return i64::try_from(bucket).unwrap_or(i64::MIN);
            }
        },
    };
    // Fixed-period bucketing from epoch.
    (ts_millis / period_ms) * period_ms
}

/// Bucket to the start of the ISO week (Monday 00:00 UTC).
///
/// Druid's `week` granularity uses ISO weeks starting Monday; a plain
/// epoch-aligned `(ts / WEEK) * WEEK` starts buckets on Thursday
/// (1970-01-01 was a Thursday), which mis-bucketed every week grain by
/// three days (found live by the Superset time-grain diff harness,
/// Section 8: Druid 36 returned 2024-01-01, FerroDruid 2023-12-28).
/// Aligned via the Monday preceding the epoch (1969-12-29T00:00:00Z)
/// with euclidean flooring so pre-1970 timestamps bucket correctly.
fn bucket_week(ts_millis: i64) -> i64 {
    const WEEK_MS: i64 = 604_800_000;
    /// 1969-12-29T00:00:00Z — the Monday before the (Thursday) epoch.
    const MONDAY_ORIGIN_MS: i64 = -259_200_000;
    // i128 intermediate: `ts - origin` overflows i64 for timestamps within
    // 3 days of i64::MAX (codex-review r3 — debug panic / release wrap to a
    // wrong bucket from an extreme ingested timestamp). The result of the
    // floor-divide-multiply-add always fits back into i64 because it is a
    // week boundary at or below a valid i64 input.
    const WEEK_MS_128: i128 = WEEK_MS as i128;
    let offset = i128::from(ts_millis) - i128::from(MONDAY_ORIGIN_MS);
    let bucket = offset.div_euclid(WEEK_MS_128) * WEEK_MS_128 + i128::from(MONDAY_ORIGIN_MS);
    // The bucket start is always <= the input, so the only unrepresentable
    // case is a bucket just below i64::MIN (input within a week of MIN):
    // saturate to MIN rather than wrap.
    i64::try_from(bucket).unwrap_or(i64::MIN)
}

/// Bucket to the start of the month.
fn bucket_month(ts_millis: i64) -> i64 {
    use chrono::{Datelike, TimeZone, Utc};
    let dt = match Utc.timestamp_millis_opt(ts_millis) {
        chrono::LocalResult::Single(dt) => dt,
        _ => return 0,
    };
    Utc.with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
        .single()
        .map(|d| d.timestamp_millis())
        .unwrap_or(0)
}

/// Bucket to the start of the quarter.
fn bucket_quarter(ts_millis: i64) -> i64 {
    use chrono::{Datelike, TimeZone, Utc};
    let dt = match Utc.timestamp_millis_opt(ts_millis) {
        chrono::LocalResult::Single(dt) => dt,
        _ => return 0,
    };
    let q_month = ((dt.month() - 1) / 3) * 3 + 1;
    Utc.with_ymd_and_hms(dt.year(), q_month, 1, 0, 0, 0)
        .single()
        .map(|d| d.timestamp_millis())
        .unwrap_or(0)
}

/// Bucket to the start of the year.
fn bucket_year(ts_millis: i64) -> i64 {
    use chrono::{Datelike, TimeZone, Utc};
    let dt = match Utc.timestamp_millis_opt(ts_millis) {
        chrono::LocalResult::Single(dt) => dt,
        _ => return 0,
    };
    Utc.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0)
        .single()
        .map(|d| d.timestamp_millis())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Row building
// ---------------------------------------------------------------------------

/// Build a row map (column name -> JSON value) from a segment at the given row index.
pub fn build_row(segment: &SegmentData, row_idx: usize) -> HashMap<String, serde_json::Value> {
    let mut row = HashMap::with_capacity(segment.columns.len());
    build_row_into(segment, row_idx, &mut row);
    row
}

/// Reusable-allocation variant of [`build_row`] — clears `row` and
/// repopulates it in place, so a hot loop iterating over segment rows
/// keeps the same `HashMap` allocation across every iteration instead
/// of paying `HashMap::new()` + `drop` per row.
///
/// **W3-SL1-B step 1 (Task #28)**: `build_row` was profiled during the
/// W2-C-perf-Q1 investigation (TPC-H SF10, 2026-06-30)
/// as the dominant per-row cost of the aggregate path — the HashMap
/// header alloc alone accounts for one of the ~30 heap allocations
/// this function performs per row, which at SF10 (60 M rows) is
/// ~60 M header allocs eliminated by this refactor. `String::clone()`
/// of each column name and `Value::Number` / `Value::String` value
/// allocations still happen; those are targeted by later
/// W3-SL1-B steps + W3-SL1-D (batch aggregator) that eliminate the
/// row map entirely on the fast path.
///
/// The [`String::clone()`] on `name` inside the loop below still
/// pays a heap alloc for column names *above* the SSO threshold —
/// `String` in Rust has no small-string optimisation, so every
/// column name is a heap `String`. The pre-fix `build_row` re-cloned
/// on every row invocation; this variant preserves that behaviour
/// (the caller's `row` HashMap uses `String` keys); a later
/// W3-SL1-B step will switch the row map to a `&'static str` /
/// `Cow<'a, str>` key type so the name clone drops out too.
pub fn build_row_into(
    segment: &SegmentData,
    row_idx: usize,
    row: &mut HashMap<String, serde_json::Value>,
) {
    row.clear();
    for (name, col) in &segment.columns {
        let val = column_value_at(col, row_idx);
        row.insert(name.clone(), val);
    }
}

/// **W3-SL1-B step 2 (Task #29) — no-alloc column-name reuse.**
/// Pre-populate a row map with `Value::Null` slots for every
/// segment column, so subsequent per-row updates via
/// [`build_row_update_only`] can `get_mut(name)` into the existing
/// entry instead of `insert(name.clone(), …)`. `String` in Rust
/// has no SSO — every `String::clone()` pays a heap allocation,
/// so eliminating one clone per column per row is worth ~15 heap
/// allocations per row on the lineitem table (~900 M allocs at
/// SF10). Combined with the [`build_row_into`] hoisting from step
/// 1 this is a straightforward heap-traffic reduction with no
/// change to observable semantics.
pub fn build_row_prealloc(segment: &SegmentData) -> HashMap<String, serde_json::Value> {
    segment
        .columns
        .keys()
        .map(|k| (k.clone(), serde_json::Value::Null))
        .collect()
}

/// Per-row update path for a pre-populated row map — writes
/// `column_value_at(col, row_idx)` into each existing slot without
/// touching the row-map key allocations. Any pre-existing key not
/// present in `segment.columns` (e.g. materialised virtual columns)
/// keeps its prior value; the caller is responsible for
/// re-augmenting virtual columns per row if their semantics depend
/// on per-row values. `segment.columns` keys not yet present in the
/// row map are inserted with a `String::clone()` (fallback path —
/// happens the first time a caller with a non-prealloc'd map calls
/// this).
pub fn build_row_update_only(
    segment: &SegmentData,
    row_idx: usize,
    row: &mut HashMap<String, serde_json::Value>,
) {
    for (name, col) in &segment.columns {
        let val = column_value_at(col, row_idx);
        if let Some(slot) = row.get_mut(name) {
            *slot = val;
        } else {
            row.insert(name.clone(), val);
        }
    }
}

/// Extract the value at `row_idx` from a column as a JSON value.
pub fn column_value_at(col: &ColumnData, row_idx: usize) -> serde_json::Value {
    match col {
        ColumnData::Long(v) => v
            .get(row_idx)
            .map(|&x| serde_json::Value::Number(serde_json::Number::from(x)))
            .unwrap_or(serde_json::Value::Null),
        // Nullable long: consult the null-row bitmap FIRST (the value vector
        // stores 0 at NULL rows), then render the value as an EXACT i64
        // JSON number — never through f64, so values beyond ±2^53 survive.
        // This single arm makes the generic row path (SUM/MIN/MAX/COUNT/
        // DISTINCT/filters/scan) null-faithful and i64-exact for nullable
        // longs.
        ColumnData::LongNullable(v, nulls) => {
            if is_null_long_row(nulls, row_idx) {
                serde_json::Value::Null
            } else {
                v.get(row_idx)
                    .map(|&x| serde_json::Value::Number(serde_json::Number::from(x)))
                    .unwrap_or(serde_json::Value::Null)
            }
        }
        ColumnData::Float(v) => v
            .get(row_idx)
            .and_then(|&x| serde_json::Number::from_f64(x as f64))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Double(v) => v
            .get(row_idx)
            .and_then(|&x| serde_json::Number::from_f64(x))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ColumnData::String(sc) => {
            // READ-side null faithfulness: a SQL-NULL row's ordinal points at
            // the `""` placeholder entry, so the null-row bitmap must be
            // consulted BEFORE dictionary resolution — otherwise every null
            // row materialises as `""` (wrong scan output, HLL over-count,
            // selector-null never matching). `is_null_row` is a no-op for
            // null-free columns (`null_rows()` is `None`).
            if sc.is_null_row(row_idx) {
                serde_json::Value::Null
            } else {
                let ord = sc.encoded_values.get(row_idx).copied().unwrap_or(0) as usize;
                sc.dictionary
                    .get(ord)
                    .map(|s| serde_json::Value::String(s.to_string()))
                    .unwrap_or(serde_json::Value::Null)
            }
        }
        // Multi-value string dimension (compat-11), rendered like Druid:
        // an empty row (ingested `[]`/`null`) is SQL NULL; a 1-element row
        // renders as the scalar string; a multi-element row renders as a
        // JSON array of its elements in stored order.
        ColumnData::StringMulti(mc) => match mc.row_ordinals(row_idx) {
            None | Some([]) => serde_json::Value::Null,
            Some([ord]) => mc
                .dictionary
                .get(*ord as usize)
                .map(|s| serde_json::Value::String(s.to_string()))
                .unwrap_or(serde_json::Value::Null),
            Some(ords) => serde_json::Value::Array(
                ords.iter()
                    .map(|&ord| {
                        mc.dictionary
                            .get(ord as usize)
                            .map(|s| serde_json::Value::String(s.to_string()))
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect(),
            ),
        },
        ColumnData::Complex(_) => serde_json::Value::Null,
        // Migrated Druid `thetaSketch` metric (compat-8 sketch #2): render
        // the row's decoded sketch as the theta partial-state envelope —
        // the same `{"@sketch":"theta","bytes":…}` shape the aggregator's
        // own `get` emits — so `ThetaSketchAggregator`'s merge feed unions
        // the per-row sketches exactly.  The sketch is Druid-origin
        // (union-only), which the envelope bytes preserve.
        ColumnData::ComplexTheta(rows) => rows
            .get(row_idx)
            .map(ferrodruid_aggregator::theta_sketch_envelope)
            .unwrap_or(serde_json::Value::Null),
    }
}

/// Finalize one aggregator into its JSON scalar, failing closed when a
/// resource cap has made the value inexact.
///
/// Fail-closed exact-cardinality program (2026-07-11): the exact-distinct
/// `cardinality` aggregator saturates at its Wave 36-G2 DoS cap
/// (`MAX_CARDINALITY_SET_SIZE`); pre-fix, a saturated set silently
/// finalized to a capped (under-counted) scalar. Druid never silently
/// returns a wrong exact distinct count — it computes the true value or
/// fails the query at a resource bound — so every executor finalization
/// site (timeseries / topN / groupBy) now routes through this helper and
/// surfaces [`DruidError::ResourceLimit`] when
/// [`Aggregator::saturation`] reports a clipped state. The caps themselves
/// are unchanged (they remain the DoS protection); only the silent wrong
/// answer is replaced by an error.
///
/// # Errors
///
/// Returns [`DruidError::ResourceLimit`] when the aggregator reports a
/// saturated (inexact) state.
pub fn finalize_agg_value(agg: &dyn Aggregator) -> Result<serde_json::Value> {
    if let Some(sat) = agg.saturation() {
        return Err(DruidError::ResourceLimit {
            kind: sat.kind,
            limit: sat.limit,
            observed: sat.observed,
        });
    }
    Ok(agg.get())
}

/// Replace each exact-cardinality aggregator's finalized bare count in an
/// executor result map with its full-set `CardinalityState` envelope
/// (multi-shard exact union, 2026-07-11).
///
/// Pre-fix, the executors emitted `finalize_agg_value` = the bare `get()`
/// count as the per-segment partial, so the broker had no per-key
/// information and its cross-segment merge degraded to a saturating-add
/// that the fail-closed program rejects — i.e. ANY exact `COUNT(DISTINCT)`
/// whose same time bucket / group was produced by two or more segments
/// failed closed (CL-C2).  With the envelope emitted instead, the broker
/// unions the exact sets across segments (up to the exact-set cap) and its
/// finalization pass collapses the result to the exact bare count before
/// anything reaches a client.
///
/// Call this as the LAST step before emitting a partial, AFTER every
/// in-executor consumer of the bare count has run (post-aggregations,
/// TopN metric ranking, groupBy HAVING) — those must keep operating on
/// the exact per-segment scalar.  The fail-closed saturation check
/// ([`finalize_agg_value`]) has already errored out saturated aggregators
/// by the time this runs, so the emitted envelopes are always exact.
///
/// Non-cardinality aggregators are left untouched
/// ([`ferrodruid_aggregator::exact_cardinality_partial`] returns `None`
/// for them), including the `APPROX_COUNT_DISTINCT` / HLL sketch path.
pub fn substitute_cardinality_partials(
    aggregations: &[AggregatorSpec],
    aggs: &[Box<dyn Aggregator>],
    map: &mut serde_json::Map<String, serde_json::Value>,
) {
    for (i, spec) in aggregations.iter().enumerate() {
        if let Some(agg) = aggs.get(i)
            && let Some(envelope) = ferrodruid_aggregator::exact_cardinality_partial(agg.as_ref())
        {
            map.insert(spec.name().to_string(), envelope);
        }
    }
}

/// Extract the value to feed into an aggregator for a given row.
pub fn extract_agg_value(
    segment: &SegmentData,
    row_idx: usize,
    spec: &AggregatorSpec,
) -> Option<serde_json::Value> {
    match spec.field_name() {
        None => Some(serde_json::Value::Null), // count aggregator
        Some(field) => segment
            .columns
            .get(field)
            .map(|col| column_value_at(col, row_idx)),
    }
}

/// Fail loud when any aggregator in `aggregations` reads a genuine
/// multi-value (`StringMulti`) segment column as its input (compat-11 MV
/// fail-loud guard).
///
/// No aggregator has element-wise MV semantics yet: pre-fix,
/// [`column_value_at`] handed the row's whole array to the aggregator as
/// ONE `Value::Array`, which was then hashed (cardinality / sketches) or
/// coerced (numeric aggs) as the stringified text `"[\"a\",\"b\"]"` —
/// silently corrupt results.  Until element-wise MV aggregation lands,
/// every executor calls this once at PLAN time (before any per-row
/// aggregate) so the query errors once, never corrupts.
///
/// Field resolution mirrors the feed paths:
/// * `filtered` wrappers are seen through to their inner aggregator, and
///   the wrapper's own filter must pass the plan-time MV filter guard
///   ([`crate::filter::FilterSpec::ensure_multi_value_supported`], compat-11
///   R2);
/// * `cardinality` checks every entry of its `fields` list (that path
///   reads segment columns directly, so virtual columns never shadow it);
/// * `grouping` reads no column values and is exempt;
/// * every other aggregator checks its `fieldName` — AND, for the
///   first/last family's two-argument forms, its `timeColumn` (read per
///   row and coerced with `as_i64`; an MV value silently became timestamp
///   `0`) — UNLESS a virtual column of that name shadows the segment
///   column ([`feed_aggregator_row_from_map`] resolves the row map first;
///   the virtual column itself is guarded by
///   [`crate::virtual_columns::VirtualColumns::ensure_no_multi_value_refs`]).
///
/// # Errors
///
/// Returns [`DruidError::Query`] naming the first multi-value column used
/// as an aggregator input.
pub fn ensure_aggregations_not_multi_value(
    segment: &SegmentData,
    aggregations: &[AggregatorSpec],
    virtual_columns: &crate::virtual_columns::VirtualColumns,
) -> Result<()> {
    for spec in aggregations {
        ensure_agg_spec_not_multi_value(segment, spec, virtual_columns)?;
    }
    Ok(())
}

/// Per-spec walk for [`ensure_aggregations_not_multi_value`].
fn ensure_agg_spec_not_multi_value(
    segment: &SegmentData,
    spec: &AggregatorSpec,
    virtual_columns: &crate::virtual_columns::VirtualColumns,
) -> Result<()> {
    match spec {
        AggregatorSpec::Filtered { filter, aggregator } => {
            // compat-11 R2: the wrapper's own filter is evaluated per row
            // through the same row-map paths as a query filter, so it must
            // pass the same plan-time MV guard (see
            // `FilterSpec::ensure_multi_value_supported`).  Unparseable
            // filter JSON is left to the per-row fail-closed path
            // (`filtered_agg_filter_matches` never matches on it).
            if let Ok(filter_spec) =
                serde_json::from_value::<crate::filter::FilterSpec>(filter.clone())
            {
                filter_spec.ensure_multi_value_supported(segment, virtual_columns)?;
            }
            ensure_agg_spec_not_multi_value(segment, aggregator, virtual_columns)
        }
        // The multi-field cardinality path reads `segment.columns` directly
        // (see `feed_aggregator_row`), so every field is checked with no
        // virtual-column shadowing exemption.
        AggregatorSpec::Cardinality { fields, .. } => {
            for field in fields {
                ensure_agg_field_not_multi_value(segment, field)?;
            }
            Ok(())
        }
        // GROUPING(...) emits a bitmask over grouping-dimension NAMES and
        // never reads column values — exempt.
        AggregatorSpec::Grouping { .. } => Ok(()),
        _ => {
            if let Some(field) = spec.field_name()
                && !virtual_columns.names().any(|n| n == field)
            {
                ensure_agg_field_not_multi_value(segment, field)?;
            }
            // The first/last two-argument forms (CL-4 / W1-H R6) ALSO read
            // their `timeColumn` per row: `feed_aggregator_row_from_map`
            // coerces that value with `as_i64`, so an MV row's array (or a
            // 1-element row's scalar string) read as `None` and was
            // silently substituted with timestamp `0` —
            // insertion-order-dependent first/last results.  Same guard,
            // same VC-shadow exemption as `fieldName` (the row map resolves
            // a shadowing virtual column first, and the virtual column
            // itself is guarded by `ensure_no_multi_value_refs`).
            if let Some(time_col) = spec.time_column()
                && !virtual_columns.names().any(|n| n == time_col)
            {
                ensure_agg_field_not_multi_value(segment, time_col)?;
            }
            Ok(())
        }
    }
}

/// Fail loud when a grouping / topN `DimensionSpec` over a genuine
/// multi-value (`StringMulti`) segment column requests a coercion the
/// explosion path cannot honour yet (compat-11 R3 MV fail-loud guard).
///
/// The oracle-verified MV grouping path is the PLAIN-STRING explosion:
/// a `default` spec with `outputType: STRING` (optionally under the
/// `listFiltered` / `regexFiltered` / `prefixFiltered` wrappers, whose
/// element filtering is Druid's documented filtered-dimension-on-MV
/// behaviour).  Two spec shapes silently diverge instead:
///
/// * a non-STRING `outputType` — Druid coerces AND MERGES per element
///   (`["01"]` and `["1"]` both become the numeric group `1`); the
///   explosion path ignored `outputType` entirely, silently keeping them
///   distinct string groups;
/// * an `extractionFn` — applied per element by the explosion path, but
///   unverified against the Druid oracle (empty-MV rows bypass the
///   transform entirely: `explode_dim_value` yields the null key without
///   running the extraction, where Druid would transform the null).
///
/// Until element-wise MV coercion lands, every grouping executor
/// (groupBy / topN) calls this once at PLAN time so the query errors
/// once, never corrupts.  A virtual column of the same name shadows the
/// segment column in every row-map path (and is itself guarded by
/// [`crate::virtual_columns::VirtualColumns::ensure_no_multi_value_refs`]),
/// so shadowed names are exempt.
///
/// # Errors
///
/// Returns [`DruidError::Query`] naming the first multi-value column
/// whose spec requests an unsupported coercion.
pub fn ensure_dimension_specs_not_multi_value_coerced(
    segment: &SegmentData,
    dimensions: &[DimensionSpec],
    virtual_columns: &crate::virtual_columns::VirtualColumns,
) -> Result<()> {
    for spec in dimensions {
        let input = dim_spec_base_input(spec);
        if virtual_columns.names().any(|n| n == input) {
            continue;
        }
        if !matches!(segment.columns.get(input), Some(ColumnData::StringMulti(_))) {
            continue;
        }
        if dim_spec_requests_coercion(spec) {
            return Err(DruidError::Query(format!(
                "outputType/extractionFn coercion over a multi-value dimension `{input}` is \
                 not supported yet (element-wise MV coercion is a follow-on; group on the \
                 exploded elements with outputType STRING and no extractionFn)"
            )));
        }
    }
    Ok(())
}

/// The base segment/virtual column a `DimensionSpec` reads (through any
/// filtered-dimension wrappers).
fn dim_spec_base_input(spec: &DimensionSpec) -> &str {
    match spec {
        DimensionSpec::Default { dimension, .. } | DimensionSpec::Extraction { dimension, .. } => {
            dimension
        }
        DimensionSpec::ListFiltered { delegate, .. }
        | DimensionSpec::RegexFiltered { delegate, .. }
        | DimensionSpec::PrefixFiltered { delegate, .. } => dim_spec_base_input(delegate),
    }
}

/// Whether the spec (or its wrapped delegate) requests a value coercion —
/// a non-STRING `outputType` or an `extractionFn` — that the MV explosion
/// path does not honour per element yet.
fn dim_spec_requests_coercion(spec: &DimensionSpec) -> bool {
    match spec {
        DimensionSpec::Default { output_type, .. } => *output_type != ColumnType::String,
        DimensionSpec::Extraction { .. } => true,
        DimensionSpec::ListFiltered { delegate, .. }
        | DimensionSpec::RegexFiltered { delegate, .. }
        | DimensionSpec::PrefixFiltered { delegate, .. } => dim_spec_requests_coercion(delegate),
    }
}

/// Error when `field` resolves to a `StringMulti` segment column.
fn ensure_agg_field_not_multi_value(segment: &SegmentData, field: &str) -> Result<()> {
    if matches!(segment.columns.get(field), Some(ColumnData::StringMulti(_))) {
        return Err(DruidError::Query(format!(
            "aggregation over a multi-value dimension `{field}` is not supported yet \
             (element-wise MV aggregation is a follow-on)"
        )));
    }
    Ok(())
}

/// Evaluate a `filtered` aggregator's per-aggregator filter against one row.
///
/// `AggregatorSpec::Filtered` carries its filter as opaque JSON (the
/// aggregator crate cannot depend on [`crate::filter::FilterSpec`]), so the
/// query layer owns per-row evaluation — `FilteredAggregator::aggregate`
/// itself is an unconditional delegate.  Pre-fix, no executor path performed
/// this evaluation, so a `filtered` aggregator silently aggregated every row
/// (e.g. a not-null selector count over-counted null rows).
///
/// Evaluation order: when the caller already has a row map (which includes
/// virtual columns), match against it directly — never against the raw
/// segment, so a virtual column shadowing a segment column is honoured.
/// Without a row map, probe the typed segment path first
/// (`matches_typed`; `None` = undecided) and only then materialise a
/// single-row map on demand.
///
/// An unparseable filter fails closed (row rejected) so a malformed spec can
/// never over-count.
///
/// Perf note: the filter JSON is re-parsed on every call, i.e. once per row
/// per filtered aggregator (correctness-first).  Caching the parsed
/// `FilterSpec` per query execution is a known follow-up.
fn filtered_agg_filter_matches(
    filter_json: &serde_json::Value,
    segment: &SegmentData,
    row_idx: usize,
    row: Option<&HashMap<String, serde_json::Value>>,
) -> bool {
    let Ok(filter) = serde_json::from_value::<crate::filter::FilterSpec>(filter_json.clone())
    else {
        return false; // fail closed: a malformed filter never matches
    };
    if let Some(row) = row {
        return filter.matches(row);
    }
    if let Some(decided) = filter.matches_typed(segment, row_idx) {
        return decided;
    }
    let mut single_row = HashMap::new();
    build_row_update_only(segment, row_idx, &mut single_row);
    filter.matches(&single_row)
}

/// Feed a single row into the aggregator, resolving the aggregator's
/// `fieldName` against an already-built row map so virtual columns (and any
/// other derived values materialised into `row`) are visible to aggregators.
///
/// Falls back to the real segment column when the field is not present in the
/// row map.  `Cardinality` keeps its dedicated multi-field path (delegated to
/// [`feed_aggregator_row`]) since its fields are not single scalar accesses.
/// `Filtered` evaluates its filter against the row map first and feeds the
/// inner spec only on a match (see `filtered_agg_filter_matches`).
pub fn feed_aggregator_row_from_map(
    segment: &SegmentData,
    row_idx: usize,
    row: &HashMap<String, serde_json::Value>,
    spec: &AggregatorSpec,
    agg: &mut dyn Aggregator,
) {
    if let AggregatorSpec::Filtered { filter, aggregator } = spec {
        if filtered_agg_filter_matches(filter, segment, row_idx, Some(row)) {
            feed_aggregator_row_from_map(segment, row_idx, row, aggregator, agg);
        }
        return;
    }
    if matches!(spec, AggregatorSpec::Cardinality { fields, .. } if !fields.is_empty()) {
        feed_aggregator_row(segment, row_idx, spec, agg);
        return;
    }
    let val = match spec.field_name() {
        None => Some(serde_json::Value::Null),
        Some(field) => row
            .get(field)
            .cloned()
            .or_else(|| extract_agg_value(segment, row_idx, spec)),
    };
    // CL-4 / W1-H R6: when a first/last spec carries a non-`__time`
    // time column, resolve the per-row timestamp from that column and
    // dispatch through `aggregate_with_time` so the aggregator orders by
    // the real column value rather than insertion order.
    if let Some(time_col) = spec.time_column() {
        let ts = row
            .get(time_col)
            .and_then(serde_json::Value::as_i64)
            .or_else(|| {
                segment
                    .columns
                    .get(time_col)
                    .and_then(|col| column_value_at(col, row_idx).as_i64())
            })
            .unwrap_or(0);
        agg.aggregate_with_time(ts, val.as_ref());
        return;
    }
    agg.aggregate(val.as_ref());
}

/// Feed a single row from `segment[row_idx]` into the aggregator for `spec`.
///
/// This is the canonical per-row dispatch helper used by every query
/// executor (timeseries / topN / groupBy).  It special-cases
/// [`AggregatorSpec::Cardinality`] with a non-empty `fields` list to walk
/// every configured field and feed it through
/// [`CardinalityAggregator::aggregate_row_values`], so that multi-field
/// `cardinality` specs honour the spec's union / by-row tuple semantics
/// instead of silently treating only the first field.
///
/// Wave 45-E (Wave 37B Medium #3 `aggregator/lib.rs:183-191`,
/// `cardinality.rs:40-67`): pre-fix, `AggregatorSpec::create()` discarded
/// the `fields` list and the query layer used the trait `aggregate(value)`
/// method which only sees a single value (the result of
/// [`extract_agg_value`], itself returning `Null` for `Cardinality` because
/// `field_name()` is `None`).  This silently degraded multi-field
/// cardinality to "always 1" in `by_row = false` mode and "always 1
/// distinct empty tuple" in `by_row = true` mode.  Now the query layer
/// walks `fields` and calls `aggregate_row_values`, matching upstream
/// Druid semantics.
///
/// All non-cardinality aggregators take the original
/// `extract_agg_value(...)` → `aggregate(...)` path unchanged.
pub fn feed_aggregator_row(
    segment: &SegmentData,
    row_idx: usize,
    spec: &AggregatorSpec,
    agg: &mut dyn Aggregator,
) {
    if let AggregatorSpec::Filtered { filter, aggregator } = spec {
        // Per-aggregator filter: feed the inner spec only when the filter
        // matches this row (see `filtered_agg_filter_matches`).  `agg` stays
        // the `FilteredAggregator` wrapper, which delegates unconditionally.
        if filtered_agg_filter_matches(filter, segment, row_idx, None) {
            feed_aggregator_row(segment, row_idx, aggregator, agg);
        }
        return;
    }
    if let AggregatorSpec::Cardinality { fields, .. } = spec
        && !fields.is_empty()
    {
        // Materialise per-field column values for this row.  Missing
        // columns are represented as `Null` so they participate in the
        // union (matching the legacy single-field behaviour where
        // `extract_agg_value` would also feed `Null` for an absent field).
        let row_vals: Vec<serde_json::Value> = fields
            .iter()
            .map(|f| {
                segment
                    .columns
                    .get(f)
                    .map_or(serde_json::Value::Null, |col| column_value_at(col, row_idx))
            })
            .collect();
        let refs: Vec<Option<&serde_json::Value>> = row_vals.iter().map(Some).collect();
        agg.aggregate_multi(&refs);
        return;
    }
    let val = extract_agg_value(segment, row_idx, spec);
    agg.aggregate(val.as_ref());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_intervals_absent_means_all_time() {
        // An empty interval list legitimately means "all time" — not an error.
        let parsed = parse_intervals(&[]).expect("empty list is valid");
        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_intervals_accepts_well_formed_interval() {
        let parsed = parse_intervals(&["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z".to_owned()])
            .expect("well-formed interval");
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].0 < parsed[0].1);
    }

    #[test]
    fn parse_intervals_rejects_malformed_interval() {
        // DD R40: a provided-but-malformed interval must FAIL CLOSED rather than
        // being silently skipped (which collapsed the list to empty and made
        // executors scan ALL rows — a fail-open time filter).
        let err = parse_intervals(&["2024-01-01T00:00:00Z/not-a-date".to_owned()])
            .expect_err("malformed interval must be rejected");
        assert!(
            matches!(err, DruidError::Query(_)),
            "expected DruidError::Query, got {err:?}"
        );
        // The whole-only-malformed case must not degrade to "all time".
        assert!(parse_intervals(&["garbage".to_owned()]).is_err());
        // A valid interval alongside a malformed one is still rejected.
        assert!(
            parse_intervals(&[
                "2024-01-01T00:00:00Z/2024-02-01T00:00:00Z".to_owned(),
                "bad/interval".to_owned(),
            ])
            .is_err()
        );
    }

    /// Regression for **TG-4-finding-001** (W2-D pydruid/druid-go). The
    /// `intervals` field on every request-side query must deserialize from
    /// both a single ISO string and an array of strings — pre-fix only
    /// the array form was accepted, yielding HTTP 400 `invalid type:
    /// string, expected a sequence` for clients that use the documented
    /// single-string shorthand.
    #[test]
    fn deserialize_intervals_accepts_single_string_and_array() {
        // Helper: drive the deserializer via serde_json over a wrapper
        // struct so the attribute is exercised the same way the native
        // query types use it.
        #[derive(Deserialize)]
        struct Wrap {
            #[serde(deserialize_with = "deserialize_intervals")]
            intervals: Vec<String>,
        }
        let from_str: Wrap =
            serde_json::from_value(serde_json::json!({"intervals": "2024-01-01/2024-01-04"}))
                .expect("string form must deserialize");
        assert_eq!(from_str.intervals, vec!["2024-01-01/2024-01-04".to_owned()]);

        let from_arr: Wrap =
            serde_json::from_value(serde_json::json!({"intervals": ["2024-01-01/2024-01-04"]}))
                .expect("array form must deserialize");
        assert_eq!(from_arr.intervals, from_str.intervals);

        let multi: Wrap = serde_json::from_value(serde_json::json!({
            "intervals": ["2024-01-01/2024-01-04", "2024-02-01/2024-02-04"]
        }))
        .expect("multi-element array must still work");
        assert_eq!(multi.intervals.len(), 2);
    }

    #[test]
    fn deserialize_optional_intervals_accepts_string_array_and_absent() {
        #[derive(Deserialize)]
        struct Wrap {
            #[serde(default, deserialize_with = "deserialize_optional_intervals")]
            intervals: Option<Vec<String>>,
        }
        let absent: Wrap = serde_json::from_value(serde_json::json!({})).expect("absent → None");
        assert!(absent.intervals.is_none());

        let one: Wrap =
            serde_json::from_value(serde_json::json!({"intervals": "2024-01-01/2024-01-04"}))
                .expect("string → Some(vec![s])");
        assert_eq!(
            one.intervals,
            Some(vec!["2024-01-01/2024-01-04".to_owned()])
        );

        let arr: Wrap =
            serde_json::from_value(serde_json::json!({"intervals": ["2024-01-01/2024-01-04"]}))
                .expect("array → Some(vec)");
        assert_eq!(arr.intervals, one.intervals);
    }

    #[test]
    fn validate_granularity_rejects_zero_and_huge_duration() {
        use ferrodruid_common::types::Granularity;
        let origin = chrono::DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let zero = GranularitySpec::Full(Granularity::Duration {
            period_ms: 0,
            origin,
        });
        assert!(
            validate_granularity(&zero).is_err(),
            "periodMs 0 must reject"
        );
        let huge = GranularitySpec::Full(Granularity::Duration {
            period_ms: u64::MAX,
            origin,
        });
        assert!(
            validate_granularity(&huge).is_err(),
            "out-of-i64 periodMs must reject"
        );
        let ok = GranularitySpec::Full(Granularity::Duration {
            period_ms: 3_600_000,
            origin,
        });
        assert!(
            validate_granularity(&ok).is_ok(),
            "valid duration must pass"
        );
        // DD R48: bucket_timestamp must not panic even if a 0 period reaches it.
        assert_eq!(bucket_timestamp(123, &zero), 123);
    }

    /// Fixed-period `duration` granularity floors toward -inf like Druid
    /// (bucket start <= timestamp, always), including timestamps BEFORE the
    /// origin. The pre-fix truncating division rounded pre-origin offsets
    /// toward zero, putting e.g. `-1 ms` into the `0` bucket (a bucket start
    /// AFTER the timestamp) instead of `-5000`.
    #[test]
    fn duration_granularity_floors_pre_origin_timestamps() {
        use ferrodruid_common::types::Granularity;
        let five_s = GranularitySpec::Full(Granularity::Duration {
            period_ms: 5_000,
            origin: chrono::DateTime::UNIX_EPOCH,
        });
        // At/after origin: plain epoch-anchored floor.
        assert_eq!(bucket_timestamp(0, &five_s), 0);
        assert_eq!(bucket_timestamp(4_999, &five_s), 0);
        assert_eq!(bucket_timestamp(5_000, &five_s), 5_000);
        assert_eq!(bucket_timestamp(12_345, &five_s), 10_000);
        // Before origin: floor toward -inf, not toward zero.
        assert_eq!(bucket_timestamp(-1, &five_s), -5_000);
        assert_eq!(bucket_timestamp(-5_000, &five_s), -5_000);
        assert_eq!(bucket_timestamp(-5_001, &five_s), -10_000);
        // Extreme inputs must not panic (i128 intermediate, like bucket_week);
        // the bucket start is always <= the input and within one period of it.
        let top = bucket_timestamp(i64::MAX, &five_s);
        assert!(i64::MAX - top < 5_000, "bucket start within one period");
        assert!(bucket_timestamp(i64::MIN + 1, &five_s) <= i64::MIN + 1);
        // A non-epoch origin anchors the phase: buckets at origin + k*period.
        let origin_ms = 1_704_067_200_000_i64; // 2024-01-01T00:00:00Z
        let shifted = GranularitySpec::Full(Granularity::Duration {
            period_ms: 30_000,
            origin: chrono::DateTime::from_timestamp_millis(origin_ms).expect("valid origin"),
        });
        assert_eq!(bucket_timestamp(origin_ms + 29_999, &shifted), origin_ms);
        assert_eq!(
            bucket_timestamp(origin_ms - 1, &shifted),
            origin_ms - 30_000
        );
    }

    /// The Sunday-anchored 7-day duration granularity (what the SQL planner
    /// lowers Superset's `week_starting_sunday` grain to) buckets
    /// `[Sunday, Sunday+7d)` labeled by the Sunday start.
    #[test]
    fn sunday_anchored_week_duration_buckets_on_sundays() {
        use ferrodruid_common::types::Granularity;
        let sunday_week = GranularitySpec::Full(Granularity::Duration {
            period_ms: 604_800_000,
            // 1969-12-28T00:00:00Z — the Sunday before the epoch.
            origin: chrono::DateTime::from_timestamp_millis(-345_600_000).expect("valid origin"),
        });
        let ms = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("ts")
                .timestamp_millis()
        };
        // 2024-01-03 (Wednesday) -> 2023-12-31 (Sunday).
        assert_eq!(
            bucket_timestamp(ms("2024-01-03T12:00:00Z"), &sunday_week),
            ms("2023-12-31T00:00:00Z")
        );
        // A Sunday is its own bucket start; the preceding Saturday belongs
        // to the previous bucket.
        assert_eq!(
            bucket_timestamp(ms("2024-01-07T00:00:00Z"), &sunday_week),
            ms("2024-01-07T00:00:00Z")
        );
        assert_eq!(
            bucket_timestamp(ms("2024-01-06T23:59:59Z"), &sunday_week),
            ms("2023-12-31T00:00:00Z")
        );
    }

    /// Week buckets are ISO weeks starting Monday 00:00 UTC — NOT
    /// epoch-aligned (1970-01-01 was a Thursday). Ground truth: Druid 36
    /// buckets 2024-01-01..2024-01-03 data under `2024-01-01` (a Monday);
    /// the pre-fix epoch alignment produced `2023-12-28` (a Thursday) —
    /// found live by the Superset time-grain harness (Section 8).
    #[test]
    fn week_buckets_start_on_monday() {
        let week = GranularitySpec::Simple("week".to_string());
        let ms = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .expect("ts")
                .timestamp_millis()
        };
        // Monday itself is a fixed point.
        assert_eq!(
            bucket_timestamp(ms("2024-01-01T00:00:00Z"), &week),
            ms("2024-01-01T00:00:00Z")
        );
        // Mid-week and Sunday collapse to the preceding Monday.
        assert_eq!(
            bucket_timestamp(ms("2024-01-03T08:00:00Z"), &week),
            ms("2024-01-01T00:00:00Z")
        );
        assert_eq!(
            bucket_timestamp(ms("2024-01-07T23:59:59Z"), &week),
            ms("2024-01-01T00:00:00Z")
        );
        // The next Monday starts a new bucket.
        assert_eq!(
            bucket_timestamp(ms("2024-01-08T00:00:00Z"), &week),
            ms("2024-01-08T00:00:00Z")
        );
        // Full-enum spelling agrees with the Simple spelling.
        let week_full = GranularitySpec::Full(ferrodruid_common::types::Granularity::Week);
        assert_eq!(
            bucket_timestamp(ms("2024-01-03T08:00:00Z"), &week_full),
            ms("2024-01-01T00:00:00Z")
        );
        // Pre-epoch timestamps floor correctly (euclidean division):
        // 1969-12-30 (Tuesday) buckets to Monday 1969-12-29.
        assert_eq!(
            bucket_timestamp(ms("1969-12-30T12:00:00Z"), &week),
            ms("1969-12-29T00:00:00Z")
        );
        // codex-review r3: extreme timestamps must not overflow (debug
        // panic / release wrap). The bucket is a valid week boundary at or
        // below the input, for both i64 extremes.
        let hi = bucket_timestamp(i64::MAX, &week);
        assert!(i64::MAX - hi < 604_800_000, "hi bucket within a week: {hi}");
        let lo = bucket_timestamp(i64::MIN, &week);
        assert!(
            lo - i64::MIN <= 604_800_000,
            "lo bucket saturates near MIN: {lo}"
        );
    }
}
