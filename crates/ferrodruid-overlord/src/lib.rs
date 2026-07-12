// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Ingestion task assignment for FerroDruid.
//!
//! The [`Overlord`] manages the lifecycle of ingestion tasks (Kafka
//! supervisors, batch ingestion jobs) and their assignment to
//! MiddleManager/Indexer nodes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod task;

pub use task::{
    Interval, LockDecision, LockType, RetryPolicy, TaskLock, TaskState, Worker,
    WorkerSelectStrategy, WorkerSelector, evaluate_lock_request,
};

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Utc};
use ferrodruid_common::{DruidError, Result};
use ferrodruid_historical::{Historical, SegmentSwapEntry};
use ferrodruid_ingest_batch::BatchIngester;
use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow, SupervisorRow, TaskLockRow, TaskRow};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

// ---------------------------------------------------------------------------
// Helpers for the Druid `index_parallel` + inline-data path
// ---------------------------------------------------------------------------

/// Format epoch milliseconds as a Druid-style ISO-8601 UTC string with
/// millisecond precision, e.g. `2024-01-01T00:00:00.000Z`.
fn format_epoch_millis_iso(millis: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis).map_or_else(
        || millis.to_string(),
        |dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
    )
}

/// Derive a [`TaskLocation`] from a worker id of the form `host:port`.
fn worker_location(worker_id: &str) -> Option<TaskLocation> {
    let (host, port) = worker_id.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    Some(TaskLocation {
        host: host.to_string(),
        port,
        tls_port: None,
    })
}

/// Parse an ISO-8601 `"start/end"` interval string into epoch-millis bounds.
fn parse_iso_interval(s: &str) -> Result<(i64, i64)> {
    let (start_s, end_s) = s
        .split_once('/')
        .ok_or_else(|| DruidError::Ingestion(format!("interval missing '/': {s}")))?;
    let start = parse_interval_bound_millis(start_s.trim())
        .map_err(|e| DruidError::Ingestion(format!("interval start '{start_s}': {e}")))?;
    let end = parse_interval_bound_millis(end_s.trim())
        .map_err(|e| DruidError::Ingestion(format!("interval end '{end_s}': {e}")))?;
    if start >= end {
        return Err(DruidError::Ingestion(format!(
            "interval start must be before end: {s}"
        )));
    }
    Ok((start, end))
}

/// Parse one interval bound to epoch millis, accepting both RFC3339 timestamps
/// and the bare `YYYY-MM-DD` date form (UTC midnight) common in Druid batch
/// ingestion specs (DD R47).
fn parse_interval_bound_millis(s: &str) -> std::result::Result<i64, String> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(naive) = date.and_hms_opt(0, 0, 0)
    {
        return Ok(naive.and_utc().timestamp_millis());
    }
    Err(format!("not an RFC3339 timestamp or YYYY-MM-DD date: {s}"))
}

/// Segment-granularity bucketing used to compute the interval an
/// `appendToExisting: false` (Druid's batch default) task overwrites.
///
/// Druid's replace semantics operate on **segmentGranularity buckets**, not
/// on the raw data range: a DAY-granularity task whose rows land anywhere in
/// `2026-01-01` overshadows every existing segment of that day, even ones
/// whose row timestamps do not intersect the new rows'.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentGranularity {
    /// `ALL`: a single bucket covering all time (the task replaces the
    /// datasource's entire used-segment set).
    All,
    /// Fixed-length buckets of `millis` anchored at epoch offset `anchor`
    /// (everything from `NONE`/`SECOND` up to `DAY`; `WEEK` uses a Monday
    /// anchor for ISO-week alignment).
    Fixed {
        /// Bucket length in milliseconds.
        millis: i64,
        /// Epoch-millis offset the bucket grid is aligned to.
        anchor: i64,
    },
    /// Calendar months (variable length, UTC).
    Month,
    /// Calendar quarters (Jan/Apr/Jul/Oct starts, UTC).
    Quarter,
    /// Calendar years (UTC).
    Year,
}

/// Milliseconds in one UTC day.
const MILLIS_PER_DAY: i64 = 86_400_000;

/// Epoch-millis of Monday 1969-12-29T00:00:00Z — anchors `WEEK` buckets so
/// they start on Mondays (ISO weeks), matching Druid's `WEEK` granularity.
const WEEK_ANCHOR_MILLIS: i64 = -3 * MILLIS_PER_DAY;

/// Parse a Druid `segmentGranularity` JSON value.
///
/// Druid's `UniformGranularitySpec` defaults `segmentGranularity` to **DAY**
/// when the field (or the whole `granularitySpec`) is absent. Supported wire
/// shapes: the simple-string granularity names (case-insensitive) and the
/// `{"type": "period", "period": "P1D"}` / `{"type": "all"}` object forms.
/// Unsupported shapes fail closed — the caller only invokes this on the
/// `appendToExisting: false` path, where guessing the replace scope wrong
/// would silently duplicate or destroy data.
fn parse_segment_granularity(v: Option<&serde_json::Value>) -> Result<SegmentGranularity> {
    const DAY: SegmentGranularity = SegmentGranularity::Fixed {
        millis: MILLIS_PER_DAY,
        anchor: 0,
    };
    let Some(v) = v else { return Ok(DAY) };
    match v {
        serde_json::Value::Null => Ok(DAY),
        serde_json::Value::String(s) => segment_granularity_from_name(s),
        serde_json::Value::Object(o) => {
            let gtype = o.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match gtype {
                "all" => Ok(SegmentGranularity::All),
                "period" => {
                    let period = o.get("period").and_then(|p| p.as_str()).ok_or_else(|| {
                        DruidError::Ingestion(
                            "segmentGranularity period object is missing its 'period' string"
                                .to_string(),
                        )
                    })?;
                    segment_granularity_from_period(period)
                }
                other => Err(DruidError::Ingestion(format!(
                    "unsupported segmentGranularity object type '{other}' for \
                     appendToExisting=false (replace) ingestion (supported: all, period)"
                ))),
            }
        }
        other => Err(DruidError::Ingestion(format!(
            "segmentGranularity must be a string or object, got: {other}"
        ))),
    }
}

/// Map a simple-string granularity name (case-insensitive) onto
/// [`SegmentGranularity`].
fn segment_granularity_from_name(s: &str) -> Result<SegmentGranularity> {
    let fixed = |millis| SegmentGranularity::Fixed { millis, anchor: 0 };
    match s.to_ascii_lowercase().as_str() {
        "none" => Ok(fixed(1)),
        "second" => Ok(fixed(1_000)),
        "minute" => Ok(fixed(60_000)),
        "five_minute" => Ok(fixed(300_000)),
        "ten_minute" => Ok(fixed(600_000)),
        "fifteen_minute" => Ok(fixed(900_000)),
        "thirty_minute" => Ok(fixed(1_800_000)),
        "hour" => Ok(fixed(3_600_000)),
        "six_hour" => Ok(fixed(21_600_000)),
        "eight_hour" => Ok(fixed(28_800_000)),
        "day" => Ok(fixed(MILLIS_PER_DAY)),
        "week" => Ok(SegmentGranularity::Fixed {
            millis: 7 * MILLIS_PER_DAY,
            anchor: WEEK_ANCHOR_MILLIS,
        }),
        "month" => Ok(SegmentGranularity::Month),
        "quarter" => Ok(SegmentGranularity::Quarter),
        "year" => Ok(SegmentGranularity::Year),
        "all" => Ok(SegmentGranularity::All),
        other => Err(DruidError::Ingestion(format!(
            "unsupported segmentGranularity '{other}' for appendToExisting=false \
             (replace) ingestion"
        ))),
    }
}

/// Map a period-object `segmentGranularity` (`P1D`, `PT1H`, ...) onto
/// [`SegmentGranularity`].
fn segment_granularity_from_period(p: &str) -> Result<SegmentGranularity> {
    match p.to_ascii_uppercase().as_str() {
        "PT1S" => segment_granularity_from_name("second"),
        "PT1M" => segment_granularity_from_name("minute"),
        "PT5M" => segment_granularity_from_name("five_minute"),
        "PT10M" => segment_granularity_from_name("ten_minute"),
        "PT15M" => segment_granularity_from_name("fifteen_minute"),
        "PT30M" => segment_granularity_from_name("thirty_minute"),
        "PT1H" => segment_granularity_from_name("hour"),
        "PT6H" => segment_granularity_from_name("six_hour"),
        "PT8H" => segment_granularity_from_name("eight_hour"),
        "P1D" => segment_granularity_from_name("day"),
        "P1W" | "P7D" => segment_granularity_from_name("week"),
        "P1M" => Ok(SegmentGranularity::Month),
        "P3M" => Ok(SegmentGranularity::Quarter),
        "P1Y" => Ok(SegmentGranularity::Year),
        other => Err(DruidError::Ingestion(format!(
            "unsupported segmentGranularity period '{other}' for \
             appendToExisting=false (replace) ingestion"
        ))),
    }
}

/// Clamp an `i128` intermediate back into `i64` bucket arithmetic.
fn clamp_millis_i64(v: i128) -> i64 {
    let clamped = v.clamp(i128::from(i64::MIN), i128::from(i64::MAX));
    // Infallible after the clamp; saturate defensively rather than unwrap.
    i64::try_from(clamped).unwrap_or(if v < 0 { i64::MIN } else { i64::MAX })
}

/// `(year, month0)` of a UTC timestamp for calendar-bucket arithmetic.
fn year_month0(ts: i64) -> Result<(i32, i32)> {
    let dt = DateTime::<Utc>::from_timestamp_millis(ts).ok_or_else(|| {
        DruidError::Ingestion(format!("timestamp out of range for calendar bucket: {ts}"))
    })?;
    let month0 = i32::try_from(dt.month0())
        .map_err(|e| DruidError::Ingestion(format!("month out of range: {e}")))?;
    Ok((dt.year(), month0))
}

/// Epoch-millis of `year`/`month0` (0-based month, may exceed 11 to roll
/// into later years) at UTC midnight of day 1.
fn ym_start_millis(year: i32, month0: i32) -> Result<i64> {
    let y = year + month0.div_euclid(12);
    let m = u32::try_from(month0.rem_euclid(12) + 1)
        .map_err(|e| DruidError::Ingestion(format!("month out of range: {e}")))?;
    chrono::NaiveDate::from_ymd_opt(y, m, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|n| n.and_utc().timestamp_millis())
        .ok_or_else(|| {
            DruidError::Ingestion(format!("calendar bucket out of range: year {y} month {m}"))
        })
}

/// Compute the half-open `[start, end)` epoch-millis window that an
/// `appendToExisting: false` task overwrites: the union of the
/// segment-granularity buckets covering the ingested data's
/// `[min_ts, max_ts]` (both bounds inclusive row timestamps).
fn replace_scope(granularity: SegmentGranularity, min_ts: i64, max_ts: i64) -> Result<(i64, i64)> {
    match granularity {
        SegmentGranularity::All => Ok((i64::MIN, i64::MAX)),
        SegmentGranularity::Fixed { millis, anchor } => {
            let millis_w = i128::from(millis);
            let anchor_w = i128::from(anchor);
            let bucket_start =
                |t: i64| anchor_w + (i128::from(t) - anchor_w).div_euclid(millis_w) * millis_w;
            Ok((
                clamp_millis_i64(bucket_start(min_ts)),
                clamp_millis_i64(bucket_start(max_ts) + millis_w),
            ))
        }
        SegmentGranularity::Month => {
            let (ys, ms) = year_month0(min_ts)?;
            let (ye, me) = year_month0(max_ts)?;
            Ok((ym_start_millis(ys, ms)?, ym_start_millis(ye, me + 1)?))
        }
        SegmentGranularity::Quarter => {
            let (ys, ms) = year_month0(min_ts)?;
            let (ye, me) = year_month0(max_ts)?;
            let qs = ms.div_euclid(3) * 3;
            let qe = me.div_euclid(3) * 3;
            Ok((ym_start_millis(ys, qs)?, ym_start_millis(ye, qe + 3)?))
        }
        SegmentGranularity::Year => {
            let (ys, _) = year_month0(min_ts)?;
            let (ye, _) = year_month0(max_ts)?;
            Ok((ym_start_millis(ys, 0)?, ym_start_millis(ye, 12)?))
        }
    }
}

/// Convert a [`TaskLock`] into its persisted [`TaskLockRow`] form.
fn lock_to_lock_row(lock: &TaskLock) -> Result<TaskLockRow> {
    let start_iso = format_epoch_millis_iso(lock.interval.start_millis);
    let end_iso = format_epoch_millis_iso(lock.interval.end_millis);
    let payload = serde_json::to_value(lock)
        .map_err(|e| DruidError::Metadata(format!("serialize lock: {e}")))?;
    Ok(TaskLockRow {
        id: lock.id.clone(),
        task_id: lock.task_id.clone(),
        data_source: lock.data_source.clone(),
        interval_start: start_iso,
        interval_end: end_iso,
        lock_type: lock.lock_type.as_str().to_string(),
        priority: lock.priority,
        revoked: lock.revoked,
        payload,
    })
}

/// Reconstruct a [`TaskLock`] from its persisted [`TaskLockRow`] form.
fn lock_row_to_lock(row: &TaskLockRow) -> Result<TaskLock> {
    let start = DateTime::parse_from_rfc3339(&row.interval_start)
        .map_err(|e| DruidError::Metadata(format!("lock interval_start: {e}")))?
        .timestamp_millis();
    let end = DateTime::parse_from_rfc3339(&row.interval_end)
        .map_err(|e| DruidError::Metadata(format!("lock interval_end: {e}")))?
        .timestamp_millis();
    let lock_type = match row.lock_type.as_str() {
        "SHARED" => LockType::Shared,
        "EXCLUSIVE" => LockType::Exclusive,
        other => {
            return Err(DruidError::Metadata(format!("unknown lock type: {other}")));
        }
    };
    Ok(TaskLock {
        id: row.id.clone(),
        task_id: row.task_id.clone(),
        data_source: row.data_source.clone(),
        interval: Interval::new(start, end)?,
        lock_type,
        priority: row.priority,
        revoked: row.revoked,
    })
}

/// Parsed `index_parallel` spec ready to feed [`BatchIngester`].
#[derive(Debug)]
struct ParsedIndexSpec {
    data_source: String,
    timestamp_column: String,
    /// Typed dimension schemas: `dimensionsSpec.dimensions` object entries
    /// (`{"type":"double","name":...}`) keep their type instead of being
    /// flattened to bare names (which silently ingested numeric dimensions
    /// as strings — the typed-dimension half of the null-faithful work).
    dimensions: Vec<ferrodruid_ingest_batch::DimensionSchema>,
    metrics_specs: Vec<serde_json::Value>,
    rows: Vec<serde_json::Value>,
    /// Whether ingestion-time rollup is enabled.  Druid's
    /// `UniformGranularitySpec` defaults `rollup` to **true** when the
    /// field is absent, and the default `granularitySpec` itself (when the
    /// whole object is absent) is `{rollup: true, queryGranularity: none}`
    /// — pre-fix this was never read and every spec silently ingested raw
    /// un-rolled rows (2026-07-11 v1.1.0 build-verification bug).
    rollup: bool,
    /// Truncation grain for rollup grouping, in the form
    /// [`BatchIngester::ingest_with_rollup`] expects (`"second"` /
    /// `"minute"` / `"hour"` / `"day"` / `"month"` / `"year"`), or
    /// `"none"` for exact-millisecond grouping (Druid's rollup with
    /// `queryGranularity=none` still merges identical (timestamp, dims)
    /// rows).  Only meaningful when `rollup` is true.
    query_granularity: String,
    /// `ioConfig.appendToExisting`.  Druid's native-batch default is
    /// **false**: the task REPLACES the used segments of the
    /// segmentGranularity buckets it writes into.  `true` preserves the
    /// pre-existing append behavior (segments coexist).  Pre-fix
    /// (P1-#1, 2026-07-12) this field was never read and every task
    /// appended — re-ingesting an interval silently doubled COUNT(*).
    append_to_existing: bool,
    /// Raw `granularitySpec.segmentGranularity` value, validated lazily on
    /// the `appendToExisting: false` path only (an append-only spec with an
    /// exotic granularity must not fail parse — mirroring the
    /// `rollup: false` leniency for `queryGranularity`).
    segment_granularity: Option<serde_json::Value>,
}

/// Map a Druid `queryGranularity` simple-string form (case-insensitive)
/// onto the truncation grain [`BatchIngester::ingest_with_rollup`]
/// expects.  Grains the batch ingester cannot truncate to
/// (`fifteen_minute`, `thirty_minute`, `week`, `quarter`, `all`) fail
/// closed with an ingestion error rather than silently mis-bucketing
/// rolled rows.
fn map_granularity_name(s: &str) -> Result<String> {
    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "none" | "second" | "minute" | "hour" | "day" | "month" | "year" => Ok(lower),
        other => Err(DruidError::Ingestion(format!(
            "unsupported queryGranularity '{other}' for rollup ingestion \
             (supported: none/second/minute/hour/day/month/year, \
             or the period object form PT1S/PT1M/PT1H/P1D/P1M/P1Y)"
        ))),
    }
}

/// Map a Druid period-object `queryGranularity` (`{"type": "period",
/// "period": "PT1H"}`) onto the batch ingester's truncation grain.
fn map_granularity_period(p: &str) -> Result<String> {
    match p.to_ascii_uppercase().as_str() {
        "PT1S" => Ok("second".to_string()),
        "PT1M" => Ok("minute".to_string()),
        "PT1H" => Ok("hour".to_string()),
        "P1D" => Ok("day".to_string()),
        "P1M" => Ok("month".to_string()),
        "P1Y" => Ok("year".to_string()),
        other => Err(DruidError::Ingestion(format!(
            "unsupported queryGranularity period '{other}' for rollup ingestion \
             (supported periods: PT1S/PT1M/PT1H/P1D/P1M/P1Y)"
        ))),
    }
}

/// Parse a Druid `queryGranularity` JSON value onto the truncation grain
/// string used for rollup grouping.
///
/// Supported wire shapes:
/// * absent / `null` → `"none"` (Druid's `UniformGranularitySpec` default);
/// * simple strings, case-insensitive: `"none"`, `"second"`, `"minute"`,
///   `"hour"`, `"day"`, `"month"`, `"year"` (the fixture files use the
///   uppercase forms `"NONE"` / `"HOUR"`);
/// * object forms `{"type": "none"}`, `{"type": "all"}` (rejected — see
///   below) and `{"type": "period", "period": "PT1H"}` for the six
///   mappable periods.
///
/// `"none"` maps to exact-millisecond grouping: Druid's rollup with
/// `queryGranularity=none` still merges rows whose (timestamp, dims) are
/// identical, which is exactly what the batch ingester does for the
/// `"none"` grain (its timestamp truncation leaves it untouched).
fn parse_query_granularity(v: Option<&serde_json::Value>) -> Result<String> {
    let Some(v) = v else {
        return Ok("none".to_string());
    };
    match v {
        serde_json::Value::Null => Ok("none".to_string()),
        serde_json::Value::String(s) => map_granularity_name(s),
        serde_json::Value::Object(o) => {
            let gtype = o.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match gtype {
                "none" => Ok("none".to_string()),
                "period" => {
                    let period = o.get("period").and_then(|p| p.as_str()).ok_or_else(|| {
                        DruidError::Ingestion(
                            "queryGranularity period object is missing its 'period' string"
                                .to_string(),
                        )
                    })?;
                    map_granularity_period(period)
                }
                other => Err(DruidError::Ingestion(format!(
                    "unsupported queryGranularity object type '{other}' for rollup ingestion \
                     (supported: none, period)"
                ))),
            }
        }
        other => Err(DruidError::Ingestion(format!(
            "queryGranularity must be a string or object, got: {other}"
        ))),
    }
}

/// Parse `dataSchema.granularitySpec` into `(rollup, query_granularity)`.
///
/// Druid semantics (its `UniformGranularitySpec` defaults, matched
/// faithfully): an absent `granularitySpec` object means
/// `{rollup: true, queryGranularity: none}`, and inside a present spec an
/// absent `rollup` field defaults to **true** and an absent
/// `queryGranularity` to `none`.
///
/// The grain is only validated when rollup is enabled: with
/// `rollup: false` the batch path ingests raw rows and never consults the
/// grain, so an exotic grain must not fail a spec that ingests fine
/// (honest limitation: Druid truncates `__time` by `queryGranularity`
/// even with rollup disabled; FerroDruid's rollup=false path currently
/// stores exact timestamps).
fn parse_granularity_spec(data_schema: &serde_json::Value) -> Result<(bool, String)> {
    let gspec = data_schema.get("granularitySpec");
    let rollup = match gspec.and_then(|g| g.get("rollup")) {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(other) => {
            return Err(DruidError::Ingestion(format!(
                "granularitySpec.rollup must be a boolean, got: {other}"
            )));
        }
    };
    let query_granularity = if rollup {
        parse_query_granularity(gspec.and_then(|g| g.get("queryGranularity")))?
    } else {
        "none".to_string()
    };
    Ok((rollup, query_granularity))
}

/// Parse a Druid `index_parallel` ingestion spec into the form
/// [`BatchIngester`] consumes.
///
/// Returns `Ok(None)` if the spec is well-formed but not a shape we
/// currently support (e.g. non-inline input source); the caller should
/// fall back to stub behavior in that case so the wire envelope stays
/// honest.
fn parse_index_parallel_spec(spec: &serde_json::Value) -> Result<Option<ParsedIndexSpec>> {
    let inner = spec.get("spec").unwrap_or(spec);

    let data_schema = match inner.get("dataSchema") {
        Some(v) => v,
        None => return Ok(None),
    };

    let data_source = data_schema
        .get("dataSource")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DruidError::Ingestion("dataSchema.dataSource is required".to_string()))?
        .to_string();

    let timestamp_column = data_schema
        .get("timestampSpec")
        .and_then(|t| t.get("column"))
        .and_then(|v| v.as_str())
        .unwrap_or("__time")
        .to_string();

    let dimensions: Vec<ferrodruid_ingest_batch::DimensionSchema> = data_schema
        .get("dimensionsSpec")
        .and_then(|d| d.get("dimensions"))
        .and_then(|v| v.as_array())
        .map(|arr| ferrodruid_ingest_batch::parse_dimension_entries(arr))
        .transpose()?
        .unwrap_or_default();

    let metrics_specs: Vec<serde_json::Value> = data_schema
        .get("metricsSpec")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let (rollup, query_granularity) = parse_granularity_spec(data_schema)?;
    let segment_granularity = data_schema
        .get("granularitySpec")
        .and_then(|g| g.get("segmentGranularity"))
        .cloned();

    // Parse the inline data — Druid accepts either a JSONL string or a
    // JSON array of row objects.  We support both.
    let io_config = match inner.get("ioConfig") {
        Some(v) => v,
        None => return Ok(None),
    };

    // Druid's native-batch `ioConfig` defaults `appendToExisting` to FALSE
    // (replace).  A non-boolean value fails closed: silently coercing it
    // would pick replace-vs-append semantics by guesswork.
    let append_to_existing = match io_config.get("appendToExisting") {
        None | Some(serde_json::Value::Null) => false,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(other) => {
            return Err(DruidError::Ingestion(format!(
                "ioConfig.appendToExisting must be a boolean, got: {other}"
            )));
        }
    };
    let input_source = match io_config.get("inputSource") {
        Some(v) => v,
        None => return Ok(None),
    };
    let src_type = input_source.get("type").and_then(|v| v.as_str());
    if src_type != Some("inline") {
        // Not supported in this wave (file/HDFS/S3/etc.).
        return Ok(None);
    }

    let raw_data = input_source.get("data").ok_or_else(|| {
        DruidError::Ingestion("inputSource.data is required for inline source".to_string())
    })?;

    let rows: Vec<serde_json::Value> = match raw_data {
        serde_json::Value::String(s) => s
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line)
                    .map_err(|e| DruidError::Ingestion(format!("inline JSONL parse error: {e}")))
            })
            .collect::<Result<Vec<_>>>()?,
        serde_json::Value::Array(arr) => arr.clone(),
        other => {
            return Err(DruidError::Ingestion(format!(
                "inputSource.data must be a JSONL string or array, got: {other}"
            )));
        }
    };

    Ok(Some(ParsedIndexSpec {
        data_source,
        timestamp_column,
        dimensions,
        metrics_specs,
        rows,
        rollup,
        query_granularity,
        append_to_existing,
        segment_granularity,
    }))
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Status of an ingestion task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    /// Waiting for a worker slot.
    Pending,
    /// Currently executing.
    Running,
    /// Completed successfully.
    Success,
    /// Completed with failure.
    Failed,
}

/// Location of a running task on a worker node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLocation {
    /// Worker hostname.
    pub host: String,
    /// Worker HTTP port.
    pub port: u16,
    /// Optional TLS port.
    pub tls_port: Option<u16>,
}

/// Information about a single ingestion task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    /// Unique task identifier.
    pub id: String,
    /// Task type (e.g. `"index_kafka"`, `"index_parallel"`).
    pub task_type: String,
    /// Target data source name.
    pub data_source: String,
    /// Current status (legacy 4-state view; `WAITING` is surfaced here as
    /// `PENDING` for backward compatibility — use [`TaskInfo::state`] for the
    /// full lifecycle state).
    pub status: TaskStatus,
    /// When the task was created.
    pub created_time: DateTime<Utc>,
    /// Where the task is running (populated once `Running`).
    pub location: Option<TaskLocation>,
    /// Full lifecycle state (`WAITING`/`PENDING`/`RUNNING`/`SUCCESS`/`FAILED`).
    #[serde(default = "default_task_state")]
    pub state: TaskState,
    /// Number of execution attempts made so far.
    #[serde(default)]
    pub attempt: u32,
    /// Worker id (`host:port`) this task is currently assigned to, if any.
    #[serde(default)]
    pub worker: Option<String>,
}

fn default_task_state() -> TaskState {
    TaskState::Pending
}

impl TaskStatus {
    /// Project a full [`TaskState`] onto the legacy 4-state [`TaskStatus`].
    ///
    /// `WAITING` collapses to `Pending` so existing callers that filter on
    /// `Pending` for "waiting tasks" keep working.
    fn from_state(state: TaskState) -> Self {
        match state {
            TaskState::Waiting | TaskState::Pending => TaskStatus::Pending,
            TaskState::Running => TaskStatus::Running,
            TaskState::Success => TaskStatus::Success,
            TaskState::Failed => TaskStatus::Failed,
        }
    }
}

// ---------------------------------------------------------------------------
// Overlord
// ---------------------------------------------------------------------------

/// Internal lifecycle record for a task.
#[derive(Debug, Clone)]
struct TaskRecord {
    id: String,
    task_type: String,
    data_source: String,
    state: TaskState,
    created_time: DateTime<Utc>,
    location: Option<TaskLocation>,
    /// Number of execution attempts made so far.
    attempt: u32,
    /// Worker id this task is assigned to (`host:port`), if any.
    worker: Option<String>,
}

impl TaskRecord {
    fn to_info(&self) -> TaskInfo {
        TaskInfo {
            id: self.id.clone(),
            task_type: self.task_type.clone(),
            data_source: self.data_source.clone(),
            status: TaskStatus::from_state(self.state),
            created_time: self.created_time,
            location: self.location.clone(),
            state: self.state,
            attempt: self.attempt,
            worker: self.worker.clone(),
        }
    }

    fn to_row(&self) -> Result<TaskRow> {
        let info = self.to_info();
        let payload = serde_json::to_value(&info)
            .map_err(|e| DruidError::Metadata(format!("serialize task info: {e}")))?;
        Ok(TaskRow {
            id: self.id.clone(),
            task_type: self.task_type.clone(),
            data_source: self.data_source.clone(),
            status: self.state.as_str().to_string(),
            created_date: self.created_time.to_rfc3339(),
            attempt: i64::from(self.attempt),
            worker: self.worker.clone(),
            payload,
        })
    }
}

/// The Overlord manages the full lifecycle of ingestion tasks and
/// supervisor specs.
///
/// Tasks are stored in-memory for Phase 1; supervisor specs are persisted
/// to the [`MetadataStore`].
///
/// When constructed via [`Overlord::with_executor`], `submit_task` will
/// drive `index_parallel` + `inputSource.type=inline` ingestion specs all
/// the way through:
///
/// 1. Parse `dataSchema` (timestamp/dimensions/metrics) and the inline
///    JSONL or JSON-array data.
/// 2. Invoke [`BatchIngester::ingest`] to produce a `SegmentData`.
/// 3. With `ioConfig.appendToExisting: false` (Druid's batch default),
///    REPLACE: every used segment of the datasource whose interval
///    overlaps — and is fully contained in — the segmentGranularity
///    buckets covering the new data is swapped out for the new segment.
///    If an overlapping segment extends beyond those buckets the task
///    FAILS CLOSED instead of silently dropping its out-of-scope rows.
///    `true` appends (segments coexist). The whole publish sequence is
///    serialized per datasource on the shared
///    [`MetadataStore::datasource_publish_lock`] — the same mutex the
///    Coordinator's used-flag mutations take, so an admin disable can
///    never interleave with a publish. The segment id is allocated
///    collision-free under that lock (same-millisecond publications get a
///    numeric suffix instead of silently overwriting each other).
/// 4. Persist the metadata changes first as ONE atomic transaction
///    ([`MetadataStore::replace_segments_txn`]: victims marked unused AND
///    the new [`SegmentMetadataRow`] inserted, so coordinator endpoints
///    surface it); a metadata failure — or a crash mid-transaction —
///    leaves metadata untouched and aborts before the query path is
///    touched.
/// 5. Apply the query-visible change as ONE atomic
///    [`Historical::replace_segments`] swap (victims out, new segment +
///    datasource mapping in), so a concurrent query observes either the
///    full old segment set or the full new one — never a partial mix,
///    a double-counted interval, or an unregistered orphan. A swap
///    failure is compensated by one
///    [`MetadataStore::rollback_replace_txn`] transaction under the
///    still-held publish lock.
///
/// Constructed via [`Overlord::new`] (no executor) the task is recorded
/// as `Pending` only — the original Phase-1 stub behavior — to preserve
/// backwards compatibility with unit-test setups that do not own a
/// Historical.
pub struct Overlord {
    metadata: Arc<MetadataStore>,
    historical: Option<Arc<Historical>>,
    running_tasks: RwLock<HashMap<String, TaskRecord>>,
    /// Monotonically increasing counter for generating task IDs.
    task_counter: std::sync::atomic::AtomicU64,
    /// Pluggable worker selector (registered workers + strategy).
    workers: Mutex<WorkerSelector>,
    /// Retry policy for failed/lost tasks.
    retry_policy: RetryPolicy,
    /// Per-datasource acquisition mutexes serializing the whole
    /// read-active-locks → evaluate-conflicts → revoke → insert sequence
    /// in [`try_acquire_lock`]. Without this, two concurrent overlapping
    /// EXCLUSIVE requests could both read an empty active set and both be
    /// granted. The outer map is itself guarded so that the per-datasource
    /// mutex is created/looked up atomically; the inner `Arc<Mutex<()>>` is
    /// then held across the entire acquire sequence for that datasource.
    ///
    /// The segment-publication critical section of
    /// [`execute_index_parallel`] uses a DIFFERENT lock: the store-level
    /// [`MetadataStore::datasource_publish_lock`], shared with the
    /// Coordinator's used-flag mutations (`disable_segment` /
    /// `disable_datasource` / `enable_datasource`) so a publish and an
    /// admin disable are mutually exclusive per datasource (Codex
    /// 2026-07-12 round-2 HIGH #2/#3). It moved off this struct precisely
    /// so that lock lives where every mutator of the shared metadata can
    /// reach it; the single-process scope caveat is documented on
    /// [`MetadataStore::datasource_publish_lock`].
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    lock_acquire_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    /// Test-only fault injection: while `true`, the atomic publish
    /// metadata transaction in [`execute_index_parallel`] fails with an
    /// injected [`DruidError::Metadata`] before touching the store,
    /// exercising the publication failure path (Codex 2026-07-12 HIGH #2,
    /// re-pointed at the round-2 single-transaction publish). Sticky until
    /// cleared so every retry attempt of a task observes the same fault.
    ///
    /// [`execute_index_parallel`]: Overlord::execute_index_parallel
    #[cfg(test)]
    inject_insert_segment_failure: std::sync::atomic::AtomicBool,
}

impl Overlord {
    /// Create a new Overlord backed by the given metadata store.
    ///
    /// Tasks submitted via this Overlord are recorded as `Pending`
    /// without driving an ingestion executor.  Use [`with_executor`]
    /// for the production path that actually produces segments.
    ///
    /// [`with_executor`]: Overlord::with_executor
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self {
            metadata,
            historical: None,
            running_tasks: RwLock::new(HashMap::new()),
            task_counter: std::sync::atomic::AtomicU64::new(1),
            workers: Mutex::new(WorkerSelector::new(
                Vec::new(),
                WorkerSelectStrategy::RoundRobin,
            )),
            retry_policy: RetryPolicy::default(),
            lock_acquire_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            inject_insert_segment_failure: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Create a new Overlord wired to a [`Historical`] for in-process
    /// segment publication.
    ///
    /// `index_parallel` specs with an inline input source will be fully
    /// executed: the batch ingester runs synchronously inside
    /// `submit_task`, the resulting segment is loaded into the supplied
    /// Historical, and the metadata store is updated so coordinator and
    /// SQL paths can find it.
    pub fn with_executor(metadata: Arc<MetadataStore>, historical: Arc<Historical>) -> Self {
        Self {
            metadata,
            historical: Some(historical),
            running_tasks: RwLock::new(HashMap::new()),
            task_counter: std::sync::atomic::AtomicU64::new(1),
            workers: Mutex::new(WorkerSelector::new(
                Vec::new(),
                WorkerSelectStrategy::RoundRobin,
            )),
            retry_policy: RetryPolicy::default(),
            lock_acquire_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            inject_insert_segment_failure: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Override the retry policy (builder-style; consumes and returns self).
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Override the worker-selection strategy and pre-register workers
    /// (builder-style; consumes and returns self).
    #[must_use]
    pub fn with_workers(self, workers: Vec<Worker>, strategy: WorkerSelectStrategy) -> Self {
        self.workers
            .try_lock()
            .map(|mut guard| *guard = WorkerSelector::new(workers, strategy))
            .ok();
        self
    }

    // ----- Tasks -----------------------------------------------------------

    /// Submit a new ingestion task from its JSON spec.
    ///
    /// For an Overlord built with [`with_executor`] this drives the
    /// `index_parallel` + inline-data path end-to-end (parse spec, run
    /// batch ingester, load segment into Historical, register in
    /// metadata).  For an Overlord built with [`new`] it falls back to
    /// stub behavior (record as Pending, return ID).
    ///
    /// Returns the auto-generated task identifier.
    ///
    /// [`with_executor`]: Overlord::with_executor
    /// [`new`]: Overlord::new
    pub async fn submit_task(&self, spec: serde_json::Value) -> Result<String> {
        // The dataSource for ID generation may live at the top level
        // (legacy Phase-1 form) or under spec.spec.dataSchema.dataSource
        // (Druid `index_parallel`).
        let task_type = spec
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let mut data_source = spec
            .get("dataSource")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if data_source.is_empty() {
            data_source = spec
                .pointer("/spec/dataSchema/dataSource")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
        }

        let seq = self
            .task_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!("{task_type}_{data_source}_{seq}");

        // Every task starts WAITING (blocked on lock acquisition).
        let mut record = TaskRecord {
            id: id.clone(),
            task_type: task_type.clone(),
            data_source: data_source.clone(),
            state: TaskState::Waiting,
            created_time: Utc::now(),
            location: None,
            attempt: 0,
            worker: None,
        };

        // Acquire an interval lock if the spec carries one. A failure to
        // acquire (blocked by a higher/equal-priority holder) leaves the task
        // WAITING — the caller can retry submission or poll.
        let lock_req = self.parse_lock_request(&id, &data_source, &spec)?;
        if let Some(req) = lock_req {
            match self.try_acquire_lock(req).await? {
                true => {}
                false => {
                    // Stay WAITING; persist and return the id.
                    self.persist_and_store(record).await?;
                    return Ok(id);
                }
            }
        }

        // Locks (if any) granted: WAITING -> PENDING.
        record.state = TaskState::Pending;

        // If this Overlord owns a Historical and the spec is the
        // `index_parallel` + inline shape we support, run the full
        // ingestion through RUNNING -> terminal.  Otherwise leave it PENDING
        // (the original Phase-1 stub behavior).
        if self.historical.is_some() && task_type == "index_parallel" {
            self.run_with_retry(&mut record, &spec).await;
        }

        self.persist_and_store(record).await?;
        Ok(id)
    }

    /// Run an `index_parallel` task through the RUNNING -> terminal portion of
    /// the lifecycle, retrying on failure per the configured [`RetryPolicy`].
    ///
    /// Records that reach `SUCCESS` release their locks; records that exhaust
    /// the retry budget end `FAILED` (also releasing locks). A record that the
    /// spec does not match (`Ok(false)`) is left `PENDING` — the stub path.
    async fn run_with_retry(&self, record: &mut TaskRecord, spec: &serde_json::Value) {
        let Some(historical) = self.historical.as_ref() else {
            return;
        };
        loop {
            // Assign a worker if any are registered; bookkeeping only.
            record.worker = self.assign_worker(&record.id).await;
            record.location = record.worker.as_ref().and_then(|w| worker_location(w));
            record.state = TaskState::Running;
            record.attempt = record.attempt.saturating_add(1);

            match self
                .execute_index_parallel(&record.id, &record.data_source, spec, historical)
                .await
            {
                Ok(true) => {
                    record.state = TaskState::Success;
                    self.release_task_locks(&record.id).await;
                    return;
                }
                Ok(false) => {
                    // Unsupported shape: revert to PENDING (stub behavior),
                    // drop any worker assignment, keep locks for the caller.
                    record.state = TaskState::Pending;
                    record.worker = None;
                    record.location = None;
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %record.id,
                        attempt = record.attempt,
                        error = %e,
                        "index_parallel ingestion attempt failed"
                    );
                    if self.retry_policy.can_retry(record.attempt) {
                        // Worker (notionally) released between attempts.
                        record.worker = None;
                        record.location = None;
                        record.state = TaskState::Pending;
                        // Backoff is computed but not slept on in-process to
                        // keep submission synchronous; it is surfaced for
                        // schedulers via `RetryPolicy::backoff_millis`.
                        continue;
                    }
                    record.state = TaskState::Failed;
                    self.release_task_locks(&record.id).await;
                    return;
                }
            }
        }
    }

    /// Persist a task record and store it in the in-memory table.
    async fn persist_and_store(&self, record: TaskRecord) -> Result<()> {
        let row = record.to_row()?;
        self.metadata.insert_task(&row).await?;
        let mut tasks = self.running_tasks.write().await;
        tasks.insert(record.id.clone(), record);
        Ok(())
    }

    /// Drive the `index_parallel` + inline-data path.
    ///
    /// Returns `Ok(true)` when a segment was produced and registered,
    /// `Ok(false)` when the spec is well-formed but unsupported (e.g.
    /// non-inline source), and `Err(_)` for hard failures.
    async fn execute_index_parallel(
        &self,
        task_id: &str,
        data_source: &str,
        spec: &serde_json::Value,
        historical: &Arc<Historical>,
    ) -> Result<bool> {
        let parsed = match parse_index_parallel_spec(spec)? {
            Some(p) => p,
            None => return Ok(false),
        };
        let ParsedIndexSpec {
            data_source: ds_name,
            timestamp_column,
            dimensions,
            metrics_specs,
            rows,
            rollup,
            query_granularity,
            append_to_existing,
            segment_granularity,
        } = parsed;

        if ds_name != data_source {
            tracing::debug!(
                task_id,
                "spec dataSource '{ds_name}' differs from header '{data_source}'; using spec"
            );
        }
        if rows.is_empty() {
            return Err(DruidError::Ingestion(
                "inline inputSource.data is empty".to_string(),
            ));
        }

        let ingester = BatchIngester::with_schemas(
            ds_name.clone(),
            timestamp_column,
            dimensions,
            metrics_specs,
        );
        // Rollup enabled (Druid's default) pre-aggregates: rows sharing a
        // (queryGranularity-truncated timestamp, dimension values) key merge
        // into one stored row with summed metrics and `count`-type metrics
        // holding the merged raw-row count.  Pre-fix this branch did not
        // exist and `ingest_with_rollup` was dead code in the server binary.
        let ingested = if rollup {
            ingester.ingest_with_rollup(rows, &query_granularity)?
        } else {
            ingester.ingest(rows)?
        };

        let start_iso = format_epoch_millis_iso(ingested.interval.start_millis);
        // Use a half-open end one millisecond past the max so the
        // segment fully covers the data range.
        let end_iso = format_epoch_millis_iso(ingested.interval.end_millis.saturating_add(1));

        let version = ingested.version.clone();
        let num_rows = ingested.num_rows;

        // Serialize the whole publication sequence (id allocation →
        // replace scan → metadata transaction → segment swap → rollback)
        // per datasource so two concurrent tasks cannot interleave: each
        // publish observes the previous one's full effect. The lock lives
        // on the shared [`MetadataStore`] (round-2 HIGH #2/#3) so the
        // Coordinator's used-flag mutations (`disable_segment` /
        // `disable_datasource` / `enable_datasource`) are mutually
        // exclusive with this critical section too: an admin disable can
        // only land fully before it (then the segment is simply not a
        // victim) or fully after it (then the disable wins) — never in
        // between, where a rollback would resurrect the disable or a
        // just-disabled segment would become query-visible.
        let publish_lock = self.metadata.datasource_publish_lock(&ds_name).await;
        let _publish_guard = publish_lock.lock().await;

        // Allocate the segment id UNDER the lock (round-2 HIGH #4): the
        // base id embeds a millisecond-resolution version generated at
        // ingest time, so two tasks covering the same interval in the
        // same millisecond would collide and one task's rows would be
        // silently discarded on insert. `allocate_segment_id` uniquifies
        // with a Druid-style numeric suffix against every existing
        // metadata row (used or unused) and every loaded segment.
        let segment_id = self
            .allocate_segment_id(historical, &ds_name, &start_iso, &end_iso, &version)
            .await?;

        // --- Publication (Codex 2026-07-12 HIGH #1/#2/#3 + round-2 #1-#4) --
        //
        // Druid-faithful replace (P1-#1): with `appendToExisting: false`
        // (Druid's batch default) the task overwrites the
        // segmentGranularity buckets it writes into. The sequence is
        // ordered so that no failure, crash, or concurrent query can
        // observe data loss, a partial replace, or an unregistered orphan
        // segment:
        //
        //   1. Plan the replace read-only, FAILING CLOSED if any
        //      overlapping used segment extends beyond the replace scope
        //      (dropping it wholesale would delete out-of-scope rows).
        //   2. Write metadata as ONE atomic SQLite transaction
        //      ([`MetadataStore::replace_segments_txn`]: victims → unused
        //      AND the new row inserted). A failure — or a process crash
        //      mid-transaction — leaves metadata exactly as it was
        //      (round-2 HIGH #1); the query path has not been touched.
        //   3. Apply the query-visible change LAST as ONE atomic swap
        //      ([`Historical::replace_segments`]), so a concurrent query
        //      sees either the full old segment set or the full new one.
        //      If the swap fails, one compensating transaction
        //      ([`MetadataStore::rollback_replace_txn`]) restores the
        //      pre-publish metadata under the still-held publish lock.
        //
        // Durability note (round-2 HIGH #1 residual, documented): a crash
        // BETWEEN steps 2 and 3 leaves the committed post-replace metadata
        // with the swap unapplied. That is still a consistent recovery
        // point, not a partial one: segment data is in-memory only in the
        // current product, so a restart reloads nothing either way and
        // metadata remains the single durable record of the completed
        // replace — no victim is half-retired and no interval is
        // attributed to two segment sets.

        // Phase 1 — plan (read-only, fail-closed on partial overlap).
        let victims = if append_to_existing {
            Vec::new()
        } else {
            self.plan_replace_victims(
                &ds_name,
                segment_granularity.as_ref(),
                (ingested.interval.start_millis, ingested.interval.end_millis),
                &segment_id,
            )
            .await?
        };

        // Phase 2 — ONE atomic metadata transaction (victims -> unused AND
        // the new row inserted). On failure nothing was written — there is
        // no partial state to roll back, and queries kept serving the old
        // segments the whole time.
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let row = SegmentMetadataRow {
            id: segment_id.clone(),
            data_source: ds_name.clone(),
            created_date: now_iso,
            start: start_iso,
            end: end_iso,
            version,
            used: true,
            payload: serde_json::json!({
                "dataSource": ds_name,
                "numRows": num_rows,
                "taskId": task_id,
            }),
        };
        let victim_ids: Vec<String> = victims.iter().map(|v| v.id.clone()).collect();
        self.publish_replace_metadata(&victim_ids, &row).await?;

        // Phase 3 — one atomic query-visible swap: victims out, new
        // segment (with its datasource mapping) in, under a single
        // write-lock acquisition.
        match historical.replace_segments(
            &victim_ids,
            vec![SegmentSwapEntry {
                id: segment_id.clone(),
                data: Arc::new(ingested.segment_data),
                datasource: Some(ds_name.clone()),
            }],
        ) {
            Ok(removed) => {
                // A victim with a metadata row but no loaded segment (e.g.
                // after a restart) is tolerated: its row was already
                // flipped to unused above.
                for victim in &victims {
                    if removed.iter().any(|r| r.id == victim.id) {
                        tracing::info!(
                            task_id,
                            data_source = %ds_name,
                            segment_id = %victim.id,
                            "appendToExisting=false: replaced (dropped + marked unused) \
                             overlapping segment"
                        );
                    } else {
                        tracing::warn!(
                            task_id,
                            segment_id = %victim.id,
                            "replaced segment was not loaded in the historical; \
                             marked unused in metadata only"
                        );
                    }
                }
            }
            Err(e) => {
                // Reachable when the Historical's lock is poisoned or an
                // undeclared segment-id collision is detected (the swap
                // mutates nothing on failure). Undo the committed metadata
                // transaction with ONE compensating transaction — delete
                // the new row, restore the victim snapshots verbatim — so
                // metadata keeps matching the unchanged query-visible
                // state. The publish lock is still held, so no admin
                // disable can have landed in between for the restore to
                // overwrite (round-2 HIGH #2). A rollback failure (the
                // metadata store broken right after a successful commit)
                // is logged and the original error surfaced; its residual
                // is the documented step-2/step-3 crash window: committed
                // post-replace metadata with the swap unapplied, which a
                // restart resolves consistently because segments are
                // memory-resident only.
                if let Err(restore_err) = self
                    .metadata
                    .rollback_replace_txn(&segment_id, &victims)
                    .await
                {
                    tracing::error!(
                        task_id,
                        segment_id = %segment_id,
                        error = %restore_err,
                        "rollback could not un-publish the metadata after a \
                         historical swap failure"
                    );
                }
                return Err(e);
            }
        }

        tracing::info!(
            task_id,
            data_source = %ds_name,
            segment_id = %segment_id,
            num_rows,
            replaced = victim_ids.len(),
            "index_parallel ingestion completed"
        );
        Ok(true)
    }

    /// Apply the publish metadata change (victims → unused + new row) as
    /// one atomic transaction via [`MetadataStore::replace_segments_txn`].
    ///
    /// Split out so tests can inject a metadata failure here — and ONLY
    /// here; any rollback writes must keep using the real store — to
    /// exercise the publication failure path (Codex 2026-07-12 HIGH #2).
    /// Atomicity of a failure BETWEEN the transaction's writes is locked
    /// in at the store level
    /// (`replace_txn_failure_between_writes_leaves_no_partial_state`).
    async fn publish_replace_metadata(
        &self,
        victim_ids: &[String],
        row: &SegmentMetadataRow,
    ) -> Result<()> {
        #[cfg(test)]
        if self
            .inject_insert_segment_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(DruidError::Metadata(
                "injected publish-metadata failure (test fault hook)".to_string(),
            ));
        }
        self.metadata.replace_segments_txn(victim_ids, row).await
    }

    /// Allocate a collision-free segment id under the datasource's publish
    /// lock (Codex 2026-07-12 round-2 HIGH #4).
    ///
    /// The base id `{ds}_{start}_{end}_{version}` embeds a
    /// millisecond-resolution version, so two tasks publishing over the
    /// same interval in the same millisecond produce the SAME id; pre-fix
    /// the second `INSERT OR REPLACE` + `load_segment` silently discarded
    /// the first task's rows. If the base id is taken — by any metadata
    /// row, used or unused, or any loaded segment — a Druid-style numeric
    /// suffix `_1`, `_2`, … is appended until the id is free. The plain
    /// `INSERT` inside [`MetadataStore::replace_segments_txn`] and the
    /// collision check in [`Historical::replace_segments`] remain as
    /// fail-closed backstops should an id slip through anyway.
    ///
    /// Callers MUST hold the datasource's publish lock: it is what keeps
    /// the exists-check and the subsequent insert race-free (every other
    /// writer of this datasource's rows serializes on the same lock).
    async fn allocate_segment_id(
        &self,
        historical: &Arc<Historical>,
        ds_name: &str,
        start_iso: &str,
        end_iso: &str,
        version: &str,
    ) -> Result<String> {
        /// Bound on same-base-id publications; reaching it means the store
        /// holds this many rows for one `(interval, version)` and almost
        /// certainly indicates a bug or abuse rather than real ingestion.
        const MAX_ID_SUFFIX: u32 = 10_000;

        let base = format!("{ds_name}_{start_iso}_{end_iso}_{version}");
        let mut candidate = base.clone();
        for suffix in 1..=MAX_ID_SUFFIX {
            let taken = self.metadata.segment_exists(&candidate).await?
                || historical.has_segment(&candidate);
            if !taken {
                return Ok(candidate);
            }
            candidate = format!("{base}_{suffix}");
        }
        Err(DruidError::Ingestion(format!(
            "could not allocate a unique segment id after {MAX_ID_SUFFIX} attempts \
             (base id '{base}'); refusing to overwrite an existing segment"
        )))
    }

    /// Plan the replace step for an `appendToExisting: false` task: return
    /// every used segment of `ds_name` whose interval overlaps the
    /// segmentGranularity buckets covering the new data (`data_bounds` =
    /// inclusive `(min, max)` row-timestamp millis), so the caller can
    /// mark them unused and swap them out of the Historical atomically.
    ///
    /// **Fail-closed containment check (Codex 2026-07-12 HIGH #1):** the
    /// batch ingester publishes one raw `[min, max+1)` segment per task,
    /// so an existing segment can extend OUTSIDE the replace scope — e.g.
    /// a segment holding Jan-1 *and* Jan-2 rows when a later task
    /// re-ingests only Jan-1 at DAY granularity. Dropping such a victim
    /// wholesale would silently delete its out-of-scope rows, so this
    /// method ABORTS the ingestion instead; only victims fully contained
    /// in the replace scope are ever returned. Segment splitting is
    /// deliberately not attempted — re-ingest data covering the full
    /// spanned interval (or use `appendToExisting: true`) to proceed.
    ///
    /// Read-only; callers must hold the datasource's publish lock so the
    /// plan cannot go stale before it is applied.
    async fn plan_replace_victims(
        &self,
        ds_name: &str,
        segment_granularity: Option<&serde_json::Value>,
        data_bounds: (i64, i64),
        new_segment_id: &str,
    ) -> Result<Vec<SegmentMetadataRow>> {
        let granularity = parse_segment_granularity(segment_granularity)?;
        let (scope_start, scope_end) = replace_scope(granularity, data_bounds.0, data_bounds.1)?;

        let existing = self.metadata.get_used_segments(ds_name).await?;
        let mut victims = Vec::new();
        for seg in existing {
            // Never treat the segment being published as its own victim.
            // With collision-free id allocation (`allocate_segment_id`,
            // round-2 HIGH #4) a live row can no longer share the new id,
            // so this guard is defense-in-depth only.
            if seg.id == new_segment_id {
                continue;
            }
            let seg_start = parse_interval_bound_millis(&seg.start).map_err(|e| {
                DruidError::Ingestion(format!(
                    "cannot parse existing segment '{}' interval start '{}' for \
                     replace-overlap check: {e}",
                    seg.id, seg.start
                ))
            })?;
            let seg_end = parse_interval_bound_millis(&seg.end).map_err(|e| {
                DruidError::Ingestion(format!(
                    "cannot parse existing segment '{}' interval end '{}' for \
                     replace-overlap check: {e}",
                    seg.id, seg.end
                ))
            })?;
            if seg_start >= scope_end || seg_end <= scope_start {
                continue; // no overlap with the replaced buckets
            }
            if seg_start < scope_start || seg_end > scope_end {
                return Err(DruidError::Ingestion(format!(
                    "replace would drop data outside the ingested interval: existing \
                     segment '{}' [{}/{}] spans beyond the replace scope [{}/{}] \
                     computed from the ingested rows' segmentGranularity buckets; \
                     aborting instead of losing the out-of-scope rows (re-ingest data \
                     covering the segment's full interval, or use appendToExisting: true)",
                    seg.id,
                    seg.start,
                    seg.end,
                    format_epoch_millis_iso(scope_start),
                    format_epoch_millis_iso(scope_end),
                )));
            }
            victims.push(seg);
        }
        Ok(victims)
    }

    /// Look up a task by its identifier.
    pub async fn get_task(&self, id: &str) -> Result<Option<TaskInfo>> {
        let tasks = self.running_tasks.read().await;
        Ok(tasks.get(id).map(TaskRecord::to_info))
    }

    /// Return all tasks that are currently in `RUNNING` state.
    pub async fn get_running_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| t.state == TaskState::Running)
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Return all tasks that have completed (`SUCCESS` or `FAILED`).
    pub async fn get_complete_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| t.state.is_terminal())
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Return all tasks that are waiting (`WAITING` or `PENDING`).
    pub async fn get_waiting_tasks(&self) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| matches!(t.state, TaskState::Waiting | TaskState::Pending))
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Gracefully shut down a task by transitioning it to `FAILED`, releasing
    /// any locks it holds.
    ///
    /// `RUNNING`/`PENDING`/`WAITING` tasks transition to `FAILED`; tasks that
    /// are already terminal are left unchanged (idempotent).
    pub async fn shutdown_task(&self, id: &str) -> Result<()> {
        {
            let mut tasks = self.running_tasks.write().await;
            let task = tasks
                .get_mut(id)
                .ok_or_else(|| DruidError::Metadata(format!("task not found: {id}")))?;
            if !task.state.is_terminal() {
                task.state = TaskState::Failed;
                task.worker = None;
                task.location = None;
            }
        }
        self.release_task_locks(id).await;
        self.persist_existing(id).await?;
        Ok(())
    }

    /// Return all tasks (any state) for a given data source.
    pub async fn get_tasks_by_datasource(&self, ds: &str) -> Vec<TaskInfo> {
        let tasks = self.running_tasks.read().await;
        tasks
            .values()
            .filter(|t| t.data_source == ds)
            .map(TaskRecord::to_info)
            .collect()
    }

    /// Transition a task to a new legacy [`TaskStatus`].
    ///
    /// This is the backward-compatible entrypoint used by the REST layer. It
    /// maps the 4-state status onto the lifecycle state machine and rejects
    /// transitions that are invalid for the current state.
    pub async fn update_task_status(&self, id: &str, status: TaskStatus) -> Result<()> {
        let target = match status {
            TaskStatus::Pending => TaskState::Pending,
            TaskStatus::Running => TaskState::Running,
            TaskStatus::Success => TaskState::Success,
            TaskStatus::Failed => TaskState::Failed,
        };
        self.transition_task(id, target).await
    }

    /// Transition a task to an explicit lifecycle [`TaskState`], enforcing the
    /// state-machine's valid-edge rules.
    ///
    /// Returns an error if the task does not exist or the transition is
    /// invalid for its current state. Reaching a terminal state releases the
    /// task's locks.
    pub async fn transition_task(&self, id: &str, target: TaskState) -> Result<()> {
        {
            let mut tasks = self.running_tasks.write().await;
            let task = tasks
                .get_mut(id)
                .ok_or_else(|| DruidError::Metadata(format!("task not found: {id}")))?;
            if task.state == target {
                return Ok(());
            }
            if !task.state.can_transition_to(target) {
                return Err(DruidError::Ingestion(format!(
                    "invalid task transition for {id}: {} -> {}",
                    task.state.as_str(),
                    target.as_str()
                )));
            }
            task.state = target;
            if matches!(target, TaskState::Pending | TaskState::Waiting) {
                task.worker = None;
                task.location = None;
            }
        }
        if target.is_terminal() {
            self.release_task_locks(id).await;
        }
        self.persist_existing(id).await?;
        Ok(())
    }

    /// Persist the current in-memory state of an existing task to metadata.
    async fn persist_existing(&self, id: &str) -> Result<()> {
        let row = {
            let tasks = self.running_tasks.read().await;
            match tasks.get(id) {
                Some(r) => r.to_row()?,
                None => return Ok(()),
            }
        };
        self.metadata.update_task_status(&row).await?;
        Ok(())
    }

    // ----- Worker management -----------------------------------------------

    /// Register (or replace) a worker available for task assignment.
    pub async fn register_worker(&self, worker: Worker) {
        let mut guard = self.workers.lock().await;
        guard.register(worker);
    }

    /// Deregister a worker and reschedule any of its RUNNING tasks.
    ///
    /// Each task currently assigned to the lost worker is moved back to
    /// `PENDING` (re-assignable) if it still has retry budget, otherwise it
    /// transitions to `FAILED`. Returns the list of affected task ids.
    pub async fn lose_worker(&self, worker_id: &str) -> Result<Vec<String>> {
        {
            let mut guard = self.workers.lock().await;
            guard.deregister(worker_id);
        }
        let mut affected = Vec::new();
        let mut to_fail = Vec::new();
        {
            let mut tasks = self.running_tasks.write().await;
            for task in tasks.values_mut() {
                if task.worker.as_deref() == Some(worker_id) && task.state == TaskState::Running {
                    affected.push(task.id.clone());
                    task.worker = None;
                    task.location = None;
                    if self.retry_policy.can_retry(task.attempt) {
                        task.state = TaskState::Pending;
                    } else {
                        task.state = TaskState::Failed;
                        to_fail.push(task.id.clone());
                    }
                }
            }
        }
        for id in &to_fail {
            self.release_task_locks(id).await;
        }
        for id in &affected {
            self.persist_existing(id).await?;
        }
        Ok(affected)
    }

    /// Current number of registered workers.
    pub async fn worker_count(&self) -> usize {
        self.workers.lock().await.len()
    }

    /// Assign a worker to a task using the configured selection strategy.
    ///
    /// Load is computed from the authoritative in-memory assignment table.
    /// Returns the chosen worker id, or `None` if no worker has free capacity
    /// (the task then runs unassigned — in-process execution still proceeds).
    async fn assign_worker(&self, _task_id: &str) -> Option<String> {
        let load_map = {
            let tasks = self.running_tasks.read().await;
            let mut m: HashMap<String, u32> = HashMap::new();
            for t in tasks.values() {
                if t.state == TaskState::Running
                    && let Some(w) = &t.worker
                {
                    *m.entry(w.clone()).or_insert(0) += 1;
                }
            }
            m
        };
        let mut guard = self.workers.lock().await;
        if guard.is_empty() {
            return None;
        }
        let load = |id: &str| *load_map.get(id).unwrap_or(&0);
        guard.select(&load).map(|w| w.id())
    }

    // ----- Task locks ------------------------------------------------------

    /// Request a TimeChunk lock on behalf of a task.
    ///
    /// Returns `Ok(Some(lock))` when granted (after preempting any strictly
    /// lower-priority conflicting locks), or `Ok(None)` when blocked by an
    /// equal-or-higher-priority holder.
    pub async fn acquire_lock(
        &self,
        task_id: &str,
        data_source: &str,
        interval: Interval,
        lock_type: LockType,
        priority: i64,
    ) -> Result<Option<TaskLock>> {
        let req = TaskLock {
            id: String::new(),
            task_id: task_id.to_string(),
            data_source: data_source.to_string(),
            interval,
            lock_type,
            priority,
            revoked: false,
        };
        if self.try_acquire_lock(req.clone()).await? {
            // Re-read to recover the persisted id.
            let locks = self.metadata.get_locks_for_task(task_id).await?;
            let granted = locks
                .into_iter()
                .filter_map(|r| lock_row_to_lock(&r).ok())
                .find(|l| {
                    l.data_source == req.data_source
                        && l.interval == req.interval
                        && l.lock_type == req.lock_type
                        && !l.revoked
                });
            Ok(granted)
        } else {
            Ok(None)
        }
    }

    /// Return the active (non-revoked) locks for a data source.
    pub async fn locks_for_datasource(&self, data_source: &str) -> Result<Vec<TaskLock>> {
        let rows = self
            .metadata
            .get_locks_for_datasource(data_source, false)
            .await?;
        rows.iter().map(lock_row_to_lock).collect()
    }

    /// Core lock acquisition against persisted state. Returns whether the lock
    /// was granted (preempting strictly-lower-priority holders if needed).
    async fn try_acquire_lock(&self, req: TaskLock) -> Result<bool> {
        // Serialize the entire read → evaluate → revoke → insert sequence on
        // a per-datasource mutex. Two concurrent overlapping EXCLUSIVE
        // requests must not both observe an empty active set and both be
        // granted; holding this guard across the whole critical section makes
        // the conflict evaluation see the effect of any concurrent grant.
        let acquire_guard = {
            let mut map = self.lock_acquire_locks.lock().await;
            Arc::clone(
                map.entry(req.data_source.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _serialized = acquire_guard.lock().await;

        let existing_rows = self
            .metadata
            .get_locks_for_datasource(&req.data_source, false)
            .await?;
        let existing: Vec<TaskLock> = existing_rows
            .iter()
            .map(lock_row_to_lock)
            .collect::<Result<Vec<_>>>()?;

        match evaluate_lock_request(&req, &existing) {
            LockDecision::Blocked => Ok(false),
            LockDecision::Preempt(ids) => {
                for id in ids {
                    self.metadata.revoke_lock(&id).await?;
                }
                self.persist_lock(&req).await?;
                Ok(true)
            }
            LockDecision::Granted => {
                self.persist_lock(&req).await?;
                Ok(true)
            }
        }
    }

    async fn persist_lock(&self, lock: &TaskLock) -> Result<()> {
        let row = lock_to_lock_row(lock)?;
        self.metadata.insert_task_lock(&row).await?;
        Ok(())
    }

    /// Release (delete) all locks held by a task.
    async fn release_task_locks(&self, task_id: &str) {
        match self.metadata.get_locks_for_task(task_id).await {
            Ok(rows) => {
                for r in rows {
                    if let Err(e) = self.metadata.delete_lock(&r.id).await {
                        tracing::warn!(task_id, lock_id = %r.id, error = %e, "failed to delete lock");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(task_id, error = %e, "failed to list locks for release");
            }
        }
    }

    /// Parse an optional lock request out of a task spec.
    ///
    /// We look for `spec.spec.ioConfig.intervals` (an array of ISO-8601
    /// interval strings `"start/end"`) and acquire an EXCLUSIVE lock covering
    /// their union at the spec's `priority` (default 0). Absence of intervals
    /// means the task does not need a lock (returns `Ok(None)`).
    fn parse_lock_request(
        &self,
        task_id: &str,
        data_source: &str,
        spec: &serde_json::Value,
    ) -> Result<Option<TaskLock>> {
        let intervals = spec
            .pointer("/spec/ioConfig/intervals")
            .or_else(|| spec.pointer("/ioConfig/intervals"))
            .and_then(|v| v.as_array());
        let Some(intervals) = intervals else {
            return Ok(None);
        };
        if intervals.is_empty() {
            return Ok(None);
        }
        let priority = spec
            .pointer("/spec/tuningConfig/priority")
            .or_else(|| spec.pointer("/priority"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);

        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for iv in intervals {
            // DD R46: a non-string interval entry was silently skipped, so a spec
            // with `intervals: [123]` acquired NO lock and bypassed conflict
            // prevention / priority preemption / the exclusive-lock guard. Reject
            // any non-string entry rather than fail open.
            let Some(s) = iv.as_str() else {
                return Err(DruidError::Ingestion(format!(
                    "ingestion spec ioConfig.intervals entry is not a string: {iv}"
                )));
            };
            let (start, end) = parse_iso_interval(s)?;
            min_start = min_start.min(start);
            max_end = max_end.max(end);
        }
        // A present, non-empty `intervals` that yields no valid interval is a
        // malformed spec — fail closed rather than treat it as "no lock needed".
        if min_start == i64::MAX || max_end == i64::MIN {
            return Err(DruidError::Ingestion(
                "ingestion spec ioConfig.intervals is present but yielded no valid interval"
                    .to_string(),
            ));
        }
        let interval = Interval::new(min_start, max_end)?;
        Ok(Some(TaskLock {
            id: String::new(),
            task_id: task_id.to_string(),
            data_source: data_source.to_string(),
            interval,
            lock_type: LockType::Exclusive,
            priority,
            revoked: false,
        }))
    }

    // ----- Supervisors -----------------------------------------------------

    /// Create (persist) a supervisor spec and return its spec identifier.
    ///
    /// The spec should contain `"id"` at the top level; if absent, a
    /// synthetic identifier is generated.
    pub async fn create_supervisor(&self, spec: serde_json::Value) -> Result<String> {
        let spec_id = spec
            .get("id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let seq = self
                    .task_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                format!("supervisor_{seq}")
            });

        self.metadata.insert_supervisor(&spec_id, &spec).await?;
        Ok(spec_id)
    }

    /// Get the latest supervisor spec by identifier.
    pub async fn get_supervisor(&self, id: &str) -> Result<Option<serde_json::Value>> {
        self.metadata.get_supervisor(id).await
    }

    /// Return all persisted supervisor rows.
    pub async fn get_all_supervisors(&self) -> Result<Vec<SupervisorRow>> {
        self.metadata.get_all_supervisors().await
    }

    /// Shut down a supervisor by removing its latest spec from in-memory
    /// tracking.  The persisted row is retained for audit purposes.
    pub async fn shutdown_supervisor(&self, id: &str) -> Result<()> {
        // Verify it exists.
        let existing = self.metadata.get_supervisor(id).await?;
        if existing.is_none() {
            return Err(DruidError::Metadata(format!("supervisor not found: {id}")));
        }

        // Persist a tombstone entry indicating shutdown.
        let tombstone = serde_json::json!({
            "id": id,
            "suspended": true,
            "shutdownTime": Utc::now().to_rfc3339(),
        });
        self.metadata.insert_supervisor(id, &tombstone).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn setup() -> (Arc<MetadataStore>, Overlord) {
        let store = MetadataStore::new_in_memory().await.expect("create store");
        store.initialize().await.expect("init schema");
        let store = Arc::new(store);
        let overlord = Overlord::new(Arc::clone(&store));
        (store, overlord)
    }

    /// `dimensionsSpec.dimensions` object entries keep their declared type
    /// through spec parsing — previously they were flattened to bare names,
    /// so a `{"type":"double"}` dimension was silently ingested as a STRING
    /// column and aggregations over it returned 0.
    #[test]
    fn parse_spec_preserves_typed_dimensions() {
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "t",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": [
                        "site_id",
                        {"type": "double", "name": "value"}
                    ]},
                    "metricsSpec": []
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"site_id\":\"a\",\"value\":1.5}"
                    }
                }
            }
        });
        let parsed = parse_index_parallel_spec(&spec)
            .expect("parse ok")
            .expect("supported shape");
        assert_eq!(parsed.dimensions.len(), 2);
        assert_eq!(parsed.dimensions[0].name, "site_id");
        assert_eq!(
            parsed.dimensions[0].dim_type,
            ferrodruid_ingest_batch::DimensionType::String
        );
        assert_eq!(parsed.dimensions[1].name, "value");
        assert_eq!(
            parsed.dimensions[1].dim_type,
            ferrodruid_ingest_batch::DimensionType::Double
        );
    }

    #[tokio::test]
    async fn submit_and_get_task() {
        let (_store, overlord) = setup().await;
        let spec = json!({
            "type": "index_kafka",
            "dataSource": "wiki"
        });
        let id = overlord.submit_task(spec).await.expect("submit");
        assert!(id.contains("index_kafka"));
        assert!(id.contains("wiki"));

        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.data_source, "wiki");
    }

    #[tokio::test]
    async fn filter_by_datasource() {
        let (_store, overlord) = setup().await;
        overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit wiki");
        overlord
            .submit_task(json!({"type": "index", "dataSource": "clicks"}))
            .await
            .expect("submit clicks");
        overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit wiki2");

        let wiki_tasks = overlord.get_tasks_by_datasource("wiki").await;
        assert_eq!(wiki_tasks.len(), 2);

        let clicks_tasks = overlord.get_tasks_by_datasource("clicks").await;
        assert_eq!(clicks_tasks.len(), 1);
    }

    #[tokio::test]
    async fn status_transitions() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");

        // Pending -> Running
        overlord
            .update_task_status(&id, TaskStatus::Running)
            .await
            .expect("to running");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.status, TaskStatus::Running);

        // Running -> Success
        overlord
            .update_task_status(&id, TaskStatus::Success)
            .await
            .expect("to success");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.status, TaskStatus::Success);
    }

    #[tokio::test]
    async fn get_running_tasks_filters() {
        let (_store, overlord) = setup().await;
        let id1 = overlord
            .submit_task(json!({"type": "index", "dataSource": "a"}))
            .await
            .expect("submit");
        let _id2 = overlord
            .submit_task(json!({"type": "index", "dataSource": "b"}))
            .await
            .expect("submit");

        // Initially nothing is Running.
        assert!(overlord.get_running_tasks().await.is_empty());

        overlord
            .update_task_status(&id1, TaskStatus::Running)
            .await
            .expect("to running");

        let running = overlord.get_running_tasks().await;
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, id1);
    }

    #[tokio::test]
    async fn update_nonexistent_task_errors() {
        let (_store, overlord) = setup().await;
        let result = overlord
            .update_task_status("no_such_task", TaskStatus::Failed)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_get_shutdown_supervisor() {
        let (_store, overlord) = setup().await;
        let spec = json!({
            "id": "wiki-kafka",
            "type": "kafka",
            "dataSchema": {"dataSource": "wiki"},
            "ioConfig": {"topic": "wiki-events"}
        });

        let spec_id = overlord.create_supervisor(spec).await.expect("create");
        assert_eq!(spec_id, "wiki-kafka");

        let got = overlord
            .get_supervisor("wiki-kafka")
            .await
            .expect("get")
            .expect("some");
        assert_eq!(got["type"], "kafka");

        // List all.
        let all = overlord.get_all_supervisors().await.expect("all");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].spec_id, "wiki-kafka");

        // Shutdown.
        overlord
            .shutdown_supervisor("wiki-kafka")
            .await
            .expect("shutdown");

        // After shutdown, the latest spec is the tombstone.
        let got = overlord
            .get_supervisor("wiki-kafka")
            .await
            .expect("get")
            .expect("some");
        assert_eq!(got["suspended"], true);
    }

    #[tokio::test]
    async fn shutdown_nonexistent_supervisor_errors() {
        let (_store, overlord) = setup().await;
        let result = overlord.shutdown_supervisor("no_such").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_index_parallel_executes_and_loads_segment() {
        // Build a real Historical and a metadata store.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "wiki",
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["page", "language"]},
                    "metricsSpec": [
                        {"type": "count", "name": "count"},
                        {"type": "longSum", "name": "added", "fieldName": "added"}
                    ]
                },
                "ioConfig": {
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"page\":\"Main\",\"language\":\"en\",\"added\":100}\n{\"timestamp\":\"2024-01-01T01:00:00Z\",\"page\":\"Talk\",\"language\":\"en\",\"added\":50}"
                    }
                }
            }
        });

        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "task should complete successfully: {task:?}"
        );

        // Historical should now report a loaded segment for `wiki`.
        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1, "expected 1 segment loaded, got {segs:?}");
        let seg_id = &segs[0];
        assert_eq!(
            historical.segment_datasource(seg_id).as_deref(),
            Some("wiki"),
        );

        // MetadataStore should have the segment row.
        let rows = metadata.get_used_segments("wiki").await.expect("get rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].data_source, "wiki");
    }

    /// The 2026-07-11 rollup bug: `parse_index_parallel_spec` never read
    /// `granularitySpec.rollup` / `queryGranularity`, so the REST
    /// `/druid/indexer/v1/task` path ALWAYS ingested raw un-rolled rows and
    /// `BatchIngester::ingest_with_rollup` was dead code in the server
    /// binary.  A spec with `rollup: true` + `queryGranularity: "hour"`
    /// whose rows share an (hour, dims) key must store MERGED rows with
    /// summed metrics and the merged raw-row count in the `count`-type
    /// metric — exactly what Druid does.
    ///
    /// Pre-fix this test fails: 4 raw rows are stored (`num_rows == 4`,
    /// `cnt == [1, 1, 1, 1]`).
    #[tokio::test]
    async fn submit_index_parallel_rollup_merges_rows() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        // 4 raw rows; rows 1+2 share (hour 00, site_a) so rollup at "hour"
        // merges them: 4 raw -> 3 stored rows.
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": "rollup_ds",
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
                    "inputSource": {
                        "type": "inline",
                        "data": "{\"timestamp\":\"2024-01-01T00:05:00Z\",\"site_id\":\"site_a\",\"value\":10}\n{\"timestamp\":\"2024-01-01T00:35:00Z\",\"site_id\":\"site_a\",\"value\":5}\n{\"timestamp\":\"2024-01-01T00:20:00Z\",\"site_id\":\"site_b\",\"value\":7}\n{\"timestamp\":\"2024-01-01T01:10:00Z\",\"site_id\":\"site_a\",\"value\":3}"
                    }
                }
            }
        });

        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(
            task.status,
            TaskStatus::Success,
            "rollup task should complete successfully: {task:?}"
        );

        let segs = historical.loaded_segments();
        assert_eq!(segs.len(), 1, "expected 1 segment loaded, got {segs:?}");
        let seg = historical
            .get_segment(&segs[0])
            .expect("segment data readable");

        // 4 raw rows roll up to 3 stored rows (site_a hour-00 pair merges).
        assert_eq!(seg.num_rows, 3, "rollup must merge the hour-00 pair");

        // Stored rows sorted by (truncated __time, dims):
        //   (00:00, site_a): cnt=2, value_sum=15
        //   (00:00, site_b): cnt=1, value_sum=7
        //   (01:00, site_a): cnt=1, value_sum=3
        match seg.columns.get("cnt") {
            Some(ferrodruid_segment::column::ColumnData::Long(v)) => {
                assert_eq!(v, &vec![2, 1, 1], "count metric = merged raw-row count");
            }
            other => panic!("cnt column should be Long, got {other:?}"),
        }
        match seg.columns.get("value_sum") {
            Some(ferrodruid_segment::column::ColumnData::Double(v)) => {
                assert_eq!(v, &vec![15.0, 7.0, 3.0], "metric summed within groups");
            }
            other => panic!("value_sum column should be Double, got {other:?}"),
        }
        match seg.columns.get("__time") {
            Some(ferrodruid_segment::column::ColumnData::Long(v)) => {
                // 2024-01-01T00:00:00Z / T01:00:00Z — truncated to the hour.
                assert_eq!(
                    v,
                    &vec![1_704_067_200_000, 1_704_067_200_000, 1_704_070_800_000]
                );
            }
            other => panic!("__time column should be Long, got {other:?}"),
        }
    }

    /// `granularitySpec` parsing is Druid-faithful:
    /// * an absent `rollup` field inside a present spec defaults to true
    ///   (Druid's `UniformGranularitySpec` default);
    /// * grain strings are case-insensitive (the fixture files use
    ///   `"HOUR"` / `"NONE"`);
    /// * the period object form maps `PT1H` -> `hour`;
    /// * an entirely absent `granularitySpec` means Druid's default
    ///   `{rollup: true, queryGranularity: none}`;
    /// * `rollup: false` is honoured.
    #[test]
    fn parse_spec_reads_rollup_and_query_granularity() {
        let base = |gspec: serde_json::Value| {
            let mut data_schema = json!({
                "dataSource": "t",
                "timestampSpec": {"column": "timestamp", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["d"]},
                "metricsSpec": []
            });
            if !gspec.is_null()
                && let Some(obj) = data_schema.as_object_mut()
            {
                obj.insert("granularitySpec".to_string(), gspec);
            }
            json!({
                "type": "index_parallel",
                "spec": {
                    "dataSchema": data_schema,
                    "ioConfig": {
                        "inputSource": {
                            "type": "inline",
                            "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"d\":\"x\"}"
                        }
                    }
                }
            })
        };
        let parse = |gspec: serde_json::Value| {
            parse_index_parallel_spec(&base(gspec))
                .expect("parse ok")
                .expect("supported shape")
        };

        // rollup absent inside a present spec -> true; "HOUR" -> "hour".
        let p = parse(json!({"type": "uniform", "queryGranularity": "HOUR"}));
        assert!(p.rollup, "absent rollup defaults to true (Druid)");
        assert_eq!(p.query_granularity, "hour");

        // Period object form.
        let p = parse(
            json!({"rollup": true, "queryGranularity": {"type": "period", "period": "PT1H"}}),
        );
        assert!(p.rollup);
        assert_eq!(p.query_granularity, "hour");

        // Entirely absent granularitySpec -> Druid's default spec.
        let p = parse(serde_json::Value::Null);
        assert!(p.rollup, "absent granularitySpec means rollup=true (Druid)");
        assert_eq!(p.query_granularity, "none");

        // rollup: false honoured; "NONE" case-insensitive.
        let p = parse(json!({"rollup": false, "queryGranularity": "NONE"}));
        assert!(!p.rollup);

        // Unsupported grain fails closed when rollup is on...
        let err = parse_index_parallel_spec(&base(
            json!({"rollup": true, "queryGranularity": "fifteen_minute"}),
        ))
        .expect_err("unsupported rollup grain must be rejected");
        assert!(format!("{err}").contains("unsupported queryGranularity"));

        // ...but is ignored when rollup is off (the raw path never
        // truncates, so the spec must not fail).
        let p = parse(json!({"rollup": false, "queryGranularity": "fifteen_minute"}));
        assert!(!p.rollup);
    }

    #[tokio::test]
    async fn submit_unknown_input_source_falls_back_to_pending() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));

        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {"inputSource": {"type": "local", "baseDir": "/tmp"}}
            }
        });
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("present");
        assert_eq!(task.status, TaskStatus::Pending);
        assert!(historical.loaded_segments().is_empty());
    }

    #[tokio::test]
    async fn supervisor_auto_id() {
        let (_store, overlord) = setup().await;
        let spec = json!({
            "type": "kafka",
            "dataSchema": {"dataSource": "clicks"}
        });
        let spec_id = overlord.create_supervisor(spec).await.expect("create");
        assert!(spec_id.starts_with("supervisor_"));
    }

    // ----- lifecycle state machine via the Overlord API -----

    #[tokio::test]
    async fn invalid_transition_is_rejected() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        // The stub task is PENDING. PENDING -> SUCCESS is invalid.
        let err = overlord
            .transition_task(&id, TaskState::Success)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("invalid task transition"));

        // PENDING -> RUNNING -> SUCCESS is valid.
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("to running");
        overlord
            .transition_task(&id, TaskState::Success)
            .await
            .expect("to success");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.state, TaskState::Success);
    }

    #[tokio::test]
    async fn legacy_update_status_maps_to_machine() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        // Legacy API: PENDING -> RUNNING valid, RUNNING -> PENDING valid.
        overlord
            .update_task_status(&id, TaskStatus::Running)
            .await
            .expect("running");
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.status, TaskStatus::Running);
        assert_eq!(t.state, TaskState::Running);
    }

    // ----- locks via the Overlord -----

    #[tokio::test]
    async fn overlord_lock_grant_and_conflict() {
        let (_store, overlord) = setup().await;
        let interval = Interval::new(0, 1000).expect("iv");
        let granted = overlord
            .acquire_lock("t1", "wiki", interval, LockType::Exclusive, 5)
            .await
            .expect("acquire");
        assert!(granted.is_some(), "first lock should grant");

        // Overlapping exclusive at equal priority is blocked.
        let blocked = overlord
            .acquire_lock(
                "t2",
                "wiki",
                Interval::new(500, 1500).expect("iv"),
                LockType::Exclusive,
                5,
            )
            .await
            .expect("acquire");
        assert!(blocked.is_none(), "equal-priority overlap is blocked");

        let active = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(active.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_exclusive_lock_grants_at_most_one() {
        // DD R10 #1: without serialization of the read → evaluate → insert
        // sequence, two concurrent overlapping EXCLUSIVE requests can both read
        // an empty active set and both be granted. Fire many concurrent rounds
        // to make the race highly likely to manifest if unserialized.
        for round in 0..32 {
            let (_store, overlord) = setup().await;
            let overlord = Arc::new(overlord);
            let o1 = Arc::clone(&overlord);
            let o2 = Arc::clone(&overlord);
            let iv_a = Interval::new(0, 1000).expect("iv");
            let iv_b = Interval::new(500, 1500).expect("iv");
            let h1 = tokio::spawn(async move {
                o1.acquire_lock("t1", "wiki", iv_a, LockType::Exclusive, 5)
                    .await
            });
            let h2 = tokio::spawn(async move {
                o2.acquire_lock("t2", "wiki", iv_b, LockType::Exclusive, 5)
                    .await
            });
            let (r1, r2) = tokio::join!(h1, h2);
            let g1 = r1.expect("join1").expect("acquire1").is_some();
            let g2 = r2.expect("join2").expect("acquire2").is_some();
            assert!(
                g1 ^ g2,
                "round {round}: exactly one overlapping exclusive lock must be granted (g1={g1}, g2={g2})"
            );
            let active = overlord.locks_for_datasource("wiki").await.expect("locks");
            assert_eq!(
                active.len(),
                1,
                "round {round}: only one active exclusive lock expected"
            );
        }
    }

    #[tokio::test]
    async fn overlord_lock_preemption() {
        let (_store, overlord) = setup().await;
        // Low-priority lock first.
        overlord
            .acquire_lock(
                "low",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Exclusive,
                1,
            )
            .await
            .expect("low")
            .expect("granted");

        // High-priority overlapping lock preempts it.
        let high = overlord
            .acquire_lock(
                "high",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Exclusive,
                9,
            )
            .await
            .expect("high");
        assert!(high.is_some(), "higher priority preempts");

        // Only the high lock is active; the low one is revoked.
        let active = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].task_id, "high");
    }

    #[tokio::test]
    async fn shared_locks_coexist() {
        let (_store, overlord) = setup().await;
        let a = overlord
            .acquire_lock(
                "a",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Shared,
                5,
            )
            .await
            .expect("a");
        let b = overlord
            .acquire_lock(
                "b",
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Shared,
                5,
            )
            .await
            .expect("b");
        assert!(a.is_some() && b.is_some(), "two shared locks coexist");
        let active = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn terminal_releases_locks() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord
            .acquire_lock(
                &id,
                "wiki",
                Interval::new(0, 1000).expect("iv"),
                LockType::Exclusive,
                5,
            )
            .await
            .expect("lock")
            .expect("granted");
        assert_eq!(
            overlord
                .locks_for_datasource("wiki")
                .await
                .expect("l")
                .len(),
            1
        );

        // Drive to a terminal state; the lock must be released.
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        overlord
            .transition_task(&id, TaskState::Failed)
            .await
            .expect("failed");
        assert!(
            overlord
                .locks_for_datasource("wiki")
                .await
                .expect("l")
                .is_empty(),
            "terminal task releases its locks"
        );
    }

    // ----- retry exhaustion -----

    /// A spec whose inline data is malformed JSONL causes ingestion to error
    /// on every attempt, so a small retry budget must end in FAILED with the
    /// attempt counter at the budget.
    #[tokio::test]
    async fn retry_exhaustion_ends_failed() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), historical)
            .with_retry_policy(RetryPolicy {
                max_attempts: 3,
                base_delay_millis: 1,
                max_delay_millis: 1,
            });

        // Empty inline data is a hard ingestion error.
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {"inputSource": {"type": "inline", "data": "   "}}
            }
        });
        let id = overlord.submit_task(spec).await.expect("submit");
        let task = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(task.state, TaskState::Failed);
        assert_eq!(task.attempt, 3, "exhausted the 3-attempt budget");
    }

    // ----- worker assignment + loss -----

    #[tokio::test]
    async fn worker_assignment_and_loss_reassigns() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let (_store, overlord) = (Arc::clone(&metadata), Overlord::new(metadata));
        overlord
            .register_worker(Worker {
                host: "w1".into(),
                port: 8100,
                capacity: 4,
            })
            .await;
        assert_eq!(overlord.worker_count().await, 1);

        // Manually drive a task to RUNNING on the worker via the lifecycle.
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        // Assign the worker explicitly (in-process bookkeeping).
        {
            let mut tasks = overlord.running_tasks.write().await;
            let t = tasks.get_mut(&id).expect("task");
            t.worker = Some("w1:8100".to_string());
        }

        // Lose the worker; the running task is re-queued to PENDING (budget ok).
        let affected = overlord.lose_worker("w1:8100").await.expect("lose");
        assert_eq!(affected, vec![id.clone()]);
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Pending);
        assert!(t.worker.is_none());
        assert_eq!(overlord.worker_count().await, 0);
    }

    #[tokio::test]
    async fn worker_loss_without_budget_fails() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(metadata).with_retry_policy(RetryPolicy {
            max_attempts: 1,
            base_delay_millis: 1,
            max_delay_millis: 1,
        });
        overlord
            .register_worker(Worker {
                host: "w1".into(),
                port: 8100,
                capacity: 1,
            })
            .await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        {
            let mut tasks = overlord.running_tasks.write().await;
            let t = tasks.get_mut(&id).expect("task");
            t.worker = Some("w1:8100".to_string());
            t.attempt = 1; // budget already consumed
        }
        let affected = overlord.lose_worker("w1:8100").await.expect("lose");
        assert_eq!(affected, vec![id.clone()]);
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Failed, "no budget left -> FAILED");
    }

    // ----- persistence round-trip -----

    #[tokio::test]
    async fn task_persisted_to_metadata() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");

        // Persisted as PENDING and visible via active-task query.
        let row = metadata.get_task(&id).await.expect("get").expect("some");
        assert_eq!(row.status, "PENDING");
        assert_eq!(row.data_source, "wiki");
        let active = metadata.get_active_tasks().await.expect("active");
        assert_eq!(active.len(), 1);

        // Transition updates the persisted status.
        overlord
            .transition_task(&id, TaskState::Running)
            .await
            .expect("running");
        let row = metadata.get_task(&id).await.expect("get").expect("some");
        assert_eq!(row.status, "RUNNING");
    }

    #[tokio::test]
    async fn submit_with_intervals_acquires_lock() {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": {
                    "intervals": ["2024-01-01T00:00:00Z/2024-02-01T00:00:00Z"]
                }
            }
        });
        let _id = overlord.submit_task(spec).await.expect("submit");
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(locks.len(), 1, "interval spec should acquire one lock");
        assert_eq!(locks[0].lock_type, LockType::Exclusive);
    }

    #[tokio::test]
    async fn submit_with_date_only_interval_acquires_lock() {
        // DD R47: bare YYYY-MM-DD interval bounds (common in batch ingestion
        // specs) must acquire a lock, not be rejected by an RFC3339-only parser.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": { "intervals": ["2024-01-01/2024-02-01"] }
            }
        });
        overlord
            .submit_task(spec)
            .await
            .expect("submit date-only interval");
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert_eq!(
            locks.len(),
            1,
            "date-only interval spec should acquire a lock"
        );
    }

    #[tokio::test]
    async fn submit_with_non_string_interval_fails_closed() {
        // DD R46: a non-string ioConfig.intervals entry previously was silently
        // skipped, so the task acquired NO interval lock (bypassing conflict
        // prevention). It must now fail closed.
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let overlord = Overlord::new(Arc::clone(&metadata));
        let spec = json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {"dataSource": "wiki"},
                "ioConfig": { "intervals": [123] }
            }
        });
        assert!(
            overlord.submit_task(spec).await.is_err(),
            "a non-string interval must be rejected, not run without a lock"
        );
        // No lock leaked from the rejected submit.
        let locks = overlord.locks_for_datasource("wiki").await.expect("locks");
        assert!(locks.is_empty(), "rejected submit must not acquire a lock");
    }

    // ----- appendToExisting replace/append semantics (P1-#1, 2026-07-12) -----

    /// `parse_index_parallel_spec` reads `ioConfig.appendToExisting` with
    /// Druid's native-batch default of FALSE, and fails closed on
    /// non-boolean values.
    #[test]
    fn parse_spec_reads_append_to_existing() {
        let base = |io_extra: serde_json::Value| {
            let mut io_config = json!({
                "inputSource": {
                    "type": "inline",
                    "data": "{\"timestamp\":\"2024-01-01T00:00:00Z\",\"d\":\"x\"}"
                }
            });
            if let (Some(obj), Some(extra)) = (io_config.as_object_mut(), io_extra.as_object()) {
                for (k, v) in extra {
                    obj.insert(k.clone(), v.clone());
                }
            }
            json!({
                "type": "index_parallel",
                "spec": {
                    "dataSchema": {
                        "dataSource": "t",
                        "timestampSpec": {"column": "timestamp", "format": "iso"},
                        "dimensionsSpec": {"dimensions": ["d"]},
                        "metricsSpec": []
                    },
                    "ioConfig": io_config
                }
            })
        };

        // Absent -> false (Druid's ioConfig default for native batch).
        let p = parse_index_parallel_spec(&base(json!({})))
            .expect("parse")
            .expect("supported");
        assert!(!p.append_to_existing, "absent appendToExisting -> false");

        // Explicit true / false honored.
        let p = parse_index_parallel_spec(&base(json!({"appendToExisting": true})))
            .expect("parse")
            .expect("supported");
        assert!(p.append_to_existing);
        let p = parse_index_parallel_spec(&base(json!({"appendToExisting": false})))
            .expect("parse")
            .expect("supported");
        assert!(!p.append_to_existing);

        // Non-boolean fails closed.
        let err = parse_index_parallel_spec(&base(json!({"appendToExisting": "yes"})))
            .expect_err("non-bool appendToExisting must be rejected");
        assert!(format!("{err}").contains("appendToExisting"));
    }

    /// Segment-granularity parsing + replace-scope bucket math.
    #[test]
    fn segment_granularity_replace_scope_math() {
        let day = MILLIS_PER_DAY;
        // 2026-01-01T00:00:01Z .. 2026-01-01T00:00:08Z
        let t1 = 1_767_225_601_000;
        let t8 = 1_767_225_608_000;
        let day_start = 1_767_225_600_000; // 2026-01-01T00:00:00Z

        // Default (absent) -> DAY buckets.
        let g = parse_segment_granularity(None).expect("default");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (day_start, day_start + day),
            "absent segmentGranularity defaults to DAY"
        );

        // Case-insensitive string + period-object forms.
        let g = parse_segment_granularity(Some(&json!("DAY"))).expect("DAY");
        assert_eq!(replace_scope(g, t1, t8).expect("scope").0, day_start);
        let g = parse_segment_granularity(Some(&json!({"type": "period", "period": "P1D"})))
            .expect("P1D");
        assert_eq!(replace_scope(g, t1, t8).expect("scope").0, day_start);

        // HOUR: data crossing an hour boundary widens the scope.
        let g = parse_segment_granularity(Some(&json!("hour"))).expect("hour");
        let t_0130 = day_start + 5_400_000; // 01:30
        assert_eq!(
            replace_scope(g, t1, t_0130).expect("scope"),
            (day_start, day_start + 7_200_000),
            "00:00:01..01:30 covers the 00:00 and 01:00 hour buckets"
        );

        // WEEK: 2026-01-01 is a Thursday; its ISO week starts Mon 2025-12-29.
        let g = parse_segment_granularity(Some(&json!("WEEK"))).expect("week");
        let monday = day_start - 3 * day; // 2025-12-29T00:00:00Z
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (monday, monday + 7 * day)
        );

        // MONTH / QUARTER / YEAR calendar buckets.
        let feb_start = 1_769_904_000_000; // 2026-02-01T00:00:00Z
        let g = parse_segment_granularity(Some(&json!("MONTH"))).expect("month");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (day_start, feb_start)
        );
        let apr_start = 1_775_001_600_000; // 2026-04-01T00:00:00Z
        let g = parse_segment_granularity(Some(&json!("QUARTER"))).expect("quarter");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (day_start, apr_start)
        );
        let y2027 = 1_798_761_600_000; // 2027-01-01T00:00:00Z
        let g = parse_segment_granularity(Some(&json!("YEAR"))).expect("year");
        assert_eq!(replace_scope(g, t1, t8).expect("scope"), (day_start, y2027));

        // ALL covers everything.
        let g = parse_segment_granularity(Some(&json!("ALL"))).expect("all");
        assert_eq!(
            replace_scope(g, t1, t8).expect("scope"),
            (i64::MIN, i64::MAX)
        );

        // Unsupported shapes fail closed.
        assert!(parse_segment_granularity(Some(&json!("fortnight"))).is_err());
        assert!(parse_segment_granularity(Some(&json!(42))).is_err());
        assert!(
            parse_segment_granularity(Some(&json!({"type": "period", "period": "P2D"}))).is_err()
        );
    }

    /// Build an executor-backed Overlord plus its Historical + metadata.
    async fn setup_executor() -> (
        Arc<MetadataStore>,
        Arc<ferrodruid_historical::Historical>,
        Overlord,
        tempfile::TempDir,
    ) {
        let metadata = Arc::new(MetadataStore::new_in_memory().await.expect("create"));
        metadata.initialize().await.expect("init");
        let cache_dir = tempfile::tempdir().expect("tempdir");
        let historical = Arc::new(ferrodruid_historical::Historical::new(
            cache_dir.path().to_path_buf(),
            10_000_000,
        ));
        let overlord = Overlord::with_executor(Arc::clone(&metadata), Arc::clone(&historical));
        (metadata, historical, overlord, cache_dir)
    }

    /// An `index_parallel` spec shaped like the 2026-07-12 audit fixtures:
    /// DAY segment granularity, rollup off, explicit `appendToExisting`.
    fn batch_spec(
        ds: &str,
        append_to_existing: serde_json::Value,
        data: &str,
    ) -> serde_json::Value {
        let mut io_config = json!({
            "type": "index_parallel",
            "inputSource": {"type": "inline", "data": data},
            "inputFormat": {"type": "json"}
        });
        if !append_to_existing.is_null()
            && let Some(obj) = io_config.as_object_mut()
        {
            obj.insert("appendToExisting".to_string(), append_to_existing);
        }
        json!({
            "type": "index_parallel",
            "spec": {
                "dataSchema": {
                    "dataSource": ds,
                    "timestampSpec": {"column": "timestamp", "format": "iso"},
                    "dimensionsSpec": {"dimensions": ["grp", "uid"]},
                    "metricsSpec": [{"type": "longSum", "name": "a", "fieldName": "a"}],
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": "DAY",
                        "queryGranularity": "NONE",
                        "rollup": false
                    }
                },
                "ioConfig": io_config
            }
        })
    }

    /// 4 rows at 2026-01-01T00:00:01..04Z (the audit's specA data).
    fn rows_first() -> &'static str {
        "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":10}\n\
         {\"timestamp\":\"2026-01-01T00:00:02Z\",\"grp\":\"x\",\"uid\":\"u2\",\"a\":20}\n\
         {\"timestamp\":\"2026-01-01T00:00:03Z\",\"grp\":\"y\",\"uid\":\"u1\",\"a\":30}\n\
         {\"timestamp\":\"2026-01-01T00:00:04Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":40}"
    }

    /// 4 rows at 2026-01-01T00:00:05..08Z — same DAY bucket as
    /// [`rows_first`] but a disjoint raw-millisecond range (the audit's
    /// specB data), so a Druid-faithful replace must be bucket-scoped,
    /// not raw-data-interval-scoped.
    fn rows_second() -> &'static str {
        "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"grp\":\"x\",\"uid\":\"u2\",\"a\":100}\n\
         {\"timestamp\":\"2026-01-01T00:00:06Z\",\"grp\":\"x\",\"uid\":\"u3\",\"a\":200}\n\
         {\"timestamp\":\"2026-01-01T00:00:07Z\",\"grp\":\"y\",\"uid\":\"u4\",\"a\":300}\n\
         {\"timestamp\":\"2026-01-01T00:00:08Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":400}"
    }

    /// Run a `count` timeseries over the full time range through the real
    /// query path ([`Historical::execute_query`]) and sum across segments —
    /// this is what a SQL `COUNT(*)` ultimately observes, so it proves
    /// replaced rows are gone from query results, not just from metadata.
    fn queried_row_count(historical: &ferrodruid_historical::Historical, ds: &str) -> i64 {
        let query: ferrodruid_query::DruidQuery = serde_json::from_value(json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": ds},
            "intervals": ["2000-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z"],
            "granularity": "all",
            "aggregations": [{"type": "count", "name": "cnt"}]
        }))
        .expect("build count query");
        let results = historical
            .execute_query(&query)
            .expect("execute count query");
        results
            .iter()
            .map(|r| match r {
                ferrodruid_query::QueryResult::Timeseries(ts) => ts
                    .iter()
                    .map(|row| {
                        row.result
                            .get("cnt")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0)
                    })
                    .sum::<i64>(),
                _ => 0,
            })
            .sum()
    }

    /// P1-#1 (2026-07-12): re-ingesting an interval with
    /// `appendToExisting: false` (Druid's batch default) must REPLACE the
    /// interval's existing segments, not silently duplicate them.
    ///
    /// Measured live against Druid 36: two same-DAY-bucket tasks with
    /// `appendToExisting: false` leave COUNT(*) = 4 (second task's rows
    /// only); pre-fix FerroDruid kept 8 rows across 2 coexisting segments.
    ///
    /// The second task's rows live at 00:00:05..08Z while the first task's
    /// are at 00:00:01..04Z — disjoint raw ranges inside the same DAY
    /// bucket — so this also locks in segmentGranularity-bucket scoping.
    #[tokio::test]
    async fn replace_semantics_same_day_bucket_overwrites() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        let id1 = overlord
            .submit_task(batch_spec("replace_ds", json!(false), rows_first()))
            .await
            .expect("submit first");
        let t1 = overlord.get_task(&id1).await.expect("get").expect("some");
        assert_eq!(t1.status, TaskStatus::Success, "first task: {t1:?}");
        assert_eq!(queried_row_count(&historical, "replace_ds"), 4);

        let id2 = overlord
            .submit_task(batch_spec("replace_ds", json!(false), rows_second()))
            .await
            .expect("submit second");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(t2.status, TaskStatus::Success, "second task: {t2:?}");

        // Druid semantics: the second appendToExisting:false task replaces
        // the DAY bucket — 4 rows, 1 segment, and only the new rows.
        assert_eq!(
            queried_row_count(&historical, "replace_ds"),
            4,
            "replace must not double-count: expected 4 rows (second task only)"
        );
        assert_eq!(
            historical.segment_count(),
            1,
            "old segment must be dropped from the Historical"
        );

        // Metadata: exactly one used segment; the old one is retained as
        // used=false (Druid keeps replaced segments as unused rows).
        let used = metadata
            .get_used_segments("replace_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1, "exactly one used segment after replace");
        let all = metadata.get_all_segments().await.expect("all segments");
        assert_eq!(all.len(), 2, "replaced segment row is kept (unused)");
        assert_eq!(
            all.iter().filter(|s| !s.used).count(),
            1,
            "exactly one unused (replaced) segment row"
        );

        // The surviving segment holds the SECOND task's rows (sum(a)=1000).
        let seg = historical
            .get_segment(&historical.loaded_segments()[0])
            .expect("segment data");
        match seg.columns.get("a") {
            Some(ferrodruid_segment::column::ColumnData::Double(v)) => {
                assert_eq!(
                    v.iter().sum::<f64>(),
                    1000.0,
                    "second task's rows survive (100+200+300+400)"
                );
            }
            other => panic!("metric column 'a' should be Double, got {other:?}"),
        }
    }

    /// `appendToExisting: true` keeps the pre-existing behavior: both
    /// segments coexist and COUNT doubles (Druid parity: 8 rows).
    #[tokio::test]
    async fn append_true_keeps_both_segments() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        overlord
            .submit_task(batch_spec("append_ds", json!(false), rows_first()))
            .await
            .expect("submit first");
        overlord
            .submit_task(batch_spec("append_ds", json!(true), rows_second()))
            .await
            .expect("submit second");

        assert_eq!(queried_row_count(&historical, "append_ds"), 8);
        assert_eq!(historical.segment_count(), 2);
        let used = metadata
            .get_used_segments("append_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 2, "append keeps both segments used");
    }

    /// Replace is scoped to the overlapping segmentGranularity buckets:
    /// an `appendToExisting: false` task for a DIFFERENT day must not
    /// touch the first day's segment.
    #[tokio::test]
    async fn replace_disjoint_day_buckets_keeps_both() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        overlord
            .submit_task(batch_spec("days_ds", json!(false), rows_first()))
            .await
            .expect("submit day 1");
        let day2 = "{\"timestamp\":\"2026-01-02T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-02T00:00:02Z\",\"grp\":\"y\",\"uid\":\"u2\",\"a\":2}";
        overlord
            .submit_task(batch_spec("days_ds", json!(false), day2))
            .await
            .expect("submit day 2");

        assert_eq!(
            queried_row_count(&historical, "days_ds"),
            6,
            "disjoint DAY buckets must both survive an appendToExisting:false task"
        );
        assert_eq!(historical.segment_count(), 2);
        assert_eq!(
            metadata
                .get_used_segments("days_ds")
                .await
                .expect("used")
                .len(),
            2
        );
    }

    /// Replace is scoped per datasource: an `appendToExisting: false` task
    /// for datasource B must never drop datasource A's segments even when
    /// the intervals coincide exactly.
    #[tokio::test]
    async fn replace_does_not_cross_datasources() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        overlord
            .submit_task(batch_spec("ds_a", json!(false), rows_first()))
            .await
            .expect("submit ds_a");
        overlord
            .submit_task(batch_spec("ds_b", json!(false), rows_first()))
            .await
            .expect("submit ds_b");

        assert_eq!(queried_row_count(&historical, "ds_a"), 4);
        assert_eq!(queried_row_count(&historical, "ds_b"), 4);
        assert_eq!(historical.segment_count(), 2);
        assert_eq!(
            metadata.get_used_segments("ds_a").await.expect("a").len(),
            1
        );
        assert_eq!(
            metadata.get_used_segments("ds_b").await.expect("b").len(),
            1
        );
    }

    /// An absent `appendToExisting` defaults to FALSE (Druid's native-batch
    /// `ioConfig` default) — re-submitting the same-bucket spec without the
    /// field must replace, not append.
    #[tokio::test]
    async fn append_to_existing_defaults_to_false() {
        let (_metadata, historical, overlord, _dir) = setup_executor().await;

        overlord
            .submit_task(batch_spec(
                "default_ds",
                serde_json::Value::Null,
                rows_first(),
            ))
            .await
            .expect("submit first");
        overlord
            .submit_task(batch_spec(
                "default_ds",
                serde_json::Value::Null,
                rows_second(),
            ))
            .await
            .expect("submit second");

        assert_eq!(
            queried_row_count(&historical, "default_ds"),
            4,
            "absent appendToExisting must default to false (replace)"
        );
        assert_eq!(historical.segment_count(), 1);
    }

    /// A non-boolean `appendToExisting` fails closed instead of being
    /// silently coerced.
    #[tokio::test]
    async fn append_to_existing_non_bool_fails_closed() {
        let (_metadata, historical, overlord, _dir) = setup_executor().await;
        let id = overlord
            .submit_task(batch_spec("bad_ds", json!("yes"), rows_first()))
            .await
            .expect("submit records the task");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "non-boolean appendToExisting must fail the task, got {t:?}"
        );
        assert!(historical.loaded_segments().is_empty());
    }

    #[tokio::test]
    async fn shutdown_idempotent_on_terminal() {
        let (_store, overlord) = setup().await;
        let id = overlord
            .submit_task(json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");
        overlord.shutdown_task(&id).await.expect("shutdown");
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Failed);
        // Second shutdown is a no-op (still found, still FAILED).
        overlord.shutdown_task(&id).await.expect("shutdown again");
        let t = overlord.get_task(&id).await.expect("g").expect("s");
        assert_eq!(t.state, TaskState::Failed);
    }

    /// Codex 2026-07-12 HIGH #1: the ingester publishes ONE raw
    /// `[min, max+1)` segment even when its rows span multiple
    /// segmentGranularity buckets, so an existing segment can extend
    /// OUTSIDE a later task's replace scope. Pre-fix, such a victim was
    /// dropped WHOLESALE: replacing only Jan-1 silently deleted the
    /// victim's Jan-2 rows. The fix fails the task CLOSED and drops
    /// nothing.
    #[tokio::test]
    async fn replace_partial_overlap_fails_closed_and_preserves_data() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // One task whose rows span TWO DAY buckets -> one raw segment
        // [2026-01-01T00:00:01, 2026-01-02T00:00:02.001) crossing the
        // bucket boundary.
        let two_day_rows = "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":10}\n\
             {\"timestamp\":\"2026-01-01T00:00:02Z\",\"grp\":\"x\",\"uid\":\"u2\",\"a\":20}\n\
             {\"timestamp\":\"2026-01-02T00:00:01Z\",\"grp\":\"y\",\"uid\":\"u3\",\"a\":30}\n\
             {\"timestamp\":\"2026-01-02T00:00:02Z\",\"grp\":\"y\",\"uid\":\"u4\",\"a\":40}";
        let id1 = overlord
            .submit_task(batch_spec("span_ds", json!(false), two_day_rows))
            .await
            .expect("submit spanning ingest");
        let t1 = overlord.get_task(&id1).await.expect("get").expect("some");
        assert_eq!(t1.status, TaskStatus::Success, "first task: {t1:?}");
        assert_eq!(queried_row_count(&historical, "span_ds"), 4);
        let original_used = metadata
            .get_used_segments("span_ds")
            .await
            .expect("used segments");
        assert_eq!(original_used.len(), 1);
        let original_id = original_used[0].id.clone();

        // Replace ONLY Jan-1: the scope is [Jan-1, Jan-2) but the existing
        // segment extends into Jan-2 — a partial overlap.
        let jan1_only = "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"grp\":\"x\",\"uid\":\"u5\",\"a\":100}\n\
             {\"timestamp\":\"2026-01-01T00:00:06Z\",\"grp\":\"x\",\"uid\":\"u6\",\"a\":200}\n\
             {\"timestamp\":\"2026-01-01T00:00:07Z\",\"grp\":\"y\",\"uid\":\"u7\",\"a\":300}";
        let id2 = overlord
            .submit_task(batch_spec("span_ds", json!(false), jan1_only))
            .await
            .expect("submission itself is recorded");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(
            t2.state,
            TaskState::Failed,
            "a partial-overlap replace must fail closed, got {t2:?}"
        );

        // Nothing was dropped: all 4 original rows — including the
        // out-of-scope Jan-2 rows — are still queryable, the segment is
        // still loaded, and its metadata row is still used.
        assert_eq!(
            queried_row_count(&historical, "span_ds"),
            4,
            "out-of-scope rows must survive a failed partial-overlap replace"
        );
        assert_eq!(historical.segment_count(), 1);
        let used = metadata
            .get_used_segments("span_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), 1, "victim must stay used after the abort");
        assert_eq!(used[0].id, original_id);

        // Remediation: re-ingesting data covering the FULL spanned interval
        // replaces cleanly (the victim is then fully contained in scope).
        let full_span = "{\"timestamp\":\"2026-01-01T00:00:05Z\",\"grp\":\"x\",\"uid\":\"u5\",\"a\":100}\n\
             {\"timestamp\":\"2026-01-02T00:00:05Z\",\"grp\":\"y\",\"uid\":\"u6\",\"a\":200}";
        let id3 = overlord
            .submit_task(batch_spec("span_ds", json!(false), full_span))
            .await
            .expect("submit full-span replace");
        let t3 = overlord.get_task(&id3).await.expect("get").expect("some");
        assert_eq!(t3.status, TaskStatus::Success, "full-span replace: {t3:?}");
        assert_eq!(queried_row_count(&historical, "span_ds"), 2);
        assert_eq!(historical.segment_count(), 1);
    }

    /// Codex 2026-07-12 HIGH #2: pre-fix the publication sequence was
    /// drop victims -> mark unused -> load new -> insert metadata row, so
    /// an `insert_segment` failure left the new segment query-visible but
    /// unregistered (an orphan) and the victims dropped + unused (a lost
    /// interval). The fix writes metadata first and rolls it back on
    /// failure; the query-visible state is never touched on the failure
    /// path.
    #[tokio::test]
    async fn metadata_insert_failure_rolls_back_without_orphan() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // Two coexisting segments in the same DAY bucket -> the replace
        // below has TWO victims.
        overlord
            .submit_task(batch_spec("rb_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");
        overlord
            .submit_task(batch_spec("rb_ds", json!(true), rows_second()))
            .await
            .expect("append ingest");
        assert_eq!(queried_row_count(&historical, "rb_ds"), 8);
        let victims_before: std::collections::HashSet<String> = metadata
            .get_used_segments("rb_ds")
            .await
            .expect("used segments")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(victims_before.len(), 2);

        // Inject: every new-segment metadata insert fails (sticky across
        // the task's retry attempts).
        overlord
            .inject_insert_segment_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let replacement = "{\"timestamp\":\"2026-01-01T00:00:09Z\",\"grp\":\"z\",\"uid\":\"u9\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-01T00:00:10Z\",\"grp\":\"z\",\"uid\":\"u10\",\"a\":2}";
        let id = overlord
            .submit_task(batch_spec("rb_ds", json!(false), replacement))
            .await
            .expect("submission itself is recorded");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(
            t.state,
            TaskState::Failed,
            "task must fail when segment registration fails, got {t:?}"
        );

        // No query-visible orphan and no lost interval: the historical
        // still serves exactly the ORIGINAL 8 rows from 2 segments, and
        // the victims' metadata rows are restored to used=true.
        assert_eq!(
            queried_row_count(&historical, "rb_ds"),
            8,
            "a failed publication must not lose the replaced interval or \
             expose an unregistered orphan segment"
        );
        assert_eq!(historical.segment_count(), 2);
        let used_after: std::collections::HashSet<String> = metadata
            .get_used_segments("rb_ds")
            .await
            .expect("used segments")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(
            used_after, victims_before,
            "victims must be restored to used=true after the rollback"
        );

        // Recovery: clear the fault and re-submit — the replace completes
        // cleanly, proving the rollback left no poisoned state behind.
        overlord
            .inject_insert_segment_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let id2 = overlord
            .submit_task(batch_spec("rb_ds", json!(false), replacement))
            .await
            .expect("resubmit");
        let t2 = overlord.get_task(&id2).await.expect("get").expect("some");
        assert_eq!(t2.status, TaskStatus::Success, "recovery replace: {t2:?}");
        assert_eq!(queried_row_count(&historical, "rb_ds"), 2);
        assert_eq!(historical.segment_count(), 1);
        assert_eq!(
            metadata
                .get_used_segments("rb_ds")
                .await
                .expect("used segments")
                .len(),
            1
        );
    }

    /// Codex 2026-07-12 HIGH #3: a multi-victim replace must be atomic to
    /// concurrent queries. Pre-fix, victims were dropped one lock
    /// acquisition at a time (with awaits between) before the replacement
    /// loaded, so a concurrent count could observe only SOME of the old
    /// segments. With `Historical::replace_segments` the whole swap
    /// happens under one write-lock acquisition, and `execute_query`
    /// holds the read lock for its entire run — so a reader hammering
    /// counts across repeated append+replace cycles may only ever observe
    /// the full pre-state (7 rows: 4 + 3 appended) or the full post-state
    /// (4 rows), never a partial mix (0, 3, 8, 11, ...).
    #[tokio::test]
    async fn multi_victim_replace_never_shows_partial_state_to_queries() {
        let (_metadata, historical, overlord, _dir) = setup_executor().await;

        // Seed: 4 rows -> count 4.
        overlord
            .submit_task(batch_spec("atomic_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let violations = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
        let reader = {
            let hist = Arc::clone(&historical);
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let n = queried_row_count(&hist, "atomic_ds");
                    if n != 4 && n != 7 {
                        violations.lock().expect("violations lock").push(n);
                    }
                }
            })
        };

        // 3 rows in the same DAY bucket: each append makes the datasource
        // a two-victim (4-row + 3-row) target for the following replace.
        let append3 = "{\"timestamp\":\"2026-01-01T00:01:00Z\",\"grp\":\"p\",\"uid\":\"v1\",\"a\":1}\n\
             {\"timestamp\":\"2026-01-01T00:01:01Z\",\"grp\":\"p\",\"uid\":\"v2\",\"a\":2}\n\
             {\"timestamp\":\"2026-01-01T00:01:02Z\",\"grp\":\"p\",\"uid\":\"v3\",\"a\":3}";
        for _ in 0..15 {
            overlord
                .submit_task(batch_spec("atomic_ds", json!(true), append3))
                .await
                .expect("append");
            overlord
                .submit_task(batch_spec("atomic_ds", json!(false), rows_second()))
                .await
                .expect("replace");
        }
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        reader.join().expect("reader thread");
        let seen = violations.lock().expect("violations lock").clone();
        assert!(
            seen.is_empty(),
            "concurrent queries observed partial replace states: {seen:?}"
        );
        assert_eq!(queried_row_count(&historical, "atomic_ds"), 4);
    }

    /// Codex 2026-07-12 round-2 HIGH #2/#3: the publication critical
    /// section must serialize on the SHARED store-level datasource publish
    /// lock — the same mutex the Coordinator's `disable_segment` /
    /// `disable_datasource` take. While that lock is held (here directly;
    /// in production by an in-flight admin mutation) a publish must BLOCK
    /// rather than interleave. Pre-fix the Overlord used a private lock
    /// map, so this publish would complete while the store lock was held
    /// and the timeout assertion below would fail.
    #[tokio::test]
    async fn publish_blocks_while_shared_datasource_lock_held() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;
        let overlord = Arc::new(overlord);

        let lock = metadata.datasource_publish_lock("lk_ds").await;
        let guard = lock.lock().await;

        let mut publishing = {
            let overlord = Arc::clone(&overlord);
            tokio::spawn(async move {
                overlord
                    .submit_task(batch_spec("lk_ds", json!(false), rows_first()))
                    .await
            })
        };

        // Deterministic: a held tokio::Mutex can never be acquired, so the
        // publish cannot complete within the timeout.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), &mut publishing)
                .await
                .is_err(),
            "a publish must block while the datasource's shared publish lock is held"
        );
        assert_eq!(
            historical.segment_count(),
            0,
            "nothing may become query-visible while the lock is held"
        );
        assert!(
            metadata
                .get_used_segments("lk_ds")
                .await
                .expect("used")
                .is_empty(),
            "no metadata row may be written while the lock is held"
        );

        // Release the lock: the publish completes normally.
        drop(guard);
        let id = publishing
            .await
            .expect("join publish task")
            .expect("publish succeeds after the lock is released");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(t.status, TaskStatus::Success, "task: {t:?}");
        assert_eq!(queried_row_count(&historical, "lk_ds"), 4);
    }

    /// Codex 2026-07-12 round-2 HIGH #4 (deterministic unit): segment-id
    /// allocation must skip ids taken by ANY metadata row (used or
    /// unused) or any loaded segment, appending a numeric suffix. Pre-fix
    /// the id was `format!`ed unconditionally, so both same-millisecond
    /// tasks got the same id and one silently overwrote the other.
    #[tokio::test]
    async fn allocate_segment_id_uniquifies_on_collision() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        let base = "u_ds_2026-01-01T00:00:00.000Z_2026-01-02T00:00:00.000Z_v1";
        let alloc = || {
            overlord.allocate_segment_id(
                &historical,
                "u_ds",
                "2026-01-01T00:00:00.000Z",
                "2026-01-02T00:00:00.000Z",
                "v1",
            )
        };

        // Free -> base id.
        assert_eq!(alloc().await.expect("alloc"), base);

        // Base taken by a USED metadata row -> first suffix.
        let mut row = SegmentMetadataRow {
            id: base.to_string(),
            data_source: "u_ds".to_string(),
            created_date: "2026-01-01T00:00:00Z".to_string(),
            start: "2026-01-01T00:00:00.000Z".to_string(),
            end: "2026-01-02T00:00:00.000Z".to_string(),
            version: "v1".to_string(),
            used: true,
            payload: json!({}),
        };
        metadata.insert_segment(&row).await.expect("seed base");
        assert_eq!(alloc().await.expect("alloc"), format!("{base}_1"));

        // An UNUSED row still occupies its id (INSERT would clobber it).
        row.id = format!("{base}_1");
        row.used = false;
        metadata.insert_segment(&row).await.expect("seed _1");
        assert_eq!(alloc().await.expect("alloc"), format!("{base}_2"));

        // A segment loaded in the Historical without a metadata row (e.g.
        // mid-publish of another task) is also skipped.
        historical
            .load_segment(&format!("{base}_2"), {
                // Any segment data works; reuse the ingester for brevity.
                let ingester = ferrodruid_ingest_batch::BatchIngester::new(
                    "u_ds".to_string(),
                    "timestamp".to_string(),
                    vec!["grp".to_string()],
                    vec![],
                );
                ingester
                    .ingest(vec![json!({
                        "timestamp": "2026-01-01T00:00:00Z",
                        "grp": "x"
                    })])
                    .expect("ingest")
                    .segment_data
            })
            .expect("load");
        assert_eq!(alloc().await.expect("alloc"), format!("{base}_3"));
    }

    /// Codex 2026-07-12 round-2 HIGH #4 (end-to-end): rapid appends over
    /// the same interval — many of which land in the same millisecond and
    /// pre-fix produced COLLIDING segment ids whose rows were silently
    /// discarded — must all survive with distinct ids.
    #[tokio::test]
    async fn rapid_same_interval_appends_lose_no_rows() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        const N: usize = 12;
        let one_row =
            "{\"timestamp\":\"2026-01-01T00:00:01Z\",\"grp\":\"x\",\"uid\":\"u1\",\"a\":1}";
        for _ in 0..N {
            let id = overlord
                .submit_task(batch_spec("rapid_ds", json!(true), one_row))
                .await
                .expect("submit append");
            let t = overlord.get_task(&id).await.expect("get").expect("some");
            assert_eq!(t.status, TaskStatus::Success, "append task: {t:?}");
        }

        assert_eq!(
            queried_row_count(&historical, "rapid_ds"),
            N as i64,
            "every appended row must survive (no same-millisecond id collision loss)"
        );
        assert_eq!(historical.segment_count(), N);
        let used = metadata
            .get_used_segments("rapid_ds")
            .await
            .expect("used segments");
        assert_eq!(used.len(), N);
        let distinct: std::collections::HashSet<&str> =
            used.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(distinct.len(), N, "all segment ids must be distinct");
    }

    /// Codex 2026-07-12 round-2 HIGH #2 (disposition lock-in): an
    /// admin-disabled segment must never be resurrected by a failing
    /// publish's rollback. With the shared publish lock the disable can
    /// only land fully BEFORE the publish critical section (the disabled
    /// segment is then not read as a victim, hence not restored) or fully
    /// AFTER it — the mid-publish interleaving that caused resurrection
    /// is structurally impossible, which the blocking tests above pin
    /// down. This test locks in the before-ordering end state.
    #[tokio::test]
    async fn disabled_segment_not_resurrected_by_failing_publish_rollback() {
        let (metadata, historical, overlord, _dir) = setup_executor().await;

        // Two coexisting segments in the same DAY bucket.
        overlord
            .submit_task(batch_spec("dis_ds", json!(false), rows_first()))
            .await
            .expect("seed ingest");
        overlord
            .submit_task(batch_spec("dis_ds", json!(true), rows_second()))
            .await
            .expect("append ingest");
        let used = metadata.get_used_segments("dis_ds").await.expect("used");
        assert_eq!(used.len(), 2);
        let disabled_id = used[0].id.clone();
        let surviving_id = used[1].id.clone();

        // Admin disables one segment (serialized via the shared lock —
        // this is the Coordinator's disable_segment path).
        {
            let lock = metadata.datasource_publish_lock("dis_ds").await;
            let _guard = lock.lock().await;
            metadata
                .mark_segment_unused(&disabled_id)
                .await
                .expect("disable");
        }

        // A replace whose metadata transaction fails: its rollback must
        // NOT touch the admin-disabled row (it was never a victim).
        overlord
            .inject_insert_segment_failure
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let id = overlord
            .submit_task(batch_spec("dis_ds", json!(false), rows_second()))
            .await
            .expect("submission recorded");
        let t = overlord.get_task(&id).await.expect("get").expect("some");
        assert_eq!(t.state, TaskState::Failed, "task: {t:?}");
        overlord
            .inject_insert_segment_failure
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let used_after: Vec<String> = metadata
            .get_used_segments("dis_ds")
            .await
            .expect("used")
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert!(
            !used_after.contains(&disabled_id),
            "the admin-disabled segment must NOT be resurrected by the \
             failing publish's rollback, used set: {used_after:?}"
        );
        assert_eq!(
            used_after,
            vec![surviving_id],
            "only the untouched victim remains used"
        );
        assert_eq!(historical.segment_count(), 2, "query state untouched");
    }
}
