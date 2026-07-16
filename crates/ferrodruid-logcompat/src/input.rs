// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Request-log line parsing.
//!
//! Apache Druid's *file* request logger (`druid.request.logging.type=file`)
//! writes one line per request. Two line layouts are handled (both observed
//! against a real Druid 35.0.1 micro-quickstart, 2026-07-11):
//!
//! * **Native query** — tab-separated:
//!   `<ISO timestamp> \t <remote addr> \t {stats JSON} \t {query JSON}`
//!   where the query JSON carries a `queryType` field.
//! * **SQL query** — tab-separated with an *empty* native-query column:
//!   `<ISO timestamp> \t <remote addr> \t \t {sqlQuery stats JSON} \t
//!   {"query": "SELECT …", "context": {…}}`.
//!
//! Rather than relying on column positions, every tab-separated field is
//! probed for a JSON object and the first field that *looks like a query*
//! (see [`query_from_json`]) wins — this also transparently accepts logs
//! that are plain one-JSON-object-per-line (e.g. pre-extracted logs).
//!
//! Lines that are JSON objects with a `feed` field are Druid *emitter*
//! request-log events (`druid.request.logging.type=emitter`); full emitter
//! support is out of scope, so these are counted and skipped cleanly
//! instead of crashing.
//!
//! # Log-form vs wire-form
//!
//! Druid does not log the client's original bytes: it logs its own
//! re-serialization of the parsed query. Three normalizations observed in
//! real Druid 35 logs are inverted by [`denormalize_native`] so that
//! classification sees the query as a client would send it on the wire:
//!
//! * `intervals` logged as a spec object
//!   (`{"type":"LegacySegmentSpec"|"intervals","intervals":[…]}`) instead
//!   of the plain interval array the client sent.
//! * simple granularities logged uppercase (`"DAY"`) or as objects
//!   (`{"type":"all"}`) instead of the lowercase strings clients send.
//! * `dataSource` wrapped in a no-op `{"type":"restrict","base":…}`
//!   policy object (Druid 35+).
//!
//! An `intervals` spec of `{"type":"segments",…}` marks a broker→data-node
//! *internal fan-out* sub-query (segment-pinned). Those are cluster
//! machinery, never client workload — FerroDruid plays both roles
//! internally and would never receive them on the wire — so they are
//! flagged and reported separately instead of polluting the compatibility
//! percentages.
//!
//! Similarly, a native query whose `context` carries `sqlQueryId` is the
//! Druid broker's own Calcite lowering of a SQL request that is *also* in
//! the log as a SQL line. FerroDruid plans SQL with its own planner and
//! never receives Druid's generated natives, so counting them would
//! double-count the workload (and mis-count it: Calcite's generated JSON
//! uses internal constructs like `windowOperator`). They are set aside as
//! [`LineOutcome::SqlLowered`].

use serde_json::Value;

/// A query extracted from one request-log line.
#[derive(Debug, Clone)]
pub enum QueryPayload {
    /// A Druid SQL query (the `query` string of a `/druid/v2/sql` request).
    Sql(String),
    /// A native JSON query (a `/druid/v2` request body with `queryType`).
    Native(Value),
}

/// The outcome of parsing one log line.
#[derive(Debug)]
pub enum LineOutcome {
    /// A client query, de-normalized back to wire form.
    Query(QueryPayload),
    /// A cluster-internal fan-out sub-query (segment-pinned `intervals`);
    /// counted separately, never part of the compatibility percentages.
    Internal(QueryPayload),
    /// The broker's own Calcite lowering of a SQL request (native query
    /// with `sqlQueryId` in its context); the workload is already counted
    /// by its SQL log line, so this is set aside like `Internal`.
    SqlLowered(QueryPayload),
    /// An emitter-format request-log event (`feed` field present) —
    /// unsupported input format, skipped cleanly.
    EmitterFormat,
    /// A structurally-valid line that carries no query (e.g. blank line or
    /// a stats-only line).
    NotAQuery,
}

/// Parse one request-log line.
pub fn parse_line(line: &str) -> LineOutcome {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return LineOutcome::NotAQuery;
    }

    // Whole-line JSON object (pre-extracted or emitter logs).
    if trimmed.starts_with('{')
        && let Ok(v) = serde_json::from_str::<Value>(trimmed)
    {
        if v.get("feed").is_some() {
            return LineOutcome::EmitterFormat;
        }
        return outcome_from_json(v);
    }

    // Tab-separated Druid file request-log line: probe every field for a
    // JSON object that looks like a query.
    for field in trimmed.split('\t') {
        let field = field.trim();
        if !field.starts_with('{') {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(field) else {
            continue;
        };
        if v.get("feed").is_some() {
            return LineOutcome::EmitterFormat;
        }
        match outcome_from_json(v) {
            LineOutcome::NotAQuery => {}
            outcome => return outcome,
        }
    }
    LineOutcome::NotAQuery
}

/// Wrap an extracted payload as `Query`, `Internal` (segment-pinned
/// fan-out) or `SqlLowered` (broker-generated Calcite lowering).
fn finish(payload: QueryPayload) -> LineOutcome {
    match payload {
        QueryPayload::Native(v) => {
            let (v, internal) = denormalize_native(v);
            if internal {
                LineOutcome::Internal(QueryPayload::Native(v))
            } else if v.get("context").and_then(|c| c.get("sqlQueryId")).is_some() {
                LineOutcome::SqlLowered(QueryPayload::Native(v))
            } else {
                LineOutcome::Query(QueryPayload::Native(v))
            }
        }
        sql @ QueryPayload::Sql(_) => LineOutcome::Query(sql),
    }
}

/// Extract a query from a parsed JSON object, if it carries one.
fn outcome_from_json(v: Value) -> LineOutcome {
    match query_from_json(v) {
        Some(q) => finish(q),
        None => LineOutcome::NotAQuery,
    }
}

/// Recognize a query payload inside a JSON object.
///
/// * `{"queryType": …}` — a native query itself.
/// * `{"query": "SELECT …"}` — a SQL request body (Druid's file logger
///   writes the `/druid/v2/sql` body as `{"query": …, "context": …}`).
/// * `{"query": {…"queryType"…}}` / `{"sqlQuery": {"query": "…"}}` —
///   wrapper objects some tooling produces around the same payloads.
///
/// Stats objects (`{"query/time": …}`) match none of these and are
/// ignored.
fn query_from_json(v: Value) -> Option<QueryPayload> {
    if v.get("queryType").is_some_and(Value::is_string) {
        return Some(QueryPayload::Native(v));
    }
    match v.get("query") {
        Some(Value::String(sql)) => return Some(QueryPayload::Sql(sql.clone())),
        Some(inner @ Value::Object(_)) if inner.get("queryType").is_some_and(Value::is_string) => {
            return Some(QueryPayload::Native(inner.clone()));
        }
        _ => {}
    }
    if let Some(sql_query) = v.get("sqlQuery")
        && let Some(Value::String(sql)) = sql_query.get("query")
    {
        return Some(QueryPayload::Sql(sql.clone()));
    }
    None
}

/// Invert Druid's request-log re-serialization back to client wire form.
///
/// Returns the de-normalized query and `true` when the query is a
/// cluster-internal fan-out sub-query (segment-pinned `intervals`).
pub fn denormalize_native(v: Value) -> (Value, bool) {
    let mut internal = false;
    let v = denorm_value(v, None, &mut internal);
    (v, internal)
}

/// Recursive worker for [`denormalize_native`]: `key` is the JSON object
/// key under which `v` was found (`None` at the root).
fn denorm_value(v: Value, key: Option<&str>, internal: &mut bool) -> Value {
    match v {
        Value::Object(map) => {
            // `dataSource: {"type":"restrict","base":…}` — a no-op policy
            // wrapper Druid 35+ adds when logging; unwrap to the base.
            if key == Some("dataSource")
                && map.get("type").and_then(Value::as_str) == Some("restrict")
                && let Some(base) = map.get("base")
            {
                return denorm_value(base.clone(), Some("dataSource"), internal);
            }
            // `intervals` logged as a spec object instead of the plain
            // interval array the client sent.
            if key == Some("intervals") {
                match map.get("type").and_then(Value::as_str) {
                    Some("intervals" | "LegacySegmentSpec") => {
                        if let Some(inner) = map.get("intervals") {
                            return inner.clone();
                        }
                    }
                    Some("segments") => {
                        // Broker→data-node fan-out marker: segment-pinned.
                        *internal = true;
                        // Recover the plain interval list from the pinned
                        // segments so the query still classifies sensibly.
                        let intervals: Vec<Value> = map
                            .get("segments")
                            .and_then(Value::as_array)
                            .map(|segs| {
                                segs.iter().filter_map(|s| s.get("itvl").cloned()).collect()
                            })
                            .unwrap_or_default();
                        return Value::Array(intervals);
                    }
                    _ => {}
                }
            }
            // Simple granularity logged as `{"type":"all"}` — clients send
            // the plain lowercase string.
            if key == Some("granularity")
                && map.len() == 1
                && let Some(t) = map.get("type").and_then(Value::as_str)
            {
                return Value::String(t.to_ascii_lowercase());
            }
            // `dimensionOrder` logged as `{"type":"numeric"}` — clients
            // send the plain string.
            if key == Some("dimensionOrder")
                && map.len() == 1
                && let Some(t) = map.get("type").and_then(Value::as_str)
            {
                return Value::String(t.to_string());
            }
            // A plain-string dimension (`"dimension": "page"`) is logged as
            // `{"type":"LegacyDimensionSpec", …}`; invert to the string the
            // client sent (or a `default` spec when it renames/coerces).
            if map.get("type").and_then(Value::as_str) == Some("LegacyDimensionSpec") {
                let dimension = map.get("dimension").and_then(Value::as_str);
                let output = map.get("outputName").and_then(Value::as_str);
                let out_type = map.get("outputType").and_then(Value::as_str);
                if let Some(d) = dimension {
                    if output.is_none_or(|o| o == d) && out_type.is_none_or(|t| t == "STRING") {
                        return Value::String(d.to_string());
                    }
                    let mut spec = map.clone();
                    spec.insert("type".to_string(), Value::String("default".to_string()));
                    return Value::Object(spec);
                }
            }
            // A plain-string topN metric (`"metric": "rows"`) is logged as
            // `{"type":"LegacyTopNMetricSpec","metric":"rows"}`.
            if map.get("type").and_then(Value::as_str) == Some("LegacyTopNMetricSpec")
                && let Some(metric) = map.get("metric").and_then(Value::as_str)
            {
                return Value::String(metric.to_string());
            }
            // A search `sort` is logged with a nested type object
            // (`{"type":{"type":"lexicographic"}}`); clients send
            // `{"type":"lexicographic"}`.
            if key == Some("sort")
                && let Some(Value::Object(inner)) = map.get("type")
                && inner.len() == 1
                && let Some(t) = inner.get("type").and_then(Value::as_str)
            {
                let mut sort = map.clone();
                sort.insert("type".to_string(), Value::String(t.to_string()));
                return Value::Object(sort);
            }
            let map = map
                .into_iter()
                .map(|(k, val)| {
                    let denormed = denorm_value(val, Some(k.as_str()), internal);
                    (k, denormed)
                })
                .collect();
            Value::Object(map)
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| denorm_value(item, key, internal))
                .collect(),
        ),
        // Simple granularity logged uppercase (`"DAY"`) — clients send
        // lowercase.
        Value::String(s) if key == Some("granularity") => Value::String(s.to_ascii_lowercase()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_native_file_log_line() {
        let line = "2026-07-11T19:42:17.905Z\t127.0.0.1\t{\"query/time\":29,\"success\":true}\t{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"wiki\"},\"intervals\":{\"type\":\"LegacySegmentSpec\",\"intervals\":[\"2024-01-01T00:00:00.000Z/2024-01-04T00:00:00.000Z\"]},\"granularity\":\"DAY\",\"aggregations\":[{\"type\":\"count\",\"name\":\"rows\"}]}";
        match parse_line(line) {
            LineOutcome::Query(QueryPayload::Native(v)) => {
                assert_eq!(v["queryType"], "timeseries");
                // intervals de-normalized to the plain array wire form
                assert!(v["intervals"].is_array());
                // granularity de-normalized to lowercase
                assert_eq!(v["granularity"], "day");
            }
            other => panic!("expected native query, got {other:?}"),
        }
    }

    #[test]
    fn parses_sql_file_log_line_with_empty_native_column() {
        let line = "2026-07-11T19:40:16.660Z\t127.0.0.1\t\t{\"sqlQuery/time\":361,\"success\":true}\t{\"query\":\"SELECT COUNT(*) AS cnt FROM wikipedia_compat\",\"context\":{\"sqlQueryId\":\"x\"}}";
        match parse_line(line) {
            LineOutcome::Query(QueryPayload::Sql(sql)) => {
                assert_eq!(sql, "SELECT COUNT(*) AS cnt FROM wikipedia_compat");
            }
            other => panic!("expected SQL query, got {other:?}"),
        }
    }

    #[test]
    fn segment_pinned_intervals_flag_internal_fanout() {
        let line = "2026-07-11T19:40:16.996Z\t127.0.0.1\t{\"query/time\":13}\t{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"wiki\"},\"intervals\":{\"type\":\"segments\",\"segments\":[{\"itvl\":\"2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z\",\"ver\":\"v\",\"part\":0}]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}]}";
        match parse_line(line) {
            LineOutcome::Internal(QueryPayload::Native(v)) => {
                assert_eq!(
                    v["intervals"][0],
                    "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"
                );
                assert_eq!(v["granularity"], "all");
            }
            other => panic!("expected internal fan-out, got {other:?}"),
        }
    }

    #[test]
    fn restrict_datasource_wrapper_unwraps() {
        let (v, internal) = denormalize_native(serde_json::json!({
            "queryType": "segmentMetadata",
            "dataSource": {"type": "restrict",
                           "base": {"type": "table", "name": "wiki"},
                           "policy": {"type": "noRestriction"}},
            "intervals": ["2024-01-01/2024-01-02"],
        }));
        assert!(!internal);
        assert_eq!(v["dataSource"]["type"], "table");
        assert_eq!(v["dataSource"]["name"], "wiki");
    }

    #[test]
    fn emitter_format_detected_not_crashed() {
        let line = "{\"feed\":\"requests\",\"timestamp\":\"2026-07-11T00:00:00Z\",\"query\":{\"queryType\":\"scan\"}}";
        assert!(matches!(parse_line(line), LineOutcome::EmitterFormat));
    }

    #[test]
    fn blank_and_garbage_lines_are_not_queries() {
        assert!(matches!(parse_line(""), LineOutcome::NotAQuery));
        assert!(matches!(parse_line("   "), LineOutcome::NotAQuery));
        assert!(matches!(
            parse_line("not json at all"),
            LineOutcome::NotAQuery
        ));
        // stats-only line
        assert!(matches!(
            parse_line("{\"query/time\":29,\"success\":true}"),
            LineOutcome::NotAQuery
        ));
    }

    #[test]
    fn plain_json_object_per_line_native() {
        let line = "{\"queryType\":\"topN\",\"dataSource\":\"wiki\",\"dimension\":\"page\",\"metric\":\"rows\",\"threshold\":5,\"granularity\":\"all\",\"intervals\":[\"2024-01-01/2024-01-04\"],\"aggregations\":[{\"type\":\"count\",\"name\":\"rows\"}]}";
        assert!(matches!(
            parse_line(line),
            LineOutcome::Query(QueryPayload::Native(_))
        ));
    }
}
