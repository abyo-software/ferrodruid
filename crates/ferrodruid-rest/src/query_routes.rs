// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Query endpoints: POST /druid/v2/, POST /druid/v2.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use ferrodruid_common::types::ColumnType;
use ferrodruid_query::{DruidQuery, QueryResult};
use ferrodruid_segment::column::ColumnData;

use crate::AppState;

/// Default query timeout in milliseconds (5 minutes).
const DEFAULT_QUERY_TIMEOUT_MS: u64 = 300_000;

/// Map a broker/executor error onto the Druid-shaped HTTP error envelope.
///
/// Fail-closed exact-cardinality program (2026-07-11):
/// `DruidError::ResourceLimit` is a DELIBERATE fail-closed guard (exact
/// COUNT(DISTINCT)/`cardinality` saturation, groupBy/topN key caps) — the
/// query asked for something the server bounds refuse to compute exactly.
/// That is the client's 4xx, not a server fault: it maps to HTTP 400 with
/// `errorClass = io.druid.query.ResourceLimitExceededException`, and the
/// message names the limit and the remedy. Every other execution error
/// keeps the existing 500 `QueryExecutionException` shape.
fn execution_error_response(
    e: &ferrodruid_common::error::DruidError,
) -> (StatusCode, Json<serde_json::Value>) {
    match e {
        ferrodruid_common::error::DruidError::ResourceLimit { .. } => crate::error_response(
            StatusCode::BAD_REQUEST,
            "Resource limit exceeded",
            &e.to_string(),
            "io.druid.query.ResourceLimitExceededException",
        ),
        _ => crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Query execution failed",
            &e.to_string(),
            "io.druid.query.QueryExecutionException",
        ),
    }
}

/// POST /druid/v2/ — execute a Druid native JSON query.
///
/// Parses the query, routes it through the Broker to all in-process Historical
/// nodes, merges partial results, and returns the unified result as JSON.
///
/// Wave 36-B: bumps `ferrodruid_queries_total{datasource}` on each
/// accepted (parsed) request and `ferrodruid_query_errors_total{class}`
/// on each error path (`parse`, `validation`, `execution`, `timeout`).
pub(crate) async fn handle_native_query(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Parse the query JSON.
    let query: DruidQuery = serde_json::from_str(&body).map_err(|e| {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["parse"])
            .inc();
        crate::error_response(
            StatusCode::BAD_REQUEST,
            "Query parse failed",
            &format!("Could not parse query JSON: {e}"),
            "io.druid.query.QueryParseException",
        )
    })?;

    // Bump the per-datasource counter as soon as the query parses.
    let ds_label = native_query_datasource(&query);
    state
        .metrics
        .queries_total
        .with_label_values(&[ds_label.as_str()])
        .inc();

    // Extract timeout from query context, defaulting to 5 minutes.
    let timeout_ms = query
        .context()
        .and_then(|c| c.timeout)
        .unwrap_or(DEFAULT_QUERY_TIMEOUT_MS);

    // Validate timeout.
    if timeout_ms == 0 {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["validation"])
            .inc();
        return Err(crate::error_response(
            StatusCode::BAD_REQUEST,
            "Query timeout",
            "timeout must be > 0",
            "io.druid.query.QueryTimeoutException",
        ));
    }

    // If there are historicals loaded, execute via the Broker with a timeout.
    if !state.historicals.is_empty() {
        let hist_refs: Vec<&ferrodruid_historical::Historical> =
            state.historicals.iter().map(|h| h.as_ref()).collect();

        let timer = state.metrics.query_duration_seconds.start_timer();
        let timeout_result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            state.broker.execute_local(&query, &hist_refs)
        })
        .await;
        timer.observe_duration();

        match timeout_result {
            Ok(Ok(mut broker_result)) => {
                // P1-#3: Druid native queries finalize aggregator outputs
                // by default (`finalize` context flag, default true) — a
                // raw sketch aggregation returns its finalized scalar on
                // the wire, not the intermediate `@sketch` envelope.  This
                // runs strictly AFTER the broker merge (which needs the
                // intermediates) and only on the native wire; the SQL path
                // finalizes via explicit estimate post-aggregations.
                ferrodruid_query::finalize_native_wire_outputs(&query, &mut broker_result.result);
                let json = result_to_json(&broker_result.result);
                return Ok(Json(json));
            }
            Ok(Err(e)) => {
                state
                    .metrics
                    .query_errors_total
                    .with_label_values(&["execution"])
                    .inc();
                return Err(execution_error_response(&e));
            }
            Err(_elapsed) => {
                state
                    .metrics
                    .query_errors_total
                    .with_label_values(&["timeout"])
                    .inc();
                return Err(crate::error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "Query timeout",
                    &format!("Query exceeded timeout of {timeout_ms}ms"),
                    "io.druid.query.QueryTimeoutException",
                ));
            }
        }
    }

    // No historicals loaded — return an empty result matching the query type.
    let result = empty_result_for(&query);
    Ok(Json(result))
}

/// Best-effort datasource label for native query metrics.
///
/// Native queries surface their data source through a `dataSource`
/// JSON object that may be a `table` (named), `union`, `query`,
/// `lookup`, or `inline`.  We only cheaply extract the `table` name
/// here — every other shape lands as `"_other_"` so unbounded user
/// input cannot blow up the Prometheus label cardinality.  Names
/// longer than 64 bytes are also bucketed as `"_other_"`.
fn native_query_datasource(query: &DruidQuery) -> String {
    let raw = match query {
        DruidQuery::Timeseries(q) => serde_json::to_value(q).ok(),
        DruidQuery::TopN(q) => serde_json::to_value(q).ok(),
        DruidQuery::GroupBy(q) => serde_json::to_value(q).ok(),
        DruidQuery::Scan(q) => serde_json::to_value(q).ok(),
        DruidQuery::Search(q) => serde_json::to_value(q).ok(),
        DruidQuery::SegmentMetadata(q) => serde_json::to_value(q).ok(),
        DruidQuery::DataSourceMetadata(q) => serde_json::to_value(q).ok(),
        DruidQuery::TimeBoundary(q) => serde_json::to_value(q).ok(),
        DruidQuery::UnionAll(_) => None,
        DruidQuery::Window(q) => serde_json::to_value(&q.inner).ok(),
    };
    if let Some(v) = raw.as_ref()
        && let Some(ds) = v.get("dataSource")
        && let Some(name) = ds.get("name").and_then(serde_json::Value::as_str)
        && name.len() <= 64
    {
        return name.to_string();
    }
    "_other_".to_string()
}

/// Convert a [`QueryResult`] into the Druid JSON wire format.
fn result_to_json(result: &QueryResult) -> serde_json::Value {
    serde_json::to_value(result).unwrap_or_else(|_| serde_json::json!([]))
}

// ---------------------------------------------------------------------------
// SQL query handler
// ---------------------------------------------------------------------------

/// Request body for `POST /druid/v2/sql`.
#[derive(serde::Deserialize)]
pub(crate) struct SqlQueryRequest {
    /// The SQL query string.
    query: String,
    /// Optional query parameters (positional).
    #[serde(default)]
    #[allow(dead_code)]
    parameters: Vec<serde_json::Value>,
    /// Optional query context. E16: `useApproximateCountDistinct`
    /// (default `true`) is parsed into [`ferrodruid_sql::PlannerOptions`]
    /// and selects the `COUNT(DISTINCT col)` lowering (approximate HLL
    /// sketch vs exact `cardinality` aggregation) — see
    /// [`planner_options_from_context`].
    #[serde(default)]
    context: Option<serde_json::Value>,
    /// Optional result format.
    #[serde(rename = "resultFormat", default)]
    #[allow(dead_code)]
    result_format: Option<String>,
}

/// E16: parse [`ferrodruid_sql::PlannerOptions`] from the SQL request's
/// query context.
///
/// Druid semantics: `useApproximateCountDistinct` defaults to `true`
/// (approximate HLL COUNT(DISTINCT), the deep-match-verified default);
/// `false` switches `COUNT(DISTINCT col)` to the exact `cardinality`
/// lowering. Both JSON booleans and their string forms (`"true"`/`"false"`,
/// case-insensitive — Druid's query context accepts stringified booleans)
/// are accepted. A present-but-unparseable value fails closed with a
/// planning error rather than silently running in the wrong mode, and a
/// non-object context is rejected the same way (Druid's request shape
/// requires an object).
fn planner_options_from_context(
    context: Option<&serde_json::Value>,
) -> Result<ferrodruid_sql::PlannerOptions, String> {
    let mut options = ferrodruid_sql::PlannerOptions::default();
    let Some(ctx) = context else {
        return Ok(options);
    };
    let obj = match ctx {
        serde_json::Value::Object(obj) => obj,
        // An explicit `"context": null` is the same as absent.
        serde_json::Value::Null => return Ok(options),
        other => {
            return Err(format!(
                "SQL query context must be a JSON object, got {other}"
            ));
        }
    };
    if let Some(raw) = obj.get("useApproximateCountDistinct") {
        options.use_approximate_count_distinct = match raw {
            serde_json::Value::Bool(b) => *b,
            serde_json::Value::String(s) if s.eq_ignore_ascii_case("true") => true,
            serde_json::Value::String(s) if s.eq_ignore_ascii_case("false") => false,
            other => {
                return Err(format!(
                    "query context key [useApproximateCountDistinct] must be a boolean \
                     (or \"true\"/\"false\"), got {other}"
                ));
            }
        };
    }
    // Codex-review HIGH finding B (R-6 hardening): FerroDruid evaluates ALL
    // SQL time semantics in UTC — timezone-less TIMESTAMP/DATE literals,
    // TIME_FLOOR bucketing, and ISO wire output. Druid would shift these by
    // the context `sqlTimeZone`; honoring a non-UTC zone is not implemented,
    // so accepting one and silently computing UTC answers would return
    // plausibly-WRONG results. Fail closed instead (fail-closed philosophy;
    // see `docs/design/compatibility-modes.md`). Measured before
    // choosing this path (2026-07-12): stock Superset + pydruid sends NO
    // sqlTimeZone at all — pydruid's DB cursor defaults `context or {}` and
    // Superset's Druid engine spec injects no context — so default BI
    // dashboards are unaffected; only a client that EXPLICITLY asked for a
    // non-UTC zone (and would otherwise get shifted-wrong data) sees the 400.
    if let Some(raw) = obj.get("sqlTimeZone") {
        let is_utc = match raw {
            // An explicit null is the same as absent (server default = UTC).
            serde_json::Value::Null => true,
            serde_json::Value::String(s) => matches!(s.as_str(), "UTC" | "Etc/UTC" | "+00:00"),
            _ => false,
        };
        if !is_utc {
            return Err(format!(
                "sqlTimeZone {raw} is not supported; FerroDruid evaluates timestamps in UTC \
                 (documented residual R-6). Omit sqlTimeZone (or set it to \"UTC\") and \
                 convert zones client-side or via TIME_FORMAT with an explicit zone"
            ));
        }
    }
    Ok(options)
}

/// POST /druid/v2/sql — execute a Druid SQL query.
///
/// Parses the SQL, plans it into a native query, executes via the broker, and
/// returns the result as an array of JSON objects (Druid SQL default format).
///
/// Wave 36-B: bumps `ferrodruid_queries_total{datasource}` on every
/// invocation and `ferrodruid_query_errors_total{class}` on each error
/// path (`parse`, `planning`, `execution`).  See
/// `crates/ferrodruid-telemetry/src/lib.rs` for the metric registry.
pub(crate) async fn handle_sql_query(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SqlQueryRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    use ferrodruid_sql::parser::DruidSqlStatement;
    use ferrodruid_sql::{parse_druid_sql, plan_sql_with_options};

    // 1. Parse the SQL.
    let stmt = parse_druid_sql(&body.query).map_err(|e| {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["parse"])
            .inc();
        crate::error_response(
            StatusCode::BAD_REQUEST,
            "SQL parse error",
            &e.to_string(),
            "io.druid.sql.SqlPlanningException",
        )
    })?;

    // 1a. Planner options from the SQL query context (E16
    //     `useApproximateCountDistinct`, default true; P1-#2 non-UTC
    //     `sqlTimeZone` fail-closed). Validated BEFORE any early return so the
    //     fail-closed gate applies to EVERY path — including the constant
    //     SELECT below, which would otherwise bypass it (Codex round 2). A
    //     malformed value fails closed: better a 400 than a silently-wrong
    //     result.
    let planner_options = planner_options_from_context(body.context.as_ref()).map_err(|msg| {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["planning"])
            .inc();
        crate::error_response(
            StatusCode::BAD_REQUEST,
            "SQL planning error",
            &msg,
            "io.druid.sql.SqlPlanningException",
        )
    })?;

    // Constant SELECT (`SELECT 1`, `SELECT 1 AS x`) — no data source to scan.
    // Materialise the single synthetic row directly (Calcite/Druid semantics).
    // This is the path Apache Superset's Druid engine `do_ping()` exercises for
    // its connection health check, so answering it natively makes Superset's
    // "Test Connection" succeed without an ORM-level workaround.
    if let DruidSqlStatement::ConstantSelect(columns) = &stmt {
        use ferrodruid_sql::parser::SqlLiteral;
        let mut row = serde_json::Map::with_capacity(columns.len());
        for col in columns {
            let value = match &col.value {
                SqlLiteral::Integer(i) => serde_json::Value::from(*i),
                SqlLiteral::Float(f) => serde_json::Value::from(*f),
                SqlLiteral::String(s) => serde_json::Value::from(s.clone()),
                SqlLiteral::Boolean(b) => serde_json::Value::from(*b),
                SqlLiteral::Null => serde_json::Value::Null,
                // `SELECT TIMESTAMP '...'` — Druid SQL renders a constant
                // TIMESTAMP as the ISO-8601 millis string (P1-#2).
                SqlLiteral::Timestamp(ms) => timestamp_to_iso_value(&serde_json::Value::from(*ms)),
            };
            row.insert(col.name.clone(), value);
        }
        return Ok(Json(serde_json::Value::Array(vec![
            serde_json::Value::Object(row),
        ])));
    }

    let is_explain = matches!(stmt, DruidSqlStatement::ExplainPlan(_));

    // 2. Build a minimal schema from the data source name in the query.
    //    For now we use a synthetic schema; a full implementation would
    //    look up the actual data source schema from the metadata store.
    let ds_name = extract_datasource_name(&stmt).unwrap_or_else(|| "unknown".to_string());

    // Bump the per-datasource query counter exactly once per accepted
    // (parsed) request, regardless of EXPLAIN vs execute.  EXPLAIN is
    // counted because it still consumes planner CPU and is observable
    // by operators as load.
    state
        .metrics
        .queries_total
        .with_label_values(&[&ds_name])
        .inc();

    // 2a. INFORMATION_SCHEMA metadata introspection (Superset dataset sync via
    //     the pydruid SQLAlchemy dialect: get_schema_names / get_table_names /
    //     get_columns). These are not real datasources; materialise the virtual
    //     table on the fly from the live segment metadata and run the SELECT
    //     against it through the normal planner + executor. EXPLAIN falls
    //     through to the standard native-JSON path below.
    if !is_explain
        && crate::infoschema::is_virtual_table(&ds_name)
        && let Some((vsegment, vschema)) = crate::infoschema::build(&state, &ds_name)
    {
        // Aggregate-comparison existence check (Superset `has_table`:
        // `SELECT COUNT(*) > 0 AS exists_ …`) — the planner can't project an
        // aggregate wrapped in a comparison, so evaluate it specially.
        if let Some(rows) = crate::infoschema::try_existence_check(&stmt, &vsegment, &vschema) {
            return Ok(Json(rows));
        }
        // Null-semantics T6: Druid SORTS INFORMATION_SCHEMA results, but the
        // virtual table executes as a Scan (which can only be time-ordered,
        // T5). Strip the ORDER BY (+ LIMIT/OFFSET, which apply AFTER the
        // sort) before planning and sort the produced rows below.
        let (stmt, post_sort) = crate::infoschema::extract_post_sort(&stmt);
        let planned = plan_sql_with_options(&stmt, &vschema, planner_options).map_err(|e| {
            state
                .metrics
                .query_errors_total
                .with_label_values(&["planning"])
                .inc();
            crate::error_response(
                StatusCode::BAD_REQUEST,
                "SQL planning error",
                &e.to_string(),
                "io.druid.sql.SqlPlanningException",
            )
        })?;
        let result =
            ferrodruid_query::execute_query(&planned.native_query, &vsegment).map_err(|e| {
                state
                    .metrics
                    .query_errors_total
                    .with_label_values(&["execution"])
                    .inc();
                execution_error_response(&e)
            })?;
        // Multi-shard exact union (2026-07-12): this path bypasses
        // `Broker::execute_local`, but the executors emit exact-cardinality
        // partials as internal `CardinalityState` envelopes.  Route the
        // single-partial result through the broker merge so envelopes are
        // collapsed to exact counts (or fail closed) before hitting the
        // SQL wire.
        let result = ferrodruid_broker::Broker::merge_results(&planned.native_query, vec![result])
            .map_err(|e| {
                state
                    .metrics
                    .query_errors_total
                    .with_label_values(&["execution"])
                    .inc();
                execution_error_response(&e)
            })?;
        // Return the flat SQL row shape (`[{col: val}]`), not the native
        // Scan/GroupBy envelope, so BI clients (pydruid DBAPI) parse it.
        // INFORMATION_SCHEMA queries never carry a time grain.
        let mut rows = result_to_sql_rows(
            &result,
            &planned.output_columns,
            &std::collections::HashSet::new(),
            None,
        );
        if let Some(post_sort) = &post_sort {
            crate::infoschema::apply_post_sort(&mut rows, post_sort);
        }
        return Ok(Json(rows));
    }

    let schema = build_schema_for(&state, &ds_name);

    // 3. Plan the SQL into a native query.
    let mut planned = plan_sql_with_options(&stmt, &schema, planner_options).map_err(|e| {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["planning"])
            .inc();
        crate::error_response(
            StatusCode::BAD_REQUEST,
            "SQL planning error",
            &e.to_string(),
            "io.druid.sql.SqlPlanningException",
        )
    })?;

    // 3a. Druid emits `SELECT *` columns as `__time`, then dimensions in
    //     SPEC order, then metrics ALPHABETICAL — measured against Druid 35
    //     (tests/druid-compat/RESULTS_wave47b_v35_run.md,
    //     `superset_preview_limit`: metricsSpec order
    //     `count,added,deleted,delta` comes back `added,count,deleted,delta`
    //     while dimensions keep spec order). The planner's wildcard scan arm
    //     emits metrics in schema (ingest) order, so re-order the
    //     metric-class output columns for the SQL wire here.
    if stmt_selects_wildcard(&stmt)
        && planned.joins.is_empty()
        && matches!(planned.native_query, DruidQuery::Scan(_))
    {
        sort_metric_output_columns(&mut planned.output_columns, &schema);
    }

    // 4. If EXPLAIN, return the native query JSON instead of executing.
    if is_explain {
        let native_json = serde_json::to_value(&planned.native_query).unwrap_or_default();
        return Ok(Json(serde_json::json!([
            {
                "PLAN": serde_json::to_string_pretty(&native_json).unwrap_or_default(),
                "RESOURCES": [{"name": ds_name, "type": "DATASOURCE"}]
            }
        ])));
    }

    // 4a. CL-4 / W1-J finding-D fail-closed guard.
    //
    // The SQL planner can lower a JOIN or a CTE (`WITH ...`) to a
    // `PlannedQuery` whose `joins` vector is non-empty or whose
    // `native_query` references a `DataSource::Query` (the subquery
    // produced by inlining the CTE body).  Until the execution layer
    // walks the joins / nested data source on every code path through
    // the broker, executing the bare `native_query` silently returns
    // the un-joined / un-grouped base rows — strictly worse than a
    // fail-closed error because clients receive plausibly-wrong
    // results.  Reject these queries with an explicit error so callers
    // see a precise failure mode instead.  The parse + plan tests in
    // `crates/ferrodruid-sql/tests/cl4_calcite.rs::cl4_join_*` and
    // `cl4_cte_*` cover the planner-side surface; once the executor is
    // wired (issue tracked under CL-4-R8) these guards can be removed
    // in favour of routing through the join / sub-query executor.
    if !planned.joins.is_empty() {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["planning"])
            .inc();
        return Err(crate::error_response(
            StatusCode::NOT_IMPLEMENTED,
            "SQL planning error",
            "JOIN execution is not yet wired into the SQL→native dispatch \
             (CL-4 / W1-J finding-D fail-closed). The query parses and plans \
             but the join executor is not invoked end-to-end, so executing it \
             would silently return un-joined base rows. Tracked under CL-4-R8.",
            "io.druid.sql.SqlPlanningException",
        ));
    }
    if native_query_uses_subquery_datasource(&planned.native_query) {
        state
            .metrics
            .query_errors_total
            .with_label_values(&["planning"])
            .inc();
        return Err(crate::error_response(
            StatusCode::NOT_IMPLEMENTED,
            "SQL planning error",
            "CTE / sub-query in FROM clause is not yet wired into the SQL→native \
             dispatch (CL-4 / W1-J finding-D fail-closed). The query parses and \
             plans but the nested data source is not executed end-to-end, so \
             running it would silently return un-grouped base rows. Tracked \
             under CL-4-R8.",
            "io.druid.sql.SqlPlanningException",
        ));
    }

    // 5. Execute via the broker.
    if !state.historicals.is_empty() {
        let hist_refs: Vec<&ferrodruid_historical::Historical> =
            state.historicals.iter().map(|h| h.as_ref()).collect();
        let timer = state.metrics.query_duration_seconds.start_timer();
        let broker_result = state
            .broker
            .execute_local(&planned.native_query, &hist_refs)
            .map_err(|e| {
                state
                    .metrics
                    .query_errors_total
                    .with_label_values(&["execution"])
                    .inc();
                execution_error_response(&e)
            })?;
        timer.observe_duration();
        // Wave 47-D §1: AVG(...) OVER (PARTITION BY ...) must keep the
        // trailing `.0` Druid sends.  We only opt window AVG outputs into
        // `preserve_float`; pre-existing scan paths (a `Double` metric
        // column projected as-is) keep their historic integer-collapse
        // behaviour so unrelated harness queries stay deep-matching.
        //
        // Post-aggregation outputs join the opt-out ONLY when the planner
        // typed them DOUBLE (AVG / arithmetic / function-over-aggregate —
        // `40.0` on Druid's wire, not `40`). BIGINT post-agg outputs
        // (COUNT(DISTINCT) / APPROX_COUNT_DISTINCT via the rounded HLL
        // estimate) integer-collapse: Druid emits `3`, not `3.0`.
        let post_agg_outputs = post_aggregation_output_columns(&planned.native_query);
        let mut preserve_float: std::collections::HashSet<String> =
            window_avg_output_columns(&planned.native_query);
        for col in &planned.output_columns {
            if post_agg_outputs.contains(&col.name)
                && matches!(
                    col.sql_type,
                    ferrodruid_sql::SqlType::Double | ferrodruid_sql::SqlType::Float
                )
            {
                preserve_float.insert(col.name.clone());
            }
        }
        // A `TIME_FLOOR(...)` GROUP BY surfaces its bucket under the column
        // the PLANNER marked by role (`PlannedQuery::time_bucket_column`,
        // codex-review HIGH finding C). The former inference — any
        // TIMESTAMP-typed output column not NAMED like an aggregation —
        // collided with hidden `$`-prefixed helper aggregators (a bucket
        // aliased `"$avg_sum_0"` lost its role and the hidden AVG sum was
        // emitted as an ISO timestamp). Role marking cannot collide:
        // hidden helpers never participate, and a TIMESTAMP-typed
        // aggregate (`MIN(__time)`/`MAX(__time)`, P1-#2) is never marked,
        // so it is never clobbered by the bucket envelope.
        let time_col = planned.time_bucket_column.clone();
        let json = result_to_sql_rows(
            &broker_result.result,
            &planned.output_columns,
            &preserve_float,
            time_col.as_deref(),
        );
        return Ok(Json(json));
    }

    // No historicals — return empty result.
    Ok(Json(serde_json::json!([])))
}

/// Convert a native [`QueryResult`] into the Druid SQL `resultFormat=object`
/// wire shape: a flat array of row objects with one field per output
/// column (e.g. `[{"cnt":10}]`, `[{"page":"Main_Page","cnt":4}]`).
///
/// Rows are projected onto the planner's `output_columns`, in SELECT-list
/// order.  This matters twice over: (1) AVG / arithmetic-over-aggregates
/// lower to hidden `$`-prefixed helper aggregators that Druid never emits
/// on the wire, and (2) pydruid / Superset map result columns to the
/// SELECT list POSITIONALLY (JSON document key order — the reason the
/// workspace enables serde_json `preserve_order`), so emitting the native
/// map order (post-aggregations last) would swap columns client-side.
///
/// * Aggregate results (Timeseries / TopN / GroupBy) use a STRICT
///   projection: exactly the output columns, in order; a named column
///   missing from the native row becomes JSON `null`.
/// * Scan results (also the wire shape of window / join queries, whose
///   `output_columns` metadata may be narrower than the actual rows) use
///   reorder-without-drop: listed columns first, in order, then any
///   remaining row fields (name-sorted, so the emitted key order is
///   deterministic — the native rows are `HashMap`s whose iteration
///   order is randomized per process).
///
/// Numeric values that are integral (e.g. `50.0`) are emitted as JSON
/// integers (`50`), matching Druid's wire format and avoiding spurious
/// type mismatches in JSON-equality clients (`50` != `50.0` per
/// `serde_json::Number`), except for columns in `preserve_float`.
fn result_to_sql_rows(
    result: &QueryResult,
    output_columns: &[ferrodruid_sql::OutputColumn],
    preserve_float: &std::collections::HashSet<String>,
    time_col: Option<&str>,
) -> serde_json::Value {
    match result {
        QueryResult::Timeseries(entries) => {
            // When the SQL grouped by a time grain (`TIME_FLOOR(__time, …)`),
            // the planner records a Timestamp output column; surface each
            // bucket's timestamp under that name so Superset time-series charts
            // get their time axis (a plain `SELECT COUNT(*)` has no such column
            // and is unaffected).  The bucket timestamp stays an ISO-8601
            // string — Druid's `/druid/v2/sql` object-format wire shape for a
            // TIMESTAMP column — and its position now follows the SELECT list
            // (the planner puts it first anyway).
            let rows: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| {
                    let mut obj = serde_json::Map::with_capacity(output_columns.len());
                    for col in output_columns {
                        // The planner-marked bucket column ALWAYS reads the
                        // envelope timestamp: on this granular path the
                        // bucket never legitimately lives in the result map,
                        // so a same-named map entry can only be a hidden
                        // `$`-helper collision (finding C) — the bucket
                        // wins, the helper value stays hidden.
                        if time_col == Some(col.name.as_str()) {
                            obj.insert(
                                col.name.clone(),
                                serde_json::Value::String(e.timestamp.clone()),
                            );
                        } else {
                            let v = e
                                .result
                                .get(&col.name)
                                .map_or(serde_json::Value::Null, |v| {
                                    // P1-#2: a TIMESTAMP-typed aggregate
                                    // output (`MIN(__time)`/`MAX(__time)`)
                                    // renders as Druid SQL's ISO-8601
                                    // millis string, not epoch millis.
                                    if matches!(col.sql_type, ferrodruid_sql::SqlType::Timestamp) {
                                        timestamp_to_iso_value(v)
                                    } else {
                                        normalize_sql_value_for(&col.name, v, preserve_float)
                                    }
                                });
                            obj.insert(col.name.clone(), v);
                        }
                    }
                    serde_json::Value::Object(obj)
                })
                .collect();
            serde_json::Value::Array(rows)
        }
        QueryResult::TopN(entries) => {
            // TopN nests rows under `result` per timestamp bucket.  In
            // SQL we want the rows flattened, in topN-internal order
            // (already sorted by metric DESC by the executor).
            let mut rows: Vec<serde_json::Value> = Vec::new();
            for entry in entries {
                for row in &entry.result {
                    rows.push(sql_row_object(row, output_columns, preserve_float));
                }
            }
            serde_json::Value::Array(rows)
        }
        QueryResult::GroupBy(entries) => {
            // codex QA r5: a TIME_FLOOR grouped WITH other dimensions lowers
            // to a granular GroupBy whose bucket lives in the native result's
            // `timestamp`, not the event map — surface it under the SQL alias
            // (the planner-marked bucket column, finding C) at its SELECT
            // position, the same injection the Timeseries arm performs. The
            // marked bucket only exists on granular plans, where the event
            // map never legitimately carries it — a same-named map entry can
            // only be a hidden `$`-helper collision, which the bucket wins.
            let rows: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| {
                    let time = time_col.map(|name| (name, e.timestamp.as_str()));
                    sql_row_object_with_time(&e.event, output_columns, preserve_float, time)
                })
                .collect();
            serde_json::Value::Array(rows)
        }
        QueryResult::Scan(scan) => {
            // ScanResult.events is a Vec<HashMap<String, Value>>;
            // re-emit as flat row objects, output columns first (in
            // SELECT order), then the remaining fields name-sorted.
            //
            // Null-semantics T7: a column the planner typed TIMESTAMP
            // (`__time` in wildcard and explicit scans) arrives from the
            // executor as epoch millis but Druid SQL renders it as an
            // ISO-8601 millis string ("2024-01-01T00:00:00.000Z") — SQL
            // endpoint only; the native /druid/v2 scan keeps epoch millis
            // exactly like Druid's native scan does.
            let rows: Vec<serde_json::Value> = scan
                .events
                .iter()
                .map(|row| {
                    let mut obj = serde_json::Map::with_capacity(row.len());
                    // Raw native keys consumed by an output column (its own
                    // name, or its `source` when aliased — codex QA r12), so
                    // the reorder-without-drop tail below doesn't re-emit
                    // the raw key alongside its alias.
                    let mut consumed: Vec<&str> = Vec::with_capacity(output_columns.len());
                    for col in output_columns {
                        // codex QA r12: an aliased scan projection reads the
                        // RAW native key (`source`) and emits under the
                        // SELECT alias (`name`); duplicate aliases each read
                        // the same source.
                        let native_key = col.source.as_deref().unwrap_or(&col.name);
                        if let Some(v) = row.get(native_key) {
                            let value =
                                if matches!(col.sql_type, ferrodruid_sql::SqlType::Timestamp) {
                                    timestamp_to_iso_value(v)
                                } else {
                                    normalize_sql_value_for(&col.name, v, preserve_float)
                                };
                            obj.insert(col.name.clone(), value);
                            consumed.push(native_key);
                        }
                    }
                    let mut rest: Vec<(&String, &serde_json::Value)> = row
                        .iter()
                        .filter(|(k, _)| {
                            !obj.contains_key(k.as_str()) && !consumed.contains(&k.as_str())
                        })
                        .collect();
                    rest.sort_by(|a, b| a.0.cmp(b.0));
                    for (k, v) in rest {
                        obj.insert(k.clone(), normalize_sql_value_for(k, v, preserve_float));
                    }
                    serde_json::Value::Object(obj)
                })
                .collect();
            serde_json::Value::Array(rows)
        }
        QueryResult::Search(_)
        | QueryResult::SegmentMetadata(_)
        | QueryResult::DataSourceMetadata(_)
        | QueryResult::TimeBoundary(_) => {
            // These query types are not directly addressable from SQL
            // SELECT — fall back to the native serialization so callers
            // can still inspect the result.
            serde_json::to_value(result).unwrap_or_else(|_| serde_json::json!([]))
        }
    }
}

/// Build an SQL row object from a native aggregate result map by STRICT
/// projection onto `output_columns`: exactly those columns, in SELECT
/// order; a listed column missing from the map becomes JSON `null`;
/// unlisted map entries (hidden `$`-helper aggregators) are dropped.
/// Values are normalized via [`normalize_sql_value_for`].
fn sql_row_object(
    map: &serde_json::Map<String, serde_json::Value>,
    output_columns: &[ferrodruid_sql::OutputColumn],
    preserve_float: &std::collections::HashSet<String>,
) -> serde_json::Value {
    sql_row_object_with_time(map, output_columns, preserve_float, None)
}

/// [`sql_row_object`] with an optional `(column name, ISO timestamp)` pair:
/// when the projected column is the PLANNER-MARKED time-bucket column
/// ([`ferrodruid_sql::PlannedQuery::time_bucket_column`]), the row's bucket
/// timestamp is emitted in its place (in SELECT position). Used by the
/// GroupBy arm, where a TIME_FLOOR bucket lives in the result envelope's
/// `timestamp` rather than the event map. The bucket wins over a same-named
/// event-map entry: the marked column only exists on granular plans, where
/// the map never legitimately carries it — a collision can only be a hidden
/// `$`-prefixed helper aggregation (codex-review HIGH finding C).
fn sql_row_object_with_time(
    map: &serde_json::Map<String, serde_json::Value>,
    output_columns: &[ferrodruid_sql::OutputColumn],
    preserve_float: &std::collections::HashSet<String>,
    time: Option<(&str, &str)>,
) -> serde_json::Value {
    let mut out = serde_json::Map::with_capacity(output_columns.len());
    for col in output_columns {
        if let Some((time_name, timestamp)) = time
            && col.name == time_name
        {
            out.insert(
                col.name.clone(),
                serde_json::Value::String(timestamp.to_string()),
            );
            continue;
        }
        let v = map.get(&col.name).map_or(serde_json::Value::Null, |v| {
            // P1-#2: a TIMESTAMP-typed aggregate output
            // (`MIN(__time)`/`MAX(__time)` in a grouped or topN query)
            // renders as Druid SQL's ISO-8601 millis string, not epoch
            // millis. Scoped to the SQL wire — the native /druid/v2
            // result keeps epoch millis exactly like Druid.
            if matches!(col.sql_type, ferrodruid_sql::SqlType::Timestamp) {
                timestamp_to_iso_value(v)
            } else {
                normalize_sql_value_for(&col.name, v, preserve_float)
            }
        });
        out.insert(col.name.clone(), v);
    }
    serde_json::Value::Object(out)
}

/// Returns the output names of the top-level post-aggregations carried by
/// `query` (recursing through `UnionAll`).  The caller intersects these
/// with the planner's output-column types: DOUBLE post-agg outputs (AVG /
/// arithmetic / function-over-aggregate) keep their trailing `.0` on the
/// wire, while BIGINT post-agg outputs (COUNT(DISTINCT) via the rounded
/// HLL estimate) integer-collapse (`3`, not `3.0`) — both matching Druid.
/// Only post-aggregation outputs are affected; every pre-existing column
/// type keeps its historic collapse behaviour (harness deep-matching
/// relies on it).
fn post_aggregation_output_columns(query: &DruidQuery) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    let post_aggs = match query {
        DruidQuery::Timeseries(q) => q.post_aggregations.as_ref(),
        DruidQuery::TopN(q) => q.post_aggregations.as_ref(),
        DruidQuery::GroupBy(q) => q.post_aggregations.as_ref(),
        DruidQuery::UnionAll(parts) => {
            for part in parts {
                out.extend(post_aggregation_output_columns(part));
            }
            None
        }
        _ => None,
    };
    if let Some(specs) = post_aggs {
        for spec in specs {
            out.insert(spec.name().to_string());
        }
    }
    out
}

/// Render a TIMESTAMP-typed SQL value as Druid's SQL wire shape: epoch
/// millis become an ISO-8601 millis string (`2024-01-01T00:00:00.000Z`,
/// UTC).  An integral double (an aggregate path that carries millis as
/// `f64`) is collapsed to millis first.  Non-numeric values
/// (already-formatted strings, nulls) pass through unchanged.
fn timestamp_to_iso_value(v: &serde_json::Value) -> serde_json::Value {
    let millis = match v.as_i64() {
        Some(ms) => ms,
        None => {
            let Some(f) = v.as_f64().filter(|f| {
                f.is_finite() && f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64
            }) else {
                return v.clone();
            };
            #[allow(clippy::cast_possible_truncation)]
            let ms = f as i64;
            ms
        }
    };
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
        .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
        .unwrap_or_else(|| v.clone())
}

/// Like [`normalize_sql_value`] but preserves an integral float (e.g.
/// `150.0`) when the column was planned as `DOUBLE` / `FLOAT` so that
/// `AVG(...)` window output keeps the trailing `.0` Druid emits.
fn normalize_sql_value_for(
    column: &str,
    v: &serde_json::Value,
    preserve_float: &std::collections::HashSet<String>,
) -> serde_json::Value {
    if preserve_float.contains(column) {
        return v.clone();
    }
    normalize_sql_value(v)
}

/// Returns the output column names produced by `AVG(...) OVER (...)`
/// window specs in `query`.  Used to opt those columns out of the
/// integer-collapse normalisation so that `lang_avg=150.0` does not
/// silently downgrade to `lang_avg=150`, which would diverge from the
/// `.0`-suffixed value Druid emits.
fn window_avg_output_columns(query: &DruidQuery) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    if let DruidQuery::Window(wq) = query {
        for spec in &wq.windows {
            // W1-J finding-A polish: Druid emits CUME_DIST and
            // PERCENT_RANK as `1.0` / `0.0` (always-float wire shape),
            // while FerroDruid's `normalize_sql_value` integer-collapses
            // those integral-valued doubles to `1` / `0`.  Preserve the
            // trailing `.0` for both surfaces so the harness deep-match
            // doesn't trip on a wire-format-only divergence.  AVG is
            // the original Wave 47-D opt-in.
            if matches!(
                spec.function,
                ferrodruid_query::WindowFunctionKind::Avg { .. }
                    | ferrodruid_query::WindowFunctionKind::CumeDist
                    | ferrodruid_query::WindowFunctionKind::PercentRank
            ) {
                out.insert(spec.output_name.clone());
            }
        }
    }
    out
}

/// Normalize a single JSON value for SQL wire output.
///
/// In particular: integral `f64` values like `50.0`, `1150.0` are
/// re-emitted as JSON integers, matching Druid's wire format.  Array
/// values (the typical wire shape of `ARRAY_AGG(...)` etc.) are
/// JSON-encoded into a string because Druid's SQL endpoint emits
/// complex column types serialised — e.g. `ARRAY_AGG(page)` round-trips
/// as `"[\"Main_Page\",\"Talk:Main_Page\",...]"` on the wire, not as a
/// raw JSON array (W1-J finding ARRAY_AGG wire-format parity).  Other
/// values pass through unchanged.
fn normalize_sql_value(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                return v.clone();
            }
            if let Some(f) = n.as_f64()
                && f.is_finite()
                && f.fract() == 0.0
                && f >= i64::MIN as f64
                && f <= i64::MAX as f64
            {
                #[allow(clippy::cast_possible_truncation)]
                let i = f as i64;
                return serde_json::Value::Number(serde_json::Number::from(i));
            }
            v.clone()
        }
        serde_json::Value::Array(_) => {
            // Stringify with `serde_json::to_string` so element commas
            // are bare (no surrounding whitespace) — matching Druid's
            // compact JSON serialiser output.
            serde_json::to_string(v).map_or_else(|_| v.clone(), serde_json::Value::String)
        }
        _ => v.clone(),
    }
}

/// Extract the data source name from a parsed SQL statement.
/// Return `true` when `query` (or any nested query inside a `UnionAll`)
/// references a [`ferrodruid_common::types::DataSource::Query`] — i.e.,
/// a SQL CTE / subquery in FROM that the planner lowered to a nested
/// native query.  The execution layer does not yet walk these on the
/// REST happy path, so we use this predicate to fail closed in
/// [`handle_sql_query`] (CL-4 / W1-J finding-D) until the nested
/// executor is wired (tracked under CL-4-R8).
fn native_query_uses_subquery_datasource(query: &DruidQuery) -> bool {
    use ferrodruid_common::types::DataSource;
    let ds = match query {
        DruidQuery::Timeseries(q) => &q.data_source,
        DruidQuery::TopN(q) => &q.data_source,
        DruidQuery::GroupBy(q) => &q.data_source,
        DruidQuery::Scan(q) => &q.data_source,
        DruidQuery::Search(q) => &q.data_source,
        DruidQuery::SegmentMetadata(q) => &q.data_source,
        DruidQuery::DataSourceMetadata(q) => &q.data_source,
        DruidQuery::TimeBoundary(q) => &q.data_source,
        DruidQuery::UnionAll(parts) => {
            return parts.iter().any(native_query_uses_subquery_datasource);
        }
        DruidQuery::Window(q) => &q.inner.data_source,
    };
    matches!(ds, DataSource::Query { .. })
}

fn extract_datasource_name(stmt: &ferrodruid_sql::parser::DruidSqlStatement) -> Option<String> {
    use ferrodruid_sql::parser::DruidSqlStatement;
    match stmt {
        DruidSqlStatement::Select(sel) => Some(sel.from.name.clone()),
        DruidSqlStatement::ExplainPlan(inner) => extract_datasource_name(inner),
        DruidSqlStatement::UnionAll(parts) => parts.first().and_then(extract_datasource_name),
        DruidSqlStatement::ConstantSelect(_) => None,
    }
}

/// `true` when the parsed statement is a SELECT carrying a `*` projection.
fn stmt_selects_wildcard(stmt: &ferrodruid_sql::parser::DruidSqlStatement) -> bool {
    match stmt {
        ferrodruid_sql::parser::DruidSqlStatement::Select(sel) => sel
            .projections
            .iter()
            .any(|p| matches!(p, ferrodruid_sql::parser::Projection::Wildcard)),
        _ => false,
    }
}

/// Re-order a wildcard scan's output columns to Druid's `SELECT *` layout:
/// non-metric columns (`__time`, then dimensions) keep their relative order
/// and metric columns follow, sorted alphabetically by name (see the call
/// site for the measured Druid 35 evidence).
fn sort_metric_output_columns(
    output_columns: &mut Vec<ferrodruid_sql::OutputColumn>,
    schema: &ferrodruid_sql::DataSourceSchema,
) {
    let metric_names: std::collections::HashSet<&str> =
        schema.metrics.iter().map(|m| m.name.as_str()).collect();
    let (mut non_metrics, mut metrics): (Vec<_>, Vec<_>) = output_columns
        .drain(..)
        .partition(|c| !metric_names.contains(c.name.as_str()));
    metrics.sort_by(|a, b| a.name.cmp(&b.name));
    non_metrics.append(&mut metrics);
    *output_columns = non_metrics;
}

/// Build a [`ferrodruid_sql::DataSourceSchema`] for the given data
/// source by inspecting the in-process Historical(s) for any loaded
/// segment of that name.
///
/// We use the first matching segment's `dimensions`/`metrics`/`columns`
/// fields to derive a real schema (so the SQL planner sees the actual
/// column types instead of falling back to `STRING` for every column).
/// Falls back to an empty schema if no segment is loaded — the planner
/// then defaults dimension references to `STRING` and metric
/// aggregations to `DOUBLE` arithmetic.
fn build_schema_for(state: &AppState, ds_name: &str) -> ferrodruid_sql::DataSourceSchema {
    use ferrodruid_sql::ColumnSchema;

    let mut dimensions: Vec<ColumnSchema> = Vec::new();
    let mut metrics: Vec<ColumnSchema> = Vec::new();
    let mut found = false;

    'outer: for hist in &state.historicals {
        for seg_id in hist.loaded_segments() {
            // Match only segments belonging to this data source.
            if let Some(seg_ds) = hist.segment_datasource(&seg_id)
                && seg_ds != ds_name
            {
                continue;
            }
            if let Some(seg) = hist.get_segment(&seg_id) {
                for dim in &seg.dimensions {
                    let col_type = seg
                        .columns
                        .get(dim)
                        .map_or(ColumnType::String, column_to_type);
                    dimensions.push(ColumnSchema {
                        name: dim.clone(),
                        column_type: col_type,
                    });
                }
                for met in &seg.metrics {
                    let col_type = seg
                        .columns
                        .get(met)
                        .map_or(ColumnType::Double, column_to_type);
                    metrics.push(ColumnSchema {
                        name: met.clone(),
                        column_type: col_type,
                    });
                }
                found = true;
                break 'outer;
            }
        }
    }

    if !found {
        tracing::debug!(data_source = ds_name, "no segment found for SQL schema");
    }

    ferrodruid_sql::DataSourceSchema {
        name: ds_name.to_string(),
        dimensions,
        metrics,
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

/// Map a segment column to a SQL/native [`ColumnType`].
pub(crate) fn column_to_type(col: &ColumnData) -> ColumnType {
    match col {
        ColumnData::Long(_) => ColumnType::Long,
        ColumnData::Float(_) => ColumnType::Float,
        ColumnData::Double(_) => ColumnType::Double,
        ColumnData::String(_) => ColumnType::String,
        ColumnData::Complex(_) => ColumnType::Complex("opaque".to_string()),
    }
}

/// Return an empty result array appropriate for the given query type.
fn empty_result_for(query: &DruidQuery) -> serde_json::Value {
    match query {
        DruidQuery::Timeseries(_) => serde_json::json!([]),
        DruidQuery::TopN(_) => serde_json::json!([]),
        DruidQuery::GroupBy(_) => serde_json::json!([]),
        DruidQuery::Scan(_) => serde_json::json!([]),
        DruidQuery::Search(_) => serde_json::json!([]),
        DruidQuery::SegmentMetadata(_) => serde_json::json!([]),
        DruidQuery::DataSourceMetadata(_) => serde_json::json!([]),
        DruidQuery::TimeBoundary(_) => serde_json::json!([]),
        DruidQuery::UnionAll(_) => serde_json::json!([]),
        DruidQuery::Window(_) => serde_json::json!([]),
    }
}
