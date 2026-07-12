// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Broker application logic — kept in its own module so it can be
//! exercised by unit tests with a `MockHistoricalClient` swapped in
//! for the real HTTP transport.
//!
//! ## Upgrade path (Wave 40.LL → 41.OO → 42.RR → 43.TT)
//!
//! - Wave 40.LL added a **scatter path** (`POST /druid/v2/sql/scatter`)
//!   that fans a SQL string out to every configured historical and
//!   echoes per-segment fragments back. No translation; opaque.
//! - Wave 41.OO added a **real native-query path** (`POST /druid/v2/native`)
//!   that accepts a `NativeQuery` body plus a `segmentIds` list and
//!   merges per-segment results with `merge_timeseries` /
//!   `merge_scan`.
//! - Wave 42.RR extended the merge surface to `groupBy` + `topN` and
//!   added S3 deep storage.
//! - **Wave 43.TT (this wave)** wires `POST /druid/v2/sql` to a real
//!   SQL → native query bridge: parse + plan via `ferrodruid_sql`,
//!   translate the planned `DruidQuery` to the wire-side
//!   [`NativeQuery`] subset via [`ferrodruid_rpc::sql_bridge`], scatter
//!   per-segment, merge, and format a Druid-aligned [`SqlResponse`].
//!   The W3 echo behaviour is gone — every broker invocation now goes
//!   through the bridge. Callers wanting the legacy single-binary
//!   in-process path should hit `ferrodruid` directly.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use ferrodruid_rpc::broker_server::BrokerServerState;
use ferrodruid_rpc::native_query::{
    Aggregation, NativeQuery, NativeQueryResult, TimeseriesBucket, merge_group_by, merge_scan,
    merge_timeseries, merge_top_n,
};
use ferrodruid_rpc::sql_bridge::{
    BridgedQuery, TranslateError, default_schema_for_sql, translate_sql,
};
use ferrodruid_rpc::{
    BrokerInfo, HistoricalClient, HttpHistoricalClient, RpcError, SegmentQuery,
    SegmentQueryResponse, SqlQuery, SqlResponse,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Shared state held by the broker HTTP server when it is configured
/// with one or more historicals to scatter to.
#[derive(Clone)]
pub struct BrokerScatterState {
    /// Identity / tier metadata mounted on `/druid/v2/info` and
    /// `/druid/v2/sql`.
    pub server_state: Arc<BrokerServerState>,
    /// One client per configured historical.
    pub historicals: Arc<Vec<Arc<dyn HistoricalClient>>>,
}

impl BrokerScatterState {
    /// Build a broker state from broker identity + historical base
    /// URLs.
    ///
    /// Pre-W1-I production path. Today's binary entry point goes via
    /// [`Self::from_historical_urls_with_client`] so the outbound TLS
    /// identity is loaded exactly once; kept here for tests + external
    /// callers that do not need a shared `reqwest::Client`.
    ///
    /// # Errors
    ///
    /// Propagates [`RpcError`] if any underlying client fails to build.
    #[allow(dead_code)] // Kept for tests / non-binary callers.
    pub fn from_historical_urls(
        server_state: BrokerServerState,
        urls: &[String],
    ) -> Result<Self, RpcError> {
        let mut clients: Vec<Arc<dyn HistoricalClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpHistoricalClient::try_new(u.clone())?;
            clients.push(Arc::new(c));
        }
        Ok(Self {
            server_state: Arc::new(server_state),
            historicals: Arc::new(clients),
        })
    }

    /// W1-I (CL-J1): build a broker state where every per-historical
    /// `HttpHistoricalClient` reuses the supplied pre-built
    /// `reqwest::Client` (typically TLS-aware).
    #[must_use]
    pub fn from_historical_urls_with_client(
        server_state: BrokerServerState,
        urls: &[String],
        http: reqwest::Client,
    ) -> Self {
        let mut clients: Vec<Arc<dyn HistoricalClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpHistoricalClient::with_client(u.clone(), http.clone());
            clients.push(Arc::new(c));
        }
        Self {
            server_state: Arc::new(server_state),
            historicals: Arc::new(clients),
        }
    }

    /// Build a broker state from already-constructed historical
    /// clients. Used by tests that inject a mock.
    #[must_use]
    #[allow(dead_code)] // Test-only helper; production goes via from_historical_urls.
    pub fn from_clients(
        server_state: BrokerServerState,
        clients: Vec<Arc<dyn HistoricalClient>>,
    ) -> Self {
        Self {
            server_state: Arc::new(server_state),
            historicals: Arc::new(clients),
        }
    }
}

/// Aggregated scatter response returned from
/// `POST /druid/v2/sql/scatter`.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ScatterResponse {
    /// Per-query identifier the broker assigns.
    #[serde(rename = "queryId")]
    pub query_id: String,
    /// One entry per historical the broker fanned out to, in
    /// configuration order.
    pub fragments: Vec<SegmentQueryResponse>,
}

/// Wave 41.OO: native-query envelope the broker accepts at
/// `POST /druid/v2/native`. The broker uses `query.data_source` for
/// future coordinator-metadata resolution but in this wave the caller
/// supplies the target `segment_ids` explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerNativeRequest {
    /// Target segments. The broker dispatches one fragment per
    /// segment, round-robin across the configured historicals.
    #[serde(rename = "segmentIds", default)]
    pub segment_ids: Vec<String>,
    /// The native query to execute. Tagged by `queryType`.
    #[serde(flatten)]
    pub query: NativeQuery,
}

/// Wave 41.OO: response shape returned from
/// `POST /druid/v2/native`. Carries the merged per-tier result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerNativeResponse {
    /// Per-query identifier the broker assigns.
    #[serde(rename = "queryId")]
    pub query_id: String,
    /// Number of historicals the broker fanned out to.
    #[serde(rename = "historicalCount")]
    pub historical_count: usize,
    /// Number of segments the broker dispatched to.
    #[serde(rename = "segmentCount")]
    pub segment_count: usize,
    /// Merged result across every segment.
    pub result: NativeQueryResult,
}

/// Build the axum [`Router`] the broker binary mounts.
///
/// Wave 43.TT replaces the W3 `POST /druid/v2/sql` echo with a real
/// SQL → native query bridge: the broker parses + plans the SQL
/// (via `ferrodruid_sql`), translates the planned native query down
/// to the wire-side [`NativeQuery`] subset, scatters it to the
/// configured historicals, and formats the merged result back into a
/// Druid-aligned [`SqlResponse`]. With no historicals configured the
/// SQL endpoint returns 503 (mirroring the existing native-scatter
/// behaviour) — callers wanting in-process query execution should hit
/// the single-binary `ferrodruid` path.
pub fn build_router(state: BrokerScatterState) -> Router {
    Router::new()
        .route("/druid/v2/sql", post(sql_bridge_handler))
        .route("/druid/v2/sql/scatter", post(scatter_sql))
        .route("/druid/v2/native", post(native_scatter))
        .route("/druid/v2/info", get(info_handler))
        .with_state(state)
}

async fn info_handler(State(state): State<BrokerScatterState>) -> Json<BrokerInfo> {
    Json(BrokerInfo {
        version: state.server_state.version.clone(),
        role: "broker".to_string(),
        tier: state.server_state.tier.clone(),
        broker_id: state.server_state.broker_id.clone(),
    })
}

/// Wave 43.TT — SQL bridge handler.
///
/// Parses the SQL via `ferrodruid_sql`, translates the planned native
/// query down to the wire subset, scatters per-segment fragments to
/// the configured historicals, merges, and formats a Druid-aligned
/// [`SqlResponse`].
///
/// Segment selection: when the request `context` carries a JSON array
/// at key `segmentIds`, those are used (round-robin across
/// historicals). Otherwise the broker synthesises one segment id per
/// historical (mirroring [`scatter_sql`] for the schema-less / catalog-
/// less default path).
async fn sql_bridge_handler(
    State(state): State<BrokerScatterState>,
    Json(query): Json<SqlQuery>,
) -> Result<Json<SqlResponse>, BrokerError> {
    if state.historicals.is_empty() {
        return Err(BrokerError::NoHistoricals);
    }

    // Build a permissive default schema from the SQL's FROM table.
    let schema = match default_schema_for_sql(&query.query) {
        Some(s) => s,
        None => return Err(BrokerError::SqlParse("could not extract FROM table".into())),
    };

    let bridged: BridgedQuery = match translate_sql(&query.query, &schema) {
        Ok(b) => b,
        Err(e) => return Err(BrokerError::SqlBridge(e)),
    };

    // Resolve segment ids — either from context.segmentIds or one
    // synthetic per historical.
    let query_id = format!("q-sql-{}", Uuid::new_v4());
    let segment_ids: Vec<String> = match query
        .context
        .as_ref()
        .and_then(|c| c.get("segmentIds"))
        .and_then(|v| v.as_array())
    {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        None => (0..state.historicals.len())
            .map(|i| format!("sql-{}-h{}", query_id, i))
            .collect(),
    };

    if segment_ids.is_empty() {
        return Err(BrokerError::MissingSegments);
    }

    let n_historicals = state.historicals.len();
    let mut fragments: Vec<SegmentQueryResponse> = Vec::with_capacity(segment_ids.len());
    for (idx, seg_id) in segment_ids.iter().enumerate() {
        let h = &state.historicals[idx % n_historicals];
        match h.native_scatter(seg_id, &bridged.native_query).await {
            Ok(resp) => fragments.push(resp),
            Err(e) => return Err(BrokerError::Upstream(e)),
        }
    }

    let merged = merge_fragments(&bridged.native_query, fragments);
    let (columns, rows) = sql_response_rows(&bridged, &merged);

    Ok(Json(SqlResponse {
        query_id,
        columns,
        rows,
        elapsed_ms: 0,
    }))
}

/// Render a merged [`NativeQueryResult`] into Druid-aligned
/// `(columns, rows)` matching the `BridgedQuery` output schema.
fn sql_response_rows(
    bridged: &BridgedQuery,
    merged: &NativeQueryResult,
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    let columns: Vec<String> = bridged
        .output_columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    // For timeseries projections that include a TIME_FLOOR / time
    // column, the planner emits a `SqlType::Timestamp` as the first
    // OutputColumn. We use that to decide whether the per-row first
    // cell is the bucket timestamp or a regular aggregation value.
    let timeseries_first_col_is_timestamp = bridged
        .output_columns
        .first()
        .map(|c| matches!(c.sql_type, ferrodruid_rpc::sql_bridge::SqlType::Timestamp))
        .unwrap_or(false);
    let rows = match merged {
        NativeQueryResult::Timeseries(buckets) => buckets
            .iter()
            .map(|b| {
                let mut row: Vec<serde_json::Value> = Vec::with_capacity(columns.len());
                for (i, col) in columns.iter().enumerate() {
                    if i == 0 && timeseries_first_col_is_timestamp {
                        row.push(serde_json::Value::from(b.timestamp_ms));
                    } else {
                        let v = b
                            .result
                            .get(col.as_str())
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        row.push(v);
                    }
                }
                row
            })
            .collect(),
        NativeQueryResult::Scan(rows)
        | NativeQueryResult::GroupBy(rows)
        | NativeQueryResult::TopN(rows) => rows
            .iter()
            .map(|m| {
                if columns.is_empty() {
                    // No output schema metadata — fall back to a single
                    // raw-object cell so the caller still sees the
                    // payload.
                    vec![serde_json::Value::Object(m.clone())]
                } else {
                    columns
                        .iter()
                        .map(|c| {
                            m.get(c.as_str())
                                .cloned()
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .collect()
                }
            })
            .collect(),
    };
    (columns, rows)
}

async fn scatter_sql(
    State(state): State<BrokerScatterState>,
    Json(query): Json<SqlQuery>,
) -> Result<Json<ScatterResponse>, BrokerError> {
    if state.historicals.is_empty() {
        return Err(BrokerError::NoHistoricals);
    }

    let query_id = format!("q-scatter-{}", Uuid::new_v4());
    let mut fragments = Vec::with_capacity(state.historicals.len());

    // Wave 40.LL: fan out one fragment per historical with a
    // per-historical synthetic segment id. W5 will replace this with
    // real `data_source -> segment list` resolution from the
    // coordinator.
    for (i, h) in state.historicals.iter().enumerate() {
        let segment_id = format!("scatter-{}-h{}", query_id, i);
        let frag = SegmentQuery {
            query: query.query.clone(),
            segment_id: segment_id.clone(),
            context: query.context.clone(),
        };
        match h.scatter_query(frag).await {
            Ok(resp) => fragments.push(resp),
            Err(e) => return Err(BrokerError::Upstream(e)),
        }
    }

    Ok(Json(ScatterResponse {
        query_id,
        fragments,
    }))
}

/// Wave 41.OO: real native-query scatter+merge handler.
async fn native_scatter(
    State(state): State<BrokerScatterState>,
    Json(req): Json<BrokerNativeRequest>,
) -> Result<Json<BrokerNativeResponse>, BrokerError> {
    if state.historicals.is_empty() {
        return Err(BrokerError::NoHistoricals);
    }
    if req.segment_ids.is_empty() {
        return Err(BrokerError::MissingSegments);
    }

    let query_id = format!("q-native-{}", Uuid::new_v4());

    // Round-robin assignment of segments to historicals.
    let n_historicals = state.historicals.len();
    let mut fragments: Vec<SegmentQueryResponse> = Vec::with_capacity(req.segment_ids.len());
    for (idx, seg_id) in req.segment_ids.iter().enumerate() {
        let h = &state.historicals[idx % n_historicals];
        match h.native_scatter(seg_id, &req.query).await {
            Ok(resp) => fragments.push(resp),
            Err(e) => return Err(BrokerError::Upstream(e)),
        }
    }

    // Merge per-segment results back into a single tier-wide answer.
    let merged = merge_fragments(&req.query, fragments);

    Ok(Json(BrokerNativeResponse {
        query_id,
        historical_count: n_historicals,
        segment_count: req.segment_ids.len(),
        result: merged,
    }))
}

/// Decode every per-segment `SegmentQueryResponse` into the matching
/// `NativeQueryResult` shape and merge.
fn merge_fragments(query: &NativeQuery, fragments: Vec<SegmentQueryResponse>) -> NativeQueryResult {
    match query {
        NativeQuery::Timeseries(spec) => {
            let parts: Vec<Vec<TimeseriesBucket>> = fragments
                .into_iter()
                .map(decode_timeseries_fragment)
                .collect();
            let aggs: &[Aggregation] = &spec.aggregations;
            NativeQueryResult::Timeseries(merge_timeseries(parts, aggs))
        }
        NativeQuery::Scan(spec) => {
            let parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>> =
                fragments.into_iter().map(decode_scan_fragment).collect();
            NativeQueryResult::Scan(merge_scan(parts, spec.limit))
        }
        NativeQuery::GroupBy(spec) => {
            // GroupBy / TopN fragments share the same row-vector wire
            // shape as `scan` so we reuse the scan decoder.
            let parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>> =
                fragments.into_iter().map(decode_scan_fragment).collect();
            NativeQueryResult::GroupBy(merge_group_by(parts, spec))
        }
        NativeQuery::TopN(spec) => {
            let parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>> =
                fragments.into_iter().map(decode_scan_fragment).collect();
            NativeQueryResult::TopN(merge_top_n(parts, spec))
        }
    }
}

/// Decode a per-segment timeseries fragment from the row-vector wire
/// shape produced by `historical_server::handle_query`.
fn decode_timeseries_fragment(frag: SegmentQueryResponse) -> Vec<TimeseriesBucket> {
    let mut out = Vec::with_capacity(frag.rows.len());
    for row in frag.rows {
        if row.len() != 2 {
            continue;
        }
        let ts = match row[0].as_i64() {
            Some(t) => t,
            None => continue,
        };
        let result = match &row[1] {
            serde_json::Value::Object(m) => m.clone(),
            _ => continue,
        };
        out.push(TimeseriesBucket {
            timestamp_ms: ts,
            result,
        });
    }
    out
}

/// Decode a per-segment scan fragment from the row-vector wire shape.
fn decode_scan_fragment(
    frag: SegmentQueryResponse,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut out = Vec::with_capacity(frag.rows.len());
    for row in frag.rows {
        if let Some(serde_json::Value::Object(m)) = row.into_iter().next() {
            out.push(m);
        }
    }
    out
}

/// Axum response wrapper that maps [`RpcError`] variants and the
/// no-historicals case onto well-known HTTP statuses.
#[derive(Debug)]
enum BrokerError {
    NoHistoricals,
    MissingSegments,
    Upstream(RpcError),
    /// SQL parse error when extracting the FROM table.
    SqlParse(String),
    /// SQL → wire native-query translation failure.
    SqlBridge(TranslateError),
}

impl IntoResponse for BrokerError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            BrokerError::NoHistoricals => (
                StatusCode::SERVICE_UNAVAILABLE,
                "broker has no historicals configured for scatter".to_string(),
            ),
            BrokerError::MissingSegments => (
                StatusCode::BAD_REQUEST,
                "broker native query requires at least one segmentId".to_string(),
            ),
            BrokerError::Upstream(RpcError::Http { status: code, body }) => (
                StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
                body,
            ),
            BrokerError::Upstream(RpcError::Transport(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("transport: {msg}"))
            }
            BrokerError::Upstream(RpcError::Serde(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("serde: {msg}"))
            }
            BrokerError::Upstream(RpcError::NotFound(msg)) => (StatusCode::NOT_FOUND, msg),
            BrokerError::Upstream(RpcError::Custom(msg)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
            BrokerError::SqlParse(msg) => {
                (StatusCode::BAD_REQUEST, format!("sql parse error: {msg}"))
            }
            BrokerError::SqlBridge(e) => {
                (StatusCode::BAD_REQUEST, format!("sql bridge error: {e}"))
            }
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_rpc::MockHistoricalClient;
    use ferrodruid_rpc::native_query::{
        GroupBySpec, ScanSpec, SortDirection, SortSpec, TimeseriesSpec, TopNSpec,
    };

    #[tokio::test]
    async fn broker_scatter_state_from_clients_preserves_order() {
        let m1 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let m2 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = BrokerScatterState::from_clients(
            BrokerServerState::default(),
            vec![Arc::clone(&m1), Arc::clone(&m2)],
        );
        assert_eq!(state.historicals.len(), 2);
        assert!(Arc::ptr_eq(&state.historicals[0], &m1));
        assert!(Arc::ptr_eq(&state.historicals[1], &m2));
    }

    #[tokio::test]
    async fn broker_scatter_dispatches_to_every_historical() {
        let m1 = Arc::new(MockHistoricalClient::new());
        let m2 = Arc::new(MockHistoricalClient::new());
        m1.push_scatter_response(SegmentQueryResponse {
            segment_id: "h1-frag".into(),
            rows: vec![vec![serde_json::Value::String("h1".into())]],
            elapsed_ms: 0,
        });
        m2.push_scatter_response(SegmentQueryResponse {
            segment_id: "h2-frag".into(),
            rows: vec![vec![serde_json::Value::String("h2".into())]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m1.clone(), m2.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        // Drive the scatter path directly (axum is exercised by the
        // integration tests in tests/cross_role_wire.rs).
        let resp = scatter_sql(State(state.clone()), Json(SqlQuery::new("SELECT 1")))
            .await
            .expect("scatter ok");
        assert_eq!(resp.0.fragments.len(), 2);
        assert_eq!(m1.recorded_scatter_queries().len(), 1);
        assert_eq!(m2.recorded_scatter_queries().len(), 1);
        assert_eq!(m1.recorded_scatter_queries()[0].query, "SELECT 1");
    }

    #[tokio::test]
    async fn broker_scatter_with_no_historicals_returns_no_historicals_error() {
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), vec![]);
        let result = scatter_sql(State(state), Json(SqlQuery::new("SELECT 1"))).await;
        match result {
            Ok(_) => panic!("expected NoHistoricals, got Ok"),
            Err(BrokerError::NoHistoricals) => {}
            Err(other) => panic!("unexpected variant: {other:?}"),
        }
    }

    // =====================================================================
    // Wave 41.OO native-query handler tests
    // =====================================================================

    #[tokio::test]
    async fn native_scatter_dispatches_one_fragment_per_segment_round_robin() {
        let m1 = Arc::new(MockHistoricalClient::new());
        let m2 = Arc::new(MockHistoricalClient::new());

        // Three segments → m1 gets segs 0+2, m2 gets seg 1 (round-robin).
        // Each historical returns a single-bucket timeseries fragment.
        for h in [&m1, &m2] {
            for _ in 0..2 {
                h.push_scatter_response(SegmentQueryResponse {
                    segment_id: String::new(),
                    rows: vec![vec![
                        serde_json::Value::Number(1714694400000_i64.into()),
                        serde_json::json!({"rows": 4}),
                    ]],
                    elapsed_ms: 0,
                });
            }
        }

        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m1.clone(), m2.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let req = BrokerNativeRequest {
            segment_ids: vec!["seg-0".into(), "seg-1".into(), "seg-2".into()],
            query: NativeQuery::Timeseries(TimeseriesSpec {
                data_source: "wiki".into(),
                granularity_ms: 0,
                aggregations: vec![Aggregation::Count {
                    name: "rows".into(),
                }],
                filter: None,
            }),
        };
        let resp = native_scatter(State(state.clone()), Json(req))
            .await
            .expect("native ok");
        assert_eq!(resp.0.historical_count, 2);
        assert_eq!(resp.0.segment_count, 3);

        // m1 saw segs 0+2; m2 saw seg 1.
        let m1_seen: Vec<String> = m1
            .recorded_native_scatters()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let m2_seen: Vec<String> = m2
            .recorded_native_scatters()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(m1_seen, vec!["seg-0".to_string(), "seg-2".to_string()]);
        assert_eq!(m2_seen, vec!["seg-1".to_string()]);

        // Merge collapsed three fragments (each `{rows: 4}`) into a
        // single bucket with `rows = 12`.
        match resp.0.result {
            NativeQueryResult::Timeseries(buckets) => {
                assert_eq!(buckets.len(), 1);
                assert_eq!(buckets[0].timestamp_ms, 1714694400000);
                assert_eq!(buckets[0].result.get("rows"), Some(&serde_json::json!(12)));
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_scatter_with_no_historicals_returns_503() {
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), vec![]);
        let req = BrokerNativeRequest {
            segment_ids: vec!["seg-0".into()],
            query: NativeQuery::Scan(ScanSpec {
                data_source: "wiki".into(),
                columns: None,
                limit: None,
                filter: None,
            }),
        };
        let result = native_scatter(State(state), Json(req)).await;
        match result {
            Err(BrokerError::NoHistoricals) => {}
            other => panic!("expected NoHistoricals, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_scatter_with_no_segments_returns_400() {
        let m = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), vec![m]);
        let req = BrokerNativeRequest {
            segment_ids: vec![],
            query: NativeQuery::Scan(ScanSpec {
                data_source: "wiki".into(),
                columns: None,
                limit: None,
                filter: None,
            }),
        };
        let result = native_scatter(State(state), Json(req)).await;
        match result {
            Err(BrokerError::MissingSegments) => {}
            other => panic!("expected MissingSegments, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_scatter_merges_scan_rows_with_caller_limit() {
        let m = Arc::new(MockHistoricalClient::new());
        // Two segments, each returns 2 rows → 4 total but limit=3.
        for _ in 0..2 {
            m.push_scatter_response(SegmentQueryResponse {
                segment_id: String::new(),
                rows: vec![
                    vec![serde_json::json!({"page": "home"})],
                    vec![serde_json::json!({"page": "about"})],
                ],
                elapsed_ms: 0,
            });
        }
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let req = BrokerNativeRequest {
            segment_ids: vec!["seg-0".into(), "seg-1".into()],
            query: NativeQuery::Scan(ScanSpec {
                data_source: "wiki".into(),
                columns: None,
                limit: Some(3),
                filter: None,
            }),
        };
        let resp = native_scatter(State(state), Json(req))
            .await
            .expect("native ok");
        match resp.0.result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 3, "limit=3 caps merged result");
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    // =====================================================================
    // Wave 42.RR — groupBy + topN broker scatter+merge tests
    // =====================================================================

    #[tokio::test]
    async fn native_scatter_merges_group_by_across_segments() {
        let m = Arc::new(MockHistoricalClient::new());
        // Segment 1: home=3, about=1.
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![
                vec![serde_json::json!({"page":"home","total":3})],
                vec![serde_json::json!({"page":"about","total":1})],
            ],
            elapsed_ms: 0,
        });
        // Segment 2: home=7 (cluster total home=10).
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![vec![serde_json::json!({"page":"home","total":7})]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let req = BrokerNativeRequest {
            segment_ids: vec!["seg-0".into(), "seg-1".into()],
            query: NativeQuery::GroupBy(GroupBySpec {
                data_source: "wiki".into(),
                dimensions: vec!["page".into()],
                aggregations: vec![Aggregation::LongSum {
                    name: "total".into(),
                    field_name: "count".into(),
                }],
                filter: None,
                having: None,
                sort: Some(vec![SortSpec {
                    dimension: "total".into(),
                    direction: SortDirection::Descending,
                }]),
                limit: None,
            }),
        };
        let resp = native_scatter(State(state), Json(req))
            .await
            .expect("groupBy ok");
        match resp.0.result {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(10)));
                assert_eq!(rows[1].get("page"), Some(&serde_json::json!("about")));
                assert_eq!(rows[1].get("total"), Some(&serde_json::json!(1)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_scatter_merges_top_n_with_global_re_rank() {
        let m = Arc::new(MockHistoricalClient::new());
        // Segment 1's local winner is "a"=5, but cluster-wide "b" wins.
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![
                vec![serde_json::json!({"page":"a","total":5})],
                vec![serde_json::json!({"page":"b","total":4})],
            ],
            elapsed_ms: 0,
        });
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![vec![serde_json::json!({"page":"b","total":10})]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let req = BrokerNativeRequest {
            segment_ids: vec!["seg-0".into(), "seg-1".into()],
            query: NativeQuery::TopN(TopNSpec {
                data_source: "wiki".into(),
                dimension: "page".into(),
                aggregations: vec![Aggregation::LongSum {
                    name: "total".into(),
                    field_name: "count".into(),
                }],
                metric: "total".into(),
                threshold: 1,
                filter: None,
            }),
        };
        let resp = native_scatter(State(state), Json(req))
            .await
            .expect("topN ok");
        match resp.0.result {
            NativeQueryResult::TopN(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("b")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(14)));
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    // =====================================================================
    // Wave 43.TT — POST /druid/v2/sql SQL→native bridge handler tests
    // =====================================================================

    #[tokio::test]
    async fn sql_bridge_count_star_to_timeseries_returns_merged_count() {
        let m = Arc::new(MockHistoricalClient::new());
        // One historical, one synthetic segment → broker fans out a
        // single fragment that produces a 1-bucket count=7 response.
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![vec![
                serde_json::Value::Number(0_i64.into()),
                serde_json::json!({"cnt": 7}),
            ]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let resp = sql_bridge_handler(
            State(state),
            Json(SqlQuery::new("SELECT COUNT(*) AS cnt FROM wikipedia")),
        )
        .await
        .expect("sql bridge ok");

        assert!(resp.0.query_id.starts_with("q-sql-"));
        // Output column for COUNT(*) AS cnt — single column.
        assert_eq!(resp.0.columns, vec!["cnt".to_string()]);
        // Single row with the merged count.
        assert_eq!(resp.0.rows.len(), 1);
        assert_eq!(resp.0.rows[0][0], serde_json::json!(7));

        // The historical received exactly one native_scatter call
        // carrying a Timeseries query body.
        let scatters = m.recorded_native_scatters();
        assert_eq!(scatters.len(), 1);
        assert!(matches!(scatters[0].1, NativeQuery::Timeseries(_)));
    }

    #[tokio::test]
    async fn sql_bridge_select_star_to_scan_passes_columns_and_rows() {
        let m = Arc::new(MockHistoricalClient::new());
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![
                vec![serde_json::json!({"page": "home", "country": "us"})],
                vec![serde_json::json!({"page": "about", "country": "us"})],
            ],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let resp = sql_bridge_handler(
            State(state),
            Json(SqlQuery::new("SELECT page, country FROM wikipedia")),
        )
        .await
        .expect("sql bridge ok");
        assert_eq!(
            resp.0.columns,
            vec!["page".to_string(), "country".to_string()]
        );
        assert_eq!(resp.0.rows.len(), 2);
        assert_eq!(resp.0.rows[0][0], serde_json::json!("home"));
        assert_eq!(resp.0.rows[1][0], serde_json::json!("about"));

        let scatters = m.recorded_native_scatters();
        assert_eq!(scatters.len(), 1);
        assert!(matches!(scatters[0].1, NativeQuery::Scan(_)));
    }

    #[tokio::test]
    async fn sql_bridge_group_by_dimension_merges_across_segments() {
        // Two synthetic segments → broker reuses one historical
        // round-robin twice when the request supplies two segment ids
        // in context.
        let m = Arc::new(MockHistoricalClient::new());
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![
                vec![serde_json::json!({"page": "home", "cnt": 3})],
                vec![serde_json::json!({"page": "about", "cnt": 1})],
            ],
            elapsed_ms: 0,
        });
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![vec![serde_json::json!({"page": "home", "cnt": 7})]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let mut req = SqlQuery::new("SELECT page, COUNT(*) AS cnt FROM wikipedia GROUP BY page");
        req.context = Some(serde_json::json!({"segmentIds": ["seg-0", "seg-1"]}));

        let resp = sql_bridge_handler(State(state), Json(req))
            .await
            .expect("sql bridge ok");
        assert_eq!(resp.0.columns, vec!["page".to_string(), "cnt".to_string()]);
        // Merged: home=10, about=1 (order is encounter-order from the
        // merge_group_by reducer).
        assert_eq!(resp.0.rows.len(), 2);
        let by_page: std::collections::HashMap<String, i64> = resp
            .0
            .rows
            .iter()
            .filter_map(|r| {
                let page = r.first()?.as_str()?.to_string();
                let cnt = r.get(1)?.as_i64()?;
                Some((page, cnt))
            })
            .collect();
        assert_eq!(by_page.get("home"), Some(&10));
        assert_eq!(by_page.get("about"), Some(&1));
    }

    #[tokio::test]
    async fn sql_bridge_top_n_selects_global_winner() {
        let m = Arc::new(MockHistoricalClient::new());
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![
                vec![serde_json::json!({"page": "a", "cnt": 5})],
                vec![serde_json::json!({"page": "b", "cnt": 4})],
            ],
            elapsed_ms: 0,
        });
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![vec![serde_json::json!({"page": "b", "cnt": 10})]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let mut req = SqlQuery::new(
            "SELECT page, COUNT(*) AS cnt FROM wikipedia \
             GROUP BY page ORDER BY cnt DESC LIMIT 1",
        );
        req.context = Some(serde_json::json!({"segmentIds": ["seg-0", "seg-1"]}));

        let resp = sql_bridge_handler(State(state), Json(req))
            .await
            .expect("sql bridge ok");
        assert_eq!(resp.0.rows.len(), 1);
        assert_eq!(resp.0.rows[0][0], serde_json::json!("b"));
        assert_eq!(resp.0.rows[0][1], serde_json::json!(14));
    }

    #[tokio::test]
    async fn sql_bridge_no_historicals_returns_503() {
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), vec![]);
        let result =
            sql_bridge_handler(State(state), Json(SqlQuery::new("SELECT * FROM wikipedia"))).await;
        match result {
            Err(BrokerError::NoHistoricals) => {}
            other => panic!("expected NoHistoricals, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sql_bridge_malformed_sql_returns_400() {
        let m = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), vec![m]);
        let result =
            sql_bridge_handler(State(state), Json(SqlQuery::new("THIS IS NOT VALID SQL"))).await;
        match result {
            Err(BrokerError::SqlParse(_)) | Err(BrokerError::SqlBridge(_)) => {}
            other => panic!("expected SqlParse / SqlBridge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sql_bridge_unsupported_filter_returns_400() {
        let m = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), vec![m]);
        // LIKE filter is not on the wire surface yet → bridge error.
        let result = sql_bridge_handler(
            State(state),
            Json(SqlQuery::new(
                "SELECT * FROM wikipedia WHERE page LIKE 'A%'",
            )),
        )
        .await;
        match result {
            Err(BrokerError::SqlBridge(TranslateError::UnsupportedFilter(_))) => {}
            other => panic!("expected SqlBridge::UnsupportedFilter, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sql_bridge_explicit_segment_ids_round_robin_across_historicals() {
        let m1 = Arc::new(MockHistoricalClient::new());
        let m2 = Arc::new(MockHistoricalClient::new());
        for h in [&m1, &m2] {
            for _ in 0..2 {
                h.push_scatter_response(SegmentQueryResponse {
                    segment_id: String::new(),
                    rows: vec![vec![
                        serde_json::Value::Number(0_i64.into()),
                        serde_json::json!({"cnt": 1}),
                    ]],
                    elapsed_ms: 0,
                });
            }
        }
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m1.clone(), m2.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let mut req = SqlQuery::new("SELECT COUNT(*) AS cnt FROM wikipedia");
        req.context = Some(serde_json::json!({
            "segmentIds": ["a", "b", "c", "d"],
        }));

        let resp = sql_bridge_handler(State(state), Json(req))
            .await
            .expect("ok");
        assert_eq!(resp.0.rows.len(), 1);
        // 4 fragments × 1 = 4.
        assert_eq!(resp.0.rows[0][0], serde_json::json!(4));

        // m1 saw a, c; m2 saw b, d (round-robin).
        let m1_seen: Vec<String> = m1
            .recorded_native_scatters()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let m2_seen: Vec<String> = m2
            .recorded_native_scatters()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(m1_seen, vec!["a".to_string(), "c".to_string()]);
        assert_eq!(m2_seen, vec!["b".to_string(), "d".to_string()]);
    }

    #[tokio::test]
    async fn sql_bridge_synthesises_one_segment_per_historical_when_context_missing() {
        let m1 = Arc::new(MockHistoricalClient::new());
        let m2 = Arc::new(MockHistoricalClient::new());
        for h in [&m1, &m2] {
            h.push_scatter_response(SegmentQueryResponse {
                segment_id: String::new(),
                rows: vec![vec![
                    serde_json::Value::Number(0_i64.into()),
                    serde_json::json!({"cnt": 5}),
                ]],
                elapsed_ms: 0,
            });
        }
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m1.clone(), m2.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let resp = sql_bridge_handler(
            State(state),
            Json(SqlQuery::new("SELECT COUNT(*) AS cnt FROM wikipedia")),
        )
        .await
        .expect("ok");
        // Two historicals → two synthetic segments → cnt=5+5=10.
        assert_eq!(resp.0.rows[0][0], serde_json::json!(10));
        assert_eq!(m1.recorded_native_scatters().len(), 1);
        assert_eq!(m2.recorded_native_scatters().len(), 1);
    }

    #[tokio::test]
    async fn sql_bridge_time_floor_emits_timestamp_first_column() {
        let m = Arc::new(MockHistoricalClient::new());
        m.push_scatter_response(SegmentQueryResponse {
            segment_id: String::new(),
            rows: vec![vec![
                serde_json::Value::Number(1714694400000_i64.into()),
                serde_json::json!({"cnt": 3}),
            ]],
            elapsed_ms: 0,
        });
        let clients: Vec<Arc<dyn HistoricalClient>> = vec![m.clone()];
        let state = BrokerScatterState::from_clients(BrokerServerState::default(), clients);

        let resp = sql_bridge_handler(
            State(state),
            Json(SqlQuery::new(
                "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS cnt \
                 FROM wikipedia GROUP BY 1",
            )),
        )
        .await
        .expect("ok");

        assert_eq!(resp.0.columns, vec!["t".to_string(), "cnt".to_string()]);
        assert_eq!(resp.0.rows.len(), 1);
        // First column is the bucket-floor timestamp in ms.
        assert_eq!(resp.0.rows[0][0], serde_json::json!(1714694400000_i64));
        // Second column is the count.
        assert_eq!(resp.0.rows[0][1], serde_json::json!(3));

        // The historical received a Timeseries query body with
        // hour granularity.
        let scatters = m.recorded_native_scatters();
        if let NativeQuery::Timeseries(spec) = &scatters[0].1 {
            assert_eq!(spec.granularity_ms, 3_600_000);
        } else {
            panic!("expected Timeseries");
        }
    }
}
