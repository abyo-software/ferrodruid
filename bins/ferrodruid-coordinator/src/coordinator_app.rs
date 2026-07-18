// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Coordinator application logic — kept in its own module so it can
//! be exercised by unit tests with a `MockHistoricalClient` swapped
//! in for the real HTTP transport.
//!
//! Wave 40.LL wires the coordinator → historical hop. The coordinator
//! exposes:
//!
//! - `POST /druid/coordinator/v1/loadqueue/{historical}` — load a
//!   segment on a specific historical (selected by index in the
//!   configured `--historical-url` list).
//! - `POST /druid/coordinator/v1/dropqueue/{historical}` — drop a
//!   segment on a specific historical.
//! - `GET /druid/coordinator/v1/loadstatus` — aggregated load status
//!   across every configured historical.
//! - `GET /druid/coordinator/v1/health` — simple health probe.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use ferrodruid_rpc::{
    HistoricalClient, HttpHistoricalClient, LoadStatusReport, RpcError, SegmentDropCommand,
    SegmentLoadCommand, SegmentLoadState,
};
use serde::Serialize;

/// Shared state held by the coordinator HTTP server.
#[derive(Clone)]
pub struct CoordinatorState {
    historicals: Arc<Vec<Arc<dyn HistoricalClient>>>,
}

impl CoordinatorState {
    /// Build a state from historical base URLs. One
    /// [`HttpHistoricalClient`] is constructed per URL.
    ///
    /// Pre-W1-I production path; today's binary entry point goes via
    /// [`Self::from_historical_urls_with_client`].
    ///
    /// # Errors
    ///
    /// Propagates [`RpcError`] if any underlying client fails to build.
    #[allow(dead_code)] // Kept for tests / non-binary callers.
    pub fn from_historical_urls(urls: &[String]) -> Result<Self, RpcError> {
        let mut clients: Vec<Arc<dyn HistoricalClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpHistoricalClient::try_new(u.clone())?;
            clients.push(Arc::new(c));
        }
        Ok(Self {
            historicals: Arc::new(clients),
        })
    }

    /// W1-I (CL-J1): build a state where every per-historical
    /// `HttpHistoricalClient` reuses the supplied pre-built
    /// `reqwest::Client` (typically TLS-aware).
    #[must_use]
    pub fn from_historical_urls_with_client(urls: &[String], http: reqwest::Client) -> Self {
        let mut clients: Vec<Arc<dyn HistoricalClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpHistoricalClient::with_client(u.clone(), http.clone());
            clients.push(Arc::new(c));
        }
        Self {
            historicals: Arc::new(clients),
        }
    }

    /// Build a state from already-constructed historical clients.
    /// Used by tests that want to inject a [`ferrodruid_rpc::MockHistoricalClient`].
    #[must_use]
    #[allow(dead_code)] // Test-only helper.
    pub fn from_clients(clients: Vec<Arc<dyn HistoricalClient>>) -> Self {
        Self {
            historicals: Arc::new(clients),
        }
    }

    /// Number of configured historicals. Used for the health probe
    /// + tests.
    #[must_use]
    #[allow(dead_code)] // used by tests; production reads len() inline.
    pub fn historical_count(&self) -> usize {
        self.historicals.len()
    }
}

/// Build the axum [`Router`] for the coordinator binary.
pub fn build_router(state: CoordinatorState) -> Router {
    Router::new()
        .route(
            "/druid/coordinator/v1/loadqueue/{historical}",
            post(load_segment),
        )
        .route(
            "/druid/coordinator/v1/dropqueue/{historical}",
            post(drop_segment),
        )
        .route(
            "/druid/coordinator/v1/loadstatus",
            get(aggregated_load_status),
        )
        .route("/druid/coordinator/v1/health", get(health))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    historicals: usize,
}

async fn health(State(state): State<CoordinatorState>) -> Json<HealthBody> {
    Json(HealthBody {
        status: "ok",
        historicals: state.historicals.len(),
    })
}

async fn load_segment(
    State(state): State<CoordinatorState>,
    Path(historical_idx): Path<String>,
    Json(cmd): Json<SegmentLoadCommand>,
) -> Result<Json<LoadStatusReport>, CoordinatorError> {
    let client = pick_historical(&state, &historical_idx)?;
    match client.load_segment(cmd).await {
        Ok(report) => Ok(Json(report)),
        Err(e) => Err(CoordinatorError::Upstream(e)),
    }
}

async fn drop_segment(
    State(state): State<CoordinatorState>,
    Path(historical_idx): Path<String>,
    Json(cmd): Json<SegmentDropCommand>,
) -> Result<Json<LoadStatusReport>, CoordinatorError> {
    let client = pick_historical(&state, &historical_idx)?;
    match client.drop_segment(cmd).await {
        Ok(report) => Ok(Json(report)),
        Err(e) => Err(CoordinatorError::Upstream(e)),
    }
}

/// Aggregated load-status response: `historical_idx -> { segment_id ->
/// SegmentLoadState }`.
async fn aggregated_load_status(
    State(state): State<CoordinatorState>,
) -> Result<Json<HashMap<String, HashMap<String, SegmentLoadState>>>, CoordinatorError> {
    let mut out = HashMap::new();
    for (i, h) in state.historicals.iter().enumerate() {
        match h.load_status().await {
            Ok(table) => {
                out.insert(format!("historical-{i}"), table);
            }
            Err(e) => return Err(CoordinatorError::Upstream(e)),
        }
    }
    Ok(Json(out))
}

fn pick_historical(
    state: &CoordinatorState,
    target: &str,
) -> Result<Arc<dyn HistoricalClient>, CoordinatorError> {
    if state.historicals.is_empty() {
        return Err(CoordinatorError::NoHistoricals);
    }
    // Accept either a 0-based index, or `historical-<idx>` for clients
    // that mirror the load-status key shape.
    let idx = if let Some(rest) = target.strip_prefix("historical-") {
        rest.parse::<usize>().ok()
    } else {
        target.parse::<usize>().ok()
    };
    let idx = idx.ok_or_else(|| {
        CoordinatorError::Upstream(RpcError::NotFound(format!(
            "historical id {target} is not a recognised index"
        )))
    })?;
    state.historicals.get(idx).cloned().ok_or_else(|| {
        CoordinatorError::Upstream(RpcError::NotFound(format!(
            "historical index {idx} out of range (count={})",
            state.historicals.len()
        )))
    })
}

/// Axum response wrapper that maps [`RpcError`] variants and the
/// no-historicals case onto well-known HTTP statuses.
#[derive(Debug)]
enum CoordinatorError {
    NoHistoricals,
    Upstream(RpcError),
}

impl IntoResponse for CoordinatorError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            CoordinatorError::NoHistoricals => (
                StatusCode::SERVICE_UNAVAILABLE,
                "coordinator has no historicals configured".to_string(),
            ),
            CoordinatorError::Upstream(RpcError::Http { status: code, body }) => (
                StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
                body,
            ),
            CoordinatorError::Upstream(RpcError::Transport(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("transport: {msg}"))
            }
            CoordinatorError::Upstream(RpcError::Serde(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("serde: {msg}"))
            }
            CoordinatorError::Upstream(RpcError::NotFound(msg)) => (StatusCode::NOT_FOUND, msg),
            CoordinatorError::Upstream(RpcError::Custom(msg)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_rpc::MockHistoricalClient;

    #[tokio::test]
    async fn coordinator_state_from_clients_preserves_count() {
        let m1 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let m2 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = CoordinatorState::from_clients(vec![m1, m2]);
        assert_eq!(state.historical_count(), 2);
    }

    #[tokio::test]
    async fn pick_historical_accepts_numeric_index() {
        let m1 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = CoordinatorState::from_clients(vec![Arc::clone(&m1)]);
        let picked = pick_historical(&state, "0").expect("idx 0");
        assert!(Arc::ptr_eq(&picked, &m1));
    }

    #[tokio::test]
    async fn pick_historical_accepts_historical_prefix() {
        let m1 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = CoordinatorState::from_clients(vec![Arc::clone(&m1)]);
        let picked = pick_historical(&state, "historical-0").expect("idx 0 with prefix");
        assert!(Arc::ptr_eq(&picked, &m1));
    }

    #[tokio::test]
    async fn pick_historical_rejects_out_of_range() {
        let m1 = Arc::new(MockHistoricalClient::new()) as Arc<dyn HistoricalClient>;
        let state = CoordinatorState::from_clients(vec![m1]);
        let result = pick_historical(&state, "3");
        match result {
            Ok(_) => panic!("expected NotFound, got Ok"),
            Err(CoordinatorError::Upstream(RpcError::NotFound(_))) => {}
            Err(other) => panic!("unexpected variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn coordinator_load_segment_records_command_on_chosen_historical() {
        let m0 = Arc::new(MockHistoricalClient::new());
        let m1 = Arc::new(MockHistoricalClient::new());
        m1.push_load_response(LoadStatusReport {
            segment_id: "seg-1".into(),
            state: SegmentLoadState::Loaded,
            message: "ack".into(),
        });
        let state = CoordinatorState::from_clients(vec![m0.clone(), m1.clone()]);
        let cmd = SegmentLoadCommand::new("seg-1", "ds-A", "deepstore://A/seg-1");

        let report = load_segment(
            State(state.clone()),
            Path("1".to_string()),
            Json(cmd.clone()),
        )
        .await
        .expect("load ok");
        assert_eq!(report.0.state, SegmentLoadState::Loaded);
        assert_eq!(m0.recorded_load_commands().len(), 0);
        assert_eq!(m1.recorded_load_commands().len(), 1);
        assert_eq!(m1.recorded_load_commands()[0].segment_id, "seg-1");
    }

    #[tokio::test]
    async fn coordinator_drop_segment_records_command_on_chosen_historical() {
        let m0 = Arc::new(MockHistoricalClient::new());
        let state = CoordinatorState::from_clients(vec![m0.clone()]);
        let report = drop_segment(
            State(state),
            Path("0".to_string()),
            Json(SegmentDropCommand::new("seg-A")),
        )
        .await
        .expect("drop ok");
        assert_eq!(report.0.state, SegmentLoadState::Dropped);
        assert_eq!(m0.recorded_drop_commands().len(), 1);
        assert_eq!(m0.recorded_drop_commands()[0].segment_id, "seg-A");
    }

    #[tokio::test]
    async fn coordinator_aggregated_load_status_merges_per_historical_tables() {
        let m0 = Arc::new(MockHistoricalClient::new());
        let m1 = Arc::new(MockHistoricalClient::new());
        m0.set_load_state("seg-A", SegmentLoadState::Loaded);
        m1.set_load_state("seg-B", SegmentLoadState::Loading);
        let state = CoordinatorState::from_clients(vec![m0, m1]);
        let agg = aggregated_load_status(State(state)).await.expect("agg ok");
        let h0 = agg.0.get("historical-0").expect("h0");
        let h1 = agg.0.get("historical-1").expect("h1");
        assert_eq!(h0.get("seg-A"), Some(&SegmentLoadState::Loaded));
        assert_eq!(h1.get("seg-B"), Some(&SegmentLoadState::Loading));
    }

    #[tokio::test]
    async fn coordinator_with_no_historicals_returns_no_historicals_error() {
        let state = CoordinatorState::from_clients(vec![]);
        let result = pick_historical(&state, "0");
        match result {
            Ok(_) => panic!("expected NoHistoricals, got Ok"),
            Err(CoordinatorError::NoHistoricals) => {}
            Err(other) => panic!("unexpected variant: {other:?}"),
        }
    }
}
