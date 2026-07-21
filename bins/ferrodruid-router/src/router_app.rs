// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Router application logic — kept in its own module so it can be
//! exercised by unit tests with a [`MockBrokerClient`] swapped in for
//! the real HTTP transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use ferrodruid_rpc::{BrokerClient, HttpBrokerClient, RpcError, SqlQuery, SqlResponse};
use serde::Serialize;

/// Shared state held by the router HTTP server.
#[derive(Clone)]
pub struct RouterState {
    brokers: Arc<Vec<Arc<dyn BrokerClient>>>,
    next: Arc<AtomicUsize>,
}

impl RouterState {
    /// Build a state from broker base URLs. One [`HttpBrokerClient`]
    /// is constructed per URL.
    ///
    /// Pre-W1-I production path. Today's binary entry point goes via
    /// [`Self::from_broker_urls_with_client`]; kept here for tests +
    /// external callers.
    ///
    /// # Errors
    ///
    /// Propagates [`RpcError`] if any underlying client fails to
    /// build.
    #[allow(dead_code)] // Kept for tests / non-binary callers.
    pub fn from_broker_urls(urls: &[String]) -> Result<Self, RpcError> {
        let mut brokers: Vec<Arc<dyn BrokerClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpBrokerClient::try_new(u.clone())?;
            brokers.push(Arc::new(c));
        }
        Ok(Self {
            brokers: Arc::new(brokers),
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// W1-I (CL-J1): build a state where every per-broker
    /// `HttpBrokerClient` reuses the supplied pre-built `reqwest::Client`.
    /// Lets the binary load TLS credentials once at start-up and share
    /// them across every outbound peer.
    #[must_use]
    pub fn from_broker_urls_with_client(urls: &[String], http: reqwest::Client) -> Self {
        let mut brokers: Vec<Arc<dyn BrokerClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpBrokerClient::with_client(u.clone(), http.clone());
            brokers.push(Arc::new(c));
        }
        Self {
            brokers: Arc::new(brokers),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Build a state from already-constructed broker clients. Used
    /// by tests that want to inject a [`MockBrokerClient`].
    #[must_use]
    #[allow(dead_code)] // Test-only helper; production path goes via `from_broker_urls`.
    pub fn from_clients(brokers: Vec<Arc<dyn BrokerClient>>) -> Self {
        Self {
            brokers: Arc::new(brokers),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Pick the next broker via round-robin. Returns `None` when no
    /// brokers are configured (the binary entry point already
    /// rejects this case, so `None` is a defensive belt).
    fn select(&self) -> Option<Arc<dyn BrokerClient>> {
        if self.brokers.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.brokers.len();
        self.brokers.get(idx).cloned()
    }
}

/// Build the axum [`Router`] for the router binary.
pub fn build_router(state: RouterState) -> Router {
    Router::new()
        .route("/druid/v2/sql", post(forward_sql))
        .route("/druid/router/v1/health", get(health))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    brokers: usize,
}

async fn health(State(state): State<RouterState>) -> Json<HealthBody> {
    Json(HealthBody {
        status: "ok",
        brokers: state.brokers.len(),
    })
}

async fn forward_sql(
    State(state): State<RouterState>,
    Json(query): Json<SqlQuery>,
) -> Result<Json<SqlResponse>, RouterError> {
    let Some(client) = state.select() else {
        return Err(RouterError::NoBrokers);
    };
    match client.query(query).await {
        Ok(resp) => Ok(Json(resp)),
        Err(e) => Err(RouterError::Upstream(e)),
    }
}

/// Axum response wrapper that maps [`RpcError`] variants and the
/// no-brokers case onto well-known HTTP statuses.
#[derive(Debug)]
enum RouterError {
    NoBrokers,
    Upstream(RpcError),
}

impl IntoResponse for RouterError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            RouterError::NoBrokers => (
                StatusCode::SERVICE_UNAVAILABLE,
                "router has no upstream brokers configured".to_string(),
            ),
            RouterError::Upstream(RpcError::Http { status: code, body }) => (
                StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
                body,
            ),
            RouterError::Upstream(RpcError::Transport(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("transport: {msg}"))
            }
            RouterError::Upstream(RpcError::Serde(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("serde: {msg}"))
            }
            RouterError::Upstream(RpcError::NotFound(msg)) => (StatusCode::NOT_FOUND, msg),
            RouterError::Upstream(RpcError::Custom(msg)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_rpc::MockBrokerClient;

    #[tokio::test]
    async fn router_state_round_robins_broker_selection() {
        let m1 = Arc::new(MockBrokerClient::new()) as Arc<dyn BrokerClient>;
        let m2 = Arc::new(MockBrokerClient::new()) as Arc<dyn BrokerClient>;
        let state = RouterState::from_clients(vec![Arc::clone(&m1), Arc::clone(&m2)]);

        // 4 selections cycle through broker-0, broker-1, broker-0,
        // broker-1. We verify by Arc::ptr_eq on the underlying
        // clients.
        let s0 = state.select().expect("first broker");
        let s1 = state.select().expect("second broker");
        let s2 = state.select().expect("third broker");
        let s3 = state.select().expect("fourth broker");
        assert!(Arc::ptr_eq(&s0, &m1));
        assert!(Arc::ptr_eq(&s1, &m2));
        assert!(Arc::ptr_eq(&s2, &m1));
        assert!(Arc::ptr_eq(&s3, &m2));
    }

    #[tokio::test]
    async fn router_state_select_with_no_brokers_returns_none() {
        let state = RouterState::from_clients(vec![]);
        assert!(state.select().is_none());
    }

    #[tokio::test]
    async fn forward_sql_dispatches_to_chosen_broker() {
        let mock = Arc::new(MockBrokerClient::new());
        mock.push_response(SqlResponse {
            query_id: "q-router-test".into(),
            columns: vec!["echo".into()],
            rows: vec![vec![serde_json::Value::String("SELECT 1".into())]],
            elapsed_ms: 0,
        });
        let state = RouterState::from_clients(vec![mock.clone() as Arc<dyn BrokerClient>]);

        // Drive the handler through axum's `.oneshot` wouldn't work
        // without `tower::ServiceExt` — instead, exercise the public
        // RouterState path directly to keep test deps minimal.
        let client = state.select().expect("broker");
        let resp = client
            .query(SqlQuery::new("SELECT 1"))
            .await
            .expect("forward");
        assert_eq!(resp.query_id, "q-router-test");
        assert_eq!(mock.recorded_queries().len(), 1);
        assert_eq!(mock.recorded_queries()[0].query, "SELECT 1");
    }
}
