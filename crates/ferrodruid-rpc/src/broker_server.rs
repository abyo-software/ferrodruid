// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Callee-side axum router for the broker role.
//!
//! Wave 39.HH ships a **canned echo** SQL handler so the cross-process
//! wire is observable end to end. The handler returns a single-row
//! `SqlResponse` whose first column echoes the incoming `query`
//! string. Real query execution against `ferrodruid-query` lands in
//! W4.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::{get, post};
use uuid::Uuid;

use crate::types::{BrokerInfo, SqlQuery, SqlResponse};

/// Per-server broker identity used to populate `BrokerInfo`.
#[derive(Debug, Clone)]
pub struct BrokerServerState {
    /// Cluster-unique broker identifier. Generated fresh at boot if
    /// the operator does not pin it.
    pub broker_id: String,
    /// Tier label this broker serves (e.g. `"default"`, `"hot"`).
    pub tier: String,
    /// FerroDruid version banner.
    pub version: String,
}

impl Default for BrokerServerState {
    fn default() -> Self {
        Self {
            broker_id: format!("broker-{}", Uuid::new_v4()),
            tier: "default".into(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Build the axum [`Router`] the broker binary mounts on its HTTP
/// server. Routes:
///
/// - `POST /druid/v2/sql` — accept a [`SqlQuery`], return a canned
///   [`SqlResponse`].
/// - `GET /druid/v2/info` — return [`BrokerInfo`] populated from
///   `state`.
pub fn router(state: BrokerServerState) -> Router {
    Router::new()
        .route("/druid/v2/sql", post(handle_sql))
        .route("/druid/v2/info", get(handle_info))
        .with_state(Arc::new(state))
}

async fn handle_sql(
    State(_state): State<Arc<BrokerServerState>>,
    Json(query): Json<SqlQuery>,
) -> Json<SqlResponse> {
    let query_id = format!("q-{}", Uuid::new_v4());
    tracing::debug!(query_id = %query_id, query = %query.query, "broker received sql");
    let echo = serde_json::Value::String(query.query.clone());
    let resp = SqlResponse {
        query_id,
        columns: vec!["echo".to_string()],
        rows: vec![vec![echo]],
        elapsed_ms: 0,
    };
    Json(resp)
}

async fn handle_info(State(state): State<Arc<BrokerServerState>>) -> Json<BrokerInfo> {
    Json(BrokerInfo {
        version: state.version.clone(),
        role: "broker".to_string(),
        tier: state.tier.clone(),
        broker_id: state.broker_id.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker_client::{BrokerClient, HttpBrokerClient};
    use crate::types::SqlQuery;

    async fn spawn(state: BrokerServerState) -> String {
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn sql_endpoint_echoes_query_and_assigns_id() {
        let url = spawn(BrokerServerState::default()).await;
        let client = HttpBrokerClient::try_new(&url).expect("client builds");
        let resp = client
            .query(SqlQuery::new("SELECT 42"))
            .await
            .expect("query returns");
        assert!(resp.query_id.starts_with("q-"));
        assert_eq!(resp.columns, vec!["echo".to_string()]);
        assert_eq!(resp.rows.len(), 1);
        assert_eq!(
            resp.rows[0][0],
            serde_json::Value::String("SELECT 42".into())
        );
    }

    #[tokio::test]
    async fn info_endpoint_returns_role_broker() {
        let state = BrokerServerState {
            broker_id: "broker-test".into(),
            tier: "hot".into(),
            version: "0.0.0".into(),
        };
        let url = spawn(state).await;
        let client = HttpBrokerClient::try_new(&url).expect("client builds");
        let info = client.info().await.expect("info returns");
        assert_eq!(info.role, "broker");
        assert_eq!(info.tier, "hot");
        assert_eq!(info.broker_id, "broker-test");
    }
}
