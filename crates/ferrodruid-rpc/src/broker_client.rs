// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Caller-side trait + implementations for the router → broker hop.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::RpcError;
use crate::types::{BrokerInfo, SqlQuery, SqlResponse};

/// Caller-side abstraction used by the router to forward queries to a
/// broker.
///
/// The trait is `async-trait` rather than a hand-rolled future so the
/// router binary can store `Arc<dyn BrokerClient>` and swap in either
/// the [`HttpBrokerClient`] (production) or [`MockBrokerClient`]
/// (tests) without monomorphisation pressure.
#[async_trait]
pub trait BrokerClient: Send + Sync + 'static {
    /// Forward a SQL query to the broker and return its response.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] for network failures,
    /// [`RpcError::Http`] for non-2xx responses, [`RpcError::Serde`]
    /// for body decode failures.
    async fn query(&self, sql: SqlQuery) -> Result<SqlResponse, RpcError>;

    /// Fetch broker introspection metadata
    /// (`GET /druid/v2/info`). Used by the router to validate a
    /// broker's tier before forwarding.
    ///
    /// # Errors
    ///
    /// Same surface as [`BrokerClient::query`].
    async fn info(&self) -> Result<BrokerInfo, RpcError>;
}

/// Real HTTP implementation backed by `reqwest`. Targets the broker's
/// `/druid/v2/sql` and `/druid/v2/info` endpoints.
#[derive(Debug, Clone)]
pub struct HttpBrokerClient {
    base_url: String,
    http: reqwest::Client,
}

impl HttpBrokerClient {
    /// Construct a client targeting `base_url`. The base URL must
    /// include scheme and host (e.g. `http://127.0.0.1:8082`); any
    /// trailing slash is stripped so endpoint paths concatenate
    /// cleanly.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] if the underlying `reqwest`
    /// client cannot be built (e.g. no TLS backend available).
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, RpcError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| RpcError::Transport(format!("build reqwest: {e}")))?;
        Ok(Self::with_client(base_url, http))
    }

    /// W1-I (CL-J1): construct a client targeting `base_url` and using
    /// the supplied pre-built `reqwest::Client`. The per-role binaries
    /// use this constructor when the cross-role wire is wired for mTLS
    /// (the client is built once via
    /// [`crate::build_cross_role_client`] and shared across every
    /// outbound peer) so the TLS identity + CA root store are loaded
    /// from disk exactly once at start-up.
    #[must_use]
    pub fn with_client(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        let mut base = base_url.into();
        while base.ends_with('/') {
            base.pop();
        }
        Self {
            base_url: base,
            http,
        }
    }

    /// Returns the canonical base URL (no trailing slash).
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

#[async_trait]
impl BrokerClient for HttpBrokerClient {
    async fn query(&self, sql: SqlQuery) -> Result<SqlResponse, RpcError> {
        let url = format!("{}/druid/v2/sql", self.base_url);
        let resp = self.http.post(&url).json(&sql).send().await?;
        unwrap_response(resp).await
    }

    async fn info(&self) -> Result<BrokerInfo, RpcError> {
        let url = format!("{}/druid/v2/info", self.base_url);
        let resp = self.http.get(&url).send().await?;
        unwrap_response(resp).await
    }
}

async fn unwrap_response<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, RpcError> {
    let status = resp.status();
    if status.is_success() {
        resp.json::<T>()
            .await
            .map_err(|e| RpcError::Serde(e.to_string()))
    } else {
        let code = status.as_u16();
        let body = resp.text().await.unwrap_or_default();
        let truncated = if body.len() > 4096 {
            body[..4096].to_string()
        } else {
            body
        };
        Err(RpcError::Http {
            status: code,
            body: truncated,
        })
    }
}

/// In-memory mock used by router unit tests. Records every query the
/// router sends and replays canned responses (or canned errors) in
/// FIFO order.
#[derive(Debug, Default, Clone)]
pub struct MockBrokerClient {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    queries: Vec<SqlQuery>,
    responses: VecDeque<Result<SqlResponse, RpcError>>,
    info_responses: VecDeque<Result<BrokerInfo, RpcError>>,
}

impl MockBrokerClient {
    /// Construct an empty mock with no queued responses. Calls made
    /// before responses are queued return [`RpcError::NotFound`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a successful response for the next [`BrokerClient::query`]
    /// call.
    pub fn push_response(&self, resp: SqlResponse) {
        if let Ok(mut g) = self.inner.lock() {
            g.responses.push_back(Ok(resp));
        }
    }

    /// Queue an error for the next [`BrokerClient::query`] call.
    pub fn push_error(&self, err: RpcError) {
        if let Ok(mut g) = self.inner.lock() {
            g.responses.push_back(Err(err));
        }
    }

    /// Queue a successful response for the next [`BrokerClient::info`]
    /// call.
    pub fn push_info(&self, info: BrokerInfo) {
        if let Ok(mut g) = self.inner.lock() {
            g.info_responses.push_back(Ok(info));
        }
    }

    /// Snapshot of every query the router forwarded, in send order.
    /// Useful for assert-on-call-args style tests.
    #[must_use]
    pub fn recorded_queries(&self) -> Vec<SqlQuery> {
        self.inner
            .lock()
            .map(|g| g.queries.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl BrokerClient for MockBrokerClient {
    async fn query(&self, sql: SqlQuery) -> Result<SqlResponse, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        g.queries.push(sql);
        g.responses
            .pop_front()
            .unwrap_or_else(|| Err(RpcError::NotFound("no queued mock response".into())))
    }

    async fn info(&self) -> Result<BrokerInfo, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        g.info_responses
            .pop_front()
            .unwrap_or_else(|| Err(RpcError::NotFound("no queued mock info".into())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_returns_queued_response_in_fifo_order() {
        let mock = MockBrokerClient::new();
        mock.push_response(SqlResponse {
            query_id: "q1".into(),
            columns: vec!["c".into()],
            rows: vec![],
            elapsed_ms: 0,
        });
        mock.push_response(SqlResponse {
            query_id: "q2".into(),
            columns: vec!["c".into()],
            rows: vec![],
            elapsed_ms: 0,
        });

        let r1 = mock
            .query(SqlQuery::new("SELECT 1"))
            .await
            .expect("first response");
        let r2 = mock
            .query(SqlQuery::new("SELECT 2"))
            .await
            .expect("second response");
        assert_eq!(r1.query_id, "q1");
        assert_eq!(r2.query_id, "q2");

        let recorded = mock.recorded_queries();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].query, "SELECT 1");
        assert_eq!(recorded[1].query, "SELECT 2");
    }

    #[tokio::test]
    async fn mock_returns_not_found_when_response_queue_empty() {
        let mock = MockBrokerClient::new();
        let err = mock
            .query(SqlQuery::new("SELECT 1"))
            .await
            .expect_err("empty queue should error");
        match err {
            RpcError::NotFound(_) => {}
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_replays_queued_error() {
        let mock = MockBrokerClient::new();
        mock.push_error(RpcError::Http {
            status: 503,
            body: "overloaded".into(),
        });
        let err = mock
            .query(SqlQuery::new("SELECT 1"))
            .await
            .expect_err("queued error replays");
        match err {
            RpcError::Http { status, body } => {
                assert_eq!(status, 503);
                assert_eq!(body, "overloaded");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn http_client_strips_trailing_slashes() {
        let c = HttpBrokerClient::try_new("http://localhost:8082///").expect("client builds");
        assert_eq!(c.base_url(), "http://localhost:8082");
    }
}
