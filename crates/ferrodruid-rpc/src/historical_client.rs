// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Caller-side trait + implementations for the broker → historical
//! and coordinator → historical hops.
//!
//! Wave 40.LL closes the last two cross-role flows:
//!
//! | caller       | callee     | endpoint                                 |
//! |--------------|------------|------------------------------------------|
//! | broker       | historical | `POST /druid/v2/native` (per-segment SQL) |
//! | coordinator  | historical | `POST /druid/v1/historical/load`         |
//! | coordinator  | historical | `POST /druid/v1/historical/drop`         |
//! | coordinator  | historical | `GET /druid/v1/historical/loadstatus`    |
//!
//! As with the W3 [`crate::BrokerClient`] /
//! [`crate::MiddleManagerClient`] pattern, this module ships:
//!
//! - The async [`HistoricalClient`] trait so callers can store
//!   `Arc<dyn HistoricalClient>` and substitute a mock for tests.
//! - A real HTTP impl ([`HttpHistoricalClient`]) backed by `reqwest`.
//! - An in-memory mock ([`MockHistoricalClient`]) that records every
//!   call and replays canned responses in FIFO order.
//!
//! Real query execution against a loaded segment, and real segment
//! loading from deep storage, both stay stubbed in this wave (the
//! [`crate::historical_server`] handler echoes the query / accepts
//! the load command and immediately reports `Loaded`). W5 lands the
//! real implementation.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::RpcError;
use crate::native_query::NativeQuery;
use crate::types::{
    LoadStatusReport, SegmentDropCommand, SegmentLoadCommand, SegmentLoadState, SegmentQuery,
    SegmentQueryResponse,
};

/// Caller-side abstraction used by the broker (for query scatter) and
/// by the coordinator (for segment-placement commands) to talk to a
/// historical.
#[async_trait]
pub trait HistoricalClient: Send + Sync + 'static {
    /// Forward a per-segment query fragment to the historical and
    /// return its result rows.
    ///
    /// # Errors
    ///
    /// Same surface as [`crate::BrokerClient::query`].
    async fn scatter_query(&self, query: SegmentQuery) -> Result<SegmentQueryResponse, RpcError>;

    /// Wave 41.OO: forward a real Druid-style native query
    /// ([`NativeQuery`]) targeting a specific segment. The historical
    /// executes the query against the loaded segment artifact and
    /// returns the result rows.
    ///
    /// # Errors
    ///
    /// Same surface as [`Self::scatter_query`]; in addition, returns
    /// [`RpcError::Serde`] if the native query fails to encode as
    /// JSON.
    async fn native_scatter(
        &self,
        segment_id: &str,
        query: &NativeQuery,
    ) -> Result<SegmentQueryResponse, RpcError>;

    /// Tell the historical to load a segment from deep storage. The
    /// historical answers with the resulting [`SegmentLoadState`]
    /// (typically `Loading` immediately, then `Loaded` on a poll).
    ///
    /// # Errors
    ///
    /// Network / HTTP / serde failures via [`RpcError`].
    async fn load_segment(&self, cmd: SegmentLoadCommand) -> Result<LoadStatusReport, RpcError>;

    /// Tell the historical to drop a segment. Returns the resulting
    /// [`SegmentLoadState`] (typically `Dropped`).
    ///
    /// # Errors
    ///
    /// Network / HTTP / serde failures via [`RpcError`].
    async fn drop_segment(&self, cmd: SegmentDropCommand) -> Result<LoadStatusReport, RpcError>;

    /// Poll the historical for the load status of every segment it
    /// has been told about. Returns `segment_id -> SegmentLoadState`.
    ///
    /// # Errors
    ///
    /// Network / HTTP / serde failures via [`RpcError`].
    async fn load_status(&self) -> Result<HashMap<String, SegmentLoadState>, RpcError>;
}

/// Real HTTP implementation backed by `reqwest`. Targets the
/// historical's `/druid/v2/native` and `/druid/v1/historical/*`
/// endpoints.
#[derive(Debug, Clone)]
pub struct HttpHistoricalClient {
    base_url: String,
    http: reqwest::Client,
}

impl HttpHistoricalClient {
    /// Construct a client targeting `base_url`. Same trailing-slash
    /// handling as the W3 HTTP clients.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] if the underlying `reqwest`
    /// client cannot be built.
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, RpcError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| RpcError::Transport(format!("build reqwest: {e}")))?;
        Ok(Self::with_client(base_url, http))
    }

    /// W1-I (CL-J1): construct a client targeting `base_url` with a
    /// pre-built `reqwest::Client`. The per-role binaries pass in a
    /// TLS-aware client when the cross-role wire is in `Required` /
    /// `Permissive` mode (see [`crate::build_cross_role_client`]) so
    /// the identity + CA store load once at start-up.
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
impl HistoricalClient for HttpHistoricalClient {
    async fn scatter_query(&self, query: SegmentQuery) -> Result<SegmentQueryResponse, RpcError> {
        let url = format!("{}/druid/v2/native", self.base_url);
        let resp = self.http.post(&url).json(&query).send().await?;
        unwrap_response(resp).await
    }

    async fn native_scatter(
        &self,
        segment_id: &str,
        query: &NativeQuery,
    ) -> Result<SegmentQueryResponse, RpcError> {
        let url = format!("{}/druid/v2/native", self.base_url);
        // Encode the NativeQuery as a JSON object and inject a
        // top-level `segmentId` field so the historical's dispatcher
        // can route the body to the right segment artifact.
        let mut body = serde_json::to_value(query).map_err(|e| RpcError::Serde(e.to_string()))?;
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "segmentId".into(),
                serde_json::Value::String(segment_id.to_string()),
            );
        }
        let resp = self.http.post(&url).json(&body).send().await?;
        unwrap_response(resp).await
    }

    async fn load_segment(&self, cmd: SegmentLoadCommand) -> Result<LoadStatusReport, RpcError> {
        let url = format!("{}/druid/v1/historical/load", self.base_url);
        let resp = self.http.post(&url).json(&cmd).send().await?;
        unwrap_response(resp).await
    }

    async fn drop_segment(&self, cmd: SegmentDropCommand) -> Result<LoadStatusReport, RpcError> {
        let url = format!("{}/druid/v1/historical/drop", self.base_url);
        let resp = self.http.post(&url).json(&cmd).send().await?;
        unwrap_response(resp).await
    }

    async fn load_status(&self) -> Result<HashMap<String, SegmentLoadState>, RpcError> {
        let url = format!("{}/druid/v1/historical/loadstatus", self.base_url);
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

/// In-memory mock used by broker / coordinator unit tests.
///
/// Records every scatter query and segment command, and replays
/// canned responses in FIFO order. Tests that want to mutate the
/// in-memory load-status table can do so via
/// [`MockHistoricalClient::set_load_state`].
#[derive(Debug, Default, Clone)]
pub struct MockHistoricalClient {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    scatter_queries: Vec<SegmentQuery>,
    scatter_responses: VecDeque<Result<SegmentQueryResponse, RpcError>>,
    /// Wave 41.OO: recorded `native_scatter` calls.
    native_scatters: Vec<(String, NativeQuery)>,
    load_commands: Vec<SegmentLoadCommand>,
    drop_commands: Vec<SegmentDropCommand>,
    load_responses: VecDeque<Result<LoadStatusReport, RpcError>>,
    drop_responses: VecDeque<Result<LoadStatusReport, RpcError>>,
    statuses: HashMap<String, SegmentLoadState>,
    status_overrides: VecDeque<Result<HashMap<String, SegmentLoadState>, RpcError>>,
}

impl MockHistoricalClient {
    /// Construct an empty mock with no queued responses.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a successful scatter-query response.
    pub fn push_scatter_response(&self, resp: SegmentQueryResponse) {
        if let Ok(mut g) = self.inner.lock() {
            g.scatter_responses.push_back(Ok(resp));
        }
    }

    /// Queue an error for the next scatter-query call.
    pub fn push_scatter_error(&self, err: RpcError) {
        if let Ok(mut g) = self.inner.lock() {
            g.scatter_responses.push_back(Err(err));
        }
    }

    /// Queue a successful response for the next `load_segment` call.
    pub fn push_load_response(&self, resp: LoadStatusReport) {
        if let Ok(mut g) = self.inner.lock() {
            g.load_responses.push_back(Ok(resp));
        }
    }

    /// Queue an error for the next `load_segment` call.
    pub fn push_load_error(&self, err: RpcError) {
        if let Ok(mut g) = self.inner.lock() {
            g.load_responses.push_back(Err(err));
        }
    }

    /// Queue a successful response for the next `drop_segment` call.
    pub fn push_drop_response(&self, resp: LoadStatusReport) {
        if let Ok(mut g) = self.inner.lock() {
            g.drop_responses.push_back(Ok(resp));
        }
    }

    /// Force the load state for a segment in the mock's status table
    /// so the next [`HistoricalClient::load_status`] poll observes it.
    pub fn set_load_state(&self, segment_id: impl Into<String>, state: SegmentLoadState) {
        if let Ok(mut g) = self.inner.lock() {
            g.statuses.insert(segment_id.into(), state);
        }
    }

    /// Queue a custom whole-table response for the next `load_status`
    /// call. Overrides whatever `set_load_state` has placed in the
    /// status table.
    pub fn push_status_override(&self, statuses: HashMap<String, SegmentLoadState>) {
        if let Ok(mut g) = self.inner.lock() {
            g.status_overrides.push_back(Ok(statuses));
        }
    }

    /// Snapshot every scatter query the broker forwarded.
    #[must_use]
    pub fn recorded_scatter_queries(&self) -> Vec<SegmentQuery> {
        self.inner
            .lock()
            .map(|g| g.scatter_queries.clone())
            .unwrap_or_default()
    }

    /// Snapshot every `native_scatter` call (segment_id + query) the
    /// broker forwarded.
    #[must_use]
    pub fn recorded_native_scatters(&self) -> Vec<(String, NativeQuery)> {
        self.inner
            .lock()
            .map(|g| g.native_scatters.clone())
            .unwrap_or_default()
    }

    /// Snapshot every segment-load command the coordinator dispatched.
    #[must_use]
    pub fn recorded_load_commands(&self) -> Vec<SegmentLoadCommand> {
        self.inner
            .lock()
            .map(|g| g.load_commands.clone())
            .unwrap_or_default()
    }

    /// Snapshot every segment-drop command the coordinator dispatched.
    #[must_use]
    pub fn recorded_drop_commands(&self) -> Vec<SegmentDropCommand> {
        self.inner
            .lock()
            .map(|g| g.drop_commands.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl HistoricalClient for MockHistoricalClient {
    async fn scatter_query(&self, query: SegmentQuery) -> Result<SegmentQueryResponse, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        g.scatter_queries.push(query);
        g.scatter_responses
            .pop_front()
            .unwrap_or_else(|| Err(RpcError::NotFound("no queued mock scatter response".into())))
    }

    async fn native_scatter(
        &self,
        segment_id: &str,
        query: &NativeQuery,
    ) -> Result<SegmentQueryResponse, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        g.native_scatters
            .push((segment_id.to_string(), query.clone()));
        g.scatter_responses
            .pop_front()
            .unwrap_or_else(|| Err(RpcError::NotFound("no queued mock scatter response".into())))
    }

    async fn load_segment(&self, cmd: SegmentLoadCommand) -> Result<LoadStatusReport, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        let segment_id = cmd.segment_id.clone();
        g.load_commands.push(cmd);
        let queued = g.load_responses.pop_front();
        match queued {
            Some(Ok(report)) => {
                g.statuses.insert(segment_id, report.state);
                Ok(report)
            }
            Some(Err(e)) => Err(e),
            None => {
                let report = LoadStatusReport {
                    segment_id: segment_id.clone(),
                    state: SegmentLoadState::Loading,
                    message: String::new(),
                };
                g.statuses.insert(segment_id, SegmentLoadState::Loading);
                Ok(report)
            }
        }
    }

    async fn drop_segment(&self, cmd: SegmentDropCommand) -> Result<LoadStatusReport, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        let segment_id = cmd.segment_id.clone();
        g.drop_commands.push(cmd);
        let queued = g.drop_responses.pop_front();
        match queued {
            Some(Ok(report)) => {
                g.statuses.insert(segment_id, report.state);
                Ok(report)
            }
            Some(Err(e)) => Err(e),
            None => {
                let report = LoadStatusReport {
                    segment_id: segment_id.clone(),
                    state: SegmentLoadState::Dropped,
                    message: String::new(),
                };
                g.statuses.insert(segment_id, SegmentLoadState::Dropped);
                Ok(report)
            }
        }
    }

    async fn load_status(&self) -> Result<HashMap<String, SegmentLoadState>, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        if let Some(queued) = g.status_overrides.pop_front() {
            return queued;
        }
        Ok(g.statuses.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SegmentQuery;

    #[tokio::test]
    async fn mock_scatter_query_returns_queued_response_in_fifo_order() {
        let mock = MockHistoricalClient::new();
        mock.push_scatter_response(SegmentQueryResponse {
            segment_id: "seg-1".into(),
            rows: vec![],
            elapsed_ms: 0,
        });
        mock.push_scatter_response(SegmentQueryResponse {
            segment_id: "seg-2".into(),
            rows: vec![],
            elapsed_ms: 0,
        });

        let r1 = mock
            .scatter_query(SegmentQuery::new("SELECT 1", "seg-1"))
            .await
            .expect("first response");
        let r2 = mock
            .scatter_query(SegmentQuery::new("SELECT 2", "seg-2"))
            .await
            .expect("second response");
        assert_eq!(r1.segment_id, "seg-1");
        assert_eq!(r2.segment_id, "seg-2");

        let recorded = mock.recorded_scatter_queries();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].query, "SELECT 1");
        assert_eq!(recorded[1].segment_id, "seg-2");
    }

    #[tokio::test]
    async fn mock_scatter_query_empty_queue_returns_not_found() {
        let mock = MockHistoricalClient::new();
        let err = mock
            .scatter_query(SegmentQuery::new("SELECT 1", "seg"))
            .await
            .expect_err("empty queue should error");
        match err {
            RpcError::NotFound(_) => {}
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_load_segment_default_state_is_loading() {
        let mock = MockHistoricalClient::new();
        let cmd = SegmentLoadCommand::new("seg-A", "ds-test", "deepstore://path/seg-A");
        let report = mock.load_segment(cmd).await.expect("load");
        assert_eq!(report.segment_id, "seg-A");
        assert_eq!(report.state, SegmentLoadState::Loading);
    }

    #[tokio::test]
    async fn mock_drop_segment_default_state_is_dropped() {
        let mock = MockHistoricalClient::new();
        let cmd = SegmentDropCommand::new("seg-A");
        let report = mock.drop_segment(cmd).await.expect("drop");
        assert_eq!(report.segment_id, "seg-A");
        assert_eq!(report.state, SegmentLoadState::Dropped);
    }

    #[tokio::test]
    async fn mock_load_status_reflects_set_load_state() {
        let mock = MockHistoricalClient::new();
        mock.set_load_state("seg-1", SegmentLoadState::Loaded);
        mock.set_load_state("seg-2", SegmentLoadState::Loading);
        let table = mock.load_status().await.expect("status");
        assert_eq!(table.get("seg-1"), Some(&SegmentLoadState::Loaded));
        assert_eq!(table.get("seg-2"), Some(&SegmentLoadState::Loading));
    }

    #[tokio::test]
    async fn mock_load_segment_records_command() {
        let mock = MockHistoricalClient::new();
        let _ = mock
            .load_segment(SegmentLoadCommand::new(
                "seg-1",
                "ds-A",
                "deepstore://A/seg-1",
            ))
            .await;
        let _ = mock
            .load_segment(SegmentLoadCommand::new(
                "seg-2",
                "ds-B",
                "deepstore://B/seg-2",
            ))
            .await;
        let recorded = mock.recorded_load_commands();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].segment_id, "seg-1");
        assert_eq!(recorded[1].data_source, "ds-B");
    }

    #[tokio::test]
    async fn mock_replays_queued_scatter_error() {
        let mock = MockHistoricalClient::new();
        mock.push_scatter_error(RpcError::Http {
            status: 503,
            body: "overloaded".into(),
        });
        let err = mock
            .scatter_query(SegmentQuery::new("SELECT 1", "seg"))
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
        let c = HttpHistoricalClient::try_new("http://localhost:8083///").expect("client builds");
        assert_eq!(c.base_url(), "http://localhost:8083");
    }
}
