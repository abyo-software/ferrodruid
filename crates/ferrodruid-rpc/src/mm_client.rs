// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Caller-side trait + implementations for the overlord →
//! middleManager hop.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use crate::error::RpcError;
use crate::types::{TaskAssignment, TaskState, TaskStatus};

/// Caller-side abstraction used by the overlord to dispatch ingestion
/// tasks to a middleManager. As with [`crate::BrokerClient`], the
/// trait is `async-trait` so the overlord binary can hold an
/// `Arc<dyn MiddleManagerClient>` and substitute a mock for tests.
#[async_trait]
pub trait MiddleManagerClient: Send + Sync + 'static {
    /// Dispatch an ingestion task to the middleManager and return its
    /// initial status (typically `Pending` or `Running`).
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::Transport`] / [`RpcError::Http`] /
    /// [`RpcError::Serde`] for the obvious failure modes.
    async fn assign_task(&self, task: TaskAssignment) -> Result<TaskStatus, RpcError>;

    /// Poll the current status of a task previously assigned via
    /// [`MiddleManagerClient::assign_task`].
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::NotFound`] (mock) or a 404
    /// [`RpcError::Http`] (real) if the middleManager does not know
    /// about `task_id`.
    async fn task_status(&self, task_id: &str) -> Result<TaskStatus, RpcError>;
}

/// Real HTTP implementation backed by `reqwest`. Targets the
/// middleManager's `/druid/v1/middlemanager/task` endpoints.
#[derive(Debug, Clone)]
pub struct HttpMiddleManagerClient {
    base_url: String,
    http: reqwest::Client,
}

impl HttpMiddleManagerClient {
    /// Construct a client targeting `base_url`. Same trailing-slash
    /// handling as [`crate::HttpBrokerClient::try_new`].
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
    /// pre-built `reqwest::Client`. The overlord binary passes in a
    /// TLS-aware client when the cross-role wire is in `Required` /
    /// `Permissive` mode (see [`crate::build_cross_role_client`]).
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
impl MiddleManagerClient for HttpMiddleManagerClient {
    async fn assign_task(&self, task: TaskAssignment) -> Result<TaskStatus, RpcError> {
        let url = format!("{}/druid/v1/middlemanager/task", self.base_url);
        let resp = self.http.post(&url).json(&task).send().await?;
        unwrap_response(resp).await
    }

    async fn task_status(&self, task_id: &str) -> Result<TaskStatus, RpcError> {
        let url = format!(
            "{}/druid/v1/middlemanager/task/{}/status",
            self.base_url, task_id
        );
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

/// In-memory mock used by overlord unit tests.
///
/// Holds a `task_id -> TaskStatus` map so consecutive
/// [`MiddleManagerClient::task_status`] calls return the latest known
/// state. The dispatcher can override the auto-registered initial
/// status per call via [`MockMiddleManagerClient::push_initial_status`].
#[derive(Debug, Default, Clone)]
pub struct MockMiddleManagerClient {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    assignments: Vec<TaskAssignment>,
    initial_overrides: VecDeque<Result<TaskStatus, RpcError>>,
    statuses: HashMap<String, TaskStatus>,
}

impl MockMiddleManagerClient {
    /// Construct an empty mock. With no overrides queued, every
    /// `assign_task` call returns a `Pending` status for the assigned
    /// task id.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a custom initial-status response for the next
    /// [`MiddleManagerClient::assign_task`] call.
    pub fn push_initial_status(&self, status: Result<TaskStatus, RpcError>) {
        if let Ok(mut g) = self.inner.lock() {
            g.initial_overrides.push_back(status);
        }
    }

    /// Force the stored status for `task_id` so the next
    /// [`MiddleManagerClient::task_status`] poll observes it. Used to
    /// drive lifecycle transitions in unit tests.
    pub fn set_status(&self, task_id: impl Into<String>, status: TaskStatus) {
        if let Ok(mut g) = self.inner.lock() {
            g.statuses.insert(task_id.into(), status);
        }
    }

    /// Snapshot of every task the overlord dispatched, in send order.
    #[must_use]
    pub fn recorded_assignments(&self) -> Vec<TaskAssignment> {
        self.inner
            .lock()
            .map(|g| g.assignments.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl MiddleManagerClient for MockMiddleManagerClient {
    async fn assign_task(&self, task: TaskAssignment) -> Result<TaskStatus, RpcError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        let initial = g.initial_overrides.pop_front();
        let task_id = task.task_id.clone();
        g.assignments.push(task);
        let resolved = match initial {
            Some(Ok(s)) => s,
            Some(Err(e)) => return Err(e),
            None => TaskStatus {
                task_id: task_id.clone(),
                state: TaskState::Pending,
                message: String::new(),
            },
        };
        g.statuses.insert(task_id, resolved.clone());
        Ok(resolved)
    }

    async fn task_status(&self, task_id: &str) -> Result<TaskStatus, RpcError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| RpcError::Custom("mock lock poisoned".into()))?;
        g.statuses
            .get(task_id)
            .cloned()
            .ok_or_else(|| RpcError::NotFound(format!("task {task_id} not found")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TaskKind, TaskState};

    #[tokio::test]
    async fn mock_assign_task_returns_pending_by_default() {
        let mock = MockMiddleManagerClient::new();
        let task = TaskAssignment::new(TaskKind::Index, "ds");
        let id = task.task_id.clone();
        let status = mock.assign_task(task).await.expect("assign");
        assert_eq!(status.task_id, id);
        assert_eq!(status.state, TaskState::Pending);
    }

    #[tokio::test]
    async fn mock_status_reflects_latest_set_status() {
        let mock = MockMiddleManagerClient::new();
        let task = TaskAssignment::new(TaskKind::Index, "ds");
        let id = task.task_id.clone();
        let _ = mock.assign_task(task).await.expect("assign");
        mock.set_status(
            id.clone(),
            TaskStatus {
                task_id: id.clone(),
                state: TaskState::Running,
                message: "started".into(),
            },
        );
        let status = mock.task_status(&id).await.expect("poll");
        assert_eq!(status.state, TaskState::Running);
        assert_eq!(status.message, "started");
    }

    #[tokio::test]
    async fn mock_status_for_unknown_task_is_not_found() {
        let mock = MockMiddleManagerClient::new();
        let err = mock
            .task_status("nope")
            .await
            .expect_err("missing task should error");
        match err {
            RpcError::NotFound(_) => {}
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_assignment_records_every_dispatch() {
        let mock = MockMiddleManagerClient::new();
        let _ = mock
            .assign_task(TaskAssignment::new(TaskKind::Kafka, "dsA"))
            .await;
        let _ = mock
            .assign_task(TaskAssignment::new(TaskKind::Kinesis, "dsB"))
            .await;
        let recorded = mock.recorded_assignments();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].data_source, "dsA");
        assert_eq!(recorded[1].data_source, "dsB");
    }
}
