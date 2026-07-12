// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Callee-side axum router for the middleManager role.
//!
//! Wave 39.HH ships a **simulated executor**: a task posted to
//! `/druid/v1/middlemanager/task` is registered as `Pending`,
//! transitioned to `Running` on a tokio timer, and finally
//! transitioned to `Success`. Real ingestion task execution lands in
//! W4. The simulated transitions are observable by polling
//! `/druid/v1/middlemanager/task/{id}/status`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tokio::sync::Mutex;

use crate::types::{TaskAssignment, TaskState, TaskStatus};

/// Default cap on the number of tasks the middleManager HTTP server retains.
///
/// DD R34: the HTTP assignment surface tracks task status in an in-memory map.
/// Without a cap a caller can submit unbounded unique task ids and grow retained
/// state forever (completed tasks are never evicted), bypassing the bounded
/// admission of `ferrodruid-middlemanager::MiddleManager`. New assignments
/// preferentially evict a terminal (Success/Failed) task; when every retained
/// task is still active the server returns 503.
const DEFAULT_MAX_TASKS: usize = 8192;

/// Per-server state for the middleManager axum router.
///
/// Tracks every task that has been dispatched and provides cheap
/// inspection helpers used by integration tests.
#[derive(Debug)]
pub struct MiddleManagerServerState {
    tasks: Mutex<HashMap<String, TaskStatus>>,
    /// Maximum number of tasks retained at once (DD R34 admission bound).
    max_tasks: usize,
    /// Time the simulated executor stays in `Pending` before flipping
    /// to `Running`. Defaults to 50 ms; tests can lower it to 0 for
    /// fast assertions, or raise it to verify state transitions.
    pub pending_to_running: Duration,
    /// Time the simulated executor stays in `Running` before flipping
    /// to `Success`.
    pub running_to_success: Duration,
}

impl Default for MiddleManagerServerState {
    fn default() -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
            max_tasks: DEFAULT_MAX_TASKS,
            pending_to_running: Duration::from_millis(50),
            running_to_success: Duration::from_millis(50),
        }
    }
}

impl MiddleManagerServerState {
    /// Construct fresh server state with the supplied transition
    /// timings. `pending_to_running` and `running_to_success` may be
    /// zero for tests that want immediate completion.
    #[must_use]
    pub fn with_timings(pending_to_running: Duration, running_to_success: Duration) -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
            max_tasks: DEFAULT_MAX_TASKS,
            pending_to_running,
            running_to_success,
        }
    }

    /// Override the retained-task cap (DD R34). Used by tests to exercise the
    /// admission bound cheaply.
    #[must_use]
    pub fn with_max_tasks(mut self, max_tasks: usize) -> Self {
        self.max_tasks = max_tasks.max(1);
        self
    }

    /// Snapshot the tracked task table. Useful for tests that want to
    /// assert on the full state without hitting the HTTP surface.
    pub async fn snapshot(&self) -> HashMap<String, TaskStatus> {
        self.tasks.lock().await.clone()
    }
}

/// Build the axum [`Router`] the middleManager binary mounts on its
/// HTTP server. Routes:
///
/// - `POST /druid/v1/middlemanager/task` — accept a
///   [`TaskAssignment`], register as `Pending`, return initial
///   [`TaskStatus`].
/// - `GET /druid/v1/middlemanager/task/{id}/status` — return the
///   current [`TaskStatus`] (404 if unknown).
pub fn router(state: Arc<MiddleManagerServerState>) -> Router {
    Router::new()
        .route("/druid/v1/middlemanager/task", post(handle_assign))
        .route(
            "/druid/v1/middlemanager/task/{id}/status",
            get(handle_status),
        )
        .with_state(state)
}

async fn handle_assign(
    State(state): State<Arc<MiddleManagerServerState>>,
    Json(task): Json<TaskAssignment>,
) -> Result<Json<TaskStatus>, CapacityExceeded> {
    let task_id = task.task_id.clone();
    let initial = TaskStatus {
        task_id: task_id.clone(),
        state: TaskState::Pending,
        message: String::new(),
    };
    {
        let mut g = state.tasks.lock().await;
        // DD R34: idempotent re-assignment — a duplicate task id returns the
        // current status without growing state or spawning a second driver.
        if let Some(existing) = g.get(&task_id) {
            return Ok(Json(existing.clone()));
        }
        // DD R34: bound retained state. At capacity, evict one terminal
        // (Success/Failed) task to make room; if every retained task is still
        // active, apply backpressure with 503 rather than grow unbounded.
        if g.len() >= state.max_tasks {
            let evictable = g
                .iter()
                .find(|(_, s)| matches!(s.state, TaskState::Success | TaskState::Failed))
                .map(|(k, _)| k.clone());
            match evictable {
                Some(done) => {
                    g.remove(&done);
                }
                None => return Err(CapacityExceeded(state.max_tasks)),
            }
        }
        g.insert(task_id.clone(), initial.clone());
    }
    tracing::info!(
        task_id = %task_id,
        kind = ?task.task_kind,
        data_source = %task.data_source,
        "middlemanager accepted task",
    );

    let pending_delay = state.pending_to_running;
    let running_delay = state.running_to_success;
    let driver = Arc::clone(&state);
    tokio::spawn(async move {
        if !pending_delay.is_zero() {
            tokio::time::sleep(pending_delay).await;
        }
        {
            let mut g = driver.tasks.lock().await;
            if let Some(s) = g.get_mut(&task_id) {
                s.state = TaskState::Running;
            }
        }
        if !running_delay.is_zero() {
            tokio::time::sleep(running_delay).await;
        }
        let mut g = driver.tasks.lock().await;
        if let Some(s) = g.get_mut(&task_id) {
            s.state = TaskState::Success;
            s.message = "simulated executor completed (Wave 39.HH)".into();
        }
    });

    Ok(Json(initial))
}

/// 503 response when the middleManager has no capacity for a new task and no
/// terminal task can be evicted (DD R34 admission backpressure).
struct CapacityExceeded(usize);

impl IntoResponse for CapacityExceeded {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "middlemanager at capacity ({} active tasks); retry later",
                self.0
            ),
        )
            .into_response()
    }
}

async fn handle_status(
    State(state): State<Arc<MiddleManagerServerState>>,
    Path(id): Path<String>,
) -> Result<Json<TaskStatus>, StatusCodeNotFound> {
    let g = state.tasks.lock().await;
    if let Some(s) = g.get(&id) {
        Ok(Json(s.clone()))
    } else {
        Err(StatusCodeNotFound(format!("task {id} not found")))
    }
}

/// Newtype wrapper so we can implement `IntoResponse` for the 404
/// case without leaking it into the public surface.
struct StatusCodeNotFound(String);

impl IntoResponse for StatusCodeNotFound {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::NOT_FOUND, self.0).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mm_client::{HttpMiddleManagerClient, MiddleManagerClient};
    use crate::types::{TaskKind, TaskState};

    async fn spawn(state: Arc<MiddleManagerServerState>) -> String {
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
    async fn assign_rejects_at_capacity_and_dedups() {
        // DD R34: with a tiny cap and long transition delays (tasks stay active),
        // a new distinct task beyond capacity is rejected (503), while
        // re-assigning an existing id is idempotent and does not grow state.
        let state = Arc::new(
            MiddleManagerServerState::with_timings(
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .with_max_tasks(1),
        );
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");

        let t1 = TaskAssignment::new(TaskKind::Index, "ds");
        client
            .assign_task(t1.clone())
            .await
            .expect("first assign ok");

        // A second DISTINCT task cannot be admitted (cap=1, t1 still active).
        let t2 = TaskAssignment::new(TaskKind::Index, "ds");
        assert!(
            client.assign_task(t2).await.is_err(),
            "a new task beyond capacity must be rejected (503)"
        );

        // Re-assigning the SAME id is idempotent and still succeeds.
        client
            .assign_task(t1)
            .await
            .expect("idempotent re-assign of an existing task id must succeed");

        // State never exceeded the cap.
        assert_eq!(state.snapshot().await.len(), 1);
    }

    #[tokio::test]
    async fn assign_then_poll_observes_running_then_success() {
        let state = Arc::new(MiddleManagerServerState::with_timings(
            Duration::from_millis(20),
            Duration::from_millis(20),
        ));
        let url = spawn(Arc::clone(&state)).await;
        let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");

        let task = TaskAssignment::new(TaskKind::Index, "ds-test");
        let id = task.task_id.clone();
        let initial = client.assign_task(task).await.expect("assign");
        assert_eq!(initial.state, TaskState::Pending);

        // Poll until we see Success or run out of attempts. Total
        // budget ~1 s — well above the 40 ms simulated executor.
        let mut last = TaskState::Pending;
        for _ in 0..50 {
            let s = client.task_status(&id).await.expect("poll");
            last = s.state;
            if last == TaskState::Success {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(last, TaskState::Success);
    }

    #[tokio::test]
    async fn poll_unknown_task_returns_404() {
        let state = Arc::new(MiddleManagerServerState::default());
        let url = spawn(state).await;
        let client = HttpMiddleManagerClient::try_new(&url).expect("client builds");
        let err = client
            .task_status("nope")
            .await
            .expect_err("unknown should 404");
        match err {
            crate::error::RpcError::Http { status, .. } => assert_eq!(status, 404),
            other => panic!("unexpected variant: {other:?}"),
        }
    }
}
