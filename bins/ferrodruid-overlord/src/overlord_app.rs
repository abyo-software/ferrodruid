// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Overlord application logic — kept in its own module so it can be
//! exercised by unit tests with a [`MockMiddleManagerClient`] swapped
//! in for the real HTTP transport.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use ferrodruid_rpc::{
    HttpMiddleManagerClient, MiddleManagerClient, RpcError, TaskAssignment, TaskState, TaskStatus,
};
use serde::Serialize;
use tokio::sync::Mutex;

/// Default cap on the number of `task_id -> middleManager` routes the
/// overlord HTTP server retains.
///
/// DD R35: the dispatch surface inserts one routing entry per accepted
/// task and never evicts completed tasks. Without a cap a caller can
/// submit unbounded unique task ids and grow retained state forever.
/// New dispatches preferentially evict a terminal (Success/Failed/
/// Unknown) routed task; when every routed task is still active the
/// server returns 503.
const DEFAULT_MAX_ROUTES: usize = 8192;

/// The admission/routing state of a task in the overlord routing table.
///
/// DD R36: a task is *reserved* (`Dispatching`) under the routing lock BEFORE it
/// is dispatched upstream, then upgraded to `Routed` on success (or the
/// reservation is rolled back on failure). This makes the idempotency check,
/// capacity check, and reservation a single atomic critical section, so
/// concurrent duplicates relay instead of double-dispatching and a burst of
/// unique tasks cannot exceed the cap. In-flight `Dispatching` reservations are
/// never eviction candidates (only terminal `Routed` tasks are).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RouteState {
    /// Reserved and being dispatched upstream; not yet confirmed routed.
    Dispatching(usize),
    /// Successfully dispatched to the middleManager at this index.
    Routed(usize),
}

impl RouteState {
    fn index(self) -> usize {
        match self {
            Self::Dispatching(i) | Self::Routed(i) => i,
        }
    }

    fn is_routed(self) -> bool {
        matches!(self, Self::Routed(_))
    }
}

/// Shared state held by the overlord HTTP server.
#[derive(Clone)]
pub struct OverlordState {
    middlemanagers: Arc<Vec<Arc<dyn MiddleManagerClient>>>,
    next: Arc<AtomicUsize>,
    /// Maps `task_id -> route state` so status polls can be routed to the
    /// correct middleManager. Wave 39.HH keeps this in-memory; W4 will persist
    /// it via `ferrodruid-metadata`.
    routing: Arc<Mutex<HashMap<String, RouteState>>>,
    /// Maximum number of routing entries retained at once (DD R35
    /// admission bound).
    max_routes: usize,
}

impl OverlordState {
    /// Build a state from middleManager base URLs.
    ///
    /// Pre-W1-I production path; today's binary entry point goes via
    /// [`Self::from_mm_urls_with_client`].
    ///
    /// # Errors
    ///
    /// Propagates [`RpcError`] if any underlying client fails to
    /// build.
    #[allow(dead_code)] // Kept for tests / non-binary callers.
    pub fn from_mm_urls(urls: &[String]) -> Result<Self, RpcError> {
        let mut clients: Vec<Arc<dyn MiddleManagerClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpMiddleManagerClient::try_new(u.clone())?;
            clients.push(Arc::new(c));
        }
        Ok(Self {
            middlemanagers: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
            routing: Arc::new(Mutex::new(HashMap::new())),
            max_routes: DEFAULT_MAX_ROUTES,
        })
    }

    /// W1-I (CL-J1): build a state where every per-middleManager
    /// `HttpMiddleManagerClient` reuses the supplied pre-built
    /// `reqwest::Client` (typically TLS-aware).
    #[must_use]
    pub fn from_mm_urls_with_client(urls: &[String], http: reqwest::Client) -> Self {
        let mut clients: Vec<Arc<dyn MiddleManagerClient>> = Vec::with_capacity(urls.len());
        for u in urls {
            let c = HttpMiddleManagerClient::with_client(u.clone(), http.clone());
            clients.push(Arc::new(c));
        }
        Self {
            middlemanagers: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
            routing: Arc::new(Mutex::new(HashMap::new())),
            max_routes: DEFAULT_MAX_ROUTES,
        }
    }

    /// Build a state from already-constructed middleManager clients.
    /// Used by tests that want to inject mocks.
    #[must_use]
    #[allow(dead_code)] // Test-only helper; production goes via `from_mm_urls`.
    pub fn from_clients(clients: Vec<Arc<dyn MiddleManagerClient>>) -> Self {
        Self {
            middlemanagers: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
            routing: Arc::new(Mutex::new(HashMap::new())),
            max_routes: DEFAULT_MAX_ROUTES,
        }
    }

    /// Override the routing-table cap (DD R35). Used by tests to
    /// exercise the admission bound cheaply.
    #[must_use]
    #[allow(dead_code)] // Test-only helper.
    pub fn with_max_routes(mut self, max_routes: usize) -> Self {
        self.max_routes = max_routes.max(1);
        self
    }

    fn select_index(&self) -> Option<usize> {
        if self.middlemanagers.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.middlemanagers.len();
        Some(idx)
    }
}

/// Build the axum [`Router`] for the overlord binary.
pub fn build_router(state: OverlordState) -> Router {
    Router::new()
        .route("/druid/indexer/v1/task", post(dispatch_task))
        .route("/druid/indexer/v1/task/{id}/status", get(poll_task))
        .route("/druid/indexer/v1/health", get(health))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    middlemanagers: usize,
}

async fn health(State(state): State<OverlordState>) -> Json<HealthBody> {
    Json(HealthBody {
        status: "ok",
        middlemanagers: state.middlemanagers.len(),
    })
}

async fn dispatch_task(
    State(state): State<OverlordState>,
    Json(task): Json<TaskAssignment>,
) -> Result<Json<TaskStatus>, OverlordError> {
    let task_id = task.task_id.clone();

    // DD R35/R36: idempotent dispatch — a duplicate taskId must NOT be
    // re-dispatched (round-robin could route the duplicate to a *different*
    // middleManager and execute the task twice). Relay the existing route's
    // current status instead. This is also re-checked under the reservation
    // lock below so two concurrent first-time requests cannot both dispatch.
    if let Some(existing) = {
        let routing = state.routing.lock().await;
        routing.get(&task_id).copied()
    } {
        return relay_existing(&state, existing, &task_id).await;
    }

    let Some(idx) = state.select_index() else {
        return Err(OverlordError::NoMiddlemanagers);
    };

    // DD R35/R36: if at capacity, find one evictable terminal *Routed* task
    // (Success/Failed/Unknown). Polling the upstream MM cannot happen under the
    // routing lock (a slow MM would stall every dispatch), so the candidate is
    // discovered outside the lock and only *applied* inside the reservation
    // critical section below. In-flight `Dispatching` reservations are never
    // eviction candidates.
    let at_capacity = {
        let routing = state.routing.lock().await;
        routing.len() >= state.max_routes
    };
    let evict_candidate = if at_capacity {
        let routed: Vec<(String, usize)> = {
            let routing = state.routing.lock().await;
            routing
                .iter()
                .filter(|(_, v)| v.is_routed())
                .map(|(k, v)| (k.clone(), v.index()))
                .collect()
        };
        let mut found = None;
        for (tid, midx) in routed {
            if let Ok(status) = state.middlemanagers[midx].task_status(&tid).await
                && matches!(
                    status.state,
                    TaskState::Success | TaskState::Failed | TaskState::Unknown
                )
            {
                found = Some(tid);
                break;
            }
        }
        found
    } else {
        None
    };

    // DD R36: atomic admission — re-check idempotency, enforce capacity, and
    // RESERVE the route as `Dispatching` all under one lock, BEFORE upstream
    // dispatch. A concurrent duplicate now observes the reservation and relays;
    // a burst cannot exceed the cap because the length check and the insert are
    // in the same critical section.
    {
        let mut routing = state.routing.lock().await;
        if let Some(existing) = routing.get(&task_id).copied() {
            drop(routing);
            return relay_existing(&state, existing, &task_id).await;
        }
        if routing.len() >= state.max_routes {
            match &evict_candidate {
                Some(tid) if routing.contains_key(tid) => {
                    routing.remove(tid);
                }
                _ => return Err(OverlordError::CapacityExceeded(state.max_routes)),
            }
        }
        routing.insert(task_id.clone(), RouteState::Dispatching(idx));
    }

    // DD R37: dispatch upstream inside a SPAWNED task so the upgrade-to-`Routed`
    // (on success) or rollback (on failure) ALWAYS runs — even if this request
    // future is dropped mid-await (client disconnect, server shutdown, a timeout
    // layer). Otherwise the `Dispatching` reservation would leak permanently,
    // is not terminal-evictable, and would consume routing capacity forever.
    // The request awaits the spawned task's result.
    let driver = state.clone();
    let tid = task_id.clone();
    let handle = tokio::spawn(async move {
        let result = driver.middlemanagers[idx].assign_task(task).await;
        let mut routing = driver.routing.lock().await;
        match &result {
            Ok(_) => {
                routing.insert(tid, RouteState::Routed(idx));
            }
            Err(_) => {
                routing.remove(&tid);
            }
        }
        result
    });
    match handle.await {
        Ok(Ok(status)) => Ok(Json(status)),
        Ok(Err(e)) => Err(OverlordError::Upstream(e)),
        Err(join_err) => {
            // The dispatch task panicked before its own cleanup ran; best-effort
            // clear the reservation so it does not leak.
            state.routing.lock().await.remove(&task_id);
            Err(OverlordError::Upstream(RpcError::Custom(format!(
                "dispatch task failed: {join_err}"
            ))))
        }
    }
}

/// Relay status for an existing routing entry. A `Routed` entry polls the
/// middleManager; a still-in-flight `Dispatching` reservation reports `Pending`
/// (DD R37) rather than polling the MM for a task it has not registered yet,
/// which would 404 and falsely reject the duplicate.
async fn relay_existing(
    state: &OverlordState,
    route: RouteState,
    task_id: &str,
) -> Result<Json<TaskStatus>, OverlordError> {
    match route {
        RouteState::Routed(idx) => relay_status(state, idx, task_id).await,
        RouteState::Dispatching(_) => Ok(Json(TaskStatus {
            task_id: task_id.to_string(),
            state: TaskState::Pending,
            message: String::new(),
        })),
    }
}

/// Relay the current status of an already-routed task from its middleManager.
async fn relay_status(
    state: &OverlordState,
    idx: usize,
    task_id: &str,
) -> Result<Json<TaskStatus>, OverlordError> {
    let client = state.middlemanagers[idx].clone();
    match client.task_status(task_id).await {
        Ok(status) => Ok(Json(status)),
        Err(e) => Err(OverlordError::Upstream(e)),
    }
}

async fn poll_task(
    State(state): State<OverlordState>,
    Path(id): Path<String>,
) -> Result<Json<TaskStatus>, OverlordError> {
    let route = {
        let routing = state.routing.lock().await;
        routing.get(&id).copied()
    };
    let Some(route) = route else {
        return Err(OverlordError::Upstream(RpcError::NotFound(format!(
            "task {id} not known to overlord"
        ))));
    };
    // DD R38: route through `relay_existing` so an in-flight `Dispatching`
    // reservation reports `Pending` instead of polling the MM for a task it has
    // not registered yet (which would 404) — consistent with `dispatch_task`.
    relay_existing(&state, route, &id).await
}

/// Axum response wrapper that maps [`RpcError`] variants and the
/// no-middleManagers case onto well-known HTTP statuses.
#[derive(Debug)]
enum OverlordError {
    NoMiddlemanagers,
    /// DD R35: the routing table is full and every routed task is still
    /// active, so no entry could be evicted to admit the new task.
    CapacityExceeded(usize),
    Upstream(RpcError),
}

impl IntoResponse for OverlordError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            OverlordError::NoMiddlemanagers => (
                StatusCode::SERVICE_UNAVAILABLE,
                "overlord has no middleManagers configured".to_string(),
            ),
            OverlordError::CapacityExceeded(cap) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("overlord routing table at capacity ({cap} active tasks); retry later"),
            ),
            OverlordError::Upstream(RpcError::Http { status: code, body }) => (
                StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
                body,
            ),
            OverlordError::Upstream(RpcError::Transport(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("transport: {msg}"))
            }
            OverlordError::Upstream(RpcError::Serde(msg)) => {
                (StatusCode::BAD_GATEWAY, format!("serde: {msg}"))
            }
            OverlordError::Upstream(RpcError::NotFound(msg)) => (StatusCode::NOT_FOUND, msg),
            OverlordError::Upstream(RpcError::Custom(msg)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
        };
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_rpc::{MockMiddleManagerClient, TaskKind, TaskState};

    #[tokio::test]
    async fn overlord_state_round_robins_middlemanager_selection() {
        let m1 = Arc::new(MockMiddleManagerClient::new()) as Arc<dyn MiddleManagerClient>;
        let m2 = Arc::new(MockMiddleManagerClient::new()) as Arc<dyn MiddleManagerClient>;
        let state = OverlordState::from_clients(vec![m1, m2]);
        assert_eq!(state.select_index(), Some(0));
        assert_eq!(state.select_index(), Some(1));
        assert_eq!(state.select_index(), Some(0));
    }

    #[tokio::test]
    async fn overlord_state_with_no_middlemanagers_returns_none() {
        let state = OverlordState::from_clients(vec![]);
        assert_eq!(state.select_index(), None);
    }

    #[tokio::test]
    async fn overlord_dispatch_records_routing_and_relays_status() {
        let mock = Arc::new(MockMiddleManagerClient::new());
        let state = OverlordState::from_clients(vec![mock.clone() as Arc<dyn MiddleManagerClient>]);

        let task = TaskAssignment::new(TaskKind::Index, "ds-test");
        let id = task.task_id.clone();

        // Drive the dispatch path directly (bypassing axum) to keep
        // this test free of `tower::ServiceExt` plumbing.
        let idx = state.select_index().expect("idx");
        let initial = state.middlemanagers[idx]
            .assign_task(task)
            .await
            .expect("assign");
        state
            .routing
            .lock()
            .await
            .insert(id.clone(), RouteState::Routed(idx));
        assert_eq!(initial.state, TaskState::Pending);

        // Force-set the status on the mock so the poll observes a
        // non-pending state, then poll via the routing table.
        mock.set_status(
            id.clone(),
            TaskStatus {
                task_id: id.clone(),
                state: TaskState::Success,
                message: "done".into(),
            },
        );
        let routed_idx = state.routing.lock().await.get(&id).expect("routed").index();
        assert_eq!(routed_idx, 0);
        let polled = state.middlemanagers[routed_idx]
            .task_status(&id)
            .await
            .expect("poll");
        assert_eq!(polled.state, TaskState::Success);
        assert_eq!(polled.message, "done");
    }

    #[tokio::test]
    async fn dispatch_rejects_at_capacity_and_dedups() {
        // DD R35: with a tiny routing cap and tasks left in an active
        // (Pending) state, a new distinct task beyond capacity is
        // rejected (503), a duplicate taskId is idempotent (no second
        // dispatch to a different MM), and the routing table never
        // exceeds the cap. A terminal task is evicted to admit a fresh
        // one.
        let mock = Arc::new(MockMiddleManagerClient::new());
        let state = OverlordState::from_clients(vec![mock.clone() as Arc<dyn MiddleManagerClient>])
            .with_max_routes(1);

        // First task: accepted, routed, left Pending (active).
        let t1 = TaskAssignment::new(TaskKind::Index, "ds");
        let id1 = t1.task_id.clone();
        let r1 = dispatch_task(State(state.clone()), Json(t1.clone()))
            .await
            .expect("first dispatch ok");
        assert_eq!(r1.0.state, TaskState::Pending);
        assert_eq!(state.routing.lock().await.len(), 1);

        // A second DISTINCT task cannot be admitted (cap=1, t1 active).
        let t2 = TaskAssignment::new(TaskKind::Index, "ds");
        let err = dispatch_task(State(state.clone()), Json(t2))
            .await
            .expect_err("distinct task beyond capacity must be rejected");
        assert!(matches!(err, OverlordError::CapacityExceeded(1)));
        assert_eq!(
            mock.recorded_assignments().len(),
            1,
            "rejected task must not have been dispatched",
        );

        // Re-dispatching the SAME id is idempotent: it relays the
        // existing routed status and does NOT dispatch a second time.
        let r1_again = dispatch_task(State(state.clone()), Json(t1))
            .await
            .expect("idempotent re-dispatch of an existing task id must succeed");
        assert_eq!(r1_again.0.task_id, id1);
        assert_eq!(
            mock.recorded_assignments().len(),
            1,
            "duplicate dispatch must not re-execute the task",
        );
        assert_eq!(state.routing.lock().await.len(), 1);

        // Drive t1 to a terminal state; now a fresh task evicts it.
        mock.set_status(
            id1.clone(),
            TaskStatus {
                task_id: id1.clone(),
                state: TaskState::Success,
                message: "done".into(),
            },
        );
        let t3 = TaskAssignment::new(TaskKind::Index, "ds");
        let id3 = t3.task_id.clone();
        let _ = dispatch_task(State(state.clone()), Json(t3))
            .await
            .expect("dispatch after terminal eviction must succeed");
        assert_eq!(mock.recorded_assignments().len(), 2);

        // State never exceeded the cap, and t1's route was evicted.
        let routing = state.routing.lock().await;
        assert_eq!(routing.len(), 1);
        assert!(routing.contains_key(&id3));
        assert!(!routing.contains_key(&id1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_duplicate_dispatch_assigns_exactly_once() {
        // DD R36: many CONCURRENT requests for the SAME task_id must result in
        // exactly ONE upstream assignment. The reservation under the routing
        // lock (a `Dispatching` marker inserted before upstream dispatch) closes
        // the check-then-dispatch race that could otherwise route a duplicate to
        // a different middleManager and execute it twice.
        let mock = Arc::new(MockMiddleManagerClient::new());
        let state = OverlordState::from_clients(vec![mock.clone() as Arc<dyn MiddleManagerClient>]);
        let task = TaskAssignment::new(TaskKind::Index, "ds");

        let mut handles = Vec::new();
        for _ in 0..24 {
            let s = state.clone();
            let t = task.clone();
            handles.push(tokio::spawn(async move {
                dispatch_task(State(s), Json(t)).await
            }));
        }
        for h in handles {
            let _ = h.await.expect("join");
        }

        assert_eq!(
            mock.recorded_assignments().len(),
            1,
            "concurrent duplicates of one task_id must dispatch upstream exactly once",
        );
        assert_eq!(state.routing.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn dispatching_reservation_relays_pending_not_404() {
        // DD R37: a duplicate that arrives while the first dispatch is still
        // in flight (route is `Dispatching`) must report `Pending`, NOT poll the
        // middleManager for a task it has not registered yet (which would 404
        // and falsely reject the duplicate).
        let mock = Arc::new(MockMiddleManagerClient::new());
        let state = OverlordState::from_clients(vec![mock as Arc<dyn MiddleManagerClient>]);
        let report = relay_existing(&state, RouteState::Dispatching(0), "task-x")
            .await
            .expect("a Dispatching reservation must report Pending, not error");
        assert_eq!(report.0.task_id, "task-x");
        assert_eq!(report.0.state, TaskState::Pending);
    }

    #[tokio::test]
    async fn failed_dispatch_rolls_back_reservation() {
        // DD R37: when upstream assignment fails, the `Dispatching` reservation
        // must be rolled back (not leaked), so capacity is reclaimed.
        let mock = Arc::new(MockMiddleManagerClient::new());
        mock.push_initial_status(Err(ferrodruid_rpc::RpcError::Custom("boom".into())));
        let state = OverlordState::from_clients(vec![mock as Arc<dyn MiddleManagerClient>]);
        let task = TaskAssignment::new(TaskKind::Index, "ds");
        let err = dispatch_task(State(state.clone()), Json(task))
            .await
            .expect_err("a failing upstream dispatch must surface an error");
        assert!(matches!(err, OverlordError::Upstream(_)));
        assert!(
            state.routing.lock().await.is_empty(),
            "a failed dispatch must roll back its reservation, leaving no leak",
        );
    }
}
