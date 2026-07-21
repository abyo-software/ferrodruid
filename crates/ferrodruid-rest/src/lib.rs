// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Axum HTTP server and Druid REST API for FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod auth_routes;
mod coordinator_routes;
mod indexer_routes;
mod infoschema;
mod lookup_routes;
mod metrics_routes;
pub mod middleware;
mod msq_routes;
mod query_routes;
mod status_routes;
mod ui_routes;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_historical::Historical;
use ferrodruid_lookup::LookupManager;
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_telemetry::Metrics;
use parking_lot::RwLock;

use crate::middleware::{
    AuthLayer, AuthzLayer, AuthzRule, RateLimiter, RequiredPermission, auth_middleware,
    authz_middleware, rate_limit_middleware,
};

/// Shared application state for all route handlers.
pub struct AppState {
    /// The coordinator for segment management.
    pub coordinator: Arc<Coordinator>,
    /// The overlord for task/supervisor management.
    pub overlord: Arc<Overlord>,
    /// The metadata store.
    pub metadata: Arc<MetadataStore>,
    /// The authentication store.
    ///
    /// Wrapped in a `RwLock` so a credential rotation (the change-credential
    /// endpoint) can mutate it at runtime while the auth middleware — which
    /// shares this same `Arc` via the `AuthLayer` — verifies requests under
    /// a read lock.
    pub auth_store: Arc<RwLock<AuthStore>>,
    /// Directory holding the persisted admin credential
    /// (`<data_dir>/auth`).  The change-credential handler re-writes
    /// `admin.json` here (mode `0600`) after a rotation so the new password
    /// survives a restart.  `None` disables persistence (used by tests that
    /// keep the store in memory only).
    pub auth_cred_dir: Option<PathBuf>,
    /// The RBAC authorizer.
    pub authorizer: Arc<Authorizer>,
    /// Whether to enforce authentication on every non-public route.
    /// Defaults to `true`; set to `false` only for loopback test rigs.
    pub auth_enabled: bool,
    /// The query broker for scatter/gather routing.
    pub broker: Arc<Broker>,
    /// In-process Historical nodes for single-binary mode.
    pub historicals: Vec<Arc<Historical>>,
    /// Server start time.
    pub start_time: chrono::DateTime<chrono::Utc>,
    /// Lookup manager for dimension value mapping.
    pub lookup_manager: Arc<LookupManager>,
    /// Prometheus metrics.
    pub metrics: Arc<Metrics>,
    /// Multi-Stage Query task manager.
    pub msq_manager: Arc<MsqManager>,
    /// Maximum number of concurrent in-flight requests across the
    /// rate-limited routes.  `0` disables the rate limiter (not
    /// recommended in production); the bin defaults to `100`.
    /// See `ferrodruid_common::config::RateLimitConfig`.
    pub rate_limit_max_concurrent: usize,
}

/// Create the Axum router with all Druid REST API routes.
///
/// When `state.auth_enabled` is `true`, every route except `/status/health`,
/// `/status/live`, and `/metrics` requires a valid `Authorization: Basic
/// <base64>` header matched against `state.auth_store`.  Missing or
/// invalid credentials receive `401 Unauthorized` with a Druid-shaped
/// JSON envelope and bump `ferrodruid_auth_failures_total`.
///
/// Wave 36-B wires:
/// * **Rate limiter** — concurrency-cap (`state.rate_limit_max_concurrent`)
///   layered as a `from_fn_with_state` on the entire router; over-cap
///   requests receive `429 Too Many Requests`.
/// * **Auth middleware metrics** — 401s now bump
///   `ferrodruid_auth_failures_total`.
pub fn create_router(state: Arc<AppState>) -> Router {
    let lookup_mgr = Arc::clone(&state.lookup_manager);
    let metrics = Arc::clone(&state.metrics);
    let auth_layer = Arc::new(AuthLayer::with_metrics(
        state.auth_enabled,
        Arc::clone(&state.auth_store),
        Arc::clone(&state.metrics),
    ));
    // Wave 40-C: per-route RBAC.  The same `auth_enabled` flag gates
    // authorization so test rigs (auth_enabled=false) keep their existing
    // behaviour: requests pass straight through without authn or authz.
    let authz_layer = Arc::new(AuthzLayer::new(
        state.auth_enabled,
        Arc::clone(&state.authorizer),
        build_default_policy(),
    ));
    let rate_limiter = Arc::new(RateLimiter::new(state.rate_limit_max_concurrent));

    Router::new()
        // Query endpoints
        .route("/druid/v2/", post(query_routes::handle_native_query))
        .route("/druid/v2", post(query_routes::handle_native_query))
        // SQL query endpoint
        .route("/druid/v2/sql", post(query_routes::handle_sql_query))
        .route("/druid/v2/sql/", post(query_routes::handle_sql_query))
        // MSQ (Multi-Stage Query) endpoints
        .route(
            "/druid/v2/sql/task",
            post(msq_routes::handle_submit_msq_task),
        )
        .route(
            "/druid/v2/sql/queries/{id}",
            get(msq_routes::handle_get_msq_task).delete(msq_routes::handle_cancel_msq_task),
        )
        .route(
            "/druid/v2/sql/queries/{id}/reports",
            get(msq_routes::handle_get_msq_report),
        )
        // Status endpoints
        .route("/status", get(status_routes::handle_status))
        .route("/status/health", get(status_routes::handle_health))
        .route("/status/live", get(status_routes::handle_live))
        .route("/status/properties", get(status_routes::handle_properties))
        .route(
            "/status/selfDiscovered",
            get(status_routes::handle_self_discovered),
        )
        // Coordinator endpoints
        .route(
            "/druid/coordinator/v1/datasources",
            get(coordinator_routes::handle_datasources),
        )
        .route(
            "/druid/coordinator/v1/datasources/{datasource}",
            get(coordinator_routes::handle_datasource)
                .delete(coordinator_routes::handle_disable_datasource)
                .post(coordinator_routes::handle_enable_datasource),
        )
        .route(
            "/druid/coordinator/v1/datasources/{datasource}/segments",
            get(coordinator_routes::handle_datasource_segments),
        )
        .route(
            "/druid/coordinator/v1/datasources/{datasource}/segments/{segmentId}",
            delete(coordinator_routes::handle_disable_segment),
        )
        .route(
            "/druid/coordinator/v1/rules/{datasource}",
            get(coordinator_routes::handle_rules).post(coordinator_routes::handle_set_rules),
        )
        // Coordinator dynamic config
        .route(
            "/druid/coordinator/v1/config",
            get(coordinator_routes::handle_get_config).post(coordinator_routes::handle_set_config),
        )
        .route(
            "/druid/coordinator/v1/config/history",
            get(coordinator_routes::handle_config_history),
        )
        // Load queue
        .route(
            "/druid/coordinator/v1/loadqueue",
            get(coordinator_routes::handle_loadqueue),
        )
        .route(
            "/druid/coordinator/v1/loadqueue/{serverName}",
            get(coordinator_routes::handle_loadqueue_server),
        )
        // Server inventory
        .route(
            "/druid/coordinator/v1/servers",
            get(coordinator_routes::handle_servers),
        )
        .route(
            "/druid/coordinator/v1/servers/{serverName}",
            get(coordinator_routes::handle_server),
        )
        .route(
            "/druid/coordinator/v1/servers/{serverName}/segments",
            get(coordinator_routes::handle_server_segments),
        )
        // Metadata endpoints
        .route(
            "/druid/coordinator/v1/metadata/datasources",
            get(coordinator_routes::handle_metadata_datasources),
        )
        .route(
            "/druid/coordinator/v1/metadata/datasources/{datasource}/segments",
            get(coordinator_routes::handle_metadata_datasource_segments),
        )
        // Overlord/Indexer endpoints
        .route(
            "/druid/indexer/v1/task",
            post(indexer_routes::handle_submit_task),
        )
        .route(
            "/druid/indexer/v1/tasks",
            get(indexer_routes::handle_get_tasks),
        )
        .route(
            "/druid/indexer/v1/task/{taskId}",
            get(indexer_routes::handle_get_task),
        )
        .route(
            "/druid/indexer/v1/task/{taskId}/status",
            get(indexer_routes::handle_get_task_status),
        )
        .route(
            "/druid/indexer/v1/task/{taskId}/shutdown",
            post(indexer_routes::handle_shutdown_task),
        )
        .route(
            "/druid/indexer/v1/completeTasks",
            get(indexer_routes::handle_complete_tasks),
        )
        .route(
            "/druid/indexer/v1/runningTasks",
            get(indexer_routes::handle_running_tasks),
        )
        .route(
            "/druid/indexer/v1/waitingTasks",
            get(indexer_routes::handle_waiting_tasks),
        )
        .route(
            "/druid/indexer/v1/supervisor",
            post(indexer_routes::handle_create_supervisor)
                .get(indexer_routes::handle_get_supervisors),
        )
        .route(
            "/druid/indexer/v1/supervisor/{id}",
            get(indexer_routes::handle_get_supervisor),
        )
        .route(
            "/druid/indexer/v1/supervisor/{id}/shutdown",
            post(indexer_routes::handle_shutdown_supervisor),
        )
        // Basic-security credential management: change-password endpoint.
        // Reachable by a forced-change user (the auth middleware exempts
        // their own credential path) and by admins for any user; the
        // handler enforces the own-vs-admin rule.
        .route(
            "/druid-ext/basic-security/authentication/db/basic/users/{userName}/credential",
            post(auth_routes::handle_change_credential),
        )
        .with_state(state)
        // Web Console UI routes (no state required)
        .route("/", get(ui_routes::handle_root_redirect))
        .route(
            "/unified-console.html",
            get(ui_routes::handle_unified_console),
        )
        .route(
            "/console/datasources",
            get(ui_routes::handle_console_datasources),
        )
        .route("/console/query", get(ui_routes::handle_console_query))
        .route("/console/segments", get(ui_routes::handle_console_segments))
        .route(
            "/console/supervisors",
            get(ui_routes::handle_console_supervisors),
        )
        .route("/console/tasks", get(ui_routes::handle_console_tasks))
        // Lookup endpoints (separate state: LookupManager)
        .route(
            "/druid/coordinator/v1/lookups/config",
            get(lookup_routes::handle_list_tiers),
        )
        .route(
            "/druid/coordinator/v1/lookups/config/{tier}",
            get(lookup_routes::handle_list_lookups_in_tier),
        )
        .route(
            "/druid/coordinator/v1/lookups/config/{tier}/{id}",
            get(lookup_routes::handle_get_lookup)
                .post(lookup_routes::handle_create_lookup)
                .delete(lookup_routes::handle_delete_lookup),
        )
        .route(
            "/druid/listen/v1/lookups",
            get(lookup_routes::handle_listen_lookups),
        )
        .with_state(lookup_mgr)
        // Metrics endpoint (separate state: Metrics)
        .route("/metrics", get(metrics_routes::handle_metrics))
        .with_state(metrics)
        // Authorization middleware (Wave 40-C): RBAC per-route via static
        // policy table.  Layered *inside* the auth layer so it runs
        // *after* `auth_middleware` has populated the
        // `AuthenticatedUser` extension on the request.  Public paths
        // are short-circuited via `is_public_path`.
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&authz_layer),
            authz_middleware,
        ))
        // Auth middleware: layered before the rate limiter so the rate
        // limiter is the *outermost* layer.  Public paths
        // (`/status/health`, `/status/live`, `/metrics`) are
        // short-circuited inside `auth_middleware` itself.
        .layer(axum::middleware::from_fn_with_state(
            auth_layer,
            auth_middleware,
        ))
        // Rate limiter (Wave 36-B): outermost layer so over-cap requests
        // return 429 before any auth-verify CPU is spent.  Configurable
        // via `AppState::rate_limit_max_concurrent`; `0` disables.
        .layer(axum::middleware::from_fn_with_state(
            rate_limiter,
            rate_limit_middleware,
        ))
}

/// Build the default Wave 40-C authorization policy.
///
/// Returns the static list of [`AuthzRule`]s consulted by
/// [`authz_middleware`].  Rules are matched in declaration order; the first
/// rule whose method and path-prefix match wins.  Place the most specific
/// rules (e.g. exact-path mutating routes) before broader prefix rules.
///
/// Permission scheme:
///
/// * `Datasource:Read`  — query endpoints, datasource/segment GET, server
///   inventory, metadata, lookup configuration GET.
/// * `Datasource:Write` — mutating routes on data: `POST`/`DELETE` on
///   datasources, segments, tasks, supervisors, MSQ submits, lookup
///   create/delete.
/// * `Config:Read`      — `GET /druid/coordinator/v1/config*`,
///   `/status/properties`.
/// * `Config:Write`     — `POST /druid/coordinator/v1/config`,
///   `POST /druid/coordinator/v1/rules/{ds}`.
/// * `State:Read`       — `/status` (non-properties), `/status/selfDiscovered`,
///   `/console/*`, `/unified-console.html`, `/`.
///
/// Public probe routes (`/status/health`, `/status/live`, `/metrics`) are
/// **not** in the table: they are short-circuited inside the middleware.
#[must_use]
pub fn build_default_policy() -> Vec<AuthzRule> {
    use axum::http::Method;

    let rule = |method: Method, prefix: &str, required: RequiredPermission| AuthzRule {
        method: Some(method),
        path_prefix: prefix.to_string(),
        required,
    };

    vec![
        // ---- Coordinator: write rules + config ----------------------
        rule(
            Method::POST,
            "/druid/coordinator/v1/config",
            RequiredPermission::config_write(),
        ),
        rule(
            Method::GET,
            "/druid/coordinator/v1/config",
            RequiredPermission::config_read(),
        ),
        rule(
            Method::POST,
            "/druid/coordinator/v1/rules/",
            RequiredPermission::config_write(),
        ),
        rule(
            Method::GET,
            "/druid/coordinator/v1/rules/",
            RequiredPermission::config_read(),
        ),
        // ---- Coordinator: datasources -------------------------------
        rule(
            Method::DELETE,
            "/druid/coordinator/v1/datasources",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::POST,
            "/druid/coordinator/v1/datasources",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::GET,
            "/druid/coordinator/v1/datasources",
            RequiredPermission::datasource_read(),
        ),
        // ---- Coordinator: load queue / servers / metadata (read) ----
        rule(
            Method::GET,
            "/druid/coordinator/v1/loadqueue",
            RequiredPermission::state_read(),
        ),
        rule(
            Method::GET,
            "/druid/coordinator/v1/servers",
            RequiredPermission::state_read(),
        ),
        rule(
            Method::GET,
            "/druid/coordinator/v1/metadata",
            RequiredPermission::datasource_read(),
        ),
        // ---- Lookup config ------------------------------------------
        rule(
            Method::POST,
            "/druid/coordinator/v1/lookups",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::DELETE,
            "/druid/coordinator/v1/lookups",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::GET,
            "/druid/coordinator/v1/lookups",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::GET,
            "/druid/listen/v1/lookups",
            RequiredPermission::datasource_read(),
        ),
        // ---- Indexer / Overlord -------------------------------------
        rule(
            Method::POST,
            "/druid/indexer/v1/task",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::GET,
            "/druid/indexer/v1/task",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::GET,
            "/druid/indexer/v1/tasks",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::GET,
            "/druid/indexer/v1/completeTasks",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::GET,
            "/druid/indexer/v1/runningTasks",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::GET,
            "/druid/indexer/v1/waitingTasks",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::POST,
            "/druid/indexer/v1/supervisor",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::GET,
            "/druid/indexer/v1/supervisor",
            RequiredPermission::datasource_read(),
        ),
        // ---- Native query + SQL + MSQ -------------------------------
        rule(
            Method::POST,
            "/druid/v2/sql/task",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::DELETE,
            "/druid/v2/sql/queries",
            RequiredPermission::datasource_write(),
        ),
        rule(
            Method::GET,
            "/druid/v2/sql/queries",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::POST,
            "/druid/v2/sql",
            RequiredPermission::datasource_read(),
        ),
        rule(
            Method::POST,
            "/druid/v2",
            RequiredPermission::datasource_read(),
        ),
        // ---- Status (non-public) ------------------------------------
        rule(
            Method::GET,
            "/status/properties",
            RequiredPermission::config_read(),
        ),
        rule(
            Method::GET,
            "/status/selfDiscovered",
            RequiredPermission::state_read(),
        ),
        rule(Method::GET, "/status", RequiredPermission::state_read()),
        // ---- UI / console (read-only) -------------------------------
        rule(Method::GET, "/console/", RequiredPermission::state_read()),
        rule(
            Method::GET,
            "/unified-console.html",
            RequiredPermission::state_read(),
        ),
        rule(Method::GET, "/", RequiredPermission::state_read()),
    ]
}

/// Build a Druid-compatible error JSON response.
fn error_response(
    status: axum::http::StatusCode,
    error: &str,
    message: &str,
    error_class: &str,
) -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    (
        status,
        axum::Json(serde_json::json!({
            "error": error,
            "errorMessage": message,
            "errorClass": error_class,
            "host": "localhost"
        })),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn setup() -> Router {
        let metadata = MetadataStore::new_in_memory().await.expect("create store");
        metadata.initialize().await.expect("init schema");
        let metadata = Arc::new(metadata);

        let coordinator = Arc::new(Coordinator::new(Arc::clone(&metadata)));
        let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));
        let auth_store = Arc::new(RwLock::new(AuthStore::new()));
        let authorizer = Arc::new(Authorizer::new().with_admin_role());
        let broker = Arc::new(Broker::new());
        let lookup_manager = Arc::new(LookupManager::new());
        let metrics = Arc::new(ferrodruid_telemetry::Metrics::new());

        let msq_manager = Arc::new(MsqManager::new());

        let state = Arc::new(AppState {
            coordinator,
            overlord,
            metadata,
            auth_store,
            auth_cred_dir: None,
            authorizer,
            auth_enabled: false,
            broker,
            historicals: Vec::new(),
            start_time: chrono::Utc::now(),
            lookup_manager,
            metrics,
            msq_manager,
            rate_limit_max_concurrent: 0, // disabled in unit tests
        });

        create_router(state)
    }

    #[tokio::test]
    async fn get_status_returns_version() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert!(json.get("version").is_some());
    }

    #[tokio::test]
    async fn get_health_returns_ok_envelope() {
        // Wave 36-B: `/status/health` is a real readiness probe.  When
        // every subsystem is healthy it returns the JSON envelope
        // `{"ok": true, "checks": {"metadata": true, "historical":
        // true, "auth": true}}`.  This test was previously asserting
        // the now-removed hardcoded `Json(true)`; updated to match the
        // real contract.
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status/health")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert_eq!(json["ok"], serde_json::Value::Bool(true));
        assert_eq!(json["checks"]["metadata"], serde_json::Value::Bool(true));
        assert_eq!(json["checks"]["historical"], serde_json::Value::Bool(true));
        // Auth is `true` because the test setup runs with `auth_enabled: false`
        // (auth subsystem is intentionally bypassed).
        assert_eq!(json["checks"]["auth"], serde_json::Value::Bool(true));
    }

    #[tokio::test]
    async fn get_self_discovered() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status/selfDiscovered")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert_eq!(json["selfDiscovered"], true);
    }

    #[tokio::test]
    async fn post_native_query_with_timeseries() {
        let app = setup().await;

        let query = serde_json::json!({
            "queryType": "timeseries",
            "dataSource": {"type": "table", "name": "wiki"},
            "intervals": ["2024-01-01/2024-01-02"],
            "granularity": "day",
            "aggregations": [{"type": "count", "name": "cnt"}]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/v2/")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&query).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn post_native_query_invalid_json() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/v2/")
                    .header("content-type", "application/json")
                    .body(Body::from("not valid json"))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert!(json.get("error").is_some());
        assert!(json.get("errorMessage").is_some());
    }

    #[tokio::test]
    async fn get_datasources_returns_empty() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/datasources")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert_eq!(json, serde_json::json!([]));
    }

    #[tokio::test]
    async fn submit_task_returns_task_id() {
        let app = setup().await;

        let spec = serde_json::json!({
            "type": "index_kafka",
            "dataSource": "wiki"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/indexer/v1/task")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&spec).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert!(json.get("task").is_some());
        let task_id = json["task"].as_str().expect("task is string");
        assert!(!task_id.is_empty());
    }

    #[tokio::test]
    async fn create_supervisor_returns_id() {
        let app = setup().await;

        // A VALID Kafka spec (the overlord now validates before persist in
        // every build — Codex R14).
        let spec = serde_json::json!({
            "id": "wiki-kafka",
            "type": "kafka",
            "dataSchema": {
                "dataSource": "wiki",
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": ["page"]},
                "granularitySpec": {"rollup": false}
            },
            "ioConfig": {
                "topic": "wiki-events",
                "consumerProperties": {"bootstrap.servers": "kafka:9092"}
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/indexer/v1/supervisor")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&spec).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        assert_eq!(json["id"], "wiki-kafka");
    }

    #[tokio::test]
    async fn get_supervisors_list() {
        let metadata = MetadataStore::new_in_memory().await.expect("create store");
        metadata.initialize().await.expect("init schema");
        let metadata = Arc::new(metadata);
        let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

        // Create a supervisor first (valid spec — validated before persist).
        overlord
            .create_supervisor(serde_json::json!({
                "id": "test-sup",
                "type": "kafka",
                "dataSchema": {
                    "dataSource": "wiki",
                    "timestampSpec": {"column": "__time", "format": "auto"},
                    "dimensionsSpec": {"dimensions": ["page"]},
                    "granularitySpec": {"rollup": false}
                },
                "ioConfig": {"topic": "t", "consumerProperties": {"bootstrap.servers": "kafka:9092"}}
            }))
            .await
            .expect("create sup");

        let state = Arc::new(AppState {
            coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
            overlord,
            metadata,
            auth_store: Arc::new(RwLock::new(AuthStore::new())),
            auth_cred_dir: None,
            authorizer: Arc::new(Authorizer::new().with_admin_role()),
            auth_enabled: false,
            broker: Arc::new(Broker::new()),
            historicals: Vec::new(),
            start_time: chrono::Utc::now(),
            lookup_manager: Arc::new(LookupManager::new()),
            metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
            msq_manager: Arc::new(MsqManager::new()),
            rate_limit_max_concurrent: 0, // disabled in unit tests
        });

        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/indexer/v1/supervisor")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
        let arr = json.as_array().expect("array");
        assert_eq!(arr.len(), 1);
    }

    #[tokio::test]
    async fn post_sql_query_returns_ok() {
        let app = setup().await;

        let body = serde_json::json!({
            "query": "SELECT COUNT(*) AS cnt FROM wiki"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/v2/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn post_sql_explain_returns_plan() {
        let app = setup().await;

        let body = serde_json::json!({
            "query": "EXPLAIN SELECT COUNT(*) AS cnt FROM wiki"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/v2/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let resp_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&resp_body).expect("parse json");
        let arr = json.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert!(arr[0].get("PLAN").is_some());
        assert!(arr[0].get("RESOURCES").is_some());
    }

    #[tokio::test]
    async fn post_sql_invalid_query() {
        let app = setup().await;

        let body = serde_json::json!({
            "query": "NOT VALID SQL AT ALL !!!"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/v2/sql")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn msq_submit_and_get() {
        let app = setup().await;

        let body = serde_json::json!({
            "query": "INSERT INTO wiki SELECT * FROM TABLE(EXTERN(...))"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/v2/sql/task")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);

        let resp_body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&resp_body).expect("parse");
        let task_id = json["taskId"].as_str().expect("taskId");
        assert!(!task_id.is_empty());
    }

    #[tokio::test]
    async fn msq_get_nonexistent_returns_404() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/v2/sql/queries/nonexistent")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn msq_cancel_nonexistent_returns_400() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/druid/v2/sql/queries/nonexistent")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_status_properties() {
        let app = setup().await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status/properties")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");

        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- Coordinator dynamic config ---

    #[tokio::test]
    async fn get_coordinator_config() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/config")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse");
        // Default config should have known fields
        assert!(json.get("millisToWaitBeforeDeleting").is_some());
    }

    #[tokio::test]
    async fn set_and_get_coordinator_config() {
        let app = setup().await;

        let new_config = serde_json::json!({
            "maxSegmentsToMove": 10,
            "replicationThrottleLimit": 20
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/druid/coordinator/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&new_config).expect("ser")))
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_config_history() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/config/history")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- Load queue ---

    #[tokio::test]
    async fn get_loadqueue() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/loadqueue")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_loadqueue_server() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/loadqueue/hist-1")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("parse");
        assert_eq!(json["serverName"], "hist-1");
    }

    // --- Task management ---

    #[tokio::test]
    async fn task_shutdown() {
        let metadata = MetadataStore::new_in_memory().await.expect("create store");
        metadata.initialize().await.expect("init");
        let metadata = Arc::new(metadata);
        let overlord = Arc::new(Overlord::new(Arc::clone(&metadata)));

        let task_id = overlord
            .submit_task(serde_json::json!({"type": "index", "dataSource": "wiki"}))
            .await
            .expect("submit");

        let state = Arc::new(AppState {
            coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
            overlord,
            metadata,
            auth_store: Arc::new(RwLock::new(AuthStore::new())),
            auth_cred_dir: None,
            authorizer: Arc::new(Authorizer::new().with_admin_role()),
            auth_enabled: false,
            broker: Arc::new(Broker::new()),
            historicals: Vec::new(),
            start_time: chrono::Utc::now(),
            lookup_manager: Arc::new(LookupManager::new()),
            metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
            msq_manager: Arc::new(MsqManager::new()),
            rate_limit_max_concurrent: 0, // disabled in unit tests
        });
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/druid/indexer/v1/task/{task_id}/shutdown"))
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_complete_tasks() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/indexer/v1/completeTasks")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_running_tasks() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/indexer/v1/runningTasks")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_waiting_tasks() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/indexer/v1/waitingTasks")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- Datasource management ---

    #[tokio::test]
    async fn disable_enable_datasource() {
        let metadata = MetadataStore::new_in_memory().await.expect("create store");
        metadata.initialize().await.expect("init");

        // Insert a segment
        metadata
            .insert_segment(&ferrodruid_metadata::SegmentMetadataRow {
                id: "wiki_seg1".into(),
                data_source: "wiki".into(),
                created_date: "2024-01-01T00:00:00Z".into(),
                start: "2024-01-01T00:00:00Z".into(),
                end: "2024-02-01T00:00:00Z".into(),
                version: "2024-01-01T00:00:00.000Z".into(),
                used: true,
                payload: serde_json::json!({"dataSource": "wiki"}),
            })
            .await
            .expect("insert");

        let metadata = Arc::new(metadata);
        let state = Arc::new(AppState {
            coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
            overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
            metadata: Arc::clone(&metadata),
            auth_store: Arc::new(RwLock::new(AuthStore::new())),
            auth_cred_dir: None,
            authorizer: Arc::new(Authorizer::new().with_admin_role()),
            auth_enabled: false,
            broker: Arc::new(Broker::new()),
            historicals: Vec::new(),
            start_time: chrono::Utc::now(),
            lookup_manager: Arc::new(LookupManager::new()),
            metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
            msq_manager: Arc::new(MsqManager::new()),
            rate_limit_max_concurrent: 0, // disabled in unit tests
        });
        let app = create_router(state);

        // Disable
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/druid/coordinator/v1/datasources/wiki")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);

        // Verify disabled
        let segs = metadata.get_used_segments("wiki").await.expect("get");
        assert!(segs.is_empty());
    }

    #[tokio::test]
    async fn disable_segment() {
        let metadata = MetadataStore::new_in_memory().await.expect("create store");
        metadata.initialize().await.expect("init");
        metadata
            .insert_segment(&ferrodruid_metadata::SegmentMetadataRow {
                id: "wiki_seg1".into(),
                data_source: "wiki".into(),
                created_date: "2024-01-01T00:00:00Z".into(),
                start: "2024-01-01T00:00:00Z".into(),
                end: "2024-02-01T00:00:00Z".into(),
                version: "2024-01-01T00:00:00.000Z".into(),
                used: true,
                payload: serde_json::json!({"dataSource": "wiki"}),
            })
            .await
            .expect("insert");

        let metadata = Arc::new(metadata);
        let state = Arc::new(AppState {
            coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
            overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
            metadata: Arc::clone(&metadata),
            auth_store: Arc::new(RwLock::new(AuthStore::new())),
            auth_cred_dir: None,
            authorizer: Arc::new(Authorizer::new().with_admin_role()),
            auth_enabled: false,
            broker: Arc::new(Broker::new()),
            historicals: Vec::new(),
            start_time: chrono::Utc::now(),
            lookup_manager: Arc::new(LookupManager::new()),
            metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
            msq_manager: Arc::new(MsqManager::new()),
            rate_limit_max_concurrent: 0, // disabled in unit tests
        });
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/druid/coordinator/v1/datasources/wiki/segments/wiki_seg1")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- Server inventory ---

    #[tokio::test]
    async fn get_servers() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/servers")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_server_detail() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/servers/hist-1")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_server_segments() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/servers/hist-1/segments")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- Metadata endpoints ---

    #[tokio::test]
    async fn get_metadata_datasources() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/metadata/datasources")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_metadata_datasource_segments() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/druid/coordinator/v1/metadata/datasources/wiki/segments")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- CSV ingestion E2E ---

    #[tokio::test]
    async fn csv_ingestion_e2e() {
        let csv_data = "1000,tokyo,1.5\n2000,osaka,2.5\n3000,tokyo,3.5\n";
        let columns = vec!["__time".to_string(), "city".to_string(), "val".to_string()];

        let ingester = ferrodruid_ingest_batch::BatchIngester::new(
            "test_csv_ds".into(),
            "__time".into(),
            vec!["city".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "val"})],
        );

        let result = ingester
            .ingest_csv(csv_data, &columns, ',')
            .expect("csv ingest");

        assert_eq!(result.num_rows, 3);
        assert_eq!(result.data_source, "test_csv_ds");

        // Verify the segment data was built correctly
        let seg = &result.segment_data;
        assert_eq!(seg.dimensions, vec!["city"]);
        assert_eq!(seg.metrics, vec!["val"]);
    }

    // --- UI route tests ---

    #[tokio::test]
    async fn get_unified_console_returns_html() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/unified-console.html")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get("content-type")
            .expect("content-type header")
            .to_str()
            .expect("header string");
        assert!(ct.contains("text/html"));
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let html = String::from_utf8_lossy(&body);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("FerroDruid"));
    }

    #[tokio::test]
    async fn get_console_query_returns_html() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/console/query")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get("content-type")
            .expect("content-type header")
            .to_str()
            .expect("header string");
        assert!(ct.contains("text/html"));
    }

    #[tokio::test]
    async fn get_root_redirects() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        // Permanent redirect
        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        let location = response
            .headers()
            .get("location")
            .expect("location header")
            .to_str()
            .expect("header string");
        assert_eq!(location, "/unified-console.html");
    }

    #[tokio::test]
    async fn get_console_supervisors_returns_html() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/console/supervisors")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_console_tasks_returns_html() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/console/tasks")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_console_segments_returns_html() {
        let app = setup().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/console/segments")
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("send request");
        assert_eq!(response.status(), StatusCode::OK);
    }
}
