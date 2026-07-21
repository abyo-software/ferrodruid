// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 36-A + Wave 40-C integration tests: auth-on-by-default behaviour
//! plus per-route RBAC enforcement.
//!
//! These tests boot the FerroDruid REST router with `auth_enabled = true`
//! and a real `AuthStore` containing seeded users.  They verify:
//!
//! * Wave 36-A: Mutating routes (`POST /druid/v2/sql`,
//!   `POST /druid/indexer/v1/task`) reject unauthenticated requests with
//!   `401 Unauthorized`.  `GET /status/health` is exempted.
//! * Wave 40-C: An authenticated user with role `viewer` is rejected with
//!   `403 Forbidden` from admin/datasource-write endpoints; the same user
//!   may run queries (`Datasource:Read`); an authenticated user with role
//!   `admin` may hit admin endpoints.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::{Action, Authorizer, Permission, ResourceType};
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_lookup::LookupManager;
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use tower::ServiceExt;

/// Build a router with auth enforcement on and one seeded `admin/secret123`
/// user.
async fn setup_auth_enabled() -> Router {
    let metadata = MetadataStore::new_in_memory().await.expect("create store");
    metadata.initialize().await.expect("init schema");
    let metadata = Arc::new(metadata);

    let mut auth_store = AuthStore::new();
    auth_store
        .add_user("admin", "secret123", vec!["admin".to_string()])
        .expect("seed admin");
    let auth_store = Arc::new(parking_lot::RwLock::new(auth_store));

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store,
        auth_cred_dir: None,
        authorizer: Arc::new(Authorizer::new().with_admin_role()),
        auth_enabled: true,
        broker: Arc::new(Broker::new()),
        historicals: Vec::new(),
        start_time: chrono::Utc::now(),
        lookup_manager: Arc::new(LookupManager::new()),
        metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
        msq_manager: Arc::new(MsqManager::new()),
        rate_limit_max_concurrent: 0, // disabled in this integration suite
    });

    create_router(state)
}

#[tokio::test]
async fn auth_required_on_query() {
    let app = setup_auth_enabled().await;

    let body = serde_json::json!({"query": "SELECT 1"});
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

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "POST /druid/v2/sql without auth must be 401"
    );
}

#[tokio::test]
async fn auth_required_on_task() {
    let app = setup_auth_enabled().await;

    let spec = serde_json::json!({"type": "index", "dataSource": "wiki"});
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

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "POST /druid/indexer/v1/task without auth must be 401"
    );
}

#[tokio::test]
async fn health_does_not_require_auth() {
    let app = setup_auth_enabled().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/status/health")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "/status/health must be reachable without auth"
    );
}

#[tokio::test]
async fn valid_basic_auth_passes_through() {
    use base64::Engine;
    let app = setup_auth_enabled().await;

    let token = base64::engine::general_purpose::STANDARD.encode("admin:secret123");
    let body = serde_json::json!({"query": "SELECT 1"});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/sql")
                .header("content-type", "application/json")
                .header("authorization", format!("Basic {token}"))
                .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "valid Basic auth must NOT receive 401; got {}",
        response.status()
    );
}

#[tokio::test]
async fn wrong_password_rejected() {
    use base64::Engine;
    let app = setup_auth_enabled().await;

    let token = base64::engine::general_purpose::STANDARD.encode("admin:wrong");
    let body = serde_json::json!({"query": "SELECT 1"});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/sql")
                .header("content-type", "application/json")
                .header("authorization", format!("Basic {token}"))
                .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "wrong password must be 401"
    );
}

// ---------------------------------------------------------------------------
// Wave 40-C: per-route RBAC tests.
//
// These tests close the W35-C1 STILL-OPEN finding from the Wave 39 DD
// audit: Wave 36-A wired authentication only, leaving `AppState.authorizer`
// as dead data so an authenticated viewer could hit admin endpoints.  The
// cases below boot the router with both an `admin` and a `viewer` user and
// verify that:
//
// * a `viewer`-role user is rejected from `Datasource:Write` and admin
//   endpoints with `403 Forbidden`,
// * an `admin`-role user passes through to those same endpoints,
// * any authenticated user (admin or viewer) may hit a query endpoint
//   (`Datasource:Read`).
// ---------------------------------------------------------------------------

/// Build a router with both `admin/secret123` (admin role) and
/// `bob/viewerpass` (viewer role) seeded.  The `viewer` role is granted
/// only `Datasource:Read` on `*`.
async fn setup_authz_enabled() -> Router {
    let metadata = MetadataStore::new_in_memory().await.expect("create store");
    metadata.initialize().await.expect("init schema");
    let metadata = Arc::new(metadata);

    let mut auth_store = AuthStore::new();
    auth_store
        .add_user("admin", "secret123", vec!["admin".to_string()])
        .expect("seed admin");
    auth_store
        .add_user("bob", "viewerpass", vec!["viewer".to_string()])
        .expect("seed viewer");
    let auth_store = Arc::new(parking_lot::RwLock::new(auth_store));

    let mut authorizer = Authorizer::new().with_admin_role();
    authorizer.add_permission(
        "viewer",
        Permission {
            resource_type: ResourceType::Datasource,
            resource_pattern: "*".to_string(),
            action: Action::Read,
        },
    );
    let authorizer = Arc::new(authorizer);

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store,
        auth_cred_dir: None,
        authorizer,
        auth_enabled: true,
        broker: Arc::new(Broker::new()),
        historicals: Vec::new(),
        start_time: chrono::Utc::now(),
        lookup_manager: Arc::new(LookupManager::new()),
        metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
        msq_manager: Arc::new(MsqManager::new()),
        rate_limit_max_concurrent: 0,
    });

    create_router(state)
}

fn basic_token(user: &str, pass: &str) -> String {
    use base64::Engine;
    let raw = format!("{user}:{pass}");
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
    format!("Basic {encoded}")
}

#[tokio::test]
async fn authz_blocks_viewer_role_from_admin_endpoint() {
    let app = setup_authz_enabled().await;

    // Viewer attempts to disable a datasource (Datasource:Write).
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/druid/coordinator/v1/datasources/wiki")
                .header("authorization", basic_token("bob", "viewerpass"))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "viewer must NOT be able to DELETE a datasource (Datasource:Write)"
    );
}

#[tokio::test]
async fn authz_blocks_viewer_role_from_task_submit() {
    let app = setup_authz_enabled().await;

    // Viewer attempts to submit an indexing task (Datasource:Write).
    let spec = serde_json::json!({"type": "index", "dataSource": "wiki"});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/indexer/v1/task")
                .header("content-type", "application/json")
                .header("authorization", basic_token("bob", "viewerpass"))
                .body(Body::from(serde_json::to_vec(&spec).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "viewer must NOT be able to submit an indexing task (Datasource:Write)"
    );
}

#[tokio::test]
async fn authz_blocks_viewer_role_from_config_write() {
    let app = setup_authz_enabled().await;

    // Viewer attempts to write coordinator dynamic config (Config:Write).
    let cfg = serde_json::json!({"maxSegmentsToMove": 5});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/coordinator/v1/config")
                .header("content-type", "application/json")
                .header("authorization", basic_token("bob", "viewerpass"))
                .body(Body::from(serde_json::to_vec(&cfg).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "viewer must NOT be able to POST coordinator config (Config:Write)"
    );
}

#[tokio::test]
async fn authz_allows_admin_role_on_admin_endpoint() {
    let app = setup_authz_enabled().await;

    // Admin can DELETE a datasource (Datasource:Write).  The route may
    // still 200/404/etc on the underlying lookup; the authz layer must
    // not 403/401.
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/druid/coordinator/v1/datasources/wiki")
                .header("authorization", basic_token("admin", "secret123"))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_ne!(
        response.status(),
        StatusCode::FORBIDDEN,
        "admin must NOT be 403 on a datasource DELETE"
    );
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "admin with valid Basic auth must NOT be 401"
    );
}

#[tokio::test]
async fn authz_allows_any_authenticated_user_on_query_endpoint() {
    let app = setup_authz_enabled().await;

    // Viewer can run a SQL query (Datasource:Read).
    let body = serde_json::json!({"query": "SELECT 1"});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/sql")
                .header("content-type", "application/json")
                .header("authorization", basic_token("bob", "viewerpass"))
                .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send request");

    assert_ne!(
        response.status(),
        StatusCode::FORBIDDEN,
        "viewer with Datasource:Read must NOT be 403 on POST /druid/v2/sql"
    );
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "viewer with valid Basic auth must NOT be 401"
    );
}

#[tokio::test]
async fn authz_blocks_unknown_role_default_deny() {
    let app = setup_authz_enabled().await;

    // Authenticate as admin (succeeds at authn) and request `/status` —
    // a `State:Read` route.  Both `admin` and `viewer` here are seeded;
    // we now seed a `nobody/blank` user with no roles to exercise the
    // default-deny branch on a non-public path.
    let mut auth_store = AuthStore::new();
    auth_store
        .add_user("nobody", "blank", vec![])
        .expect("seed");
    let auth_store = Arc::new(parking_lot::RwLock::new(auth_store));

    let metadata = MetadataStore::new_in_memory().await.expect("create");
    metadata.initialize().await.expect("init");
    let metadata = Arc::new(metadata);

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store,
        auth_cred_dir: None,
        authorizer: Arc::new(Authorizer::new().with_admin_role()),
        auth_enabled: true,
        broker: Arc::new(Broker::new()),
        historicals: Vec::new(),
        start_time: chrono::Utc::now(),
        lookup_manager: Arc::new(LookupManager::new()),
        metrics: Arc::new(ferrodruid_telemetry::Metrics::new()),
        msq_manager: Arc::new(MsqManager::new()),
        rate_limit_max_concurrent: 0,
    });
    let app2 = create_router(state);
    let _ = app; // silence unused

    let response = app2
        .oneshot(
            Request::builder()
                .uri("/status")
                .header("authorization", basic_token("nobody", "blank"))
                .body(Body::empty())
                .expect("build"),
        )
        .await
        .expect("send");

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "user with no roles must be 403 on /status (default-deny)"
    );
}
