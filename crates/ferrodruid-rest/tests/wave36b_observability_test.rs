// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 36-B integration tests: real `/status/health`, separate
//! `/status/live`, wired Prometheus counters, and rate-limit middleware.
//!
//! Together these close three Wave 35 Codex DD R2 High findings:
//!
//! 1. `/status/health` was hardcoded `Json(true)` — Docker/Helm/ALB/ECS
//!    health checks all greenlit broken nodes the moment the listener
//!    bound.  Wave 36-B replaces it with a real readiness probe.
//! 2. Prometheus counters existed only on paper — handlers never
//!    incremented them.  Wave 36-B wires the SQL query, native query,
//!    task submit, and auth-failure hot paths.
//! 3. The concurrency rate limiter was implemented in
//!    `crate::middleware::rate_limit_middleware` but never layered onto
//!    the router.  Wave 36-B wires it via `from_fn_with_state`.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::Authorizer;
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_lookup::LookupManager;
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::middleware::{RateLimiter, rate_limit_middleware};
use ferrodruid_rest::{AppState, create_router};
use tower::ServiceExt;

/// Build a healthy router (auth disabled, metadata initialised, no rate limit).
async fn setup_healthy() -> Router {
    let metadata = MetadataStore::new_in_memory().await.expect("create store");
    metadata.initialize().await.expect("init schema");
    let metadata = Arc::new(metadata);

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store: Arc::new(parking_lot::RwLock::new(AuthStore::new())),
        auth_cred_dir: None,
        authorizer: Arc::new(Authorizer::new().with_admin_role()),
        auth_enabled: false,
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

/// Build a router with a metadata store whose schema has *not* been
/// initialised — `get_all_data_sources` will fail with a SQL error.
async fn setup_metadata_unreachable() -> Router {
    let metadata = MetadataStore::new_in_memory().await.expect("create store");
    // Intentionally skip `metadata.initialize()` so the
    // `druid_segments` table does not exist.  The readiness probe's
    // noop read therefore returns an error.
    let metadata = Arc::new(metadata);

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store: Arc::new(parking_lot::RwLock::new(AuthStore::new())),
        auth_cred_dir: None,
        authorizer: Arc::new(Authorizer::new().with_admin_role()),
        auth_enabled: false,
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

#[tokio::test]
async fn health_returns_200_when_all_checks_pass() {
    let app = setup_healthy().await;

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
    assert_eq!(json["checks"]["auth"], serde_json::Value::Bool(true));
}

#[tokio::test]
async fn health_returns_503_when_metadata_unreachable() {
    let app = setup_metadata_unreachable().await;

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
        StatusCode::SERVICE_UNAVAILABLE,
        "uninitialised metadata schema must fail readiness"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("parse json");
    assert_eq!(json["ok"], serde_json::Value::Bool(false));
    assert_eq!(
        json["checks"]["metadata"],
        serde_json::Value::Bool(false),
        "metadata check must report false"
    );
}

#[tokio::test]
async fn live_always_returns_200() {
    let app = setup_healthy().await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/status/live")
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
}

#[tokio::test]
async fn metrics_endpoint_reflects_handler_calls() {
    let app = setup_healthy().await;

    // Hit POST /druid/v2/sql 5 times.  The data source is "wiki" so we
    // expect `ferrodruid_queries_total{datasource="wiki"} 5` after.
    let body = serde_json::json!({"query": "SELECT COUNT(*) FROM wiki"});
    for _ in 0..5 {
        let app2 = app.clone();
        let response = app2
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

    // Now scrape /metrics and assert the counter advanced.
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send request");
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let text = String::from_utf8(body.to_vec()).expect("utf8 metrics body");
    assert!(
        text.contains("ferrodruid_queries_total{datasource=\"wiki\"} 5"),
        "expected 5 queries to wiki; got body: {text}"
    );
}

/// The concurrency rate-limit middleware returns 429 when the cap is
/// saturated and admits requests again once a slot is released.
///
/// Deterministic by construction. The earlier version raced 32 client
/// requests and hoped at least two were in-flight at the same instant — but
/// `/status` is a fast handler, so on a loaded CI runner the cap=1 limiter
/// drains them one-at-a-time and observes zero 429s (a flaky false failure of
/// `throttled_count >= 1`). Here we hold the single slot ourselves, prove a
/// request to a NON-public path is rejected, that a public probe path is
/// still admitted, and that after releasing the slot the next request
/// succeeds (no counter leak). The limiter's own accounting is unit-tested in
/// `middleware.rs`; this exercises the real `rate_limit_middleware` wired onto
/// a real axum router.
#[tokio::test]
async fn rate_limiter_returns_429_after_threshold() {
    let limiter = Arc::new(RateLimiter::new(1)); // cap = 1 in-flight
    // Saturate the single slot so the next admitted request must be rejected.
    assert!(limiter.try_acquire(), "the first slot must acquire");
    assert!(!limiter.try_acquire(), "cap=1: a second acquire must fail");

    let app = Router::new()
        .route("/status", axum::routing::get(|| async { "ok" }))
        .route("/status/live", axum::routing::get(|| async { "live" }))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&limiter),
            rate_limit_middleware,
        ));

    // With the slot held, a request to a NON-public path must 429.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a saturated cap=1 limiter must reject a non-public request"
    );

    // Public probe paths stay reachable even when the data plane is saturated.
    let probe = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status/live")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(
        probe.status(),
        StatusCode::OK,
        "public probe paths must bypass the rate limit even when saturated"
    );

    // Release the slot; a follow-up legitimate request must now succeed
    // (otherwise the limiter leaks a counter).
    limiter.release();
    let after = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("oneshot");
    assert_eq!(
        after.status(),
        StatusCode::OK,
        "after release, the limiter must admit the next request"
    );
}
