// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 53 — full-stack middleware integration test.
//!
//! Boots the production [`create_router`] with `auth_enabled = true`,
//! a real [`AuthStore`] (admin + viewer users), an [`Authorizer`]
//! whose `viewer` role is restricted to `Datasource:Read`, and a
//! `rate_limit_max_concurrent = 1` cap.  Then in a single binary the
//! suite exercises four orthogonal paths:
//!
//! 1. **happy authenticated path** — admin POST `/druid/v2/sql` →
//!    `200 OK` + `ferrodruid_queries_total{datasource="wiki"}` ≥ 1.
//! 2. **unauthenticated** — same POST without `Authorization` →
//!    `401 Unauthorized` + `ferrodruid_auth_failures_total` advanced.
//! 3. **viewer-role on admin endpoint** — viewer DELETE
//!    `/druid/coordinator/v1/datasources/wiki` → `403 Forbidden` and
//!    `ferrodruid_auth_failures_total` does NOT advance (authn passed,
//!    authz denied).
//! 4. **burst** — fire 200 requests against `/status` with cap=1 over
//!    a real bound listener; assert at least one `429 Too Many Requests`,
//!    that the limiter releases all slots after the burst (a follow-up
//!    request still gets `200 OK`), and that the per-class error
//!    counter `ferrodruid_query_errors_total` is NOT inflated by the
//!    rate-limit drops (rate-limit drops belong to a different code
//!    path and must not be misclassified as query errors).
//!
//! `#[ignore]`d so the default fast-CI lane stays unaffected.  Run
//! with:
//!
//! ```text
//! cargo test -p ferrodruid-rest --test middleware_stack_integration \
//!     -- --ignored --nocapture
//! ```

#![allow(missing_docs)]

use std::sync::Arc;

use base64::Engine;
use ferrodruid_auth::AuthStore;
use ferrodruid_authz::{Action, Authorizer, Permission, ResourceType};
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_lookup::LookupManager;
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use ferrodruid_telemetry::Metrics;

fn basic_token(user: &str, pass: &str) -> String {
    let raw = format!("{user}:{pass}");
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
    format!("Basic {encoded}")
}

/// Build the full app state with authn ON, viewer role having
/// `Datasource:Read` only, and a tight rate-limit cap.  Returns the
/// `AppState` so the test can scrape `metrics` directly without
/// scraping `/metrics` (avoids the 1-in-flight cap interfering with
/// the assertion path).
async fn build_state(rate_limit_cap: usize, auth_enabled: bool) -> Arc<AppState> {
    let metadata = {
        let m = MetadataStore::new_in_memory()
            .await
            .expect("metadata create");
        m.initialize().await.expect("metadata init");
        Arc::new(m)
    };

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

    Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store,
        auth_cred_dir: None,
        authorizer,
        auth_enabled,
        broker: Arc::new(Broker::new()),
        historicals: Vec::new(),
        start_time: chrono::Utc::now(),
        lookup_manager: Arc::new(LookupManager::new()),
        metrics: Arc::new(Metrics::new()),
        msq_manager: Arc::new(MsqManager::new()),
        rate_limit_max_concurrent: rate_limit_cap,
    })
}

// ---------------------------------------------------------------------------
// Happy path + unauth + viewer-on-admin (oneshot serialised, no rate cap)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn middleware_stack_authn_authz_metrics_paths() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_state(0, true).await; // rate-limit disabled for these cases
    let metrics = Arc::clone(&state.metrics);
    let app = create_router(Arc::clone(&state));

    // -- (1) happy path: admin SELECT against wiki ----------------------
    let q = serde_json::json!({"query": "SELECT COUNT(*) FROM wiki"});
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/sql")
                .header("content-type", "application/json")
                .header("authorization", basic_token("admin", "secret123"))
                .body(Body::from(serde_json::to_vec(&q).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::OK, "admin SQL must be 200");

    let queries_after_happy = metrics.queries_total.with_label_values(&["wiki"]).get();
    assert!(
        queries_after_happy >= 1,
        "ferrodruid_queries_total{{datasource=wiki}} must advance, got {queries_after_happy}",
    );

    // -- (2) unauth: same POST, no Authorization header -----------------
    let auth_failures_before = metrics.auth_failures_total.get();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/druid/v2/sql")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&q).expect("serialize")))
                .expect("build request"),
        )
        .await
        .expect("send");
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "unauthenticated SQL must be 401",
    );
    let auth_failures_after = metrics.auth_failures_total.get();
    assert!(
        auth_failures_after > auth_failures_before,
        "auth_failures_total must advance on 401: before={auth_failures_before} after={auth_failures_after}",
    );

    // -- (3) viewer hits admin endpoint --------------------------------
    let af_before_viewer = metrics.auth_failures_total.get();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/druid/coordinator/v1/datasources/wiki")
                .header("authorization", basic_token("bob", "viewerpass"))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("send");
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "viewer on admin endpoint must be 403",
    );
    let af_after_viewer = metrics.auth_failures_total.get();
    assert_eq!(
        af_after_viewer, af_before_viewer,
        "authz failure (403) must NOT bump auth_failures_total: before={af_before_viewer} after={af_after_viewer}",
    );
}

// ---------------------------------------------------------------------------
// Burst -> 429 over a real listener (rate cap = 1)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn middleware_stack_burst_emits_429_no_query_error_inflation() {
    // Auth disabled for the burst test so we can target /status (a
    // non-public, non-rate-limit-exempt route) without authn
    // interfering.  /status/health, /status/live, and /metrics are
    // explicitly *exempt* from the rate limiter (Wave 40-B), so we
    // must use /status — which IS subject to the cap.
    let state = build_state(1, false).await; // 1 in-flight max, auth off
    let metrics = Arc::clone(&state.metrics);
    let app = create_router(Arc::clone(&state));

    // Bind on an ephemeral port so the OS picks an unused one.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    // Capture the per-class error counter BEFORE the burst.  We use
    // the "rate_limit" class probe so the family is registered even
    // if no handler increments it; then after the storm we assert the
    // delta is zero, i.e. rate-limit rejections did NOT get
    // misclassified as query errors.
    let parse_errors_before = metrics
        .query_errors_total
        .with_label_values(&["parse"])
        .get();
    let exec_errors_before = metrics
        .query_errors_total
        .with_label_values(&["execution"])
        .get();

    // Burst 200 unauthenticated requests against /status (a public,
    // no-auth route), so the 429 path is exercised cleanly without
    // tripping authn first.  /status is wired through the
    // rate_limit_middleware in create_router.
    let url = format!("http://{addr}/status");
    let client = reqwest::Client::new();
    let mut handles = Vec::with_capacity(200);
    for _ in 0..200 {
        let url = url.clone();
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.get(&url).send().await.expect("send").status().as_u16()
        }));
    }

    let mut ok = 0usize;
    let mut throttled = 0usize;
    let mut other = 0usize;
    for h in handles {
        match h.await.expect("join") {
            200 => ok += 1,
            429 => throttled += 1,
            _ => other += 1,
        }
    }
    println!("[burst] ok={ok}, throttled={throttled}, other={other}");
    assert!(
        throttled >= 1,
        "expected at least one 429 with cap=1 across 200 concurrent requests; \
         got ok={ok} throttled={throttled} other={other}",
    );
    assert_eq!(
        other, 0,
        "no unexpected non-200/429 statuses allowed: {other}",
    );

    // After the storm, a follow-up request must succeed — the limiter
    // must have released every slot.
    let final_resp = client
        .get(&url)
        .send()
        .await
        .expect("send post-burst")
        .status();
    assert_eq!(
        final_resp.as_u16(),
        200,
        "post-burst request must succeed (limiter must release)",
    );

    // Critical: rate-limit drops must NOT count as query errors.
    let parse_errors_after = metrics
        .query_errors_total
        .with_label_values(&["parse"])
        .get();
    let exec_errors_after = metrics
        .query_errors_total
        .with_label_values(&["execution"])
        .get();
    assert_eq!(
        parse_errors_after, parse_errors_before,
        "parse error counter must NOT advance from rate-limit storm",
    );
    assert_eq!(
        exec_errors_after, exec_errors_before,
        "execution error counter must NOT advance from rate-limit storm",
    );

    server.abort();
    let _ = server.await;
}
