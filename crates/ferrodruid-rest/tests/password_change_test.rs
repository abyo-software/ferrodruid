// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Force-password-change-on-first-login integration tests.
//!
//! Boots the production [`create_router`] with `auth_enabled = true` and a
//! seeded admin whose `must_change_password` flag is set (mirroring the
//! bootstrap admin on a fresh install).  Verifies:
//!
//! * (a) a forced-change admin is `403`ed on a normal endpoint with the
//!   "Password change required" message,
//! * (b) the same admin CAN POST its own credential to rotate the password,
//! * (c) after the rotation the new password works, the flag is cleared, and
//!   a normal endpoint no longer `403`s,
//! * (d) a too-short new password is rejected `400`,
//! * (e) a non-admin may not change another user's credential,
//! * persistence: with a credential directory configured, `admin.json` on
//!   disk carries `must_change_password = false` after the rotation and
//!   round-trips into a fresh store (restart safety).

#![allow(missing_docs)]

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use ferrodruid_auth::{AuthStore, UserRecord};
use ferrodruid_authz::{Action, Authorizer, Permission, ResourceType};
use ferrodruid_broker::Broker;
use ferrodruid_coordinator::Coordinator;
use ferrodruid_lookup::LookupManager;
use ferrodruid_metadata::MetadataStore;
use ferrodruid_msq::MsqManager;
use ferrodruid_overlord::Overlord;
use ferrodruid_rest::{AppState, create_router};
use parking_lot::RwLock;
use tower::ServiceExt;

fn basic(user: &str, pass: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    format!("Basic {encoded}")
}

/// Build a router with auth on, a forced-change `admin` (initial password
/// `initial-password`, admin role) and a non-forced `bob` (viewer role).
/// `cred_dir` configures credential persistence (`None` => in-memory only).
async fn setup(cred_dir: Option<PathBuf>) -> Router {
    let metadata = MetadataStore::new_in_memory().await.expect("create store");
    metadata.initialize().await.expect("init schema");
    let metadata = Arc::new(metadata);

    let mut store = AuthStore::new();
    store
        .add_user_must_change("admin", "initial-password", vec!["admin".to_string()], true)
        .expect("seed admin");
    store
        .add_user("bob", "bob-password", vec!["viewer".to_string()])
        .expect("seed viewer");
    let auth_store = Arc::new(RwLock::new(store));

    let mut authorizer = Authorizer::new().with_admin_role();
    authorizer.add_permission(
        "viewer",
        Permission {
            resource_type: ResourceType::Datasource,
            resource_pattern: "*".to_string(),
            action: Action::Read,
        },
    );

    let state = Arc::new(AppState {
        coordinator: Arc::new(Coordinator::new(Arc::clone(&metadata))),
        overlord: Arc::new(Overlord::new(Arc::clone(&metadata))),
        metadata,
        auth_store,
        auth_cred_dir: cred_dir,
        authorizer: Arc::new(authorizer),
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

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("parse json")
}

fn change_password_request(user: &str, pass: &str, target: &str, new_pw: &str) -> Request<Body> {
    let path =
        format!("/druid-ext/basic-security/authentication/db/basic/users/{target}/credential");
    Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", basic(user, pass))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "password": new_pw })).expect("ser"),
        ))
        .expect("build request")
}

// (a) -------------------------------------------------------------------------
#[tokio::test]
async fn forced_change_admin_is_blocked_on_normal_endpoint() {
    let app = setup(None).await;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .header("authorization", basic("admin", "initial-password"))
                .body(Body::empty())
                .expect("build"),
        )
        .await
        .expect("send");

    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "forced-change admin must be 403 on a normal endpoint"
    );
    let json = body_json(resp).await;
    let msg = json["errorMessage"].as_str().expect("errorMessage");
    assert!(
        msg.contains("Password change required"),
        "403 body must explain the required password change, got: {msg}"
    );
    assert!(
        msg.contains("/credential"),
        "message must point at the credential endpoint, got: {msg}"
    );
}

// (b) + (c) -------------------------------------------------------------------
#[tokio::test]
async fn admin_can_rotate_own_credential_then_use_new_password() {
    let app = setup(None).await;

    // (b) Rotate the password using the forced-change initial password.
    let resp = app
        .clone()
        .oneshot(change_password_request(
            "admin",
            "initial-password",
            "admin",
            "a-fresh-strong-password",
        ))
        .await
        .expect("send");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "forced-change admin must be able to POST its own credential"
    );

    // (c.1) The OLD password is now invalid (401).
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status")
                .header("authorization", basic("admin", "initial-password"))
                .body(Body::empty())
                .expect("build"),
        )
        .await
        .expect("send");
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "old password must no longer authenticate after rotation"
    );

    // (c.2) The NEW password works AND the must-change gate is cleared
    //       (non-403, in fact 200 on a normal endpoint).
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .header("authorization", basic("admin", "a-fresh-strong-password"))
                .body(Body::empty())
                .expect("build"),
        )
        .await
        .expect("send");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "new password must authenticate and clear the force-change gate"
    );
}

// (d) -------------------------------------------------------------------------
#[tokio::test]
async fn too_short_password_is_rejected() {
    let app = setup(None).await;

    let resp = app
        .oneshot(change_password_request(
            "admin",
            "initial-password",
            "admin",
            "short",
        ))
        .await
        .expect("send");

    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "a <8-char password must be rejected with 400"
    );
    let json = body_json(resp).await;
    assert!(
        json.get("errorMessage").is_some(),
        "400 must carry a Druid error envelope"
    );
}

#[tokio::test]
async fn empty_password_is_rejected() {
    let app = setup(None).await;
    let resp = app
        .oneshot(change_password_request(
            "admin",
            "initial-password",
            "admin",
            "",
        ))
        .await
        .expect("send");
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "empty password must be 400"
    );
}

// (e) -------------------------------------------------------------------------
#[tokio::test]
async fn non_admin_cannot_change_another_users_credential() {
    let app = setup(None).await;

    // bob (viewer, NOT forced-change) tries to set admin's password.
    let resp = app
        .oneshot(change_password_request(
            "bob",
            "bob-password",
            "admin",
            "hijacked-password",
        ))
        .await
        .expect("send");

    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a non-admin must not be able to change another user's credential"
    );
}

#[tokio::test]
async fn non_admin_can_change_own_credential() {
    let app = setup(None).await;

    // bob changing bob's own password is allowed (self-service).
    let resp = app
        .oneshot(change_password_request(
            "bob",
            "bob-password",
            "bob",
            "bob-new-strong-password",
        ))
        .await
        .expect("send");

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a user must be able to change their OWN credential regardless of role"
    );
}

// persistence + restart safety ------------------------------------------------
#[tokio::test]
async fn rotation_is_persisted_with_cleared_flag_and_round_trips() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cred_dir = tmp.path().join("auth");
    let app = setup(Some(cred_dir.clone())).await;

    // Rotate the admin password; persistence is enabled.
    let resp = app
        .oneshot(change_password_request(
            "admin",
            "initial-password",
            "admin",
            "persisted-strong-password",
        ))
        .await
        .expect("send");
    assert_eq!(resp.status(), StatusCode::OK, "rotation must succeed");

    // admin.json must now exist on disk with the cleared flag.
    let admin_json = cred_dir.join("admin.json");
    assert!(
        admin_json.exists(),
        "admin.json must be persisted after rotation"
    );

    let bytes = std::fs::read(&admin_json).expect("read admin.json");
    let record: UserRecord = serde_json::from_slice(&bytes).expect("parse admin.json");
    assert!(
        !record.must_change_password,
        "persisted record must have must_change_password = false after rotation"
    );
    assert_eq!(record.username, "admin");
    assert!(record.roles.iter().any(|r| r == "admin"));

    // Restart safety: a FRESH store reloaded from the persisted hash must
    // authenticate the NEW password and stay un-gated.
    let mut reloaded = AuthStore::new();
    reloaded.add_user_with_hash(
        &record.username,
        &record.password_hash,
        record.roles.clone(),
        record.must_change_password,
    );
    let user = reloaded
        .verify("admin", "persisted-strong-password")
        .expect("verify")
        .expect("authenticated");
    assert!(
        !user.must_change_password,
        "reloaded admin must NOT be re-gated after a rotation survived restart"
    );
}
