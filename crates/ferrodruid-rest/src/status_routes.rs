// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Status endpoints: /status, /status/health, /status/live,
//! /status/properties, /status/selfDiscovered.
//!
//! # Wave 36-B — real readiness probe
//!
//! Up through Wave 35 `/status/health` was a hardcoded `Json(true)`
//! (Codex DD R2 finding: "Docker/Helm/ALB greenlight broken nodes the
//! moment the listener binds").  Wave 36-B replaces it with a real
//! readiness probe that exercises every subsystem an orchestrator
//! actually depends on:
//!
//! * **MetadataStore reachable** — call a noop read
//!   ([`MetadataStore::get_all_data_sources`]); a connection drop or
//!   schema-uninitialised store fails this check.
//! * **Historical loaded** — every in-process Historical reports
//!   `is_initial_load_complete() == true`.
//! * **Auth store readable** (when `auth_enabled`) — at least one user
//!   exists and the store is structurally consistent.
//!
//! When any check fails, the endpoint returns `503 Service Unavailable`
//! with a `{"ok": false, "checks": {...}}` body listing exactly which
//! subsystem is unhealthy.
//!
//! `/status/live` is a separate, distinct probe — it returns
//! `200 OK` as long as the event loop is responsive.  Liveness is the
//! failsafe Docker/k8s use to decide *not* to kill a struggling-but-
//! recoverable node, so it must never fail simply because a downstream
//! is degraded.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;

use crate::AppState;

/// GET /status — version and module information.
pub(crate) async fn handle_status() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "modules": [],
        "memory": {
            "maxMemory": 0,
            "totalMemory": 0,
            "freeMemory": 0,
            "usedMemory": 0
        }
    }))
}

/// GET /status/health — readiness probe.
///
/// Returns `200 {"ok": true, "checks": {"metadata": true,
/// "historical": true, "auth": true}}` when every subsystem reports
/// healthy.  Returns `503` with `"ok": false` and the failing checks
/// flipped to `false` when any subsystem is unreachable.
pub(crate) async fn handle_health(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    // 1. Metadata store: try a noop read.
    let metadata_ok = state.metadata.get_all_data_sources().await.is_ok();

    // 2. Historical(s): every in-process Historical must have completed
    //    its initial-load sweep.  In single-binary mode the Historical
    //    is instantiated synchronously so this is `true` immediately;
    //    the flag is here so a future deep-storage bootstrap can flip
    //    it without breaking the contract.
    let historical_ok = state
        .historicals
        .iter()
        .all(|h| h.is_initial_load_complete());

    // 3. Auth store: when auth enforcement is on, the store must be
    //    structurally readable AND non-empty (an empty store with auth
    //    on means no operator can ever authenticate).  When auth is
    //    disabled the check is reported as `true` because the auth
    //    subsystem is intentionally bypassed — see `is_public_path` /
    //    `auth_middleware` in `crate::middleware`.
    let auth_ok = if state.auth_enabled {
        let store = state.auth_store.read();
        store.is_readable() && store.user_count() > 0
    } else {
        true
    };

    let all_ok = metadata_ok && historical_ok && auth_ok;
    let body = serde_json::json!({
        "ok": all_ok,
        "checks": {
            "metadata": metadata_ok,
            "historical": historical_ok,
            "auth": auth_ok,
        }
    });

    let status = if all_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body))
}

/// GET /status/live — liveness probe.
///
/// Liveness reports whether the process is alive and the async runtime
/// is responsive.  This handler is intentionally trivial: an
/// orchestrator that gets a `200` here knows the binary itself is not
/// wedged, and should NOT kill a struggling-but-recoverable node just
/// because `/status/health` is briefly unhealthy.
pub(crate) async fn handle_live() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(serde_json::json!({"ok": true})))
}

/// GET /status/properties — runtime properties.
pub(crate) async fn handle_properties() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

/// GET /status/selfDiscovered — self-discovery status.
pub(crate) async fn handle_self_discovered() -> Json<serde_json::Value> {
    Json(serde_json::json!({"selfDiscovered": true}))
}
