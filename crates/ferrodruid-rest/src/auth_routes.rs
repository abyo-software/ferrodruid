// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid-compatible basic-security credential-management routes.
//!
//! Implements the change-password endpoint
//! `POST /druid-ext/basic-security/authentication/db/basic/users/{userName}/credential`.
//!
//! Together with the bootstrap admin being created with
//! `must_change_password = true` and the force-change gate in
//! [`crate::middleware::auth_middleware`], this satisfies the AWS
//! Marketplace requirement to *force a password change on first login*: the
//! auto-generated initial admin password can reach ONLY this endpoint until
//! it has been rotated, and rotating it here clears the flag.

use std::path::Path;
use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ferrodruid_auth::{AuthError, AuthenticatedUser, UserRecord};
use serde::Deserialize;

use crate::AppState;

/// Minimum length (in Unicode scalar values) accepted for a new password.
const MIN_PASSWORD_LEN: usize = 8;

/// Username of the bootstrap account persisted to
/// `<data_dir>/auth/admin.json`.  Only this account is reloaded on restart,
/// so only its rotation is persisted (see [`persist_admin_record`]).
const PERSISTED_ADMIN_USERNAME: &str = "admin";

/// Request body for the change-credential endpoint.
#[derive(Debug, Deserialize)]
pub(crate) struct ChangeCredentialRequest {
    /// The new plaintext password.
    password: String,
}

/// `POST /druid-ext/basic-security/authentication/db/basic/users/{userName}/credential`
///
/// Rotates `user_name`'s password.  Requires authentication.  A caller may
/// only change their **own** credential unless they hold the `admin` role
/// (admins may set any user's).  On success the in-memory hash is replaced,
/// `must_change_password` is cleared, and — for the persisted bootstrap
/// admin — the updated record is re-written to `<data_dir>/auth/admin.json`
/// so the change survives a restart.
///
/// Persistence is fail-loud: if the durable write fails the in-memory change
/// is rolled back and a `500` is returned, so the running store never
/// diverges from disk.
pub(crate) async fn handle_change_credential(
    State(state): State<Arc<AppState>>,
    AxumPath(user_name): AxumPath<String>,
    maybe_user: Option<axum::Extension<AuthenticatedUser>>,
    body: Result<Json<ChangeCredentialRequest>, JsonRejection>,
) -> Response {
    // 1. Authentication: the route is non-public, so `auth_middleware` must
    //    have inserted an `AuthenticatedUser`.  Absent only when auth is
    //    disabled — in which case there is no identity to authorize.
    let Some(axum::Extension(user)) = maybe_user else {
        return error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "authentication required to change a credential",
            "io.druid.server.security.UnauthorizedException",
        );
    };

    // 2. Authorization: own credential, or any credential with the admin role.
    let is_admin = user.roles.iter().any(|r| r == "admin");
    if user.username != user_name && !is_admin {
        return error_response(
            StatusCode::FORBIDDEN,
            "Forbidden",
            "you may only change your own credential unless you have the admin role",
            "io.druid.server.security.ForbiddenException",
        );
    }

    // 3. Body must be well-formed JSON with a `password` field.
    let Ok(Json(payload)) = body else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Bad Request",
            "request body must be JSON of the form {\"password\":\"<new>\"}",
            "io.druid.server.security.BasicSecurityDBResourceException",
        );
    };

    // 4. Reject empty / too-short passwords before touching the store.
    if payload.password.chars().count() < MIN_PASSWORD_LEN {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Bad Request",
            "password too short: must be at least 8 characters",
            "io.druid.server.security.BasicSecurityDBResourceException",
        );
    }

    // 5. Rotate (and, for the persisted admin, durably persist) under a
    //    single write lock so the in-memory mutation and the disk write are
    //    atomic with respect to concurrent verifies / rotations.
    let mut store = state.auth_store.write();

    // Snapshot for rollback so a failed persist never leaves an unpersisted
    // in-memory change.
    let previous: Option<UserRecord> = store.user_record(&user_name).cloned();

    match store.set_password(&user_name, &payload.password) {
        Ok(()) => {}
        Err(AuthError::UserNotFound) => {
            drop(store);
            return error_response(
                StatusCode::NOT_FOUND,
                "Not Found",
                "no such user",
                "io.druid.server.security.BasicSecurityDBResourceException",
            );
        }
        Err(e) => {
            drop(store);
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error",
                &format!("could not hash new password: {e}"),
                "io.druid.server.security.BasicSecurityDBResourceException",
            );
        }
    }

    // Persist only the bootstrap admin (the sole account reloaded on
    // restart) and only when a credential directory is configured (tests
    // pass `None` to skip persistence).
    if user_name == PERSISTED_ADMIN_USERNAME
        && let Some(dir) = state.auth_cred_dir.as_ref()
    {
        let updated = store.user_record(&user_name).cloned();
        let persist_result = match updated {
            Some(record) => persist_admin_record(dir, &record),
            None => Err("rotated user vanished from store before persist".to_string()),
        };
        if let Err(e) = persist_result {
            // Roll back the in-memory rotation: restore the previous
            // credential so the running store matches disk.
            if let Some(prev) = previous {
                store.add_user_with_hash(
                    &prev.username,
                    &prev.password_hash,
                    prev.roles,
                    prev.must_change_password,
                );
            }
            drop(store);
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error",
                &format!("could not persist rotated credential: {e}"),
                "io.druid.server.security.BasicSecurityDBResourceException",
            );
        }
    }
    drop(store);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "ok", "user": user_name })),
    )
        .into_response()
}

/// Persist `record` to `<dir>/admin.json` with owner-only (`0600`)
/// permissions, mirroring the bootstrap write in `bins/ferrodruid`.
fn persist_admin_record(dir: &Path, record: &UserRecord) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("create auth dir {}: {e}", dir.display()))?;
    let path = dir.join("admin.json");
    let json =
        serde_json::to_vec_pretty(record).map_err(|e| format!("serialize credential: {e}"))?;
    write_private_file(&path, &json).map_err(|e| format!("write {}: {e}", path.display()))
}

/// Write `bytes` to `path`, creating/truncating with owner-only (`0600`)
/// permissions on Unix.
#[cfg(unix)]
fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)
}

/// Non-Unix fallback: best-effort write without Unix permission bits.
#[cfg(not(unix))]
fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

/// Build a Druid-compatible error response.
fn error_response(status: StatusCode, error: &str, message: &str, error_class: &str) -> Response {
    (
        status,
        Json(serde_json::json!({
            "error": error,
            "errorMessage": message,
            "errorClass": error_class,
            "host": "localhost"
        })),
    )
        .into_response()
}
