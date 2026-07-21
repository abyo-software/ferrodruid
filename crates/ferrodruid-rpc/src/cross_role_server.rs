// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-I (CL-J1) — listener-bring-up helper for the per-role binaries.
//!
//! Every role binary used to call this sequence directly:
//!
//! ```ignore
//! let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
//! axum::serve(listener, app).await?;
//! ```
//!
//! After W1-I closes CL-J1, that sequence needs to handle three modes
//! ([`crate::cross_role_tls::CrossRoleMtlsMode`]):
//!
//! - `Required` — bind a single TLS listener (mTLS, client cert
//!   required) on `tls_bind`.
//! - `Disabled` — bind a single plain HTTP listener on `plain_bind`
//!   (the v0.2.0 back-compat path).
//! - `Permissive` — bind BOTH a plain listener AND a TLS listener so
//!   peers that have not yet been provisioned with certs can keep
//!   talking over plain HTTP while the rollout completes.
//!
//! [`serve_cross_role`] is the single entry point the binaries call.
//! It owns the await loop and propagates any bind / serve error back
//! to the caller so the existing `ExitCode` plumbing stays unchanged.

use std::net::SocketAddr;

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use tracing::warn;

use crate::cross_role_tls::CrossRoleMtlsMode;

/// One listening socket the per-role binary asked us to bring up.
///
/// `bind` is the address the binary already validated; `tls` is the
/// `axum_server::tls_rustls::RustlsConfig` to wrap the listener in.
/// `None` means plain HTTP.
#[derive(Debug)]
pub struct CrossRoleListener {
    /// Address to bind (`<bind>:<port>` already parsed by the binary).
    pub bind: SocketAddr,
    /// `Some(cfg)` to wrap the listener in TLS; `None` for plain HTTP.
    pub tls: Option<RustlsConfig>,
}

impl CrossRoleListener {
    /// Construct a plain-HTTP listener bound to `bind`.
    #[must_use]
    pub fn plain(bind: SocketAddr) -> Self {
        Self { bind, tls: None }
    }

    /// Construct a TLS listener bound to `bind` with the supplied
    /// rustls config.
    #[must_use]
    pub fn tls(bind: SocketAddr, tls: RustlsConfig) -> Self {
        Self {
            bind,
            tls: Some(tls),
        }
    }
}

/// Errors raised while bringing up the cross-role HTTP server.
#[derive(Debug, thiserror::Error)]
pub enum CrossRoleServeError {
    /// `tokio::net::TcpListener::bind` failed.
    #[error("bind {addr}: {source}")]
    Bind {
        /// The address we tried to bind.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// `axum::serve` / `axum_server::bind_rustls` returned an error.
    #[error("serve: {0}")]
    Serve(String),
    /// Operator picked a mode (`Required` / `Permissive`) that demands
    /// TLS, but did not supply a TLS-enabled [`CrossRoleListener`].
    #[error("mode {mode} requires a TLS listener but none was provided")]
    MissingTls {
        /// The mode the caller selected.
        mode: CrossRoleMtlsMode,
    },
    /// Operator picked a mode (`Disabled` / `Permissive`) that demands
    /// a plain listener, but did not supply one.
    #[error("mode {mode} requires a plain HTTP listener but none was provided")]
    MissingPlain {
        /// The mode the caller selected.
        mode: CrossRoleMtlsMode,
    },
}

/// Serve `app` according to `mode`, using the supplied plain / TLS
/// listeners as required.
///
/// The exact listener requirements per mode:
///
/// | mode | plain | tls |
/// |------|-------|-----|
/// | `Required` | unused (must be `None`) | required |
/// | `Permissive` | required | required |
/// | `Disabled` | required | unused (must be `None`) |
///
/// On `Permissive` the function spawns both listeners in `tokio::join!`
/// and returns when *either* listener exits (the first error or a
/// graceful shutdown).
///
/// # Errors
///
/// Returns [`CrossRoleServeError`] for bind failures, serve failures,
/// or a missing listener that the selected mode requires.
pub async fn serve_cross_role(
    app: Router,
    mode: CrossRoleMtlsMode,
    plain: Option<CrossRoleListener>,
    tls: Option<CrossRoleListener>,
) -> Result<(), CrossRoleServeError> {
    match mode {
        CrossRoleMtlsMode::Required => {
            let tls_listener = tls.ok_or(CrossRoleServeError::MissingTls { mode })?;
            serve_tls(app, tls_listener).await
        }
        CrossRoleMtlsMode::Disabled => {
            let plain_listener = plain.ok_or(CrossRoleServeError::MissingPlain { mode })?;
            serve_plain(app, plain_listener).await
        }
        CrossRoleMtlsMode::Permissive => {
            let plain_listener = plain.ok_or(CrossRoleServeError::MissingPlain { mode })?;
            let tls_listener = tls.ok_or(CrossRoleServeError::MissingTls { mode })?;
            warn!(
                plain_bind = %plain_listener.bind,
                tls_bind = %tls_listener.bind,
                "cross-role mTLS mode=permissive — accepting both plain HTTP and TLS during \
                 rollout window. Flip to required once every peer has been migrated.",
            );
            let app_plain = app.clone();
            // The two halves run concurrently. The first one to return
            // (error or graceful shutdown) wins, mirroring the
            // single-listener semantics of the other two modes.
            tokio::select! {
                r = serve_plain(app_plain, plain_listener) => r,
                r = serve_tls(app, tls_listener) => r,
            }
        }
    }
}

async fn serve_plain(app: Router, listener: CrossRoleListener) -> Result<(), CrossRoleServeError> {
    if listener.tls.is_some() {
        // Misuse: caller asked for plain but handed us a TLS config.
        // Treat as MissingTls's mirror — the only safe response is to
        // refuse to bring up the listener.
        return Err(CrossRoleServeError::Serve(
            "serve_plain called with a TLS listener config".to_string(),
        ));
    }
    let tcp = tokio::net::TcpListener::bind(listener.bind)
        .await
        .map_err(|source| CrossRoleServeError::Bind {
            addr: listener.bind,
            source,
        })?;
    axum::serve(tcp, app)
        .await
        .map_err(|e| CrossRoleServeError::Serve(e.to_string()))
}

async fn serve_tls(app: Router, listener: CrossRoleListener) -> Result<(), CrossRoleServeError> {
    let tls = listener.tls.ok_or_else(|| {
        CrossRoleServeError::Serve("serve_tls called without a RustlsConfig".to_string())
    })?;
    axum_server::bind_rustls(listener.bind, tls)
        .serve(app.into_make_service())
        .await
        .map_err(|e| CrossRoleServeError::Serve(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    #[tokio::test]
    async fn required_without_tls_listener_returns_missing_tls() {
        let app = Router::new().route("/", get(|| async { "ok" }));
        let err = serve_cross_role(app, CrossRoleMtlsMode::Required, None, None)
            .await
            .expect_err("must fail");
        assert!(matches!(
            err,
            CrossRoleServeError::MissingTls {
                mode: CrossRoleMtlsMode::Required
            }
        ));
    }

    #[tokio::test]
    async fn disabled_without_plain_listener_returns_missing_plain() {
        let app = Router::new().route("/", get(|| async { "ok" }));
        let err = serve_cross_role(app, CrossRoleMtlsMode::Disabled, None, None)
            .await
            .expect_err("must fail");
        assert!(matches!(
            err,
            CrossRoleServeError::MissingPlain {
                mode: CrossRoleMtlsMode::Disabled
            }
        ));
    }

    #[tokio::test]
    async fn permissive_without_tls_returns_missing_tls() {
        let app = Router::new().route("/", get(|| async { "ok" }));
        let plain = CrossRoleListener::plain(([127, 0, 0, 1], 0).into());
        let err = serve_cross_role(app, CrossRoleMtlsMode::Permissive, Some(plain), None)
            .await
            .expect_err("must fail");
        assert!(matches!(
            err,
            CrossRoleServeError::MissingTls {
                mode: CrossRoleMtlsMode::Permissive
            }
        ));
    }
}
