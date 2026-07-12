// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-I (CL-J1) — per-binary cross-role startup helper.
//!
//! Each of the six per-role binaries needs the same chunk of plumbing
//! to bring its HTTP server up with mTLS:
//!
//! 1. Decide the `CrossRoleMtlsMode` from the CLI / environment.
//! 2. Locate the PEM credentials, either from explicit
//!    `--cross-role-tls-{cert,key,ca}` flags or from the default
//!    `<data_dir>/cross-role/` directory the `ferrodruid-migrate
//!    gen-cross-role-certs` helper writes.
//! 3. Build the outbound `reqwest::Client` (TLS-aware if mode is
//!    `Required` / `Permissive`, plain otherwise) the role's outbound
//!    HTTP clients reuse for every peer.
//! 4. Build the inbound listeners (plain / TLS / both, depending on
//!    mode).
//!
//! [`CrossRoleStartup::resolve`] folds all of that into one call so the
//! `main.rs` of every role binary stays under ~10 lines of new code.

use std::net::SocketAddr;
use std::path::PathBuf;

use tracing::warn;

use crate::cross_role_server::CrossRoleListener;
use crate::cross_role_tls::{
    self, CrossRoleMtlsMode, CrossRoleTlsConfig, CrossRoleTlsError, build_client,
    build_server_acceptor,
};

/// Resolved cross-role startup parameters one role binary owns.
///
/// Built by [`CrossRoleStartup::resolve`]; consumed by
/// [`CrossRoleStartup::build_outbound_client`] (for outbound HTTP
/// callers) and [`CrossRoleStartup::into_listeners`] (for the inbound
/// HTTP server). Splitting build vs. into-listeners lets the caller
/// build the outbound client *before* binding the inbound listener,
/// matching the existing binary flow that wires clients into shared
/// state first.
#[derive(Debug, Clone)]
pub struct CrossRoleStartup {
    /// Selected mode.
    pub mode: CrossRoleMtlsMode,
    /// Resolved TLS config (paths to PEM files); `None` when mode is
    /// `Disabled`.
    pub tls_cfg: Option<CrossRoleTlsConfig>,
    /// Address the plain HTTP listener should bind on (`Disabled` and
    /// `Permissive` modes).
    pub plain_bind: Option<SocketAddr>,
    /// Address the TLS listener should bind on (`Required` and
    /// `Permissive` modes).
    pub tls_bind: Option<SocketAddr>,
}

impl CrossRoleStartup {
    /// Resolve the cross-role startup from raw CLI inputs.
    ///
    /// `legacy_bind` is the plain `<bind>:<port>` socket the role
    /// already binds today; `tls_bind` is the new TLS bind (typically
    /// `<bind>:<port + 1000>` or an explicit `--tls-port`). The exact
    /// listener requirements per mode:
    ///
    /// | mode | plain_bind | tls_bind |
    /// |------|------------|----------|
    /// | `Required` | unused | required |
    /// | `Permissive` | required | required |
    /// | `Disabled` | required | unused |
    ///
    /// Explicit `tls_cert` / `tls_key` / `tls_ca` paths override the
    /// `cert_dir` fallback. If all three explicit paths are `None`, the
    /// helper attempts [`crate::load_from_dir`] on `cert_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`CrossRoleTlsError`] if the selected mode demands a
    /// TLS bundle but none can be located, or if the resolved bundle
    /// fails validation.
    pub fn resolve(
        mode: CrossRoleMtlsMode,
        legacy_bind: SocketAddr,
        tls_bind: Option<SocketAddr>,
        tls_cert: Option<PathBuf>,
        tls_key: Option<PathBuf>,
        tls_ca: Option<PathBuf>,
        cert_dir: Option<PathBuf>,
    ) -> Result<Self, CrossRoleTlsError> {
        let tls_cfg = if mode.binds_tls() || tls_cert.is_some() {
            let explicit = match (tls_cert, tls_key, tls_ca) {
                (Some(c), Some(k), Some(a)) => Some(CrossRoleTlsConfig::new(c, k, a)),
                (None, None, None) => None,
                // Partial set is a hard error — operators should not
                // accidentally launch with a half-configured bundle.
                _ => {
                    return Err(CrossRoleTlsError::Config(
                        "--cross-role-tls-cert, --cross-role-tls-key, --cross-role-tls-ca \
                         must all be set together (or all omitted to fall back to \
                         <data_dir>/cross-role/)"
                            .to_string(),
                    ));
                }
            };
            let resolved = match explicit {
                Some(cfg) => cfg,
                None => match cert_dir {
                    Some(dir) => cross_role_tls::load_from_dir(&dir)?,
                    None => {
                        return Err(CrossRoleTlsError::Config(
                            "cross-role mTLS mode requires either explicit \
                             --cross-role-tls-{cert,key,ca} or a cert_dir to fall back to"
                                .to_string(),
                        ));
                    }
                },
            };
            // Fail loud at startup if the bundle is malformed.
            cross_role_tls::validate(&resolved)?;
            Some(resolved)
        } else {
            None
        };

        let plain_bind = if mode.binds_plain() {
            Some(legacy_bind)
        } else {
            None
        };
        let tls_bind = if mode.binds_tls() {
            // `tls_bind` is required at this point; if the caller did
            // not pass one, derive from the legacy bind by adding
            // 1000 to the port (matching Apache Druid's
            // `service.tlsPort` convention).
            Some(tls_bind.unwrap_or_else(|| derive_tls_bind(legacy_bind)))
        } else {
            None
        };

        if mode == CrossRoleMtlsMode::Disabled {
            warn!(
                "cross-role mTLS mode=disabled — running v0.2.0 unauthenticated cross-role \
                 HTTP. Flip --cross-role-mtls=required once certs are deployed; see \
                 docs/SECURITY.md cross-role section.",
            );
        }

        Ok(Self {
            mode,
            tls_cfg,
            plain_bind,
            tls_bind,
        })
    }

    /// Build the outbound `reqwest::Client` every outbound peer client
    /// (`HttpBrokerClient`, `HttpHistoricalClient`,
    /// `HttpMiddleManagerClient`) should reuse.
    ///
    /// In `Required` / `Permissive` modes the client is built from the
    /// resolved [`CrossRoleTlsConfig`] (presents the leaf cert + key as
    /// a client identity, validates server certs against the CA bundle).
    /// In `Disabled` mode the client is a plain `reqwest::Client` with
    /// the 30-second per-request timeout the legacy clients used.
    ///
    /// # Errors
    ///
    /// Returns [`CrossRoleTlsError`] if the TLS-capable client cannot
    /// be built.
    pub fn build_outbound_client(&self) -> Result<reqwest::Client, CrossRoleTlsError> {
        match &self.tls_cfg {
            Some(cfg) => build_client(cfg),
            None => reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .map_err(|e| CrossRoleTlsError::Reqwest(e.to_string())),
        }
    }

    /// Build the (plain, tls) listener pair the binary feeds to
    /// [`crate::serve_cross_role`].
    ///
    /// # Errors
    ///
    /// Returns [`CrossRoleTlsError`] if a TLS listener was requested
    /// but the rustls config cannot be assembled.
    pub fn into_listeners(
        self,
    ) -> Result<
        (
            CrossRoleMtlsMode,
            Option<CrossRoleListener>,
            Option<CrossRoleListener>,
        ),
        CrossRoleTlsError,
    > {
        let plain = self.plain_bind.map(CrossRoleListener::plain);
        let tls = match (self.tls_bind, self.tls_cfg.as_ref()) {
            (Some(addr), Some(cfg)) => {
                let acceptor = build_server_acceptor(cfg, self.mode)?;
                Some(CrossRoleListener::tls(addr, acceptor))
            }
            _ => None,
        };
        Ok((self.mode, plain, tls))
    }
}

/// Derive a TLS bind address from the legacy plain bind by adding
/// 1000 to the port (matches Apache Druid's `tlsPort` convention).
fn derive_tls_bind(legacy: SocketAddr) -> SocketAddr {
    let mut tls = legacy;
    let new_port = legacy.port().saturating_add(1000);
    // Avoid colliding with the legacy port when the legacy port is
    // already > 64535; in that case fall back to a port one above the
    // legacy port (or 0 if even that overflows).
    let safe_port = if new_port > legacy.port() {
        new_port
    } else {
        legacy.port().saturating_add(1)
    };
    tls.set_port(safe_port);
    tls
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn gen_cross_role_dir(name: &str) -> TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca_key = rcgen::KeyPair::generate().expect("ca keypair");
        let ca_params = rcgen::CertificateParams::new(vec!["ferrodruid-startup-test-ca".into()])
            .expect("ca params");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

        let leaf_key = rcgen::KeyPair::generate().expect("leaf keypair");
        let leaf_params = rcgen::CertificateParams::new(vec![
            name.to_string(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("leaf params");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &ca_cert, &ca_key)
            .expect("leaf sign");

        fs::write(dir.path().join("ca.pem"), ca_cert.pem()).expect("write ca");
        fs::write(dir.path().join("leaf.pem"), leaf_cert.pem()).expect("write leaf");
        fs::write(dir.path().join("leaf.key"), leaf_key.serialize_pem()).expect("write key");
        dir
    }

    #[test]
    fn resolve_required_uses_cert_dir_fallback() {
        let dir = gen_cross_role_dir("required-leaf");
        let plain = SocketAddr::from(([127, 0, 0, 1], 8082));
        let startup = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Required,
            plain,
            None,
            None,
            None,
            None,
            Some(dir.path().to_path_buf()),
        )
        .expect("resolve");
        assert_eq!(startup.mode, CrossRoleMtlsMode::Required);
        assert!(startup.plain_bind.is_none());
        assert!(startup.tls_cfg.is_some());
        // Default-derived TLS port = legacy + 1000.
        assert_eq!(startup.tls_bind.unwrap().port(), 9082);
    }

    #[test]
    fn resolve_permissive_binds_both_listeners() {
        let dir = gen_cross_role_dir("permissive-leaf");
        let plain = SocketAddr::from(([127, 0, 0, 1], 8083));
        let tls = SocketAddr::from(([127, 0, 0, 1], 9083));
        let startup = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Permissive,
            plain,
            Some(tls),
            None,
            None,
            None,
            Some(dir.path().to_path_buf()),
        )
        .expect("resolve");
        assert_eq!(startup.plain_bind, Some(plain));
        assert_eq!(startup.tls_bind, Some(tls));
        assert!(startup.tls_cfg.is_some());
    }

    #[test]
    fn resolve_disabled_binds_plain_only() {
        let plain = SocketAddr::from(([127, 0, 0, 1], 8084));
        let startup = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Disabled,
            plain,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("resolve");
        assert_eq!(startup.mode, CrossRoleMtlsMode::Disabled);
        assert_eq!(startup.plain_bind, Some(plain));
        assert!(startup.tls_bind.is_none());
        assert!(startup.tls_cfg.is_none());
    }

    #[test]
    fn resolve_partial_explicit_tls_paths_errors() {
        let plain = SocketAddr::from(([127, 0, 0, 1], 8085));
        let err = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Required,
            plain,
            None,
            Some(PathBuf::from("/x/cert")),
            None, // missing key
            Some(PathBuf::from("/x/ca")),
            None,
        )
        .expect_err("partial set must error");
        assert!(matches!(err, CrossRoleTlsError::Config(_)));
    }

    #[test]
    fn resolve_required_without_any_paths_or_dir_errors() {
        let plain = SocketAddr::from(([127, 0, 0, 1], 8086));
        let err = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Required,
            plain,
            None,
            None,
            None,
            None,
            None,
        )
        .expect_err("must error");
        assert!(matches!(err, CrossRoleTlsError::Config(_)));
    }

    #[test]
    fn build_outbound_client_succeeds_in_disabled_mode_without_certs() {
        let plain = SocketAddr::from(([127, 0, 0, 1], 8087));
        let startup = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Disabled,
            plain,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("resolve");
        let _client = startup.build_outbound_client().expect("plain client");
    }

    #[test]
    fn into_listeners_required_yields_only_tls() {
        let dir = gen_cross_role_dir("listeners-leaf");
        let plain = SocketAddr::from(([127, 0, 0, 1], 8088));
        let startup = CrossRoleStartup::resolve(
            CrossRoleMtlsMode::Required,
            plain,
            None,
            None,
            None,
            None,
            Some(dir.path().to_path_buf()),
        )
        .expect("resolve");
        let (mode, plain, tls) = startup.into_listeners().expect("listeners");
        assert_eq!(mode, CrossRoleMtlsMode::Required);
        assert!(plain.is_none());
        assert!(tls.is_some());
    }
}
