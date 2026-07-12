// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 1 / W1-I (CL-J1) — mTLS for the four cross-role HTTP wires of
//! the classic 6-role topology.
//!
//! The 6-role topology runs `broker`, `historical`, `coordinator`,
//! `router`, `overlord`, and `middleManager` as separate processes that
//! talk to each other over plain HTTP. In v0.2.0 those wires were
//! unauthenticated (`docs/known-limitations.md` CL-J1). W1-I makes mTLS
//! the **default** posture so a clean install of the per-role binaries
//! is confidential + forward-secret + mutually authenticated.
//!
//! What this module provides
//! -------------------------
//!
//! - [`CrossRoleTlsConfig`] — `(cert_path, key_path, ca_path)` paths to
//!   PEM-encoded credentials this role presents (its leaf cert chain +
//!   private key) and the CA bundle used to verify peers. Mirrors the
//!   cluster-wire [`ferrodruid_cluster::tls`](https://docs.rs/) /
//!   `crates/ferrodruid-cluster/src/tls.rs` shape so an operator who has
//!   already wired cluster-wire mTLS can point the same PEM files at the
//!   cross-role surface.
//! - [`CrossRoleMtlsMode`] — `Required` / `Permissive` / `Disabled`.
//!   `Required` is the v1.0 default; `Permissive` binds both a plain and
//!   a TLS listener so an operator can roll certs out across the cluster
//!   without coordinated downtime; `Disabled` keeps the v0.2.0 plain
//!   HTTP behaviour for back-compat.
//! - [`build_server_acceptor`] — builds the
//!   `axum_server::tls_rustls::RustlsConfig` that the per-role binaries
//!   feed to `axum_server::bind_rustls`. In `Required` mode the
//!   acceptor demands a client cert signed by the CA bundle; in
//!   `Permissive` mode the client cert is optional but validated if
//!   presented.
//! - [`build_client`] — builds a `reqwest::Client` that presents the
//!   cert chain as a client identity and validates the server cert
//!   against the CA bundle. The HTTP client modules (`broker_client`,
//!   `historical_client`, `mm_client`) accept an `Option<reqwest::Client>`
//!   so binaries can inject this TLS-aware client.
//! - [`load_from_dir`] — convenience: reads `<dir>/ca.pem`,
//!   `<dir>/leaf.pem`, `<dir>/leaf.key` and returns a
//!   [`CrossRoleTlsConfig`]. Matches the layout
//!   `ferrodruid-migrate gen-cross-role-certs` writes.
//!
//! Threat model
//! ------------
//!
//! - **`Required` (default)**: every cross-role HTTP connection is
//!   tunnelled in TLS 1.2/1.3 with mandatory client-cert verification.
//!   A peer without a CA-signed leaf cert is rejected at the handshake.
//! - **`Permissive`**: both a plain HTTP listener (the legacy port) and
//!   a TLS listener (configured separately) are bound. The TLS listener
//!   accepts connections with or without a client cert; the plain
//!   listener accepts whatever connects. The mode is a deliberate
//!   downgrade for operators rolling certificates out across a running
//!   cluster — it MUST NOT be left enabled in steady state.
//! - **`Disabled`**: plain HTTP. v0.2.0 behaviour, kept so an operator
//!   can build with the new default and still run the legacy
//!   un-authenticated topology while certs are provisioned.
//!
//! The single-binary `ferrodruid serve` path is unaffected: it issues
//! intra-process Arc-handle calls (CL-G2 `[design]`) and does not bind
//! a cross-role wire at all.
//!
//! Design notes
//! ------------
//!
//! - The TLS code intentionally lives in `ferrodruid-rpc` rather than
//!   `ferrodruid-cluster` because the cluster TLS surface is gated on
//!   the `cluster-tls` Cargo feature and the cluster transport is a
//!   custom framed-JSON protocol, not HTTP. The cross-role wires are
//!   HTTP via `reqwest` + `axum`, so the natural home is alongside the
//!   HTTP client/server contracts.
//! - The rustls `CryptoProvider` is pinned to `ring` here, matching the
//!   cluster wire's choice, so a build with `--all-features` (which
//!   pulls in `aws-lc-rs` via `marketplace-metering`) does not panic on
//!   provider auto-detection.

#![allow(clippy::module_name_repetitions)]

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum_server::tls_rustls::RustlsConfig;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;

/// Three operating modes for the cross-role HTTP wires.
///
/// `Required` is the v1.0 default. `Permissive` is the temporary
/// migration mode an operator uses while rolling certificates out
/// across a running cluster. `Disabled` is the v0.2.0 back-compat
/// posture and MUST NOT be used in production.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CrossRoleMtlsMode {
    /// Default v1.0 posture. Cross-role HTTP wires require TLS with a
    /// client cert signed by the configured CA bundle. Plain HTTP is
    /// not accepted.
    #[default]
    Required,
    /// Migration mode. The role binary binds BOTH a plain HTTP listener
    /// (legacy port) AND a TLS listener (configured separately). The
    /// TLS listener accepts connections with or without a client cert,
    /// so peers that have not yet been provisioned with certs can keep
    /// talking to the role over plain HTTP during the rollout window.
    /// **Not safe for steady-state production**; flip to `Required`
    /// once every peer is using TLS.
    Permissive,
    /// v0.2.0 back-compat. Plain HTTP on the legacy port; no TLS.
    /// Disabled is fail-loud: the binary emits a `warn!` on startup so
    /// operators do not silently keep this posture after the v1.0
    /// upgrade.
    Disabled,
}

impl CrossRoleMtlsMode {
    /// Whether this mode binds a TLS listener at all (`Required` or
    /// `Permissive` → yes; `Disabled` → no).
    #[must_use]
    pub fn binds_tls(self) -> bool {
        matches!(self, Self::Required | Self::Permissive)
    }

    /// Whether this mode binds a plain HTTP listener (`Disabled` or
    /// `Permissive` → yes; `Required` → no).
    #[must_use]
    pub fn binds_plain(self) -> bool {
        matches!(self, Self::Disabled | Self::Permissive)
    }

    /// Whether the TLS listener (if any) requires a client cert.
    #[must_use]
    pub fn requires_client_cert(self) -> bool {
        matches!(self, Self::Required)
    }

    /// Canonical kebab-case label used on the CLI surface
    /// (`--cross-role-mtls=<mode>`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Permissive => "permissive",
            Self::Disabled => "disabled",
        }
    }
}

impl fmt::Display for CrossRoleMtlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CrossRoleMtlsMode {
    type Err = CrossRoleTlsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "required" => Ok(Self::Required),
            "permissive" => Ok(Self::Permissive),
            "disabled" => Ok(Self::Disabled),
            other => Err(CrossRoleTlsError::Mode(other.to_string())),
        }
    }
}

/// File-system paths to the PEM-encoded credentials this role presents
/// (cert chain + private key) and the CA bundle used to verify peers.
///
/// The cert chain may contain one or more certificates (leaf first,
/// intermediates after). The CA bundle is used both to verify the
/// peer's server cert (client side) and to verify the peer's client
/// cert (server side) — i.e. mTLS, not just server-auth TLS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrossRoleTlsConfig {
    /// PEM-encoded leaf certificate (and any intermediate chain).
    pub cert_path: PathBuf,
    /// PEM-encoded private key matching `cert_path`. Must be PKCS#8 or
    /// SEC1 / PKCS#1; rustls-pemfile auto-detects.
    pub key_path: PathBuf,
    /// PEM-encoded CA bundle used to verify the peer's certificate.
    /// One or more concatenated CA certs are accepted.
    pub ca_path: PathBuf,
}

impl CrossRoleTlsConfig {
    /// Construct a config with explicit paths to all three PEM files.
    #[must_use]
    pub fn new(cert_path: PathBuf, key_path: PathBuf, ca_path: PathBuf) -> Self {
        Self {
            cert_path,
            key_path,
            ca_path,
        }
    }
}

/// Convenience: load `<dir>/ca.pem`, `<dir>/leaf.pem`, `<dir>/leaf.key`
/// and return a [`CrossRoleTlsConfig`].
///
/// Mirrors the layout `ferrodruid-migrate gen-cross-role-certs` writes
/// out under `<data_dir>/cross-role/`. The returned config is purely
/// path-valued — it does NOT read or parse the files itself; that
/// happens at [`build_server_acceptor`] / [`build_client`] time so a
/// missing file fails closed at server / client bring-up rather than
/// during config decode.
///
/// # Errors
///
/// Returns [`CrossRoleTlsError::Missing`] if any of the three required
/// files is absent from `dir`. Permission / read errors on the file
/// contents themselves surface later, at acceptor / client build time.
pub fn load_from_dir(dir: &Path) -> Result<CrossRoleTlsConfig, CrossRoleTlsError> {
    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("leaf.pem");
    let key_path = dir.join("leaf.key");
    for p in [&ca_path, &cert_path, &key_path] {
        if !p.exists() {
            return Err(CrossRoleTlsError::Missing { path: p.clone() });
        }
    }
    Ok(CrossRoleTlsConfig::new(cert_path, key_path, ca_path))
}

/// Errors produced when loading PEM credentials or building a rustls
/// configuration for a cross-role wire.
#[derive(Debug, Error)]
pub enum CrossRoleTlsError {
    /// Failed to read a PEM file from disk.
    #[error("read PEM file {path}: {source}")]
    Io {
        /// The path that failed to open / read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// PEM file did not parse cleanly.
    #[error("parse PEM file {path}: {reason}")]
    Pem {
        /// The path that failed to parse.
        path: PathBuf,
        /// Human-readable reason (rustls-pemfile gives back per-line errors).
        reason: String,
    },
    /// rustls rejected the constructed config (e.g. cert / key mismatch).
    #[error("rustls config build failed: {0}")]
    Config(String),
    /// Cert / key / CA file was empty or contained no usable items.
    #[error("PEM file {path} contained no {what}")]
    Empty {
        /// The path that produced no items of the expected kind.
        path: PathBuf,
        /// Human-readable description of what was expected ("certs", etc).
        what: &'static str,
    },
    /// One of the cert-bundle files referenced by [`load_from_dir`] was
    /// missing.
    #[error("required cross-role TLS file not found: {path}")]
    Missing {
        /// The path that was expected but not present.
        path: PathBuf,
    },
    /// Unknown `CrossRoleMtlsMode` label on the CLI.
    #[error("unknown cross-role mTLS mode `{0}` (expected required|permissive|disabled)")]
    Mode(String),
    /// Failed to build the outbound `reqwest::Client`.
    #[error("build reqwest TLS client: {0}")]
    Reqwest(String),
}

/// Load and parse a PEM cert chain into `CertificateDer`s.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, CrossRoleTlsError> {
    let bytes = fs::read(path).map_err(|source| CrossRoleTlsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = io::BufReader::new(&bytes[..]);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| CrossRoleTlsError::Pem {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;
    if certs.is_empty() {
        return Err(CrossRoleTlsError::Empty {
            path: path.to_path_buf(),
            what: "certificates",
        });
    }
    Ok(certs)
}

/// Load and parse a PEM private key (PKCS#8 / SEC1 / PKCS#1).
fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, CrossRoleTlsError> {
    let bytes = fs::read(path).map_err(|source| CrossRoleTlsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = io::BufReader::new(&bytes[..]);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| CrossRoleTlsError::Pem {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?
        .ok_or_else(|| CrossRoleTlsError::Empty {
            path: path.to_path_buf(),
            what: "private key",
        })?;
    Ok(key)
}

/// Build a [`RootCertStore`] containing every cert in the given PEM file.
fn load_root_store(path: &Path) -> Result<RootCertStore, CrossRoleTlsError> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?;
    }
    Ok(roots)
}

/// Build the server-side TLS configuration the per-role binary feeds
/// to `axum_server::bind_rustls`.
///
/// In `Required` mode the acceptor demands a client cert signed by the
/// CA bundle. In `Permissive` mode the client cert is optional but
/// validated if presented.
///
/// # Errors
///
/// Returns [`CrossRoleTlsError`] variants for I/O failures, PEM parse
/// failures, or rustls config build failures.
pub fn build_server_acceptor(
    cfg: &CrossRoleTlsConfig,
    mode: CrossRoleMtlsMode,
) -> Result<RustlsConfig, CrossRoleTlsError> {
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_key(&cfg.key_path)?;
    let roots = load_root_store(&cfg.ca_path)?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    // Required ↔ mandatory client cert; Permissive ↔ optional client cert.
    let client_verifier_builder =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone());
    let client_verifier = if mode.requires_client_cert() {
        client_verifier_builder
            .build()
            .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?
    } else {
        // Permissive: a peer may connect without a client cert. If it
        // does present one, it is validated against the CA bundle.
        client_verifier_builder
            .allow_unauthenticated()
            .build()
            .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?
    };

    let server_config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?;

    Ok(RustlsConfig::from_config(Arc::new(server_config)))
}

/// Build a TLS-capable `reqwest::Client` that presents the configured
/// cert chain as a client identity and validates peer server certs
/// against the configured CA bundle.
///
/// The client uses a 30-second request timeout (mirroring the existing
/// [`crate::HttpBrokerClient`] / [`crate::HttpHistoricalClient`] /
/// [`crate::HttpMiddleManagerClient`] defaults).
///
/// # Errors
///
/// Returns [`CrossRoleTlsError`] for any PEM read / parse failure or
/// `reqwest::Client::builder()` failure.
pub fn build_client(cfg: &CrossRoleTlsConfig) -> Result<reqwest::Client, CrossRoleTlsError> {
    // For reqwest 0.12 we use the higher-level
    // `Identity::from_pem(<pem-chain-and-key>)` + `Certificate::from_pem`
    // entry points so we do not have to mirror our own rustls
    // ClientConfig through reqwest's API. The PEM file for `identity`
    // expects the leaf cert and the private key concatenated.
    let cert_pem = fs::read(&cfg.cert_path).map_err(|source| CrossRoleTlsError::Io {
        path: cfg.cert_path.clone(),
        source,
    })?;
    let key_pem = fs::read(&cfg.key_path).map_err(|source| CrossRoleTlsError::Io {
        path: cfg.key_path.clone(),
        source,
    })?;
    let ca_pem = fs::read(&cfg.ca_path).map_err(|source| CrossRoleTlsError::Io {
        path: cfg.ca_path.clone(),
        source,
    })?;

    let mut combined = Vec::with_capacity(cert_pem.len() + key_pem.len() + 1);
    combined.extend_from_slice(&cert_pem);
    if !cert_pem.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined.extend_from_slice(&key_pem);

    let identity = reqwest::Identity::from_pem(&combined)
        .map_err(|e| CrossRoleTlsError::Reqwest(e.to_string()))?;
    let ca = reqwest::Certificate::from_pem(&ca_pem)
        .map_err(|e| CrossRoleTlsError::Reqwest(e.to_string()))?;

    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity)
        .add_root_certificate(ca)
        // Keep the existing 30 s per-request timeout the plain clients use.
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| CrossRoleTlsError::Reqwest(e.to_string()))?;
    Ok(client)
}

/// Verify a [`CrossRoleTlsConfig`] is self-consistent (all three PEM
/// files load, the cert / key match, the CA store builds). Used by
/// per-role binaries at start-up so a malformed cert bundle fails loud
/// before the HTTP server starts accepting traffic.
///
/// Returns `Ok(())` on success, otherwise the first error encountered.
///
/// # Errors
///
/// Returns [`CrossRoleTlsError`] for any I/O / PEM / rustls config
/// failure.
pub fn validate(cfg: &CrossRoleTlsConfig) -> Result<(), CrossRoleTlsError> {
    let _server_acceptor = build_server_acceptor(cfg, CrossRoleMtlsMode::Required)?;
    let _client_cfg = build_internal_client_config(cfg)?;
    Ok(())
}

/// Internal helper exposed for tests that want a raw rustls
/// `ClientConfig` (e.g. to dial a `tokio_rustls::TlsConnector` directly).
/// Production code should prefer [`build_client`] which returns a
/// `reqwest::Client`.
fn build_internal_client_config(
    cfg: &CrossRoleTlsConfig,
) -> Result<RustlsClientConfig, CrossRoleTlsError> {
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_key(&cfg.key_path)?;
    let roots = load_root_store(&cfg.ca_path)?;

    let client_config = RustlsClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?
    .with_root_certificates(roots)
    .with_client_auth_cert(certs, key)
    .map_err(|e| CrossRoleTlsError::Config(e.to_string()))?;
    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Generate a self-signed CA + leaf cert pair signed by it, with
    /// SANs `[node_name, "localhost", "127.0.0.1"]`. Returns the
    /// tempdir (keep alive!) plus the [`CrossRoleTlsConfig`] paths
    /// (laid out as `ca.pem`, `leaf.pem`, `leaf.key` so
    /// [`load_from_dir`] can pick them up).
    fn gen_test_certs(node_name: &str) -> (TempDir, CrossRoleTlsConfig) {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca_key = rcgen::KeyPair::generate().expect("ca keypair");
        let ca_params = rcgen::CertificateParams::new(vec!["ferrodruid-cross-role-test-ca".into()])
            .expect("ca params");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

        let leaf_key = rcgen::KeyPair::generate().expect("leaf keypair");
        let leaf_params = rcgen::CertificateParams::new(vec![
            node_name.to_string(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("leaf params");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &ca_cert, &ca_key)
            .expect("leaf sign");

        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("leaf.pem");
        let key_path = dir.path().join("leaf.key");
        fs::write(&ca_path, ca_cert.pem()).expect("write ca");
        fs::write(&cert_path, leaf_cert.pem()).expect("write leaf");
        fs::write(&key_path, leaf_key.serialize_pem()).expect("write key");
        (dir, CrossRoleTlsConfig::new(cert_path, key_path, ca_path))
    }

    #[test]
    fn mode_round_trips_through_string() {
        for m in [
            CrossRoleMtlsMode::Required,
            CrossRoleMtlsMode::Permissive,
            CrossRoleMtlsMode::Disabled,
        ] {
            assert_eq!(m.as_str().parse::<CrossRoleMtlsMode>().unwrap(), m);
            assert_eq!(format!("{m}"), m.as_str());
        }
        let err = "off".parse::<CrossRoleMtlsMode>().unwrap_err();
        assert!(matches!(err, CrossRoleTlsError::Mode(_)));
    }

    #[test]
    fn mode_predicates_describe_listener_topology() {
        assert!(CrossRoleMtlsMode::Required.binds_tls());
        assert!(!CrossRoleMtlsMode::Required.binds_plain());
        assert!(CrossRoleMtlsMode::Required.requires_client_cert());

        assert!(CrossRoleMtlsMode::Permissive.binds_tls());
        assert!(CrossRoleMtlsMode::Permissive.binds_plain());
        assert!(!CrossRoleMtlsMode::Permissive.requires_client_cert());

        assert!(!CrossRoleMtlsMode::Disabled.binds_tls());
        assert!(CrossRoleMtlsMode::Disabled.binds_plain());
        assert!(!CrossRoleMtlsMode::Disabled.requires_client_cert());
    }

    #[test]
    fn load_from_dir_picks_up_the_three_pem_files() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-leaf");
        let parent = cfg.ca_path.parent().expect("dir");
        let loaded = load_from_dir(parent).expect("load");
        assert_eq!(loaded, cfg);
    }

    #[test]
    fn load_from_dir_fails_closed_on_missing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Only write ca.pem; leaf.pem + leaf.key are absent.
        fs::write(dir.path().join("ca.pem"), b"dummy").expect("write");
        let err = load_from_dir(dir.path()).expect_err("must fail");
        assert!(matches!(err, CrossRoleTlsError::Missing { .. }));
    }

    #[test]
    fn build_server_acceptor_required_round_trip() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-leaf");
        let _accept =
            build_server_acceptor(&cfg, CrossRoleMtlsMode::Required).expect("server acceptor");
    }

    #[test]
    fn build_server_acceptor_permissive_round_trip() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-leaf");
        let _accept =
            build_server_acceptor(&cfg, CrossRoleMtlsMode::Permissive).expect("server acceptor");
    }

    #[test]
    fn build_client_round_trip() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-leaf");
        let _client = build_client(&cfg).expect("client");
    }

    #[test]
    fn validate_returns_ok_on_self_consistent_bundle() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-leaf");
        validate(&cfg).expect("self-consistent bundle");
    }

    #[test]
    fn validate_fails_on_missing_files() {
        let cfg = CrossRoleTlsConfig::new(
            PathBuf::from("/nonexistent/cert"),
            PathBuf::from("/nonexistent/key"),
            PathBuf::from("/nonexistent/ca"),
        );
        let err = validate(&cfg).expect_err("must fail");
        assert!(matches!(err, CrossRoleTlsError::Io { .. }));
    }
}
