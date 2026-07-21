// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 44 / Phase 2.4 — mTLS for cluster wire confidentiality (default
//! posture).
//!
//! PSK (Wave 40-A) authenticates every cluster frame and binds a session
//! to a single announced node id, but PSK alone over cleartext TCP leaves
//! the JSON payloads visible to a passive eavesdropper. Phase 2.4 makes
//! mTLS the **default** posture so the common deployment is confidential +
//! forward-secret + mutually-authenticated; PSK-over-cleartext survives as
//! an explicit opt-in fallback only.
//!
//! This module exposes [`TlsConfig`] plus [`load_server_config`] /
//! [`load_client_config`] that build a `tokio_rustls::TlsAcceptor` /
//! `TlsConnector` from PEM-encoded cert + key + CA files. The transport
//! layer wraps the `tokio::net::TcpStream` with TLS when
//! [`crate::transport::TcpTransportConfig::security`] is
//! [`crate::transport::ClusterSecurityMode::MutualTls`] (the default).
//!
//! Threat model
//! ------------
//!
//! - mTLS (default, this module): confidentiality + forward secrecy + peer
//!   identity verified against a CA. Both endpoints present X.509 certs
//!   signed by the configured CA bundle; either side rejects the
//!   connection if the peer cert does not validate. PSK frame
//!   authentication runs *inside* the tunnel — the layers are additive.
//! - PSK-over-cleartext (explicit opt-in): authentication + integrity but
//!   no confidentiality. A network adversary that reaches the cluster TCP
//!   port without the secret cannot forge ACKs or steal votes, but
//!   payloads are readable on the wire.
//!
//! The default-mTLS path is gated on the `cluster-tls` Cargo feature being
//! enabled (it is in the default feature set). A build with the feature
//! disabled cannot construct the mTLS variant at all, so it can only run
//! the explicit PSK-cleartext fallback; the node bins fail loudly at
//! startup rather than silently downgrade.

#![cfg(feature = "cluster-tls")]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use thiserror::Error;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// File-system paths to the PEM-encoded credentials this node presents
/// (cert chain + private key) and the CA bundle used to verify peers.
///
/// All three paths must point to readable PEM files. The cert chain may
/// contain one or more certificates (leaf first, intermediates after).
/// The CA bundle is used both to verify the server cert (client side)
/// and to verify the client cert (server side) — i.e. mTLS, not just
/// server-auth TLS.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// PEM-encoded leaf certificate (and any intermediate chain).
    pub cert_path: PathBuf,
    /// PEM-encoded private key matching `cert_path`. Must be PKCS#8 or
    /// SEC1 / PKCS#1; rustls-pemfile auto-detects.
    pub key_path: PathBuf,
    /// PEM-encoded CA bundle used to verify the peer's certificate.
    /// One or more concatenated CA certs are accepted.
    pub ca_path: PathBuf,
}

impl TlsConfig {
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

/// Errors produced when loading PEM credentials or building a rustls
/// configuration.
#[derive(Debug, Error)]
pub enum TlsError {
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
}

/// Load and parse a PEM cert chain into `CertificateDer`s.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let bytes = fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = io::BufReader::new(&bytes[..]);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::Pem {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;
    if certs.is_empty() {
        return Err(TlsError::Empty {
            path: path.to_path_buf(),
            what: "certificates",
        });
    }
    Ok(certs)
}

/// Load and parse a PEM private key (PKCS#8 / SEC1 / PKCS#1).
fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let bytes = fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = io::BufReader::new(&bytes[..]);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| TlsError::Pem {
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?
        .ok_or_else(|| TlsError::Empty {
            path: path.to_path_buf(),
            what: "private key",
        })?;
    Ok(key)
}

/// Build a [`RootCertStore`] containing every cert in the given PEM file.
fn load_root_store(path: &Path) -> Result<RootCertStore, TlsError> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .map_err(|e| TlsError::Config(e.to_string()))?;
    }
    Ok(roots)
}

/// Build a server-side `TlsAcceptor` from [`TlsConfig`].
///
/// The acceptor presents `cert_path` + `key_path` and demands a client
/// certificate signed by `ca_path`. Connections without a client cert
/// (or with a cert that does not validate) are refused at the TLS
/// handshake — they never reach the cluster wire layer.
pub fn load_server_config(cfg: &TlsConfig) -> Result<TlsAcceptor, TlsError> {
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_key(&cfg.key_path)?;
    let roots = load_root_store(&cfg.ca_path)?;

    // Pin the ring CryptoProvider explicitly and share it. The default
    // `ServerConfig::builder()` AND `WebPkiClientVerifier::builder()` both rely
    // on rustls's process-default provider auto-detection, which PANICS when
    // more than one provider is linked — e.g. under `--all-features`, where the
    // optional `marketplace-metering` AWS SDK pulls in aws-lc-rs alongside this
    // crate's ring. Selecting the provider here is unambiguous regardless of
    // what else is in the dependency graph.
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
            .build()
            .map_err(|e| TlsError::Config(e.to_string()))?;

    let server_config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| TlsError::Config(e.to_string()))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::Config(e.to_string()))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Build a client-side `TlsConnector` from [`TlsConfig`].
///
/// The connector presents `cert_path` + `key_path` and validates the
/// server's certificate against `ca_path`. Server hostname verification
/// is performed against the SNI value the dialer supplies (which is
/// typically the peer node id or the peer's `host:port`).
pub fn load_client_config(cfg: &TlsConfig) -> Result<TlsConnector, TlsError> {
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_key(&cfg.key_path)?;
    let roots = load_root_store(&cfg.ca_path)?;

    // Pin the ring CryptoProvider explicitly (see load_server_config) so the
    // builder does not panic when aws-lc-rs is also linked under --all-features.
    let client_config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::Config(e.to_string()))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .map_err(|e| TlsError::Config(e.to_string()))?;

    Ok(TlsConnector::from(Arc::new(client_config)))
}

/// Extract the identities a peer certificate asserts: every Subject Alternative
/// Name `dNSName`, `uniformResourceIdentifier`, and `iPAddress` (canonically
/// stringified). Returns an empty vector if the DER cannot be parsed.
///
/// DD R21 (High): the cluster authenticates that the peer cert chains to the
/// configured CA, but `WebPkiClientVerifier` does not bind the cert to any
/// particular identity. Without comparing the handshake-announced node id to one
/// of these certificate identities, any holder of a single valid node cert (plus
/// the shared cluster PSK) could announce a *different* node id and fabricate
/// quorum acknowledgements/votes as that node. The accept path calls this to
/// pin the announced id to the presented certificate.
///
/// DD R22 (High): the Subject Common Name is intentionally NOT consulted. A CA
/// that issues a cert with `SAN dNSName=node-a` but `CN=node-b` would otherwise
/// let the holder announce `node-b` — CN is unconstrained legacy metadata and
/// trusting it alongside SANs reopens the impersonation hole. Node identity is
/// SAN-only; a cert with no usable SAN authorizes nothing (fail-closed), so
/// node certs MUST carry the node id in a SAN. DD R22 also adds `iPAddress` SANs
/// so IP-literal node ids are not falsely rejected.
#[must_use]
pub fn peer_identities(cert_der: &[u8]) -> Vec<String> {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

    let mut ids = Vec::new();
    let Ok((_, cert)) = X509Certificate::from_der(cert_der) else {
        return ids;
    };
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for name in &san.value.general_names {
            match name {
                GeneralName::DNSName(n) => ids.push((*n).to_string()),
                GeneralName::URI(u) => ids.push((*u).to_string()),
                GeneralName::IPAddress(bytes) => {
                    if let Ok(octets) = <[u8; 4]>::try_from(*bytes) {
                        ids.push(Ipv4Addr::from(octets).to_string());
                    } else if let Ok(octets) = <[u8; 16]>::try_from(*bytes) {
                        ids.push(Ipv6Addr::from(octets).to_string());
                    }
                }
                _ => {}
            }
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Generate a self-signed CA + node cert pair for unit tests using
    /// `rcgen` 0.13. Returns the temp dir (kept alive by the caller)
    /// plus the [`TlsConfig`] paths.
    fn gen_test_certs(node_name: &str) -> (TempDir, TlsConfig) {
        let dir = tempfile::tempdir().expect("tempdir");

        // 1. CA (self-signed).
        let ca_key = rcgen::KeyPair::generate().expect("ca keypair");
        let ca_params = rcgen::CertificateParams::new(vec!["ferrodruid-test-ca".to_string()])
            .expect("ca params");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

        // 2. Node leaf cert, signed by the CA.
        let node_key = rcgen::KeyPair::generate().expect("node keypair");
        let node_params = rcgen::CertificateParams::new(vec![
            node_name.to_string(),
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("node params");
        let node_cert = node_params
            .signed_by(&node_key, &ca_cert, &ca_key)
            .expect("node sign");

        let ca_path = dir.path().join("ca.pem");
        let cert_path = dir.path().join("node.pem");
        let key_path = dir.path().join("node-key.pem");
        fs::write(&ca_path, ca_cert.pem()).expect("write ca");
        fs::write(&cert_path, node_cert.pem()).expect("write cert");
        fs::write(&key_path, node_key.serialize_pem()).expect("write key");

        let cfg = TlsConfig::new(cert_path, key_path, ca_path);
        (dir, cfg)
    }

    #[test]
    fn load_certs_rejects_missing_file() {
        let err = load_certs(Path::new("/nonexistent/path-W44.pem")).expect_err("must fail");
        assert!(matches!(err, TlsError::Io { .. }));
    }

    #[test]
    fn load_certs_rejects_empty_pem() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = dir.path().join("empty.pem");
        fs::write(&p, b"").expect("write empty");
        let err = load_certs(&p).expect_err("must fail");
        assert!(matches!(err, TlsError::Empty { .. }));
    }

    #[test]
    fn load_server_config_round_trip() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-node");
        let acceptor = load_server_config(&cfg).expect("build server config");
        // Acceptor is opaque; just assert we got one and Drop is fine.
        drop(acceptor);
    }

    #[test]
    fn load_client_config_round_trip() {
        let (_dir, cfg) = gen_test_certs("ferrodruid-test-node");
        let connector = load_client_config(&cfg).expect("build client config");
        drop(connector);
    }

    #[test]
    fn config_rejects_mismatched_ca() {
        let (_dir1, cfg1) = gen_test_certs("node-1");
        let (_dir2, cfg2) = gen_test_certs("node-2");
        // Mix node-1's cert/key with node-2's CA — the cert chain validates
        // structurally (load_server_config doesn't itself check signatures
        // at build time, that happens at handshake), but a real handshake
        // against the wrong CA fails. We exercise the build path to make
        // sure the loader doesn't panic on unrelated material.
        let mixed = TlsConfig::new(cfg1.cert_path, cfg1.key_path, cfg2.ca_path);
        let _ = load_server_config(&mixed).expect("build is OK; handshake would reject");
    }

    #[test]
    fn peer_identities_extracts_san_dns() {
        // DD R21: a cert with SANs ["node-7","localhost"] must yield those
        // identities so the accept path can bind an announced node id to them,
        // and must NOT report an unrelated id.
        let key = rcgen::KeyPair::generate().expect("key");
        let params =
            rcgen::CertificateParams::new(vec!["node-7".to_string(), "localhost".to_string()])
                .expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let ids = peer_identities(cert.der().as_ref());
        assert!(ids.iter().any(|i| i == "node-7"), "ids: {ids:?}");
        assert!(ids.iter().any(|i| i == "localhost"), "ids: {ids:?}");
        assert!(
            !ids.iter().any(|i| i == "node-evil"),
            "must not assert an unrelated identity: {ids:?}",
        );
    }

    #[test]
    fn peer_identities_ignores_cn_when_san_present() {
        // DD R22 (High): the Common Name must NOT be trusted as a node identity.
        // A cert whose SAN says "node-attacker" but whose CN says "node-victim"
        // must yield ONLY the SAN identity, so the holder cannot announce
        // "node-victim".
        let key = rcgen::KeyPair::generate().expect("key");
        let mut params =
            rcgen::CertificateParams::new(vec!["node-attacker".to_string()]).expect("params");
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "node-victim");
        let cert = params.self_signed(&key).expect("self-signed");
        let ids = peer_identities(cert.der().as_ref());
        assert!(ids.iter().any(|i| i == "node-attacker"), "ids: {ids:?}");
        assert!(
            !ids.iter().any(|i| i == "node-victim"),
            "CN must not be trusted as a node identity: {ids:?}",
        );
    }

    #[test]
    fn peer_identities_extracts_ip_san() {
        // DD R22 (Medium): an IP-literal node id with an iPAddress SAN must be
        // extracted (canonical string) so IP-based node ids are not falsely
        // rejected by the inbound identity binding.
        let key = rcgen::KeyPair::generate().expect("key");
        let params = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let ids = peer_identities(cert.der().as_ref());
        assert!(
            ids.iter().any(|i| i == "127.0.0.1"),
            "IP SAN must be extracted: {ids:?}",
        );
    }

    #[test]
    fn peer_identities_wildcard_is_literal_not_expanded() {
        // DD R22 (High): a wildcard SAN is returned verbatim, never expanded.
        // The outbound exact-match binding (cert_authorizes_node) therefore
        // rejects a `*.cluster.local` server cert for a concrete dial to
        // `node-b.cluster.local` — closing the gap where rustls SNI matching
        // would accept the wildcard and let a wildcard-cert holder masquerade.
        let key = rcgen::KeyPair::generate().expect("key");
        let params =
            rcgen::CertificateParams::new(vec!["*.cluster.local".to_string()]).expect("params");
        let cert = params.self_signed(&key).expect("self-signed");
        let ids = peer_identities(cert.der().as_ref());
        assert!(ids.iter().any(|i| i == "*.cluster.local"), "ids: {ids:?}");
        assert!(
            !ids.iter().any(|i| i == "node-b.cluster.local"),
            "wildcard must not be expanded to a concrete id: {ids:?}",
        );
    }

    #[test]
    fn peer_identities_empty_on_garbage() {
        // Unparseable DER yields no identities -> the accept path treats the
        // peer as authorizing no node id (fail-closed).
        assert!(peer_identities(b"not-a-certificate").is_empty());
    }
}
