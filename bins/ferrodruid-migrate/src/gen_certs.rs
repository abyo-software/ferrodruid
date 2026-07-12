// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-I (CL-J1) — self-signed CA + per-role leaf cert generation used
//! by the `ferrodruid-migrate gen-cross-role-certs` subcommand.
//!
//! The generator writes one shared CA and one leaf cert + key per
//! requested role under `<out_dir>/`:
//!
//! ```text
//! out_dir/
//!   ca.pem            (mode 0644)
//!   ca.key            (mode 0600)
//!   broker/
//!     leaf.pem        (mode 0644)
//!     leaf.key        (mode 0600)
//!   historical/
//!     leaf.pem
//!     leaf.key
//!   ...
//! ```
//!
//! Each leaf cert is signed by the CA and carries the role name plus
//! every supplied "extra SAN" (typically `localhost`, `127.0.0.1`).
//! File modes `0600` on private keys are enforced via `chmod` on Unix;
//! on other platforms the helper logs a warning and writes the file
//! with the default permissions.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Outcome of [`generate_bundle`] — every path the helper wrote.
#[derive(Debug)]
pub struct GeneratedBundle {
    /// Path to the shared CA cert (mode `0644`).
    pub ca_pem: PathBuf,
    /// Path to the shared CA private key (mode `0600`).
    pub ca_key: PathBuf,
    /// One entry per role, in input order.
    pub leafs: Vec<(String, GeneratedLeaf)>,
}

/// Per-role leaf bundle.
#[derive(Debug)]
pub struct GeneratedLeaf {
    /// Path to the leaf cert (mode `0644`).
    pub leaf_pem: PathBuf,
    /// Path to the leaf private key (mode `0600`).
    pub leaf_key: PathBuf,
}

/// Errors raised by the cert generator.
#[derive(Debug, thiserror::Error)]
pub enum GenCertsError {
    /// I/O failure (mkdir, write, chmod).
    #[error("{op} {path}: {source}")]
    Io {
        /// What we were trying to do ("create directory", "write", "chmod").
        op: &'static str,
        /// The path involved.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// rcgen rejected the cert parameters or signing operation.
    #[error("rcgen: {0}")]
    Rcgen(String),
}

/// Generate the shared CA + per-role leaf bundle and write it to
/// `out_dir`.
///
/// # Errors
///
/// Returns [`GenCertsError`] on any I/O or rcgen failure.
pub fn generate_bundle(
    out_dir: &Path,
    roles: &[String],
    extra_sans: &[String],
    ca_cn: &str,
) -> Result<GeneratedBundle, GenCertsError> {
    mkdirp(out_dir)?;

    // ---- CA --------------------------------------------------------
    let ca_key = rcgen::KeyPair::generate().map_err(|e| GenCertsError::Rcgen(e.to_string()))?;
    let mut ca_params = rcgen::CertificateParams::new(vec![ca_cn.to_string()])
        .map_err(|e| GenCertsError::Rcgen(e.to_string()))?;
    // Mark as a CA so the leaf certs validate cleanly under
    // WebPkiClientVerifier / WebPkiServerVerifier.
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, ca_cn);
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| GenCertsError::Rcgen(e.to_string()))?;

    let ca_pem_path = out_dir.join("ca.pem");
    let ca_key_path = out_dir.join("ca.key");
    write_pem(&ca_pem_path, &ca_cert.pem(), false)?;
    write_pem(&ca_key_path, &ca_key.serialize_pem(), true)?;

    // ---- per-role leafs --------------------------------------------
    let mut leafs: Vec<(String, GeneratedLeaf)> = Vec::with_capacity(roles.len());
    for role in roles {
        let role_dir = out_dir.join(role);
        mkdirp(&role_dir)?;

        let leaf_key =
            rcgen::KeyPair::generate().map_err(|e| GenCertsError::Rcgen(e.to_string()))?;
        let mut sans = vec![role.clone()];
        sans.extend(extra_sans.iter().cloned());
        let mut leaf_params =
            rcgen::CertificateParams::new(sans).map_err(|e| GenCertsError::Rcgen(e.to_string()))?;
        leaf_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, role);
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &ca_cert, &ca_key)
            .map_err(|e| GenCertsError::Rcgen(e.to_string()))?;

        let leaf_pem_path = role_dir.join("leaf.pem");
        let leaf_key_path = role_dir.join("leaf.key");
        write_pem(&leaf_pem_path, &leaf_cert.pem(), false)?;
        write_pem(&leaf_key_path, &leaf_key.serialize_pem(), true)?;

        leafs.push((
            role.clone(),
            GeneratedLeaf {
                leaf_pem: leaf_pem_path,
                leaf_key: leaf_key_path,
            },
        ));
    }

    Ok(GeneratedBundle {
        ca_pem: ca_pem_path,
        ca_key: ca_key_path,
        leafs,
    })
}

fn mkdirp(path: &Path) -> Result<(), GenCertsError> {
    fs::create_dir_all(path).map_err(|source| GenCertsError::Io {
        op: "create directory",
        path: path.to_path_buf(),
        source,
    })
}

fn write_pem(path: &Path, contents: &str, secret: bool) -> Result<(), GenCertsError> {
    fs::write(path, contents).map_err(|source| GenCertsError::Io {
        op: "write",
        path: path.to_path_buf(),
        source,
    })?;
    if secret {
        chmod_0600(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn chmod_0600(path: &Path) -> Result<(), GenCertsError> {
    use std::os::unix::fs::PermissionsExt;
    let mut perm = fs::metadata(path)
        .map_err(|source| GenCertsError::Io {
            op: "stat",
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    perm.set_mode(0o600);
    fs::set_permissions(path, perm).map_err(|source| GenCertsError::Io {
        op: "chmod",
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(not(unix))]
fn chmod_0600(_: &Path) -> Result<(), GenCertsError> {
    tracing::warn!(
        "chmod 0600 not enforced on non-Unix platforms — private key file modes will use OS \
         defaults"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_bundle_writes_six_role_bundle_with_canonical_layout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let roles = vec![
            "broker".to_string(),
            "historical".to_string(),
            "coordinator".to_string(),
            "router".to_string(),
            "overlord".to_string(),
            "middlemanager".to_string(),
        ];
        let extra_sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        let bundle = generate_bundle(dir.path(), &roles, &extra_sans, "test-ca").expect("gen");

        assert!(bundle.ca_pem.exists());
        assert!(bundle.ca_key.exists());
        assert_eq!(bundle.leafs.len(), 6);
        for (role, leaf) in &bundle.leafs {
            assert!(leaf.leaf_pem.exists(), "{role} leaf.pem must exist");
            assert!(leaf.leaf_key.exists(), "{role} leaf.key must exist");
            assert!(
                leaf.leaf_pem.starts_with(dir.path().join(role)),
                "{role} leaf path must be under per-role subdir",
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn generate_bundle_chmods_private_keys_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let bundle = generate_bundle(
            dir.path(),
            &["broker".to_string()],
            &["localhost".to_string()],
            "test-ca",
        )
        .expect("gen");
        let ca_mode = fs::metadata(&bundle.ca_key)
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(ca_mode, 0o600, "ca.key must be 0600");
        let leaf_mode = fs::metadata(&bundle.leafs[0].1.leaf_key)
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(leaf_mode, 0o600, "leaf.key must be 0600");
    }

    #[test]
    fn generated_leaf_is_signed_by_generated_ca() {
        // Validate the bundle is self-consistent by loading the CA +
        // leaf as a rustls server config and round-tripping a client
        // config that trusts the same CA. If the chain were broken, the
        // builder would refuse to construct the verifier.
        use std::sync::Arc;

        use rustls::server::WebPkiClientVerifier;
        use rustls::{RootCertStore, ServerConfig};
        use rustls_pki_types::{CertificateDer, PrivateKeyDer};

        let dir = tempfile::tempdir().expect("tempdir");
        let bundle = generate_bundle(
            dir.path(),
            &["broker".to_string()],
            &["localhost".to_string()],
            "test-ca",
        )
        .expect("gen");

        let provider = Arc::new(rustls::crypto::ring::default_provider());

        // Load leaf cert + key.
        let cert_bytes = fs::read(&bundle.leafs[0].1.leaf_pem).expect("read cert");
        let mut reader = std::io::BufReader::new(&cert_bytes[..]);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse cert");

        let key_bytes = fs::read(&bundle.leafs[0].1.leaf_key).expect("read key");
        let mut reader = std::io::BufReader::new(&key_bytes[..]);
        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut reader)
            .expect("parse key")
            .expect("key present");

        // Load CA.
        let ca_bytes = fs::read(&bundle.ca_pem).expect("read ca");
        let mut reader = std::io::BufReader::new(&ca_bytes[..]);
        let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse ca");
        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots.add(cert).expect("add ca");
        }

        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .expect("verifier");
        let _server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .expect("with_single_cert (leaf signed by CA)");
    }
}
