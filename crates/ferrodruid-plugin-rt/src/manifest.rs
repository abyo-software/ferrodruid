// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Module signature manifest.
//!
//! Every plugin loaded into the runtime ships with a
//! [`ModuleManifest`] that carries (at minimum) the expected SHA-256
//! of the module bytes.  The runtime hashes the bytes at load time
//! and refuses to instantiate a module whose hash does not match,
//! catching accidental corruption and trivial tampering.
//!
//! This is intentionally **not** a full code-signing solution.
//! Production deployments should additionally verify the manifest
//! itself via cosign / Sigstore — that work is tracked under
//! `docs/known-limitations.md` EF-2 (Wave 5).  The SHA-256
//! check closes the trivial "swap the .wasm under the operator's
//! feet" attack and is the minimum bar the rest of the runtime
//! depends on.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::PluginError;

/// Manifest accompanying a plugin module on disk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModuleManifest {
    /// Stable plugin name (used in logs / error messages).
    pub name: String,
    /// Semver string for the plugin's own version.
    pub version: String,
    /// SHA-256 hex digest the runtime checks against the module bytes
    /// at load time.
    pub sha256_hex: String,
}

impl ModuleManifest {
    /// Compute a manifest from a module's bytes.  Convenience for
    /// build tooling — production manifests would be signed
    /// externally.
    #[must_use]
    pub fn from_bytes(name: impl Into<String>, version: impl Into<String>, bytes: &[u8]) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            sha256_hex: expected_sha256_hex(bytes),
        }
    }

    /// Verify that `bytes` hash to the manifest's `sha256_hex`.
    /// Returns the observed hash on success so callers can include
    /// it in logs.
    pub fn verify(&self, bytes: &[u8]) -> Result<String, PluginError> {
        let observed = expected_sha256_hex(bytes);
        if observed.eq_ignore_ascii_case(&self.sha256_hex) {
            Ok(observed)
        } else {
            Err(PluginError::SignatureMismatch {
                expected: self.sha256_hex.clone(),
                observed,
            })
        }
    }
}

/// Compute the lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn expected_sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_round_trip_verifies() {
        let bytes = b"hello plugin";
        let m = ModuleManifest::from_bytes("test", "1.0.0", bytes);
        let observed = m.verify(bytes).expect("hash must match");
        assert_eq!(observed, m.sha256_hex);
        assert_eq!(observed.len(), 64, "sha256 hex is 64 chars");
    }

    #[test]
    fn manifest_rejects_tampered_bytes() {
        let bytes = b"hello plugin";
        let m = ModuleManifest::from_bytes("test", "1.0.0", bytes);
        let tampered = b"hello plugin!";
        let err = m.verify(tampered).expect_err("tampered must reject");
        match err {
            PluginError::SignatureMismatch { expected, observed } => {
                assert_eq!(expected, m.sha256_hex);
                assert_ne!(observed, m.sha256_hex);
            }
            other => panic!("expected SignatureMismatch, got {other:?}"),
        }
    }

    #[test]
    fn manifest_serde_round_trip() {
        let m = ModuleManifest {
            name: "running-stddev".into(),
            version: "1.0.0".into(),
            sha256_hex: "deadbeef".repeat(8),
        };
        let s = serde_json::to_string(&m).expect("serialize");
        let back: ModuleManifest = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(m, back);
    }

    #[test]
    fn manifest_hex_case_insensitive() {
        let bytes = b"abc";
        let mut m = ModuleManifest::from_bytes("t", "1.0", bytes);
        m.sha256_hex = m.sha256_hex.to_uppercase();
        // Uppercase manifest still matches lowercase observed hash.
        m.verify(bytes).expect("case-insensitive compare");
    }
}
