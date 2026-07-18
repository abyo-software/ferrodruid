// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 40-A — cluster transport authentication.
//!
//! Closes the Wave 39 final-closure DD findings:
//!
//! - **\[Critical\] [NEW]** Leader trusts unauthenticated `ReplicateAck`
//!   sender ids. Forged ACKs over the cluster TCP port could fabricate
//!   quorum and commit unreplicated writes.
//! - **\[High\] [NEW]** Any TCP client can raise terms via unauthenticated
//!   heartbeats / steal votes via arbitrary `candidate_id`.
//!
//! Threat model
//! ------------
//!
//! PSK authenticates **every** cluster frame and binds a
//! `(connection, announced_node_id)` claim. It defends against a network
//! adversary that can reach the cluster TCP port but does not possess the
//! shared secret.
//!
//! It does **not** defend against:
//!
//! 1. Host compromise (attacker reads the PSK from disk / env).
//! 2. PSK leak via misconfigured logging or accidental commit.
//! 3. Replay of ACKs **inside the same connection** (a connected, authed
//!    peer can still resend its own old frames). Receiver-side
//!    application-layer dedup (the existing `voted_for` write-once latch
//!    and `match_index` monotonic advance) closes this.
//! 4. Confidentiality: over cleartext TCP the JSON payloads are visible to
//!    a passive eavesdropper.
//!
//! Posture (Phase 2.4)
//! -------------------
//!
//! mTLS is now the **default** cluster transport posture
//! ([`crate::transport::ClusterSecurityMode::MutualTls`]): PSK frame
//! authentication runs *inside* a mutually-authenticated, encrypted TLS
//! tunnel that adds confidentiality + forward secrecy + CA-verified peer
//! identity. PSK-over-cleartext
//! ([`crate::transport::ClusterSecurityMode::PskCleartext`]) is retained as
//! an explicit operator opt-in fallback for backward compatibility / test
//! rigs / sealed networks — it is never selected implicitly.
//!
//! Wire format
//! -----------
//!
//! Every cluster frame is prefixed with a 4-byte big-endian payload
//! length followed by a 32-byte HMAC-SHA256 tag computed over the JSON
//! body using the shared cluster PSK.
//!
//! ```text
//! +-------------------+--------------------+--------------------+
//! | u32 payload_len   | 32-byte HMAC tag   | JSON payload       |
//! +-------------------+--------------------+--------------------+
//! ```
//!
//! Receivers compute the HMAC over the inbound JSON payload and compare
//! in constant time; on mismatch the connection is closed and a warn is
//! logged. The first frame on every connection is a [`HandshakeFrame`]
//! whose body itself authenticates the announced sender id; subsequent
//! frames are dropped if their declared `sender_id` differs from the
//! handshake's `announced_node_id`.
//!
//! PSK derivation
//! --------------
//!
//! [`derive_psk`] accepts either a 64-hex-char string (parsed as 32
//! raw key bytes) or any other string (SHA-256 hashed to 32 bytes).
//! 32 bytes of randomness suitable for production deployment can be
//! generated with `head -c 32 /dev/urandom | xxd -p -c 64`.

use hmac::Mac;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// 32-byte cluster pre-shared key.
///
/// Constructed via [`derive_psk`]. Treated as opaque material; do **not**
/// log or serialise this type.
#[derive(Clone)]
pub struct ClusterPsk {
    /// Raw 32-byte key bytes (HMAC-SHA256 block-size aligned).
    bytes: [u8; 32],
}

impl ClusterPsk {
    /// The HMAC tag length, in bytes (output of HMAC-SHA256).
    pub const HMAC_TAG_LEN: usize = 32;

    /// Construct directly from 32 raw key bytes. Prefer [`derive_psk`]
    /// for operator-supplied input.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Return a reference to the raw key bytes.
    ///
    /// Used by the wire path to construct fresh HMAC instances; callers
    /// must not retain the slice beyond the immediate computation.
    #[doc(hidden)]
    #[must_use]
    pub fn raw(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl std::fmt::Debug for ClusterPsk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterPsk")
            .field("bytes", &"<redacted 32 bytes>")
            .finish()
    }
}

/// Errors produced by PSK derivation / HMAC verification.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The 4-byte length prefix declared a payload smaller than the
    /// 32-byte HMAC tag, so the frame cannot possibly carry a valid
    /// authenticated message.
    #[error("frame too short for HMAC tag (len = {len} bytes)")]
    FrameTooShort {
        /// Declared payload length from the frame header.
        len: usize,
    },
    /// HMAC tag did not match the recomputed value. Caller must drop
    /// the message and close the connection.
    #[error("HMAC verification failed")]
    HmacMismatch,
    /// PSK string was empty or otherwise malformed. Returned by
    /// [`derive_psk`].
    #[error("invalid PSK input: {0}")]
    InvalidPsk(&'static str),
    /// JSON decode failed after HMAC verified — the payload was
    /// authenticated but is not a valid `ReplicationMessage`.
    #[error("authenticated payload was not valid JSON: {0}")]
    PayloadDecode(String),
}

/// Derive a 32-byte cluster PSK from operator input.
///
/// - 64 hex characters → parsed as 32 raw bytes (case-insensitive).
/// - Any other input → SHA-256(input.as_bytes()).
///
/// An empty string is rejected: an "empty" PSK would otherwise hash to a
/// well-known constant, which is indistinguishable from "no auth".
pub fn derive_psk(input: &str) -> Result<ClusterPsk, AuthError> {
    if input.is_empty() {
        return Err(AuthError::InvalidPsk("PSK input must not be empty"));
    }
    if input.len() == 64 && input.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            let s = input
                .get(i * 2..i * 2 + 2)
                .ok_or(AuthError::InvalidPsk("hex slice out of range"))?;
            *byte =
                u8::from_str_radix(s, 16).map_err(|_| AuthError::InvalidPsk("invalid hex byte"))?;
        }
        return Ok(ClusterPsk::from_bytes(out));
    }
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    Ok(ClusterPsk::from_bytes(out))
}

/// Compute the HMAC-SHA256 tag of `payload` under `psk`.
///
/// Used by the encode path; receivers should compare via
/// [`verify_hmac`] which uses constant-time equality.
#[must_use]
pub fn compute_hmac(psk: &ClusterPsk, payload: &[u8]) -> [u8; 32] {
    type HmacSha256 = hmac::Hmac<Sha256>;
    // hmac::Hmac::new_from_slice cannot fail for valid SHA-256 keys.
    let mut mac =
        HmacSha256::new_from_slice(psk.raw()).expect("HMAC-SHA256 accepts any 32-byte key length");
    mac.update(payload);
    let tag = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&tag);
    out
}

/// Verify that `tag` is the HMAC-SHA256 of `payload` under `psk`,
/// in constant time.
pub fn verify_hmac(psk: &ClusterPsk, payload: &[u8], tag: &[u8]) -> Result<(), AuthError> {
    type HmacSha256 = hmac::Hmac<Sha256>;
    let mut mac =
        HmacSha256::new_from_slice(psk.raw()).expect("HMAC-SHA256 accepts any 32-byte key length");
    mac.update(payload);
    mac.verify_slice(tag).map_err(|_| AuthError::HmacMismatch)
}

/// Wave 40-A handshake frame.
///
/// Sent as the **first** authenticated frame on every newly-opened
/// outbound TCP connection.  The receiver records
/// `connection_id -> announced_node_id` and rejects subsequent frames on
/// the same connection whose `sender_id` (in the JSON body) differs from
/// the announced id.  This prevents an authenticated peer from
/// impersonating a different node id within the same authenticated
/// session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeFrame {
    /// Node id the sender claims to be.
    pub announced_node_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_psk_hex_roundtrip() {
        let hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let psk = derive_psk(hex).expect("hex parse");
        assert_eq!(psk.raw()[0], 0x00);
        assert_eq!(psk.raw()[31], 0xff);
        assert_eq!(psk.raw()[1], 0x11);
    }

    #[test]
    fn derive_psk_string_hashes_to_sha256() {
        let psk = derive_psk("hunter2").expect("string");
        // sha256("hunter2") in hex
        let expected = {
            let mut h = Sha256::new();
            h.update(b"hunter2");
            let d = h.finalize();
            let mut out = [0u8; 32];
            out.copy_from_slice(&d);
            out
        };
        assert_eq!(psk.raw(), &expected);
    }

    #[test]
    fn derive_psk_empty_rejected() {
        let err = derive_psk("").unwrap_err();
        assert!(matches!(err, AuthError::InvalidPsk(_)));
    }

    #[test]
    fn derive_psk_invalid_hex_falls_back_to_sha256() {
        // 64 chars but not all hex → must be treated as a string and
        // hashed, NOT parsed as hex.
        let almost_hex = "zz112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let p1 = derive_psk(almost_hex).expect("string fallback");
        let mut h = Sha256::new();
        h.update(almost_hex.as_bytes());
        let d = h.finalize();
        let mut expected = [0u8; 32];
        expected.copy_from_slice(&d);
        assert_eq!(p1.raw(), &expected);
    }

    #[test]
    fn hmac_compute_and_verify_roundtrip() {
        let psk = derive_psk("test-key").expect("derive");
        let payload = b"hello cluster";
        let tag = compute_hmac(&psk, payload);
        verify_hmac(&psk, payload, &tag).expect("verify ok");
    }

    #[test]
    fn hmac_verify_rejects_wrong_psk() {
        let psk_good = derive_psk("good-key").expect("derive good");
        let psk_bad = derive_psk("bad-key").expect("derive bad");
        let payload = b"hello cluster";
        let tag = compute_hmac(&psk_bad, payload);
        let err = verify_hmac(&psk_good, payload, &tag).unwrap_err();
        assert!(matches!(err, AuthError::HmacMismatch));
    }

    #[test]
    fn hmac_verify_rejects_tampered_payload() {
        let psk = derive_psk("test-key").expect("derive");
        let payload = b"hello cluster";
        let tag = compute_hmac(&psk, payload);
        let tampered = b"hello CLUSTER";
        let err = verify_hmac(&psk, tampered, &tag).unwrap_err();
        assert!(matches!(err, AuthError::HmacMismatch));
    }

    #[test]
    fn psk_debug_is_redacted() {
        let psk = derive_psk("secret").expect("derive");
        let s = format!("{psk:?}");
        assert!(s.contains("redacted"));
        assert!(!s.contains("secret"));
    }
}
