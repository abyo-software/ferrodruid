// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! HMAC-SHA-256 bearer token authenticator plugin for FerroDruid.
//!
//! Verifies bearer tokens of the form
//! `<payload>.<exp_ms>.<hmac_sha256_hex>` where the HMAC is
//! computed over the byte string `<payload>.<exp_ms>` using a
//! host-supplied secret.  No capabilities are required — the plugin
//! is pure compute.
//!
//! Return codes from `auth_verify`:
//! * `1` — token is valid and not expired.
//! * `0` — HMAC mismatch (signature invalid).
//! * `-1` — token is malformed (missing fields, bad hex, etc).
//! * `-2` — token signature is valid but the embedded
//!   `exp_ms` is in the past relative to the host-supplied `now_ms`.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_op_in_unsafe_fn)]

extern crate alloc;

#[cfg(target_arch = "wasm32")]
use alloc::alloc::{Layout, alloc as raw_alloc, dealloc as raw_dealloc};
use alloc::vec::Vec;

use hmac::{Hmac, Mac};
use sha2::Sha256;

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

type HmacSha256 = Hmac<Sha256>;

/// FerroDruid plugin ABI version (function-style export, see
/// running-stddev for the `pub static i32` footgun explanation).
#[unsafe(no_mangle)]
pub extern "C" fn plugin_abi_version() -> i32 {
    1
}

/// Verification outcome from the safe core.  Maps 1:1 onto the ABI
/// return codes; the wasm wrapper just translates this back to an
/// `i32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    /// Token is valid and (if it had an `exp_ms`) has not expired.
    Valid,
    /// Token format is well-formed but the HMAC does not match
    /// (tampered signature or wrong secret).
    InvalidSignature,
    /// Token is structurally malformed (wrong field count, non-hex
    /// signature, unparsable `exp_ms`, etc).
    Malformed,
    /// HMAC matches but `now_ms > exp_ms` (token expired).
    Expired,
}

impl AuthResult {
    /// Translate to the ABI return code defined in this module's docs.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        match self {
            AuthResult::Valid => 1,
            AuthResult::InvalidSignature => 0,
            AuthResult::Malformed => -1,
            AuthResult::Expired => -2,
        }
    }
}

// ---------------------------------------------------------------------------
// Safe core
// ---------------------------------------------------------------------------

/// Verify a bearer token against a secret + current time.  The
/// token must be of the form `<payload>.<exp_ms>.<hmac_hex>` where
/// `exp_ms` is a decimal `i64`; `exp_ms = 0` means "never expires".
#[must_use]
pub fn verify_token(token: &[u8], secret: &[u8], now_ms: i64) -> AuthResult {
    let Some((message, signature_hex)) = split_last(token, b'.') else {
        return AuthResult::Malformed;
    };
    let Some((_payload, exp_str)) = split_last(message, b'.') else {
        return AuthResult::Malformed;
    };

    let Some(expected_sig) = decode_hex(signature_hex) else {
        return AuthResult::Malformed;
    };

    let Ok(mut mac) = HmacSha256::new_from_slice(secret) else {
        return AuthResult::Malformed;
    };
    mac.update(message);
    if mac.verify_slice(&expected_sig).is_err() {
        return AuthResult::InvalidSignature;
    }

    let Ok(exp_str) = core::str::from_utf8(exp_str) else {
        return AuthResult::Malformed;
    };
    let Ok(exp_ms) = exp_str.parse::<i64>() else {
        return AuthResult::Malformed;
    };
    if exp_ms != 0 && now_ms > exp_ms {
        return AuthResult::Expired;
    }
    AuthResult::Valid
}

fn split_last(s: &[u8], byte: u8) -> Option<(&[u8], &[u8])> {
    let idx = s.iter().rposition(|&b| b == byte)?;
    Some((&s[..idx], &s[idx + 1..]))
}

fn decode_hex(s: &[u8]) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in s.chunks_exact(2) {
        let hi = nibble(pair[0])?;
        let lo = nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

const fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Sign a payload + `exp_ms` and return the canonical token string.
/// Used by tests + by host-side fixtures.
#[must_use]
pub fn sign_token(secret: &[u8], payload: &[u8], exp_ms: i64) -> Vec<u8> {
    use alloc::string::String;
    use core::fmt::Write as _;

    let mut message: Vec<u8> = Vec::with_capacity(payload.len() + 24);
    message.extend_from_slice(payload);
    message.push(b'.');
    let mut exp_buf = String::new();
    let _ = write!(&mut exp_buf, "{exp_ms}");
    message.extend_from_slice(exp_buf.as_bytes());

    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac key");
    mac.update(&message);
    let sig = mac.finalize().into_bytes();
    let mut out = message;
    out.push(b'.');
    // Write the hex signature byte by byte via a tiny adapter so we
    // can use `core::fmt::Write` without pulling in `std`.
    let mut writer = VecWriter(&mut out);
    for b in sig {
        let _ = write!(&mut writer, "{b:02x}");
    }
    out
}

struct VecWriter<'a>(&'a mut Vec<u8>);

// `Vec<u8>` doesn't implement `core::fmt::Write`; adapt with a
// trivial wrapper used only inside `sign_token`.
impl core::fmt::Write for VecWriter<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// C-ABI wrappers — wasm32 only.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: i32) -> i32 {
    if size <= 0 {
        return 0;
    }
    let Ok(layout) = Layout::from_size_align(size as usize, 1) else {
        return 0;
    };
    // SAFETY: layout valid; null on OOM surfaced as `0`.
    let ptr = unsafe { raw_alloc(layout) };
    if ptr.is_null() { 0 } else { ptr as i32 }
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn dealloc(ptr: i32, size: i32) {
    if ptr <= 0 || size <= 0 {
        return;
    }
    let Ok(layout) = Layout::from_size_align(size as usize, 1) else {
        return;
    };
    // SAFETY: host ABI contract — `ptr`/`size` from a prior `alloc`.
    unsafe { raw_dealloc(ptr as *mut u8, layout) }
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn auth_verify(
    token_ptr: i32,
    token_len: i32,
    secret_ptr: i32,
    secret_len: i32,
    now_ms: i64,
) -> i32 {
    if token_ptr <= 0 || token_len <= 0 || secret_ptr <= 0 || secret_len <= 0 {
        return AuthResult::Malformed.as_i32();
    }
    // SAFETY: pointers come from the host's `alloc` and span the
    // claimed length.
    let token = unsafe { core::slice::from_raw_parts(token_ptr as *const u8, token_len as usize) };
    let secret =
        unsafe { core::slice::from_raw_parts(secret_ptr as *const u8, secret_len as usize) };
    verify_token(token, secret, now_ms).as_i32()
}

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::String;

    fn to_string(v: &[u8]) -> String {
        core::str::from_utf8(v).expect("utf8").into()
    }

    #[test]
    fn valid_token_returns_valid() {
        let secret = b"super-secret-32-bytes-or-more-12345";
        let token = sign_token(secret, b"alice", 0);
        assert_eq!(verify_token(&token, secret, 123_456_789), AuthResult::Valid);
        assert_eq!(AuthResult::Valid.as_i32(), 1);
    }

    #[test]
    fn tampered_signature_returns_invalid() {
        let secret = b"super-secret-32-bytes-or-more-12345";
        let mut token = sign_token(secret, b"alice", 0);
        // Flip the last hex char of the signature.
        let last = *token.last().expect("non-empty");
        let new_last = if last == b'a' { b'b' } else { b'a' };
        *token.last_mut().expect("non-empty") = new_last;
        assert_eq!(
            verify_token(&token, secret, 123_456_789),
            AuthResult::InvalidSignature,
            "tampered: {}",
            to_string(&token)
        );
    }

    #[test]
    fn expired_token_returns_expired() {
        let secret = b"super-secret-32-bytes-or-more-12345";
        let token = sign_token(secret, b"alice", 1000);
        assert_eq!(verify_token(&token, secret, 10_000), AuthResult::Expired);
    }

    #[test]
    fn not_yet_expired_returns_valid() {
        let secret = b"super-secret-32-bytes-or-more-12345";
        let token = sign_token(secret, b"alice", 5_000);
        assert_eq!(verify_token(&token, secret, 1_000), AuthResult::Valid);
        assert_eq!(verify_token(&token, secret, 5_000), AuthResult::Valid);
    }

    #[test]
    fn malformed_token_returns_malformed() {
        let secret = b"x";
        // No dots at all.
        assert_eq!(verify_token(b"nosignature", secret, 0), AuthResult::Malformed);
        // One dot only (missing exp_ms section).
        assert_eq!(verify_token(b"alice.cafebabe", secret, 0), AuthResult::Malformed);
        // Odd-length hex signature.
        assert_eq!(
            verify_token(b"alice.0.abc", secret, 0),
            AuthResult::Malformed
        );
    }

    #[test]
    fn unparsable_exp_with_valid_sig_returns_malformed() {
        // Construct a token whose `exp_ms` cannot parse as i64 but
        // whose HMAC signature does verify — exercises the post-
        // signature parse-failure path that returns Malformed.
        let secret = b"super-secret";
        // We sign over "alice.notanumber" so the signature verifies,
        // then the exp_ms parse fails => Malformed.
        let message = b"alice.notanumber";
        let mut mac = HmacSha256::new_from_slice(secret).expect("hmac");
        mac.update(message);
        let sig = mac.finalize().into_bytes();
        let mut token: Vec<u8> = message.to_vec();
        token.push(b'.');
        let mut writer = VecWriter(&mut token);
        for b in sig {
            use core::fmt::Write as _;
            let _ = write!(&mut writer, "{b:02x}");
        }
        assert_eq!(verify_token(&token, secret, 0), AuthResult::Malformed);
    }

    #[test]
    fn wrong_secret_returns_invalid() {
        let token = sign_token(b"correct-secret", b"alice", 0);
        assert_eq!(
            verify_token(&token, b"different", 0),
            AuthResult::InvalidSignature
        );
    }
}
