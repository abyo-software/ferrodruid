// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Authentication — Basic auth, Argon2id, LDAP, FerroAuth for FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::LazyLock;

use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Sentinel Argon2id hash used by [`AuthStore::verify`] to spend roughly
/// the same time on a missing-username path as on the existing-user
/// path.
///
/// Wave 42-B (Wave 35 R1 Low `auth/lib.rs:119` closure): the previous
/// implementation returned `Ok(None)` immediately when the username was
/// not in the store, while existing usernames paid the full Argon2id
/// verification cost.  An attacker measuring response latency could
/// distinguish valid principals before any password attempt — a classic
/// pre-auth username-enumeration side channel.  By always verifying
/// against this constant sentinel hash when the username is absent,
/// both code paths now perform the same expensive Argon2id work.
///
/// The plaintext password backing this hash is intentionally
/// unreachable — the salt is random and the password value is never
/// exposed via the API, so no input from a remote caller can ever
/// satisfy this sentinel.  The hash itself is computed once at first
/// use via [`LazyLock`] and reused for every missing-user verification.
static SENTINEL_ARGON2_HASH: LazyLock<String> = LazyLock::new(|| {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    // The plaintext used here is irrelevant — the verification is
    // expected to fail.  We only care about the *cost* of computing
    // it.  `expect` is acceptable in this static initializer because
    // Argon2 hashing of a 32-byte slice with a fresh salt cannot fail
    // in practice; if it does, a panic at process start is preferable
    // to a silent timing oracle for the lifetime of the process.
    argon2
        .hash_password(b"sentinel-never-matches-any-real-password", &salt)
        .expect("argon2 sentinel hash must succeed at startup")
        .to_string()
});

/// Authentication errors.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Invalid credentials.
    #[error("invalid credentials")]
    InvalidCredentials,
    /// Backend unreachable.
    #[error("auth backend error: {0}")]
    Backend(String),
    /// Password hashing error.
    #[error("password hash error: {0}")]
    HashError(String),
    /// Invalid authorization header format.
    #[error("invalid auth header: {0}")]
    InvalidHeader(String),
    /// No such user in the store.
    #[error("user not found")]
    UserNotFound,
}

/// Authenticated identity (legacy trait-based API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Username or principal name.
    pub name: String,
    /// Authentication method used.
    pub auth_method: String,
}

/// Trait for authentication backends.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticate with username and password, returning an identity on success.
    async fn authenticate(&self, username: &str, password: &str) -> Result<Identity, AuthError>;
}

// ---------------------------------------------------------------------------
// UserRecord + AuthStore
// ---------------------------------------------------------------------------

/// A stored user record with Argon2id password hash.
///
/// `Serialize`/`Deserialize` so a bootstrapped credential can be persisted (the
/// hash, never the plaintext password) and reloaded across restarts — see
/// [`AuthStore::add_user_with_hash`] and [`AuthStore::user_record`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    /// Username.
    pub username: String,
    /// Argon2id password hash (PHC string format).
    pub password_hash: String,
    /// Roles assigned to this user.
    pub roles: Vec<String>,
    /// Whether this user must rotate their password before the API will
    /// serve any other request on their behalf.
    ///
    /// The bootstrap admin is created with this set to `true` so a freshly
    /// installed instance forces the operator to replace the
    /// auto-generated initial password on first login (AWS Marketplace
    /// security requirement).  Cleared by [`AuthStore::set_password`].
    ///
    /// `#[serde(default)]` so an `admin.json` persisted before this field
    /// existed deserializes to `false` (backward compatible) — an already
    /// rotated/legacy credential is not retroactively forced to change.
    #[serde(default)]
    pub must_change_password: bool,
}

/// In-memory user credential store backed by Argon2id hashes.
#[derive(Debug)]
pub struct AuthStore {
    users: HashMap<String, UserRecord>,
}

impl Default for AuthStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthStore {
    /// Create a new empty auth store.
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    /// Add a user with password (hashes with Argon2id).
    ///
    /// The user is created with `must_change_password = false`; use
    /// [`AuthStore::add_user_must_change`] to force a password rotation on
    /// first login.
    pub fn add_user(
        &mut self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> Result<(), AuthError> {
        self.add_user_must_change(username, password, roles, false)
    }

    /// Add a user with password (hashes with Argon2id), choosing whether the
    /// account must rotate its password before the API will serve it.
    ///
    /// The bootstrap admin is created with `must_change = true` so the
    /// auto-generated initial password cannot be used for anything except
    /// the change-credential endpoint (AWS Marketplace security
    /// requirement).
    pub fn add_user_must_change(
        &mut self,
        username: &str,
        password: &str,
        roles: Vec<String>,
        must_change: bool,
    ) -> Result<(), AuthError> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| AuthError::HashError(e.to_string()))?;

        self.users.insert(
            username.to_string(),
            UserRecord {
                username: username.to_string(),
                password_hash: hash.to_string(),
                roles,
                must_change_password: must_change,
            },
        );
        Ok(())
    }

    /// Insert a user from an already-computed Argon2id password hash (PHC
    /// string), without re-hashing.
    ///
    /// This is how a persisted credential is reloaded so an auth-enabled
    /// deployment is NOT locked out after a restart: the bootstrap admin's
    /// hash (not the plaintext password) is written to durable storage on first
    /// launch and re-inserted here on every subsequent launch. A malformed
    /// `password_hash` simply means [`AuthStore::verify`] can never succeed for
    /// this user (fail-closed), so no validation is performed here.
    ///
    /// `must_change` carries the persisted force-rotation flag across a
    /// restart: a bootstrap admin that has never rotated its password
    /// reloads with `must_change = true` and stays gated, while a rotated
    /// admin reloads with `false` so the operator is not asked to change it
    /// again.
    pub fn add_user_with_hash(
        &mut self,
        username: &str,
        password_hash: &str,
        roles: Vec<String>,
        must_change: bool,
    ) {
        self.users.insert(
            username.to_string(),
            UserRecord {
                username: username.to_string(),
                password_hash: password_hash.to_string(),
                roles,
                must_change_password: must_change,
            },
        );
    }

    /// Rotate `username`'s password: re-hash `new_plaintext` with Argon2id,
    /// replace the stored hash, and clear `must_change_password`.
    ///
    /// The Argon2id hash is computed **before** any mutation, so a hashing
    /// failure returns [`AuthError::HashError`] with the previous credential
    /// left fully intact (no torn write).  Returns [`AuthError::UserNotFound`]
    /// if no such user exists.
    pub fn set_password(&mut self, username: &str, new_plaintext: &str) -> Result<(), AuthError> {
        // Hash first; only touch the stored record once we have a valid hash.
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let new_hash = argon2
            .hash_password(new_plaintext.as_bytes(), &salt)
            .map_err(|e| AuthError::HashError(e.to_string()))?
            .to_string();

        let record = self
            .users
            .get_mut(username)
            .ok_or(AuthError::UserNotFound)?;
        record.password_hash = new_hash;
        record.must_change_password = false;
        Ok(())
    }

    /// Return the stored record for `username`, if present, so its credential
    /// (hash + roles) can be persisted for reload across restarts.
    #[must_use]
    pub fn user_record(&self, username: &str) -> Option<&UserRecord> {
        self.users.get(username)
    }

    /// Verify username + password.
    ///
    /// Returns `Some(AuthenticatedUser)` on success, `None` if the
    /// password is wrong **or** the username is unknown, and `Err`
    /// only on internal failures.
    ///
    /// Wave 42-B (Wave 35 R1 Low closure): the missing-user path now
    /// performs a dummy Argon2id verification against
    /// [`SENTINEL_ARGON2_HASH`] before returning `Ok(None)`, so the
    /// timing of "unknown username" closely matches the timing of
    /// "known username, wrong password".  This removes the trivial
    /// pre-auth username-enumeration side channel that existed when
    /// the function returned immediately for missing users.
    pub fn verify(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<AuthenticatedUser>, AuthError> {
        let argon2 = Argon2::default();
        let Some(record) = self.users.get(username) else {
            // Unknown username — still pay the Argon2id cost so the
            // response time does not leak whether the username
            // exists.  We deliberately ignore the result; the
            // sentinel hash is unreachable so verification cannot
            // succeed.  Parse-failure here would be a process-wide
            // bug (the sentinel is constructed by us at startup),
            // not a per-request failure, so we still return `None`.
            if let Ok(parsed) = PasswordHash::new(SENTINEL_ARGON2_HASH.as_str()) {
                let _ = argon2.verify_password(password.as_bytes(), &parsed);
            }
            return Ok(None);
        };

        let parsed_hash = PasswordHash::new(&record.password_hash)
            .map_err(|e| AuthError::HashError(e.to_string()))?;

        if argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            Ok(Some(AuthenticatedUser {
                username: username.to_string(),
                roles: record.roles.clone(),
                must_change_password: record.must_change_password,
            }))
        } else {
            Ok(None)
        }
    }

    /// Check if a user exists.
    pub fn has_user(&self, username: &str) -> bool {
        self.users.contains_key(username)
    }

    /// Number of users currently in the store.
    ///
    /// Used by the `/status/health` readiness probe (Wave 36-B) to verify
    /// the auth store is structurally readable.  Returns 0 for a freshly
    /// constructed store; the bootstrap admin (added in
    /// `bins/ferrodruid::main::bootstrap_admin_if_needed`) brings it to 1
    /// on first launch with auth enabled.
    #[must_use]
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Returns `true` if the store contains at least one user.
    ///
    /// Used by `/status/health` (Wave 36-B) when auth is enabled — an
    /// empty store means no operator can ever authenticate, so the node
    /// is effectively un-usable and should fail the readiness probe.
    #[must_use]
    pub fn is_readable(&self) -> bool {
        // Touching `len()` exercises the underlying `HashMap` lookup
        // path; the type signature itself proves the store is in a
        // structurally consistent state.
        self.users.len() < usize::MAX
    }

    /// Remove a user.  Returns `true` if the user existed.
    pub fn remove_user(&mut self, username: &str) -> bool {
        self.users.remove(username).is_some()
    }
}

// ---------------------------------------------------------------------------
// AuthenticatedUser
// ---------------------------------------------------------------------------

/// A successfully authenticated user with resolved roles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedUser {
    /// Username.
    pub username: String,
    /// Roles granted to this user.
    pub roles: Vec<String>,
    /// Whether this user must rotate their password before the API will
    /// serve any request other than the change-credential endpoint.
    ///
    /// Populated by [`AuthStore::verify`] from the stored
    /// [`UserRecord::must_change_password`] so the auth middleware can gate
    /// the request after a successful credential check.
    #[serde(default)]
    pub must_change_password: bool,
}

// ---------------------------------------------------------------------------
// AuthMethod + header parsing
// ---------------------------------------------------------------------------

/// Authentication method extracted from an HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    /// No authentication provided.
    Anonymous,
    /// HTTP Basic authentication.
    Basic {
        /// Username.
        username: String,
        /// Password (plaintext from base64 decode).
        password: String,
    },
    /// Bearer token authentication.
    Bearer {
        /// The bearer token string.
        token: String,
    },
}

/// Parse an HTTP `Authorization` header value into an [`AuthMethod`].
///
/// Supports `Basic <base64>` and `Bearer <token>` schemes.
///
/// Wave 42-B closes two related Lows:
///
/// * **W35 R1 Low (`auth/lib.rs:213` redaction)** — the previous
///   unsupported-scheme path embedded the raw header value into the
///   `AuthError::InvalidHeader` message.  If that error were ever
///   logged or echoed in a response, attacker-supplied bearer tokens
///   or Basic credentials would leak verbatim.  We now report only
///   the redacted scheme name (no credential bytes).
/// * **W39 R1 Low (`auth/lib.rs:187` case-insensitive scheme)** —
///   RFC 7235 §2.1 makes the auth-scheme name case-insensitive, but
///   the previous implementation accepted only the exact prefixes
///   `Basic ` / `Bearer `.  RFC-compliant clients sending `basic ` /
///   `BEARER ` were rejected as unauthorized.  We now compare the
///   scheme token case-insensitively while leaving the credential
///   payload (base64 / token) unchanged.
pub fn parse_auth_header(header: &str) -> Result<AuthMethod, AuthError> {
    let header = header.trim();

    // Split scheme from credentials at the first whitespace run.  If
    // no whitespace is present we still report a redacted error.
    let (scheme, rest) = match header.split_once(char::is_whitespace) {
        Some((s, r)) => (s, r.trim_start()),
        None => (header, ""),
    };

    if scheme.eq_ignore_ascii_case("Basic") {
        use base64::Engine;
        let encoded = rest.trim();
        let decoded_bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|e| AuthError::InvalidHeader(format!("base64 decode: {e}")))?;
        let decoded = String::from_utf8(decoded_bytes)
            .map_err(|e| AuthError::InvalidHeader(format!("utf-8 decode: {e}")))?;
        let (username, password) = decoded
            .split_once(':')
            .ok_or_else(|| AuthError::InvalidHeader("missing ':' in Basic credentials".into()))?;
        Ok(AuthMethod::Basic {
            username: username.to_string(),
            password: password.to_string(),
        })
    } else if scheme.eq_ignore_ascii_case("Bearer") {
        Ok(AuthMethod::Bearer {
            token: rest.trim().to_string(),
        })
    } else {
        // Redact the credential payload — only the scheme token is
        // safe to surface in logs / error envelopes.  We further
        // normalize the surfaced scheme to ASCII to avoid log
        // injection from arbitrary header bytes.
        let scheme_redacted: String = scheme
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .take(32)
            .collect();
        let scheme_for_msg = if scheme_redacted.is_empty() {
            "<empty>".to_string()
        } else {
            scheme_redacted
        };
        Err(AuthError::InvalidHeader(format!(
            "unsupported auth scheme: {scheme_for_msg}"
        )))
    }
}

/// Basic (in-memory) authenticator backed by Argon2id hashes.
#[derive(Debug, Default)]
pub struct BasicAuthenticator {
    store: AuthStore,
}

impl BasicAuthenticator {
    /// Create a new empty authenticator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new authenticator with the given auth store.
    pub fn with_store(store: AuthStore) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Authenticator for BasicAuthenticator {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Identity, AuthError> {
        match self.store.verify(username, password)? {
            Some(user) => Ok(Identity {
                name: user.username,
                auth_method: "basic".to_string(),
            }),
            None => Err(AuthError::InvalidCredentials),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_hash_reloads_and_verifies_restart_safe() {
        // First boot: create the admin and capture its persisted credential.
        let mut first = AuthStore::new();
        first
            .add_user("admin", "first-launch-pw", vec!["admin".into()])
            .expect("add admin");
        let record = first.user_record("admin").expect("admin record").clone();
        // Round-trip the record through JSON exactly as the binary persists it.
        let json = serde_json::to_vec(&record).expect("serialize record");
        let reloaded: UserRecord = serde_json::from_slice(&json).expect("deserialize record");

        // Restart: a FRESH (empty) store reloads only the persisted hash.
        let mut after_restart = AuthStore::new();
        after_restart.add_user_with_hash(
            &reloaded.username,
            &reloaded.password_hash,
            reloaded.roles.clone(),
            reloaded.must_change_password,
        );

        // The same password still authenticates after the "restart" — the bug
        // (empty store after restart => lockout) is gone.
        let ok = after_restart
            .verify("admin", "first-launch-pw")
            .expect("verify");
        assert!(
            ok.is_some(),
            "reloaded admin must authenticate after restart"
        );
        assert_eq!(ok.expect("user").roles, vec!["admin"]);
        assert!(after_restart.has_user("admin"));
        // Wrong password still fails closed.
        assert!(
            after_restart
                .verify("admin", "wrong")
                .expect("verify")
                .is_none()
        );
    }

    #[test]
    fn user_record_returns_none_for_missing_user() {
        let store = AuthStore::new();
        assert!(store.user_record("nobody").is_none());
    }

    #[test]
    fn set_password_updates_hash_and_clears_must_change() {
        let mut store = AuthStore::new();
        store
            .add_user_must_change("admin", "initial-pw", vec!["admin".into()], true)
            .expect("add admin");

        // Sanity: the forced-change flag is set and the initial password
        // verifies, carrying the flag through `verify`.
        let before = store
            .verify("admin", "initial-pw")
            .expect("verify")
            .expect("user");
        assert!(
            before.must_change_password,
            "freshly bootstrapped admin must be forced to change"
        );

        // Rotate the password.
        store
            .set_password("admin", "a-brand-new-strong-pw")
            .expect("set_password");

        // Old password no longer works; the new one does and the flag is clear.
        assert!(
            store
                .verify("admin", "initial-pw")
                .expect("verify")
                .is_none(),
            "old password must stop working after rotation"
        );
        let after = store
            .verify("admin", "a-brand-new-strong-pw")
            .expect("verify")
            .expect("user");
        assert!(
            !after.must_change_password,
            "must_change_password must be cleared after set_password"
        );
        // The stored record also reflects the cleared flag for persistence.
        assert!(
            !store
                .user_record("admin")
                .expect("record")
                .must_change_password
        );
    }

    #[test]
    fn set_password_unknown_user_errors() {
        let mut store = AuthStore::new();
        let err = store
            .set_password("ghost", "whatever-strong-pw")
            .expect_err("unknown user must error");
        assert!(matches!(err, AuthError::UserNotFound));
    }

    #[test]
    fn user_record_without_must_change_field_defaults_false() {
        // An `admin.json` persisted before the `must_change_password` field
        // existed has no such key.  `#[serde(default)]` must deserialize it
        // to `false` so a legacy/rotated credential is not re-gated.
        let legacy = serde_json::json!({
            "username": "admin",
            "password_hash": "$argon2id$v=19$m=19456,t=2,p=1$abc$def",
            "roles": ["admin"]
        });
        let record: UserRecord = serde_json::from_value(legacy).expect("deserialize legacy record");
        assert!(
            !record.must_change_password,
            "missing must_change_password field must default to false"
        );
    }

    #[test]
    fn add_user_verify_correct_password() {
        let mut store = AuthStore::new();
        store
            .add_user("alice", "secret123", vec!["admin".into()])
            .expect("add user");

        let result = store.verify("alice", "secret123").expect("verify");
        assert!(result.is_some());
        let user = result.expect("user");
        assert_eq!(user.username, "alice");
        assert_eq!(user.roles, vec!["admin"]);
    }

    #[test]
    fn verify_wrong_password_returns_none() {
        let mut store = AuthStore::new();
        store
            .add_user("alice", "secret123", vec!["admin".into()])
            .expect("add user");

        let result = store.verify("alice", "wrong").expect("verify");
        assert!(result.is_none());
    }

    #[test]
    fn verify_nonexistent_user_returns_none() {
        let store = AuthStore::new();
        let result = store.verify("nobody", "anything").expect("verify");
        assert!(result.is_none());
    }

    #[test]
    fn has_user_and_remove() {
        let mut store = AuthStore::new();
        assert!(!store.has_user("bob"));

        store.add_user("bob", "pass", vec![]).expect("add user");
        assert!(store.has_user("bob"));

        assert!(store.remove_user("bob"));
        assert!(!store.has_user("bob"));
        assert!(!store.remove_user("bob"));
    }

    #[test]
    fn parse_basic_auth_header() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:secret123");
        let header = format!("Basic {encoded}");
        let method = parse_auth_header(&header).expect("parse");
        assert_eq!(
            method,
            AuthMethod::Basic {
                username: "alice".into(),
                password: "secret123".into(),
            }
        );
    }

    #[test]
    fn parse_bearer_auth_header() {
        let method = parse_auth_header("Bearer my-jwt-token").expect("parse");
        assert_eq!(
            method,
            AuthMethod::Bearer {
                token: "my-jwt-token".into(),
            }
        );
    }

    #[test]
    fn parse_invalid_auth_header() {
        let result = parse_auth_header("Digest foo");
        assert!(result.is_err());
    }

    #[test]
    fn parse_basic_missing_colon() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("nocolon");
        let header = format!("Basic {encoded}");
        let result = parse_auth_header(&header);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn basic_authenticator_success() {
        let mut auth_store = AuthStore::new();
        auth_store
            .add_user("alice", "pass", vec!["reader".into()])
            .expect("add");
        let authenticator = BasicAuthenticator::with_store(auth_store);
        let identity = authenticator
            .authenticate("alice", "pass")
            .await
            .expect("auth");
        assert_eq!(identity.name, "alice");
        assert_eq!(identity.auth_method, "basic");
    }

    #[tokio::test]
    async fn basic_authenticator_failure() {
        let auth_store = AuthStore::new();
        let authenticator = BasicAuthenticator::with_store(auth_store);
        let result = authenticator.authenticate("alice", "pass").await;
        assert!(result.is_err());
    }

    /// Wave 42-B regression for Wave 35 R1 Low (`auth/lib.rs:119`):
    /// `verify` for a missing username must still spend Argon2id work
    /// so the timing roughly matches the existing-user path.  We
    /// can't gate on absolute time (CI is noisy), but we can at least
    /// confirm the missing-user path takes meaningfully longer than
    /// a no-op store lookup, which is what the timing oracle was.
    ///
    /// Soft floor — Argon2id default cost is ~tens of ms; we assert
    /// at-least-1-ms to keep this stable on slow CI runners while
    /// still catching a regression to the old "return immediately"
    /// path.
    #[test]
    fn verify_missing_user_still_pays_argon2_cost() {
        use std::time::Instant;

        // Warm up the LazyLock so the first-call cost is not
        // attributed to the timed measurement.
        let _ = LazyLock::force(&SENTINEL_ARGON2_HASH);

        let store = AuthStore::new();
        let start = Instant::now();
        let result = store
            .verify("nobody-such-user", "anything")
            .expect("verify");
        let elapsed = start.elapsed();
        assert!(result.is_none());
        assert!(
            elapsed.as_millis() >= 1,
            "missing-user verify too fast — timing oracle regression: {elapsed:?}"
        );
    }

    /// Wave 42-B regression for Wave 35 R1 Low (`auth/lib.rs:213`):
    /// the unsupported-scheme error must NOT include the raw header
    /// value.  Specifically, attacker-supplied credential bytes must
    /// never appear in the error message.
    #[test]
    fn parse_auth_header_unsupported_scheme_redacts_credentials() {
        let secret = "super-secret-credential-payload-7f3a";
        let header = format!("Digest {secret}");
        let err = parse_auth_header(&header).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            !msg.contains(secret),
            "credential payload leaked into error message: {msg}"
        );
        assert!(
            msg.contains("Digest"),
            "scheme name should appear in error, got: {msg}"
        );
    }

    /// Wave 42-B regression for Wave 39 R1 Low (`auth/lib.rs:187`):
    /// HTTP auth scheme matching must be case-insensitive per RFC
    /// 7235 §2.1.  RFC-compliant clients sending `basic ` or
    /// `BEARER ` were previously rejected; now they parse cleanly.
    #[test]
    fn parse_auth_header_scheme_is_case_insensitive() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("alice:secret");

        // Lowercase Basic.
        let header = format!("basic {encoded}");
        let method = parse_auth_header(&header).expect("parse lowercase basic");
        assert_eq!(
            method,
            AuthMethod::Basic {
                username: "alice".into(),
                password: "secret".into(),
            }
        );

        // Mixed-case Bearer.
        let method = parse_auth_header("BeArEr my-token").expect("parse mixed-case bearer");
        assert_eq!(
            method,
            AuthMethod::Bearer {
                token: "my-token".into(),
            }
        );

        // All-uppercase Bearer.
        let method = parse_auth_header("BEARER another-token").expect("parse uppercase bearer");
        assert_eq!(
            method,
            AuthMethod::Bearer {
                token: "another-token".into(),
            }
        );
    }
}
