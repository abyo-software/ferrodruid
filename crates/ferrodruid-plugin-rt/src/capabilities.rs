// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Per-instance host capability grants.
//!
//! The runtime starts with every capability denied.  A host that
//! wants to grant a plugin (e.g.) network access must construct a
//! [`PluginCapabilities`] with `net = true` and pass it to
//! [`crate::PluginRuntime::instantiate`].  The corresponding
//! `ferro_*` host imports are only linked when the matching flag is
//! true; a plugin that imports an ungranted function fails at link
//! time with [`crate::PluginError::CapabilityDenied`] (the runtime
//! pre-checks the module's import section before instantiation so the
//! operator gets a precise error message rather than the generic
//! "unknown import" from Wasmtime's linker).

use serde::{Deserialize, Serialize};

/// Set of host capabilities granted to a single plugin instance.
///
/// Construct via [`PluginCapabilities::deny_all`] (the recommended
/// default) and call the builder-style `with_*` methods to opt in to
/// individual capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginCapabilities {
    /// Allow the plugin to import `ferro_log(level, ptr, len)`.
    ///
    /// Writes the bytes at the supplied pointer through the host's
    /// `tracing` subscriber at the requested severity level.
    pub log: bool,

    /// Allow the plugin to import `ferro_clock_now_ms()`.
    ///
    /// Returns wall-clock milliseconds since the Unix epoch.
    /// Denied by default because access to a real clock is a
    /// non-trivial side-channel for sandboxed code (it enables
    /// timing oracles).
    pub clock: bool,

    /// Allow the plugin to import `ferro_random_u64()`.
    ///
    /// Returns 64 bits of OS-provided cryptographic randomness.
    pub random: bool,

    /// Allow the plugin to import `ferro_http_get(url_ptr, url_len, out_ptr, out_cap) -> i32`.
    ///
    /// Performs a blocking HTTP GET from the host on behalf of the
    /// plugin and copies the response body into the plugin's linear
    /// memory.  Available only when the `net-capability` crate
    /// feature is enabled (off by default — operators must opt in
    /// both via the crate feature AND this per-instance flag).
    pub net: bool,
}

impl Default for PluginCapabilities {
    fn default() -> Self {
        Self::deny_all()
    }
}

impl PluginCapabilities {
    /// All capabilities denied.  This is the recommended starting
    /// point: explicitly opt-in by chaining `.with_log()`, etc.
    #[must_use]
    pub const fn deny_all() -> Self {
        Self {
            log: false,
            clock: false,
            random: false,
            net: false,
        }
    }

    /// All capabilities granted.  Useful only in tests.  Production
    /// hosts should never pass this — the whole point of the runtime
    /// is to deny by default.
    #[must_use]
    pub const fn allow_all() -> Self {
        Self {
            log: true,
            clock: true,
            random: true,
            net: true,
        }
    }

    /// Grant the `log` capability.
    #[must_use]
    pub const fn with_log(mut self) -> Self {
        self.log = true;
        self
    }

    /// Grant the `clock` capability.
    #[must_use]
    pub const fn with_clock(mut self) -> Self {
        self.clock = true;
        self
    }

    /// Grant the `random` capability.
    #[must_use]
    pub const fn with_random(mut self) -> Self {
        self.random = true;
        self
    }

    /// Grant the `net` capability.  No-op unless the `net-capability`
    /// crate feature is enabled; the runtime will still refuse to
    /// link `ferro_http_get` in that case so the plugin fails at
    /// link time rather than silently being denied at call time.
    #[must_use]
    pub const fn with_net(mut self) -> Self {
        self.net = true;
        self
    }

    /// True iff at least one capability is granted.
    #[must_use]
    pub const fn any(&self) -> bool {
        self.log || self.clock || self.random || self.net
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_denies_all() {
        let caps = PluginCapabilities::default();
        assert!(!caps.log);
        assert!(!caps.clock);
        assert!(!caps.random);
        assert!(!caps.net);
        assert!(!caps.any());
    }

    #[test]
    fn deny_all_explicit_matches_default() {
        assert_eq!(
            PluginCapabilities::default(),
            PluginCapabilities::deny_all()
        );
    }

    #[test]
    fn builders_compose() {
        let caps = PluginCapabilities::deny_all().with_log().with_clock();
        assert!(caps.log);
        assert!(caps.clock);
        assert!(!caps.random);
        assert!(!caps.net);
        assert!(caps.any());
    }

    #[test]
    fn allow_all_grants_every_flag() {
        let caps = PluginCapabilities::allow_all();
        assert!(caps.log);
        assert!(caps.clock);
        assert!(caps.random);
        assert!(caps.net);
    }
}
