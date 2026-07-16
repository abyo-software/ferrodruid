// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! WASM plugin runtime for FerroDruid (CL-6 / W1-F closure).
//!
//! This crate hosts third-party Rust-compiled-to-WebAssembly modules that
//! extend FerroDruid with custom aggregators, input sources, and
//! authenticators without requiring a Java-compatible Druid extension
//! SPI.  It uses Wasmtime as the execution engine and enforces three
//! independent safety boundaries on every loaded module:
//!
//! 1. **Deny-by-default capabilities** — host imports (`ferro_log`,
//!    `ferro_clock_now_ms`, `ferro_http_get`, …) are only linked when
//!    the host explicitly grants the matching [`PluginCapabilities`]
//!    flag.  A plugin that tries to import an ungranted host function
//!    fails at link time, not at runtime.
//! 2. **Fuel-bounded execution** — every call is budgeted in Wasmtime
//!    fuel units; out-of-gas trips trap and propagate as a typed
//!    [`PluginError::OutOfFuel`].
//! 3. **Memory cap** — the per-store
//!    [`wasmtime::ResourceLimiter`] hard-caps linear-memory growth.
//!
//! Plus a module-level **SHA-256 signature manifest** is verified
//! before a module is loaded into the engine.
//!
//! The companion plugin set under `plugins/` provides three reference
//! implementations:
//!
//! * `running-stddev` — Welford online standard deviation aggregator
//! * `http-jsonl` — HTTP-JSONL input source (capability-gated network)
//! * `hmac-bearer` — HMAC-SHA-256 bearer token authenticator (no caps)
//!
//! See `tests/end_to_end.rs` for the load → call → unload → reload
//! leak audit.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod capabilities;
mod error;
mod host;
mod manifest;
mod module;
mod runtime;

pub use capabilities::PluginCapabilities;
pub use error::PluginError;
pub use host::HostState;
pub use manifest::{ModuleManifest, expected_sha256_hex};
pub use module::{LoadedModule, PluginInstance};
pub use runtime::{EngineStats, PluginRuntime, RuntimeConfig};

/// Default per-call fuel budget (≈ tens of millions of WASM operations).
pub const DEFAULT_FUEL_BUDGET: u64 = 50_000_000;

/// Default per-instance memory cap (16 MiB).
pub const DEFAULT_MEMORY_CAP_BYTES: usize = 16 * 1024 * 1024;

/// Plugin ABI version negotiated between the host and a loaded module.
///
/// Plugins export this as a `plugin_abi_version() -> i32` function; the
/// runtime refuses to load modules that report a different version.
/// Bumping this number is a breaking change for every plugin in the
/// ecosystem and must accompany a migration note in
/// `docs/known-limitations.md`.
pub const PLUGIN_ABI_VERSION: u32 = 1;
