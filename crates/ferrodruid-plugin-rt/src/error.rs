// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Typed errors surfaced by the WASM plugin runtime.
//!
//! Every failure mode visible to the host is one of these variants —
//! we deliberately do not leak `anyhow::Error` from public APIs so
//! callers can `match` on the failure category (gas vs link vs
//! signature) and apply the appropriate operator policy (kill plugin,
//! quarantine module, etc.).

use thiserror::Error;

/// Errors emitted by the WASM plugin runtime.
#[derive(Debug, Error)]
pub enum PluginError {
    /// The module's actual SHA-256 did not match the expected hash in
    /// the manifest.  Indicates a tampered or corrupted artifact.
    #[error(
        "module signature mismatch: expected sha256={expected}, observed sha256={observed} \
         (module bytes were modified after manifest was signed)"
    )]
    SignatureMismatch {
        /// Expected SHA-256 (hex) from the manifest.
        expected: String,
        /// Observed SHA-256 (hex) over the loaded module bytes.
        observed: String,
    },

    /// The module was not valid WebAssembly or could not be compiled
    /// by Wasmtime.
    #[error("module compile failed: {0}")]
    Compile(String),

    /// The module's exported `plugin_abi_version` global did not match
    /// the host's [`crate::PLUGIN_ABI_VERSION`].
    #[error(
        "plugin ABI version mismatch: module reports {module}, host requires {host} \
         (rebuild plugin against the current host crate)"
    )]
    AbiMismatch {
        /// ABI version reported by the module's exported global.
        module: u32,
        /// ABI version the host requires.
        host: u32,
    },

    /// A required export was missing from the module.
    #[error("module missing required export `{0}`")]
    MissingExport(&'static str),

    /// The module imported a host function whose capability was not
    /// granted by the host (e.g. `ferro_http_get` without
    /// [`crate::PluginCapabilities::net`]).
    #[error("module imports host function `{name}` but capability `{capability}` was not granted")]
    CapabilityDenied {
        /// The host-function name the module tried to import.
        name: &'static str,
        /// The capability flag the operator would need to set.
        capability: &'static str,
    },

    /// Wasmtime linker rejected the instantiation (typically an
    /// imported function the host did not register at all).  Distinct
    /// from [`PluginError::CapabilityDenied`] because the latter is
    /// raised proactively before instantiation when we recognize the
    /// import name; this variant covers names we don't recognize.
    #[error("module linker error: {0}")]
    LinkerRejected(String),

    /// The plugin call exhausted its fuel budget.
    #[error("plugin call exceeded fuel budget of {budget} units")]
    OutOfFuel {
        /// Fuel budget that was supplied to the call.
        budget: u64,
    },

    /// The plugin tried to grow linear memory past the configured
    /// memory cap.
    #[error("plugin exceeded memory cap of {cap_bytes} bytes")]
    MemoryCapExceeded {
        /// The cap that was tripped (bytes).
        cap_bytes: usize,
    },

    /// The plugin trapped (panic, divide-by-zero, unreachable, …).
    #[error("plugin trap: {0}")]
    Trap(String),

    /// A guest pointer pair (ptr, len) addressed memory outside the
    /// plugin's linear memory.  Indicates a buggy or hostile plugin;
    /// we always return this rather than reading wild bytes.
    #[error("guest pointer out of bounds: ptr=0x{ptr:x} len={len} memory_size={mem_size}")]
    PointerOutOfBounds {
        /// Guest pointer (linear memory offset).
        ptr: u32,
        /// Length the guest claimed.
        len: u32,
        /// Current size of the guest's linear memory in bytes.
        mem_size: usize,
    },

    /// A guest call returned a status code the host could not
    /// interpret.  Includes the plugin name and raw code for
    /// debugging.
    #[error("plugin `{plugin}` returned unrecognised status code {code}")]
    InvalidStatus {
        /// Plugin name.
        plugin: String,
        /// Raw status code.
        code: i32,
    },

    /// I/O error while loading the module bytes from disk.
    #[error("module load i/o: {0}")]
    Io(#[from] std::io::Error),

    /// A host capability invocation failed (e.g. the HTTP GET host
    /// import returned a network error).
    #[error("host capability error: {0}")]
    HostCapability(String),

    /// Catch-all for Wasmtime errors that don't map to a more
    /// specific variant.
    #[error("wasmtime: {0}")]
    Wasmtime(String),
}

impl PluginError {
    /// Wrap a Wasmtime error, classifying common cases (trap, OOG,
    /// memory cap) into typed variants where possible.
    ///
    /// We accept the error by reference / borrow shape (Wasmtime
    /// surfaces `wasmtime::Error`, an alias for `anyhow::Error`) and
    /// walk the chain via the public `std::error::Error::source()`
    /// trail so we don't have to pull in `anyhow` directly as a
    /// dependency of this crate.
    #[must_use]
    pub fn from_wasmtime(err: wasmtime::Error, fuel_budget: u64, memory_cap: usize) -> Self {
        // Wasmtime traps include the original error chain.  We look
        // for the canonical strings the engine emits.  This is the
        // documented way to classify traps from a stable wasmtime
        // release (the typed `wasmtime::Trap` enum only covers a
        // handful of well-known codes; OOG and the resource limiter's
        // memory-cap trip are surfaced via the chained error).  We
        // walk the chain via the public `Error::source()` API so we
        // don't depend on `anyhow` directly here.
        let mut chain: Vec<String> = vec![err.to_string()];
        let err_ref: &dyn std::error::Error = err.as_ref();
        let mut next = err_ref.source();
        while let Some(s) = next {
            chain.push(s.to_string());
            next = s.source();
        }
        let joined = chain.join(" || ");

        if joined.contains("all fuel consumed") || joined.contains("out of fuel") {
            return Self::OutOfFuel {
                budget: fuel_budget,
            };
        }
        if joined.contains("exceeds the limit") || joined.contains("memory limit") {
            return Self::MemoryCapExceeded {
                cap_bytes: memory_cap,
            };
        }
        // Bare traps like "wasm trap: unreachable" land here.
        if joined.contains("wasm trap") {
            return Self::Trap(joined);
        }
        Self::Wasmtime(joined)
    }
}
