// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Top-level runtime facade.
//!
//! A single [`PluginRuntime`] owns one Wasmtime [`Engine`] and the
//! catalog of [`LoadedModule`]s that have been verified and compiled
//! against it.  Hosts call [`PluginRuntime::load`] to register a
//! module and [`PluginRuntime::instantiate`] to spin up a fresh
//! [`PluginInstance`] with per-instance capability + fuel + memory
//! budgets.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use wasmtime::{Config, Engine};

use crate::manifest::ModuleManifest;
use crate::module::{KNOWN_HOST_IMPORTS, LoadedModule, PluginInstance};
use crate::{DEFAULT_FUEL_BUDGET, DEFAULT_MEMORY_CAP_BYTES, PluginCapabilities, PluginError};

/// Per-instance runtime knobs.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Wasmtime fuel granted on every call.
    pub fuel_budget: u64,
    /// Hard cap on linear memory growth.
    pub memory_cap_bytes: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            fuel_budget: DEFAULT_FUEL_BUDGET,
            memory_cap_bytes: DEFAULT_MEMORY_CAP_BYTES,
        }
    }
}

/// Lightweight snapshot of the engine + loaded modules — used by the
/// leak audit test to confirm load/unload cycles don't accumulate
/// module objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineStats {
    /// Number of modules currently loaded into the runtime catalog.
    pub loaded_modules: usize,
}

/// The host-facing WASM plugin runtime.
///
/// One [`PluginRuntime`] is enough for an entire FerroDruid process;
/// concurrent instantiations from multiple threads are safe.
pub struct PluginRuntime {
    engine: Engine,
    modules: HashMap<String, LoadedModule>,
}

impl std::fmt::Debug for PluginRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginRuntime")
            .field("module_names", &self.modules.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl PluginRuntime {
    /// Construct a fresh runtime.  The Wasmtime engine is configured
    /// with consumption-style fuel metering (required for the
    /// per-call fuel budget) and a single-threaded compiler so the
    /// build doesn't pull in `parking_lot`'s spin loops at startup
    /// inside a unit test.
    pub fn new() -> Result<Self, PluginError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        // Cranelift's optimisation level — fast compile rather than
        // fast runtime, since plugins are typically small.
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        let engine = Engine::new(&config).map_err(|e| PluginError::Wasmtime(e.to_string()))?;
        Ok(Self {
            engine,
            modules: HashMap::new(),
        })
    }

    /// Load a module from its raw bytes + manifest.
    ///
    /// Verifies the manifest hash, compiles the module, and validates
    /// the import section — every imported function must come from
    /// the `ferro` module and be one of [`KNOWN_HOST_IMPORTS`].  An
    /// import outside that set is a hard error (a plugin asking for
    /// `wasi_snapshot_preview1::fd_write` will be rejected at load
    /// time, not at link time, because plugins should be built
    /// against the host SPI rather than wasi).
    pub fn load(
        &mut self,
        manifest: ModuleManifest,
        bytes: &[u8],
    ) -> Result<&LoadedModule, PluginError> {
        manifest.verify(bytes)?;
        let module = wasmtime::Module::new(&self.engine, bytes)
            .map_err(|e| PluginError::Compile(e.to_string()))?;

        let mut required: HashSet<&'static str> = HashSet::new();
        for imp in module.imports() {
            let module_name = imp.module();
            let item_name = imp.name();
            if module_name != "ferro" {
                return Err(PluginError::Compile(format!(
                    "plugin imports from unknown module `{module_name}`; \
                     only `ferro::*` host imports are permitted"
                )));
            }
            // Find the matching static name.
            let entry = KNOWN_HOST_IMPORTS
                .iter()
                .find(|(name, _)| *name == item_name);
            match entry {
                Some((name, _cap)) => {
                    required.insert(name);
                }
                None => {
                    return Err(PluginError::Compile(format!(
                        "plugin imports unknown host function `ferro::{item_name}`"
                    )));
                }
            }
        }

        let name = manifest.name.clone();
        let loaded = LoadedModule {
            manifest: Arc::new(manifest),
            module,
            required_imports: Arc::new(required),
        };
        self.modules.insert(name.clone(), loaded);
        Ok(self.modules.get(&name).expect("just inserted"))
    }

    /// Load a module from disk by path + manifest.
    pub fn load_from_path<P: AsRef<Path>>(
        &mut self,
        manifest: ModuleManifest,
        path: P,
    ) -> Result<&LoadedModule, PluginError> {
        let bytes = std::fs::read(path.as_ref())?;
        self.load(manifest, &bytes)
    }

    /// Look up a previously-loaded module by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&LoadedModule> {
        self.modules.get(name)
    }

    /// Unload a module by name.  Returns `true` if the module was
    /// present.  Any [`PluginInstance`] previously created from this
    /// module stays alive until its own `Drop` — Wasmtime
    /// internally `Arc`-shares compiled code with live instances.
    pub fn unload(&mut self, name: &str) -> bool {
        self.modules.remove(name).is_some()
    }

    /// Instantiate a previously-loaded module with the given
    /// capabilities + config.  This is the per-call entry point —
    /// instantiation is cheap (small allocations + import linkage),
    /// and dropping the returned [`PluginInstance`] frees every byte
    /// the plugin allocated.
    pub fn instantiate(
        &self,
        name: &str,
        caps: PluginCapabilities,
        config: RuntimeConfig,
    ) -> Result<PluginInstance, PluginError> {
        let loaded = self
            .modules
            .get(name)
            .ok_or_else(|| PluginError::HostCapability(format!("plugin `{name}` not loaded")))?;
        PluginInstance::new(&self.engine, loaded, caps, &config)
    }

    /// Engine-wide stats.
    #[must_use]
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            loaded_modules: self.modules.len(),
        }
    }

    /// Borrow the underlying Wasmtime engine.  Exposed so a host
    /// can build secondary stores for advanced cases; most callers
    /// should never need this.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal WAT module that exports an i32 const global named
    /// `plugin_abi_version` and the required `alloc`/`dealloc`/
    /// `memory` symbols, plus a single `add` function.  Used as a
    /// smoke check for the runtime without depending on any of the
    /// reference plugins (which live as separate crates).
    const MINIMAL_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "plugin_abi_version") (result i32) i32.const 1)
          (func (export "alloc") (param i32) (result i32) i32.const 16)
          (func (export "dealloc") (param i32) (param i32))
          (func (export "add") (param i32) (param i32) (result i32)
            local.get 0 local.get 1 i32.add))
    "#;

    fn minimal_module_bytes() -> Vec<u8> {
        wat::parse_str(MINIMAL_WAT).expect("wat parses")
    }

    #[test]
    fn load_minimal_module_succeeds() {
        let mut rt = PluginRuntime::new().expect("runtime");
        let bytes = minimal_module_bytes();
        let manifest = ModuleManifest::from_bytes("minimal", "1.0.0", &bytes);
        rt.load(manifest, &bytes).expect("load");
        assert_eq!(rt.stats().loaded_modules, 1);
        assert!(rt.get("minimal").is_some());
    }

    #[test]
    fn load_rejects_tampered_bytes() {
        let mut rt = PluginRuntime::new().expect("runtime");
        let bytes = minimal_module_bytes();
        let mut manifest = ModuleManifest::from_bytes("minimal", "1.0.0", &bytes);
        manifest.sha256_hex = "ff".repeat(32);
        let err = rt.load(manifest, &bytes).expect_err("must reject");
        assert!(matches!(err, PluginError::SignatureMismatch { .. }));
        assert_eq!(rt.stats().loaded_modules, 0);
    }

    #[test]
    fn unload_releases_module() {
        let mut rt = PluginRuntime::new().expect("runtime");
        let bytes = minimal_module_bytes();
        let manifest = ModuleManifest::from_bytes("minimal", "1.0.0", &bytes);
        rt.load(manifest, &bytes).expect("load");
        assert!(rt.unload("minimal"));
        assert_eq!(rt.stats().loaded_modules, 0);
        assert!(!rt.unload("minimal"));
    }

    #[test]
    fn instantiate_minimal_runs_typed_call() {
        let mut rt = PluginRuntime::new().expect("runtime");
        let bytes = minimal_module_bytes();
        let manifest = ModuleManifest::from_bytes("minimal", "1.0.0", &bytes);
        rt.load(manifest, &bytes).expect("load");
        let mut inst = rt
            .instantiate(
                "minimal",
                PluginCapabilities::deny_all(),
                RuntimeConfig::default(),
            )
            .expect("instantiate");
        let sum: i32 = inst.call("add", (2_i32, 40_i32)).expect("call add");
        assert_eq!(sum, 42);
        // A minimal module exporting only an i32.const expr in its
        // global initialiser consumes ~zero fuel; ensure no
        // catastrophic overspend.
        assert!(inst.fuel_consumed() < 1_000);
    }

    #[test]
    fn instantiate_rejects_unknown_import() {
        let wat = r#"
            (module
              (import "wasi_snapshot_preview1" "fd_write"
                (func $fd_write (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (func (export "plugin_abi_version") (result i32) i32.const 1)
              (func (export "alloc") (param i32) (result i32) i32.const 16)
              (func (export "dealloc") (param i32) (param i32)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        let manifest = ModuleManifest::from_bytes("bad", "1.0.0", &bytes);
        let mut rt = PluginRuntime::new().expect("runtime");
        let err = rt.load(manifest, &bytes).expect_err("must reject wasi");
        match err {
            PluginError::Compile(msg) => {
                assert!(
                    msg.contains("wasi_snapshot_preview1"),
                    "error must mention rejected module name: {msg}"
                );
            }
            other => panic!("expected Compile, got {other:?}"),
        }
    }

    #[test]
    fn instantiate_denies_ungranted_capability() {
        let wat = r#"
            (module
              (import "ferro" "ferro_clock_now_ms" (func $clock (result i64)))
              (memory (export "memory") 1)
              (func (export "plugin_abi_version") (result i32) i32.const 1)
              (func (export "alloc") (param i32) (result i32) i32.const 16)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "now") (result i64) call $clock))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        let manifest = ModuleManifest::from_bytes("needs-clock", "1.0.0", &bytes);
        let mut rt = PluginRuntime::new().expect("runtime");
        rt.load(manifest, &bytes).expect("load");

        // Default caps deny everything.
        let err = rt
            .instantiate(
                "needs-clock",
                PluginCapabilities::deny_all(),
                RuntimeConfig::default(),
            )
            .expect_err("must deny ungranted cap");
        match err {
            PluginError::CapabilityDenied { name, capability } => {
                assert_eq!(name, "ferro_clock_now_ms");
                assert_eq!(capability, "clock");
            }
            other => panic!("expected CapabilityDenied, got {other:?}"),
        }

        // Granting the cap lets the same module instantiate cleanly.
        let mut inst = rt
            .instantiate(
                "needs-clock",
                PluginCapabilities::deny_all().with_clock(),
                RuntimeConfig::default(),
            )
            .expect("with cap");
        let now: i64 = inst.call("now", ()).expect("call now");
        assert!(now > 0, "clock returned non-positive {now}");
    }

    #[test]
    fn fuel_budget_traps_infinite_loop() {
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "plugin_abi_version") (result i32) i32.const 1)
              (func (export "alloc") (param i32) (result i32) i32.const 16)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "spin")
                (loop $forever br $forever)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        let manifest = ModuleManifest::from_bytes("spin", "1.0.0", &bytes);
        let mut rt = PluginRuntime::new().expect("runtime");
        rt.load(manifest, &bytes).expect("load");
        let mut inst = rt
            .instantiate(
                "spin",
                PluginCapabilities::deny_all(),
                RuntimeConfig {
                    fuel_budget: 10_000,
                    memory_cap_bytes: 64 * 1024,
                },
            )
            .expect("instantiate");
        let err = inst.call::<(), ()>("spin", ()).expect_err("must trap");
        match err {
            PluginError::OutOfFuel { budget } => assert_eq!(budget, 10_000),
            other => panic!("expected OutOfFuel, got {other:?}"),
        }
    }

    #[test]
    fn memory_cap_blocks_grow_past_limit() {
        // grow by 4 pages = 256 KiB; cap at 128 KiB.
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "plugin_abi_version") (result i32) i32.const 1)
              (func (export "alloc") (param i32) (result i32) i32.const 16)
              (func (export "dealloc") (param i32) (param i32))
              (func (export "grow") (result i32)
                i32.const 4 memory.grow))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        let manifest = ModuleManifest::from_bytes("grow", "1.0.0", &bytes);
        let mut rt = PluginRuntime::new().expect("runtime");
        rt.load(manifest, &bytes).expect("load");
        let mut inst = rt
            .instantiate(
                "grow",
                PluginCapabilities::deny_all(),
                RuntimeConfig {
                    fuel_budget: DEFAULT_FUEL_BUDGET,
                    memory_cap_bytes: 128 * 1024,
                },
            )
            .expect("instantiate");
        // memory.grow returns -1 on failure (which is what the
        // limiter forces by denying the grow).  We assert that path
        // here.
        let res: i32 = inst.call("grow", ()).expect("call grow");
        assert_eq!(res, -1, "grow must fail when over cap");
    }

    #[test]
    fn missing_abi_global_is_rejected() {
        // No plugin_abi_version global at all.
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) i32.const 16)
              (func (export "dealloc") (param i32) (param i32)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        let manifest = ModuleManifest::from_bytes("noabi", "1.0.0", &bytes);
        let mut rt = PluginRuntime::new().expect("runtime");
        rt.load(manifest, &bytes).expect("load");
        let err = rt
            .instantiate(
                "noabi",
                PluginCapabilities::deny_all(),
                RuntimeConfig::default(),
            )
            .expect_err("must reject");
        assert!(matches!(
            err,
            PluginError::MissingExport("plugin_abi_version")
        ));
    }

    #[test]
    fn abi_version_mismatch_is_rejected() {
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "plugin_abi_version") (result i32) i32.const 99)
              (func (export "alloc") (param i32) (result i32) i32.const 16)
              (func (export "dealloc") (param i32) (param i32)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        let manifest = ModuleManifest::from_bytes("wrongabi", "1.0.0", &bytes);
        let mut rt = PluginRuntime::new().expect("runtime");
        rt.load(manifest, &bytes).expect("load");
        let err = rt
            .instantiate(
                "wrongabi",
                PluginCapabilities::deny_all(),
                RuntimeConfig::default(),
            )
            .expect_err("must reject");
        match err {
            PluginError::AbiMismatch { module, host } => {
                assert_eq!(module, 99);
                assert_eq!(host, crate::PLUGIN_ABI_VERSION);
            }
            other => panic!("expected AbiMismatch, got {other:?}"),
        }
    }
}
