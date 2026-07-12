// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Loaded module + per-call instance wrapper.
//!
//! [`LoadedModule`] is the bytes-on-disk + compiled-artifact pair,
//! produced by [`crate::PluginRuntime::load`].  [`PluginInstance`]
//! is the per-call instantiation that the host actually drives.
//!
//! The split lets us load a module once and instantiate it many
//! times (each with its own memory + fuel budget), which is the
//! pattern the leak audit test exercises.

use std::collections::HashSet;
use std::sync::Arc;

use wasmtime::{Instance, Linker, Module, Store};

use crate::PluginCapabilities;
use crate::host::{HostState, install_host_imports};
use crate::manifest::ModuleManifest;
use crate::runtime::RuntimeConfig;
use crate::{PLUGIN_ABI_VERSION, PluginError};

/// Known host functions under the `ferro` module name + the
/// capability flag that gates each.  Kept centrally so import
/// pre-validation and host-import installation can agree on the set.
pub(crate) const KNOWN_HOST_IMPORTS: &[(&str, &str)] = &[
    ("ferro_log", "log"),
    ("ferro_clock_now_ms", "clock"),
    ("ferro_random_u64", "random"),
    ("ferro_http_get", "net"),
];

/// A WASM module that has been signature-verified and compiled.
///
/// Cloning is cheap: the underlying [`wasmtime::Module`] is internally
/// `Arc`-shared and the manifest is small.
#[derive(Clone)]
pub struct LoadedModule {
    /// The plugin's own manifest (name + version + expected SHA-256).
    pub manifest: Arc<ModuleManifest>,
    /// The compiled module artifact (Wasmtime shares the compiled
    /// code internally; instantiating is cheap).
    pub module: Module,
    /// Set of host imports the module references, validated against
    /// [`KNOWN_HOST_IMPORTS`] at load time.
    pub required_imports: Arc<HashSet<&'static str>>,
}

impl std::fmt::Debug for LoadedModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedModule")
            .field("name", &self.manifest.name)
            .field("version", &self.manifest.version)
            .field("required_imports", &self.required_imports)
            .finish()
    }
}

impl LoadedModule {
    /// Pre-flight check that every host import the module requires is
    /// either unknown (then it's a hard linker error caught later)
    /// or has its capability granted.  Returns the
    /// `CapabilityDenied` variant on the first mismatch — operators
    /// see the exact import name + flag they need to set.
    pub(crate) fn check_caps_or_deny(&self, caps: PluginCapabilities) -> Result<(), PluginError> {
        for name in self.required_imports.iter() {
            let Some(&(_, cap)) = KNOWN_HOST_IMPORTS.iter().find(|(n, _)| n == name) else {
                continue;
            };
            let granted = match cap {
                "log" => caps.log,
                "clock" => caps.clock,
                "random" => caps.random,
                "net" => caps.net,
                _ => false,
            };
            if !granted {
                // Match the static lifetime of the field.
                let static_name: &'static str = KNOWN_HOST_IMPORTS
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(n, _)| *n)
                    .unwrap_or("<unknown>");
                let static_cap: &'static str = KNOWN_HOST_IMPORTS
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, c)| *c)
                    .unwrap_or("<unknown>");
                return Err(PluginError::CapabilityDenied {
                    name: static_name,
                    capability: static_cap,
                });
            }
        }
        Ok(())
    }
}

/// A live plugin instance.  Owns its own Wasmtime [`Store`] (which
/// owns the linear memory + the [`HostState`]), so dropping the
/// instance returns every byte to the host allocator.
pub struct PluginInstance {
    plugin_name: String,
    fuel_budget: u64,
    memory_cap_bytes: usize,
    store: Store<HostState>,
    instance: Instance,
}

impl std::fmt::Debug for PluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginInstance")
            .field("plugin_name", &self.plugin_name)
            .field("fuel_budget", &self.fuel_budget)
            .field("memory_cap_bytes", &self.memory_cap_bytes)
            .finish()
    }
}

impl PluginInstance {
    pub(crate) fn new(
        engine: &wasmtime::Engine,
        loaded: &LoadedModule,
        caps: PluginCapabilities,
        config: &RuntimeConfig,
    ) -> Result<Self, PluginError> {
        loaded.check_caps_or_deny(caps)?;

        let host_state = HostState::new(config.memory_cap_bytes);

        let mut store = Store::new(engine, host_state);
        store
            .set_fuel(config.fuel_budget)
            .map_err(|e| PluginError::Wasmtime(e.to_string()))?;
        store.limiter(|s| s as &mut dyn wasmtime::ResourceLimiter);

        let mut linker = Linker::new(engine);
        install_host_imports(&mut linker, caps)
            .map_err(|e| PluginError::LinkerRejected(e.to_string()))?;

        let instance = linker
            .instantiate(&mut store, &loaded.module)
            .map_err(|e| {
                PluginError::from_wasmtime(e, config.fuel_budget, config.memory_cap_bytes)
            })?;

        // Verify ABI version export.  Plugins must export a
        // function `plugin_abi_version() -> i32`; we reject anything
        // missing or mismatched as a hard error so an out-of-date
        // plugin can never be silently loaded.
        //
        // We deliberately prefer a function export over a `const
        // global` because Rust→wasm32's `pub static FOO: i32 = …`
        // exports a `(global (i32 const <linear-mem-offset>))` —
        // i.e. the global value is the address of the static in
        // linear memory, not the integer payload — so reading the
        // global directly would yield a meaningless pointer.  A
        // function call sidesteps that footgun and lets the plugin
        // author write a plain `fn plugin_abi_version() -> i32`.
        let module_abi: u32 =
            match instance.get_typed_func::<(), i32>(&mut store, "plugin_abi_version") {
                Ok(f) => match f.call(&mut store, ()) {
                    Ok(v) => v as u32,
                    Err(e) => {
                        return Err(PluginError::from_wasmtime(
                            e,
                            config.fuel_budget,
                            config.memory_cap_bytes,
                        ));
                    }
                },
                Err(_) => {
                    return Err(PluginError::MissingExport("plugin_abi_version"));
                }
            };
        if module_abi != PLUGIN_ABI_VERSION {
            return Err(PluginError::AbiMismatch {
                module: module_abi,
                host: PLUGIN_ABI_VERSION,
            });
        }

        Ok(Self {
            plugin_name: loaded.manifest.name.clone(),
            fuel_budget: config.fuel_budget,
            memory_cap_bytes: config.memory_cap_bytes,
            store,
            instance,
        })
    }

    /// Plugin name from the manifest.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.plugin_name
    }

    /// Remaining fuel for this instance (Wasmtime's internal counter).
    pub fn fuel_remaining(&mut self) -> u64 {
        self.store.get_fuel().unwrap_or(0)
    }

    /// Reset fuel back to the budget for a new call.  Useful when
    /// the host wants to amortise instance creation across multiple
    /// per-call budgets.
    pub fn refuel(&mut self) -> Result<(), PluginError> {
        self.store
            .set_fuel(self.fuel_budget)
            .map_err(|e| PluginError::Wasmtime(e.to_string()))
    }

    /// Inject an HTTP fixture (URL → bytes) that the `net` capability's
    /// host import will return without going to the network.
    /// Useful for tests and for production hosts that want to
    /// enforce a strict allowlist.
    pub fn add_http_fixture(&mut self, url: impl Into<String>, body: impl Into<Vec<u8>>) {
        self.store.data_mut().add_http_fixture(url, body);
    }

    /// Call the plugin's `alloc(size: i32) -> i32` export and return
    /// the resulting guest pointer.
    pub fn alloc(&mut self, size: u32) -> Result<u32, PluginError> {
        let func = self
            .instance
            .get_typed_func::<i32, i32>(&mut self.store, "alloc")
            .map_err(|_| PluginError::MissingExport("alloc"))?;
        let ptr = func
            .call(&mut self.store, size as i32)
            .map_err(|e| PluginError::from_wasmtime(e, self.fuel_budget, self.memory_cap_bytes))?;
        if ptr <= 0 {
            return Err(PluginError::HostCapability(format!(
                "plugin alloc returned non-positive ptr {ptr} for size {size}"
            )));
        }
        Ok(ptr as u32)
    }

    /// Call the plugin's `dealloc(ptr: i32, size: i32)` export.
    pub fn dealloc(&mut self, ptr: u32, size: u32) -> Result<(), PluginError> {
        let func = self
            .instance
            .get_typed_func::<(i32, i32), ()>(&mut self.store, "dealloc")
            .map_err(|_| PluginError::MissingExport("dealloc"))?;
        func.call(&mut self.store, (ptr as i32, size as i32))
            .map_err(|e| PluginError::from_wasmtime(e, self.fuel_budget, self.memory_cap_bytes))?;
        Ok(())
    }

    /// Copy `bytes` into the plugin's linear memory at `ptr`.
    /// `ptr..ptr+bytes.len()` must lie within the memory; the
    /// runtime returns [`PluginError::PointerOutOfBounds`] otherwise.
    pub fn write_bytes(&mut self, ptr: u32, bytes: &[u8]) -> Result<(), PluginError> {
        let memory = self
            .instance
            .get_memory(&mut self.store, "memory")
            .ok_or(PluginError::MissingExport("memory"))?;
        let mem_size = memory.data_size(&self.store);
        let start = ptr as usize;
        let end = start.saturating_add(bytes.len());
        if end > mem_size {
            return Err(PluginError::PointerOutOfBounds {
                ptr,
                len: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
                mem_size,
            });
        }
        memory.data_mut(&mut self.store)[start..end].copy_from_slice(bytes);
        Ok(())
    }

    /// Read `len` bytes from the plugin's linear memory at `ptr`.
    pub fn read_bytes(&mut self, ptr: u32, len: u32) -> Result<Vec<u8>, PluginError> {
        let memory = self
            .instance
            .get_memory(&mut self.store, "memory")
            .ok_or(PluginError::MissingExport("memory"))?;
        let mem_size = memory.data_size(&self.store);
        let start = ptr as usize;
        let end = start.saturating_add(len as usize);
        if end > mem_size {
            return Err(PluginError::PointerOutOfBounds { ptr, len, mem_size });
        }
        Ok(memory.data(&self.store)[start..end].to_vec())
    }

    /// Helper: write `bytes` to a freshly-allocated buffer in the
    /// plugin and return (ptr, len).  The caller is responsible for
    /// later calling [`PluginInstance::dealloc`] (typically after
    /// the plugin has consumed the buffer).
    pub fn write_owned(&mut self, bytes: &[u8]) -> Result<(u32, u32), PluginError> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| PluginError::HostCapability("input larger than 4 GiB".into()))?;
        let ptr = self.alloc(len)?;
        self.write_bytes(ptr, bytes)?;
        Ok((ptr, len))
    }

    /// Generic typed-function call.  Resets fuel before the call so
    /// each invocation gets a fresh budget; the host's
    /// [`RuntimeConfig::fuel_budget`] applies to **each** call.
    pub fn call<Params, Results>(
        &mut self,
        export: &str,
        params: Params,
    ) -> Result<Results, PluginError>
    where
        Params: wasmtime::WasmParams,
        Results: wasmtime::WasmResults,
    {
        self.refuel()?;
        let func = self
            .instance
            .get_typed_func::<Params, Results>(&mut self.store, export)
            .map_err(|_| PluginError::MissingExport(static_export(export)))?;
        func.call(&mut self.store, params)
            .map_err(|e| PluginError::from_wasmtime(e, self.fuel_budget, self.memory_cap_bytes))
    }

    /// Capture the per-call log buffer's records and clear it.
    /// Production callers should drain this via
    /// [`crate::host::LogBuffer::flush_to_tracing`] after every call.
    pub fn take_log_records(&mut self) -> Vec<(u32, String)> {
        std::mem::take(&mut self.store.data_mut().log_buffer.records)
    }

    /// Fuel consumed since the last refuel or instantiation.
    pub fn fuel_consumed(&mut self) -> u64 {
        self.fuel_budget.saturating_sub(self.fuel_remaining())
    }
}

/// We cannot generate a `&'static str` from an arbitrary &str, but
/// we can intern the small set of known export names so
/// [`PluginError::MissingExport`] gets a useful label.  Anything we
/// don't recognise gets a generic placeholder rather than the
/// caller's transient slice.
const fn static_export(name: &str) -> &'static str {
    match name.as_bytes() {
        b"alloc" => "alloc",
        b"dealloc" => "dealloc",
        b"agg_new" => "agg_new",
        b"agg_aggregate" => "agg_aggregate",
        b"agg_merge" => "agg_merge",
        b"agg_finalize" => "agg_finalize",
        b"agg_drop" => "agg_drop",
        b"auth_verify" => "auth_verify",
        b"input_fetch" => "input_fetch",
        _ => "<unknown export>",
    }
}
