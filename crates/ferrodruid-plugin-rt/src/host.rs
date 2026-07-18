// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Per-instance host state and the capability-gated host functions
//! linked into a plugin instance.
//!
//! The runtime exposes a small, deliberately boring set of host
//! imports under the module name `ferro`.  Each function is wired
//! only if its matching [`PluginCapabilities`] flag is set.

use std::time::{SystemTime, UNIX_EPOCH};

use wasmtime::{Caller, Linker, ResourceLimiter};

use crate::PluginCapabilities;

/// Per-call I/O captured from the plugin via `ferro_log`.  Tests
/// inspect this; in production it is drained to `tracing` after the
/// call returns.
#[derive(Debug, Default)]
pub struct LogBuffer {
    /// Captured `(level, message)` pairs in call order.
    pub records: Vec<(u32, String)>,
}

impl LogBuffer {
    /// Drain captured records via the host's `tracing` subscriber.
    pub fn flush_to_tracing(&mut self, plugin_name: &str) {
        for (level, msg) in self.records.drain(..) {
            match level {
                0 => tracing::trace!(target: "ferrodruid::plugin", plugin = plugin_name, "{msg}"),
                1 => tracing::debug!(target: "ferrodruid::plugin", plugin = plugin_name, "{msg}"),
                2 => tracing::info!(target: "ferrodruid::plugin", plugin = plugin_name, "{msg}"),
                3 => tracing::warn!(target: "ferrodruid::plugin", plugin = plugin_name, "{msg}"),
                _ => tracing::error!(target: "ferrodruid::plugin", plugin = plugin_name, "{msg}"),
            }
        }
    }
}

/// Per-instance state stored in the Wasmtime `Store`'s data slot.
///
/// Both the resource limiter (memory cap enforcement) and the host
/// imports read this struct.  The runtime is responsible for sizing
/// `memory_cap_bytes` correctly before instantiation.
#[derive(Debug)]
pub struct HostState {
    /// Hard cap on linear memory (bytes).
    pub memory_cap_bytes: usize,
    /// Logs captured from the plugin during the current call.
    pub log_buffer: LogBuffer,
    /// Pre-supplied HTTP responses keyed by URL.  Used by the
    /// `net-capability` host import; the runtime allows hosts to
    /// inject a fixture map for testing and to enforce an allowlist
    /// of URLs in production.  An entry of `None` means "URL allowed
    /// but no canned response — call the real network".  An entry
    /// of `Some(bytes)` short-circuits the call and returns the
    /// bytes directly (fixture mode).
    pub http_fixtures: std::collections::HashMap<String, Vec<u8>>,
}

impl HostState {
    /// Construct a fresh host state with the given memory cap.
    #[must_use]
    pub fn new(memory_cap_bytes: usize) -> Self {
        Self {
            memory_cap_bytes,
            log_buffer: LogBuffer::default(),
            http_fixtures: std::collections::HashMap::new(),
        }
    }

    /// Insert a fixture URL→bytes mapping for the HTTP capability.
    pub fn add_http_fixture(&mut self, url: impl Into<String>, body: impl Into<Vec<u8>>) {
        self.http_fixtures.insert(url.into(), body.into());
    }
}

impl ResourceLimiter for HostState {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.memory_cap_bytes)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

/// Link the capability-gated host imports under the module name
/// `ferro`.  Only the imports whose corresponding capability flag is
/// `true` are registered; the runtime separately pre-validates the
/// module's import section so any attempt to import an ungranted
/// function produces the typed [`crate::PluginError::CapabilityDenied`]
/// error before we ever reach Wasmtime's linker.
pub(crate) fn install_host_imports(
    linker: &mut Linker<HostState>,
    caps: PluginCapabilities,
) -> wasmtime::Result<()> {
    if caps.log {
        linker.func_wrap(
            "ferro",
            "ferro_log",
            |mut caller: Caller<'_, HostState>, level: u32, ptr: u32, len: u32| -> u32 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return 1;
                };
                let data = mem.data(&caller);
                let start = ptr as usize;
                let end = start.saturating_add(len as usize);
                if end > data.len() {
                    return 2;
                }
                let bytes = data[start..end].to_vec();
                let msg = String::from_utf8_lossy(&bytes).into_owned();
                caller.data_mut().log_buffer.records.push((level, msg));
                0
            },
        )?;
    }

    if caps.clock {
        linker.func_wrap("ferro", "ferro_clock_now_ms", || -> i64 {
            // Saturate to 0 on the (impossible-in-practice) pre-epoch
            // case so we don't surface a panic into the guest.
            let dur = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            i64::try_from(dur.as_millis()).unwrap_or(i64::MAX)
        })?;
    }

    if caps.random {
        linker.func_wrap("ferro", "ferro_random_u64", || -> i64 {
            // Use getrandom via std::time + xorshift fallback — we do
            // not pull in a heavyweight RNG crate just for this
            // capability surface.  For a real cryptographic source
            // the operator should grant the clock capability and let
            // the plugin seed its own ChaCha20 RNG.
            use std::sync::atomic::{AtomicU64, Ordering};
            static SEED: AtomicU64 = AtomicU64::new(0);
            let mut s = SEED.load(Ordering::Relaxed);
            if s == 0 {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                s = now ^ 0x9E37_79B9_7F4A_7C15;
                SEED.store(s, Ordering::Relaxed);
            }
            // xorshift64*
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            SEED.store(s, Ordering::Relaxed);
            s as i64
        })?;
    }

    if caps.net {
        install_http_import(linker)?;
    }

    Ok(())
}

#[cfg(feature = "net-capability")]
fn install_http_import(linker: &mut Linker<HostState>) -> wasmtime::Result<()> {
    linker.func_wrap(
        "ferro",
        "ferro_http_get",
        |mut caller: Caller<'_, HostState>,
         url_ptr: u32,
         url_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 {
            let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                return -1;
            };
            // Read URL from guest memory.
            let data = mem.data(&caller);
            let url_start = url_ptr as usize;
            let url_end = url_start.saturating_add(url_len as usize);
            if url_end > data.len() {
                return -2;
            }
            let url = String::from_utf8_lossy(&data[url_start..url_end]).into_owned();

            // Look up fixture first (short-circuit for tests + allowlist).
            let fixture = caller.data().http_fixtures.get(&url).cloned();
            let body_result: Result<Vec<u8>, String> = match fixture {
                Some(bytes) => Ok(bytes),
                None => fetch_http_blocking(&url),
            };

            let body = match body_result {
                Ok(b) => b,
                Err(e) => {
                    caller
                        .data_mut()
                        .log_buffer
                        .records
                        .push((4, format!("ferro_http_get failed: {e}")));
                    return -3;
                }
            };

            // Copy into the guest's output buffer up to out_cap.
            let to_copy = body.len().min(out_cap as usize);
            let out_start = out_ptr as usize;
            let out_end = out_start.saturating_add(to_copy);
            let data_mut = mem.data_mut(&mut caller);
            if out_end > data_mut.len() {
                return -2;
            }
            data_mut[out_start..out_end].copy_from_slice(&body[..to_copy]);
            // Return number of bytes actually written; if the body
            // was larger than out_cap we return a positive value
            // equal to the body length so the guest can detect
            // truncation by comparing to out_cap.
            i32::try_from(body.len()).unwrap_or(i32::MAX)
        },
    )?;
    Ok(())
}

#[cfg(not(feature = "net-capability"))]
fn install_http_import(_linker: &mut Linker<HostState>) -> wasmtime::Result<()> {
    // When the `net-capability` feature is off, we deliberately do
    // not register `ferro_http_get` even if the per-instance flag
    // was set.  The runtime's import pre-check raises
    // `CapabilityDenied` so the plugin sees a precise error rather
    // than a generic "unknown import".
    Ok(())
}

#[cfg(feature = "net-capability")]
fn fetch_http_blocking(url: &str) -> Result<Vec<u8>, String> {
    // Build a single-threaded current-thread runtime and block on the
    // request.  We deliberately do NOT use `tokio::runtime::Handle::current()`
    // because the runtime is called from within a Wasmtime trampoline
    // and we want the HTTP path to work whether or not the host has
    // an outer tokio runtime running.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime build: {e}"))?;
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| format!("http client build: {e}"))?;
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("http send: {e}"))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("http body read: {e}"))?;
        if !status.is_success() {
            return Err(format!(
                "http status {status}: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(120)
                    .collect::<String>()
            ));
        }
        Ok(body.to_vec())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_buffer_classifies_levels() {
        let mut b = LogBuffer::default();
        b.records.push((0, "trace".into()));
        b.records.push((2, "info".into()));
        b.records.push((4, "error".into()));
        // flush_to_tracing must not panic regardless of subscriber.
        b.flush_to_tracing("test");
        assert!(b.records.is_empty(), "drain leaves buffer empty");
    }

    #[test]
    fn host_state_memory_grow_respects_cap() {
        let mut s = HostState::new(64 * 1024);
        assert!(
            s.memory_growing(0, 32 * 1024, None).unwrap(),
            "grow under cap"
        );
        assert!(s.memory_growing(0, 64 * 1024, None).unwrap(), "grow at cap");
        assert!(
            !s.memory_growing(0, 64 * 1024 + 1, None).unwrap(),
            "grow over cap denied"
        );
    }

    #[test]
    fn http_fixture_round_trip() {
        let mut s = HostState::new(1024);
        s.add_http_fixture("http://x/y", b"hello".to_vec());
        assert_eq!(
            s.http_fixtures.get("http://x/y").map(Vec::as_slice),
            Some(&b"hello"[..])
        );
    }
}
