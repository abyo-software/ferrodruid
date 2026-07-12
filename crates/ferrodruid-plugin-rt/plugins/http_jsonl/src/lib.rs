// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! HTTP-JSONL input-source plugin for FerroDruid.
//!
//! Demonstrates a capability-gated network plugin: imports
//! `ferro_http_get` from the host (requires the `net` capability)
//! and exposes a simple `input_fetch_line_count` entry point that
//! fetches a URL via the host and counts the newline-delimited JSON
//! rows in the response body.
//!
//! ABI:
//! * `input_fetch_line_count(url_ptr, url_len, scratch_ptr, scratch_cap) -> i64`
//!   * Positive value = number of non-empty lines observed.
//!   * `-1` = host signalled HTTP failure / capability mis-link.
//!   * `-2` = scratch buffer too small for the response body
//!     (host should retry with a larger buffer).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_op_in_unsafe_fn)]

#[cfg(target_arch = "wasm32")]
extern crate alloc;

#[cfg(target_arch = "wasm32")]
use alloc::alloc::{Layout, alloc as raw_alloc, dealloc as raw_dealloc};

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

/// FerroDruid plugin ABI version (see running-stddev for why this is
/// a function and not a `pub static i32`).
#[unsafe(no_mangle)]
pub extern "C" fn plugin_abi_version() -> i32 {
    1
}

// ---------------------------------------------------------------------------
// Safe core
// ---------------------------------------------------------------------------

/// Count the number of non-empty newline-delimited rows in `body`.
/// Recognises both `\n` and `\r\n` line endings; rows consisting only
/// of whitespace count as empty.
#[must_use]
pub fn count_jsonl_rows(body: &[u8]) -> u64 {
    let mut count: u64 = 0;
    let mut in_line = false;
    for &b in body {
        if b == b'\n' {
            if in_line {
                count = count.saturating_add(1);
                in_line = false;
            }
        } else if b != b'\r' && b != b' ' && b != b'\t' {
            in_line = true;
        }
    }
    if in_line {
        count = count.saturating_add(1);
    }
    count
}

// ---------------------------------------------------------------------------
// C-ABI wrappers — wasm32 only.  On wasm we link against the host's
// `ferro_http_get` import; on native the symbol is unresolved so we
// gate the entire ABI surface behind `#[cfg(target_arch = "wasm32")]`.
// ---------------------------------------------------------------------------

// Place the import in the `ferro::*` namespace rather than the
// default `env::*` so the host's deny-by-default capability gate
// matches it.  Without the `wasm_import_module` attribute,
// Rust→wasm32 puts every `extern "C"` import under `env`, which
// the host runtime rejects as an unknown module.
#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "ferro")]
unsafe extern "C" {
    fn ferro_http_get(url_ptr: i32, url_len: i32, out_ptr: i32, out_cap: i32) -> i32;
}

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
    // SAFETY: host ABI contract — `ptr`/`size` round-trip from `alloc`.
    unsafe { raw_dealloc(ptr as *mut u8, layout) }
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn input_fetch_line_count(
    url_ptr: i32,
    url_len: i32,
    scratch_ptr: i32,
    scratch_cap: i32,
) -> i64 {
    if url_ptr <= 0 || url_len <= 0 || scratch_ptr <= 0 || scratch_cap <= 0 {
        return -1;
    }
    // SAFETY: ferro_http_get is a host import; signature matches the
    // host's `func_wrap` registration.
    let n = unsafe { ferro_http_get(url_ptr, url_len, scratch_ptr, scratch_cap) };
    if n < 0 {
        return -1;
    }
    if n > scratch_cap {
        return -2;
    }
    let body_len = n as usize;
    // SAFETY: scratch_ptr came from the host's `alloc`; the host
    // guarantees scratch_cap addressable bytes and `n <= scratch_cap`.
    let body = unsafe { core::slice::from_raw_parts(scratch_ptr as *const u8, body_len) };
    count_jsonl_rows(body) as i64
}

/// Smoke entry point: count JSONL rows in a host-supplied byte
/// buffer without invoking the network capability.  Useful for
/// the "deny-net" negative test in the host-side test suite — the
/// plugin's parser can still be exercised without the host import.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn count_lines_in_buffer(ptr: i32, len: i32) -> i64 {
    if ptr <= 0 || len <= 0 {
        return 0;
    }
    // SAFETY: host ABI contract.
    let body = unsafe { core::slice::from_raw_parts(ptr as *const u8, len as usize) };
    count_jsonl_rows(body) as i64
}

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_non_empty_lines() {
        let buf = b"{\"a\":1}\n{\"b\":2}\n\n{\"c\":3}\n";
        assert_eq!(count_jsonl_rows(buf), 3);
    }

    #[test]
    fn counts_trailing_unterminated_line() {
        let buf = b"{\"a\":1}\n{\"b\":2}";
        assert_eq!(count_jsonl_rows(buf), 2);
    }

    #[test]
    fn empty_buffer_returns_zero() {
        assert_eq!(count_jsonl_rows(b""), 0);
    }

    #[test]
    fn whitespace_only_lines_are_ignored() {
        let buf = b"{\"a\":1}\n   \n\t\t\n{\"b\":2}\n";
        assert_eq!(count_jsonl_rows(buf), 2);
    }

    #[test]
    fn crlf_line_endings_count() {
        let buf = b"{\"a\":1}\r\n{\"b\":2}\r\n";
        assert_eq!(count_jsonl_rows(buf), 2);
    }
}
