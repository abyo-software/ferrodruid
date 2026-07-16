// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Running standard-deviation aggregator plugin for FerroDruid.
//!
//! Implements Welford's online algorithm (Wikipedia: "Algorithms for
//! calculating variance") so the population standard deviation can be
//! computed in a single pass with O(1) state and merged across shards
//! with Chan's parallel formula.  Exported as a FerroDruid plugin via
//! the small `agg_*` ABI defined in `ferrodruid-plugin-rt`.
//!
//! Failure modes:
//! * `agg_new` returns `0` if the heap is exhausted (the host's
//!   memory cap rejected the grow).
//! * Any other entry point treats a `0` handle as a no-op (returns
//!   `0` / `NaN` as appropriate) rather than trapping, so a buggy
//!   host call cannot crash the plugin instance and waste the
//!   fuel budget.

// `no_std` only for the production wasm build; native tests use std so
// the test runner + `assert!` machinery is available.  Switching via
// `cfg_attr(not(test), no_std)` keeps the wasm artifact tiny without
// blocking `cargo test` on the host toolchain.
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_op_in_unsafe_fn)]

#[cfg(target_arch = "wasm32")]
extern crate alloc;

#[cfg(target_arch = "wasm32")]
use alloc::alloc::{Layout, alloc as raw_alloc, dealloc as raw_dealloc};

// On the wasm32 target dlmalloc provides a `GlobalDlmalloc` global
// allocator suitable for `#![no_std]` cdylibs.  On native (i.e. the
// host-side `cargo test` for these plugin sources) the standard
// allocator is used because the `#[cfg(target_arch = "wasm32")]`
// gate compiles this stanza away.
#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOC: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

/// FerroDruid plugin ABI version.  The host refuses to instantiate a
/// module whose value differs from `ferrodruid_plugin_rt::PLUGIN_ABI_VERSION`.
///
/// Exported as a *function* rather than a `pub static i32` because
/// Rust→wasm32 lowers `pub static FOO: i32 = …` to a wasm `global`
/// whose value is the *linear-memory address* of the static, not
/// the integer payload — a footgun that would have the host read
/// back e.g. `1048608` instead of `1`.  A `fn` returning the
/// constant sidesteps the pointer-vs-value confusion.
#[unsafe(no_mangle)]
pub extern "C" fn plugin_abi_version() -> i32 {
    1
}

// ---------------------------------------------------------------------------
// Safe core — tested natively, called from both the C ABI wrappers and
// the host-side integration tests via the WASM runtime.
// ---------------------------------------------------------------------------

/// Welford state — kept tiny so the plugin's heap footprint stays
/// well below the host's default 16 MiB memory cap even with millions
/// of long-lived aggregator states.
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct State {
    /// Number of values observed.
    pub count: u64,
    /// Running mean.
    pub mean: f64,
    /// Running sum of squared deviations from the mean.
    pub m2: f64,
}

impl State {
    /// Welford update: ingest one finite value.
    pub fn aggregate(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.count = self.count.saturating_add(1);
        let delta = value - self.mean;
        let count_f64 = self.count as f64;
        self.mean += delta / count_f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    /// Chan's parallel merge of `other` into `self`.
    pub fn merge(&mut self, other: &State) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = *other;
            return;
        }
        let na = self.count as f64;
        let nb = other.count as f64;
        let nt = na + nb;
        let delta = other.mean - self.mean;
        let new_mean = self.mean + delta * (nb / nt);
        let new_m2 = self.m2 + other.m2 + delta * delta * (na * nb / nt);
        self.count = self.count.saturating_add(other.count);
        self.mean = new_mean;
        self.m2 = new_m2;
    }

    /// Population standard deviation; `NaN` for an empty state.
    pub fn finalize_stddev(&self) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        let variance = self.m2 / (self.count as f64);
        sqrt_f64(variance)
    }
}

fn sqrt_f64(x: f64) -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        libm::sqrt(x)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        x.sqrt()
    }
}

// ---------------------------------------------------------------------------
// C-ABI wrappers — only meaningful on the wasm32 target, where the
// host calls them through the plugin-rt facade.  Compiled away on
// native so the host's `cargo test` can exercise the safe core
// without colliding with the host process's own allocator semantics.
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
const STATE_SIZE: usize = core::mem::size_of::<State>();
#[cfg(target_arch = "wasm32")]
const STATE_ALIGN: usize = core::mem::align_of::<State>();

#[cfg(target_arch = "wasm32")]
fn state_layout() -> Layout {
    // STATE_SIZE / STATE_ALIGN come from a `#[repr(C)]` type so
    // `Layout::from_size_align` cannot fail in practice; fall back
    // to a u64 layout if it ever does.
    match Layout::from_size_align(STATE_SIZE, STATE_ALIGN) {
        Ok(l) => l,
        Err(_) => Layout::new::<u64>(),
    }
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
    // SAFETY: layout is valid; allocator may return null on OOM.
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
    // SAFETY: host ABI requires the caller to supply the original
    // (ptr, size) pair from a previous `alloc`.
    unsafe { raw_dealloc(ptr as *mut u8, layout) }
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_new() -> i32 {
    // SAFETY: layout is statically known valid.
    let ptr = unsafe { raw_alloc(state_layout()) } as *mut State;
    if ptr.is_null() {
        return 0;
    }
    // SAFETY: freshly allocated, exclusively owned.
    unsafe { ptr.write(State::default()) };
    ptr as i32
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_drop(handle: i32) {
    if handle == 0 {
        return;
    }
    // SAFETY: handle came from `agg_new`; we are the sole owner.
    unsafe { raw_dealloc(handle as *mut u8, state_layout()) }
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_aggregate(handle: i32, value: f64) {
    if handle == 0 {
        return;
    }
    // SAFETY: handle came from `agg_new`, single-threaded host ABI.
    let s = unsafe { &mut *(handle as *mut State) };
    s.aggregate(value);
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_merge(a: i32, b: i32) {
    if a == 0 || b == 0 || a == b {
        return;
    }
    // SAFETY: distinct, non-null, host-owned handles.
    let sb = unsafe { &*(b as *const State) };
    let snapshot = *sb;
    let sa = unsafe { &mut *(a as *mut State) };
    sa.merge(&snapshot);
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_finalize(handle: i32) -> f64 {
    if handle == 0 {
        return f64::NAN;
    }
    // SAFETY: handle came from `agg_new`.
    let s = unsafe { &*(handle as *const State) };
    s.finalize_stddev()
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_count(handle: i32) -> i64 {
    if handle == 0 {
        return 0;
    }
    // SAFETY: handle came from `agg_new`.
    let s = unsafe { &*(handle as *const State) };
    i64::try_from(s.count).unwrap_or(i64::MAX)
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn agg_mean(handle: i32) -> f64 {
    if handle == 0 {
        return f64::NAN;
    }
    // SAFETY: handle came from `agg_new`.
    let s = unsafe { &*(handle as *const State) };
    if s.count == 0 {
        return f64::NAN;
    }
    s.mean
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
    fn welford_matches_naive_population_stddev() {
        let mut s = State::default();
        for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            s.aggregate(x);
        }
        // Population stddev of the example data set is exactly 2.0
        // (Wikipedia "Standard deviation" worked example).
        let observed = s.finalize_stddev();
        assert!(
            (observed - 2.0).abs() < 1e-12,
            "expected 2.0, got {observed}"
        );
        assert_eq!(s.count, 8);
        assert!((s.mean - 5.0).abs() < 1e-12);
    }

    #[test]
    fn empty_state_returns_nan() {
        let s = State::default();
        assert!(s.finalize_stddev().is_nan());
        assert_eq!(s.count, 0);
    }

    #[test]
    fn merge_matches_single_pass() {
        let mut combined = State::default();
        let mut left = State::default();
        let mut right = State::default();
        let xs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        for x in xs {
            combined.aggregate(x);
        }
        for x in &xs[..5] {
            left.aggregate(*x);
        }
        for x in &xs[5..] {
            right.aggregate(*x);
        }
        left.merge(&right);
        let merged = left.finalize_stddev();
        let direct = combined.finalize_stddev();
        assert!(
            (merged - direct).abs() < 1e-12,
            "merge mismatch: merged={merged} direct={direct}"
        );
    }

    #[test]
    fn non_finite_input_is_ignored() {
        let mut s = State::default();
        s.aggregate(1.0);
        s.aggregate(f64::NAN);
        s.aggregate(f64::INFINITY);
        s.aggregate(3.0);
        assert_eq!(s.count, 2);
        assert!((s.mean - 2.0).abs() < 1e-12);
        assert!((s.finalize_stddev() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn merge_with_empty_is_noop() {
        let mut a = State::default();
        a.aggregate(1.0);
        a.aggregate(3.0);
        let before = a;
        let empty = State::default();
        a.merge(&empty);
        assert_eq!(a.count, before.count);
        assert!((a.mean - before.mean).abs() < 1e-12);
    }

    #[test]
    fn merge_into_empty_copies() {
        let mut empty = State::default();
        let mut other = State::default();
        other.aggregate(5.0);
        other.aggregate(7.0);
        empty.merge(&other);
        assert_eq!(empty.count, 2);
        assert!((empty.mean - 6.0).abs() < 1e-12);
    }
}
