// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Memory tracking and OOM guard for query execution.
//!
//! Provides an atomic memory tracker that enforces a configurable byte limit.
//! When the limit would be exceeded, allocation attempts return an error instead
//! of proceeding. Released memory is tracked via an RAII [`MemoryGuard`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{DruidError, Result};

/// Atomic memory tracker that enforces a maximum byte budget.
///
/// Safe for concurrent use from multiple query execution threads.
#[derive(Debug)]
pub struct MemoryTracker {
    used_bytes: AtomicU64,
    max_bytes: u64,
}

impl MemoryTracker {
    /// Create a new memory tracker with the given maximum byte budget.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            used_bytes: AtomicU64::new(0),
            max_bytes,
        }
    }

    /// Try to allocate `bytes` from the budget.
    ///
    /// On success, returns a [`MemoryGuard`] that releases the allocation when
    /// dropped. On failure (budget exceeded), returns a query error.
    pub fn allocate(self: &Arc<Self>, bytes: u64) -> Result<MemoryGuard> {
        let prev = self.used_bytes.fetch_add(bytes, Ordering::AcqRel);
        if prev + bytes > self.max_bytes {
            self.used_bytes.fetch_sub(bytes, Ordering::Release);
            return Err(DruidError::Query(format!(
                "query memory limit exceeded: requested {bytes} bytes, \
                 {prev} of {} in use",
                self.max_bytes
            )));
        }
        Ok(MemoryGuard {
            tracker: Arc::clone(self),
            bytes,
        })
    }

    /// Returns the number of bytes currently in use.
    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    /// Returns the number of bytes still available.
    pub fn available(&self) -> u64 {
        let used = self.used();
        self.max_bytes.saturating_sub(used)
    }

    /// Returns the maximum byte budget.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
}

/// RAII guard that releases memory back to the tracker on drop.
#[derive(Debug)]
pub struct MemoryGuard {
    tracker: Arc<MemoryTracker>,
    bytes: u64,
}

impl MemoryGuard {
    /// Returns the number of bytes held by this guard.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryGuard {
    fn drop(&mut self) {
        self.tracker
            .used_bytes
            .fetch_sub(self.bytes, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_and_release() {
        let tracker = Arc::new(MemoryTracker::new(1000));
        assert_eq!(tracker.used(), 0);
        assert_eq!(tracker.available(), 1000);

        let g1 = tracker.allocate(400).expect("allocate 400");
        assert_eq!(tracker.used(), 400);
        assert_eq!(tracker.available(), 600);

        let g2 = tracker.allocate(500).expect("allocate 500");
        assert_eq!(tracker.used(), 900);
        assert_eq!(tracker.available(), 100);

        drop(g1);
        assert_eq!(tracker.used(), 500);
        assert_eq!(tracker.available(), 500);

        drop(g2);
        assert_eq!(tracker.used(), 0);
        assert_eq!(tracker.available(), 1000);
    }

    #[test]
    fn oom_when_budget_exceeded() {
        let tracker = Arc::new(MemoryTracker::new(100));
        let _g1 = tracker.allocate(80).expect("allocate 80");
        let err = tracker.allocate(50).unwrap_err();
        assert!(err.to_string().contains("memory limit exceeded"));
        // Used should still be 80 (the failed allocation was rolled back).
        assert_eq!(tracker.used(), 80);
    }

    #[test]
    fn zero_allocation_succeeds() {
        let tracker = Arc::new(MemoryTracker::new(100));
        let _g = tracker.allocate(0).expect("allocate 0");
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn exact_budget_succeeds() {
        let tracker = Arc::new(MemoryTracker::new(100));
        let _g = tracker.allocate(100).expect("allocate exactly max");
        assert_eq!(tracker.used(), 100);
        assert_eq!(tracker.available(), 0);
    }

    #[test]
    fn guard_bytes_accessor() {
        let tracker = Arc::new(MemoryTracker::new(1000));
        let g = tracker.allocate(42).expect("allocate");
        assert_eq!(g.bytes(), 42);
    }
}
