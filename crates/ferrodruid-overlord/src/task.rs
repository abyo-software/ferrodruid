// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Ingestion-task lifecycle: state machine, interval locks, retry policy,
//! and worker assignment.
//!
//! This module models Apache Druid's task lifecycle in-process for Phase 1.
//! A task moves through the states
//! `WAITING -> PENDING -> RUNNING -> SUCCESS | FAILED`, acquires
//! interval-based [`TaskLock`]s on a datasource to prevent conflicting
//! concurrent ingestion, retries with exponential backoff on failure up to a
//! configurable budget, and is assigned to a registered [`Worker`] by a
//! pluggable [`WorkerSelector`].
//!
//! HONEST scope: there is no real distributed worker RPC here. Workers are
//! registered descriptors and "assignment" is bookkeeping the Overlord owns;
//! execution still happens in-process. The state machine, lock conflict
//! resolution, retry accounting, and worker-loss re-assignment are real and
//! independently tested.

use std::cmp::Ordering;

use ferrodruid_common::{DruidError, Result};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Task state machine
// ---------------------------------------------------------------------------

/// Lifecycle state of an ingestion task.
///
/// This unifies Druid's `TaskState` (the terminal-or-not view) with the
/// `RunnerTaskState` distinctions that matter for scheduling
/// (`WAITING` vs `PENDING` vs `RUNNING`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    /// Blocked on lock acquisition; not yet eligible for a worker.
    Waiting,
    /// Ready to run but not yet assigned to a worker.
    Pending,
    /// Assigned to a worker and executing.
    Running,
    /// Terminal: completed successfully.
    Success,
    /// Terminal: completed with failure (after retry budget exhausted).
    Failed,
}

impl TaskState {
    /// The `SCREAMING_SNAKE_CASE` wire string for this state.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Waiting => "WAITING",
            TaskState::Pending => "PENDING",
            TaskState::Running => "RUNNING",
            TaskState::Success => "SUCCESS",
            TaskState::Failed => "FAILED",
        }
    }

    /// Whether this is a terminal state (`SUCCESS` or `FAILED`).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, TaskState::Success | TaskState::Failed)
    }

    /// Whether `next` is a valid transition from `self`.
    ///
    /// Valid edges:
    /// - `WAITING -> PENDING` (locks acquired) or `WAITING -> FAILED`
    /// - `PENDING -> RUNNING` (worker assigned), `PENDING -> WAITING`
    ///   (lost a lock / preempted), or `PENDING -> FAILED`
    /// - `RUNNING -> SUCCESS`, `RUNNING -> FAILED`, or `RUNNING -> PENDING`
    ///   (worker lost, re-assignable within retry budget)
    /// - terminal states have no outgoing edges
    #[must_use]
    pub fn can_transition_to(self, next: TaskState) -> bool {
        use TaskState::{Failed, Pending, Running, Success, Waiting};
        matches!(
            (self, next),
            (Waiting, Pending)
                | (Waiting, Failed)
                | (Pending, Running)
                | (Pending, Waiting)
                | (Pending, Failed)
                | (Running, Success)
                | (Running, Failed)
                | (Running, Pending)
        )
    }
}

// ---------------------------------------------------------------------------
// Interval & locks
// ---------------------------------------------------------------------------

/// A half-open `[start, end)` time interval in epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interval {
    /// Inclusive start, epoch millis.
    pub start_millis: i64,
    /// Exclusive end, epoch millis.
    pub end_millis: i64,
}

impl Interval {
    /// Construct an interval, rejecting `end <= start`.
    pub fn new(start_millis: i64, end_millis: i64) -> Result<Self> {
        if end_millis <= start_millis {
            return Err(DruidError::Ingestion(format!(
                "invalid interval: end {end_millis} <= start {start_millis}"
            )));
        }
        Ok(Self {
            start_millis,
            end_millis,
        })
    }

    /// Whether this interval overlaps `other` (half-open semantics, so
    /// abutting intervals do not overlap).
    #[must_use]
    pub fn overlaps(&self, other: &Interval) -> bool {
        self.start_millis < other.end_millis && other.start_millis < self.end_millis
    }
}

/// Lock granularity / exclusivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LockType {
    /// Multiple `SHARED` locks may co-exist on an overlapping interval;
    /// a `SHARED` lock only conflicts with an overlapping `EXCLUSIVE` lock.
    Shared,
    /// Conflicts with any overlapping lock of either type.
    Exclusive,
}

impl LockType {
    /// Wire string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LockType::Shared => "SHARED",
            LockType::Exclusive => "EXCLUSIVE",
        }
    }
}

/// A TimeChunk lock held (or requested) by a task on a datasource interval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskLock {
    /// Persisted lock id (empty until persisted).
    pub id: String,
    /// Task that owns the lock.
    pub task_id: String,
    /// Target datasource.
    pub data_source: String,
    /// Locked interval.
    pub interval: Interval,
    /// Shared or exclusive.
    pub lock_type: LockType,
    /// Priority; higher priority preempts lower on contention.
    pub priority: i64,
    /// Whether the lock has been revoked (preempted by a higher-priority task).
    pub revoked: bool,
}

impl TaskLock {
    /// Whether two locks conflict.
    ///
    /// Conflict requires the same datasource and overlapping intervals.
    /// Given that, `EXCLUSIVE` conflicts with anything overlapping while two
    /// `SHARED` locks never conflict. Revoked locks never conflict (they no
    /// longer hold the interval).
    #[must_use]
    pub fn conflicts_with(&self, other: &TaskLock) -> bool {
        if self.revoked || other.revoked {
            return false;
        }
        if self.data_source != other.data_source {
            return false;
        }
        if !self.interval.overlaps(&other.interval) {
            return false;
        }
        self.lock_type == LockType::Exclusive || other.lock_type == LockType::Exclusive
    }
}

/// Outcome of a lock-acquisition attempt against a set of existing locks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockDecision {
    /// No conflict; the lock may be granted.
    Granted,
    /// Conflicting lower-or-equal-priority locks must be revoked first.
    /// Holds the ids of locks to revoke (only strictly-lower-priority locks;
    /// see [`evaluate_lock_request`]).
    Preempt(Vec<String>),
    /// Blocked by a conflicting lock of equal-or-higher priority; the task
    /// must wait.
    Blocked,
}

/// Decide whether a new lock request can be granted against `existing` locks.
///
/// - If no existing lock conflicts: [`LockDecision::Granted`].
/// - If every conflicting lock has strictly lower priority than the request:
///   [`LockDecision::Preempt`] with their ids (caller revokes them, then grants).
/// - If any conflicting lock has priority `>=` the request:
///   [`LockDecision::Blocked`] (the requester waits; it never preempts an
///   equal-or-higher-priority holder).
#[must_use]
pub fn evaluate_lock_request(request: &TaskLock, existing: &[TaskLock]) -> LockDecision {
    let mut to_preempt = Vec::new();
    for held in existing {
        if held.task_id == request.task_id {
            // A task never conflicts with its own locks.
            continue;
        }
        if !request.conflicts_with(held) {
            continue;
        }
        if held.priority >= request.priority {
            return LockDecision::Blocked;
        }
        to_preempt.push(held.id.clone());
    }
    if to_preempt.is_empty() {
        LockDecision::Granted
    } else {
        LockDecision::Preempt(to_preempt)
    }
}

// ---------------------------------------------------------------------------
// Retry policy
// ---------------------------------------------------------------------------

/// Exponential-backoff retry policy for failed tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts (an attempt count of `max_attempts` means
    /// no further retries are allowed).
    pub max_attempts: u32,
    /// Base backoff in milliseconds for the first retry.
    pub base_delay_millis: u64,
    /// Cap on the backoff delay in milliseconds.
    pub max_delay_millis: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_millis: 1_000,
            max_delay_millis: 60_000,
        }
    }
}

impl RetryPolicy {
    /// Whether a task that has already made `attempt` attempts may retry.
    ///
    /// `attempt` is the count of attempts *already made*; a fresh task that
    /// has run once has `attempt == 1`.
    #[must_use]
    pub fn can_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }

    /// Backoff delay before the retry that follows `attempt` completed
    /// attempts. `attempt == 1` yields `base_delay_millis`, then doubles each
    /// time, saturating at `max_delay_millis`.
    #[must_use]
    pub fn backoff_millis(&self, attempt: u32) -> u64 {
        if attempt == 0 {
            return 0;
        }
        let cap = u128::from(self.max_delay_millis);
        let shift = attempt.saturating_sub(1);
        // `base_delay_millis` is at most 64 bits; keeping `64 + shift < 128`
        // guarantees the u128 shift below cannot wrap. Any larger shift has
        // already blown past any u64 cap, so clamp straight to the cap.
        if shift >= 64 {
            return self.max_delay_millis;
        }
        let scaled = u128::from(self.base_delay_millis) << shift;
        u64::try_from(scaled.min(cap)).unwrap_or(self.max_delay_millis)
    }
}

// ---------------------------------------------------------------------------
// Workers & assignment
// ---------------------------------------------------------------------------

/// A registered worker (MiddleManager / Indexer) descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Worker {
    /// Worker host.
    pub host: String,
    /// Worker port.
    pub port: u16,
    /// Maximum concurrent task slots ("capacity").
    pub capacity: u32,
}

impl Worker {
    /// Stable identity string (`host:port`).
    #[must_use]
    pub fn id(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Strategy for picking a worker for a pending task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerSelectStrategy {
    /// Cycle through eligible workers in registration order.
    RoundRobin,
    /// Pick the eligible worker with the fewest assigned tasks (ties broken
    /// by registration order).
    LeastLoaded,
}

/// Pluggable worker selector that tracks per-worker load.
#[derive(Debug, Clone)]
pub struct WorkerSelector {
    workers: Vec<Worker>,
    strategy: WorkerSelectStrategy,
    /// Round-robin cursor.
    cursor: usize,
}

impl WorkerSelector {
    /// Build a selector over `workers` using `strategy`.
    #[must_use]
    pub fn new(workers: Vec<Worker>, strategy: WorkerSelectStrategy) -> Self {
        Self {
            workers,
            strategy,
            cursor: 0,
        }
    }

    /// Register (or replace by id) a worker.
    pub fn register(&mut self, worker: Worker) {
        let id = worker.id();
        if let Some(slot) = self.workers.iter_mut().find(|w| w.id() == id) {
            *slot = worker;
        } else {
            self.workers.push(worker);
        }
    }

    /// Remove a worker by id. Returns whether it was present.
    pub fn deregister(&mut self, worker_id: &str) -> bool {
        let before = self.workers.len();
        self.workers.retain(|w| w.id() != worker_id);
        if self.cursor >= self.workers.len() {
            self.cursor = 0;
        }
        self.workers.len() != before
    }

    /// Whether a worker with this id is currently registered.
    #[must_use]
    pub fn is_registered(&self, worker_id: &str) -> bool {
        self.workers.iter().any(|w| w.id() == worker_id)
    }

    /// Number of registered workers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Whether no workers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Select a worker for a new task, given current `load` (worker-id ->
    /// number of assigned tasks). Returns `None` if no worker has free
    /// capacity.
    ///
    /// `load` is supplied by the caller so the selector stays a pure policy
    /// over the Overlord's authoritative assignment table.
    pub fn select(&mut self, load: &impl Fn(&str) -> u32) -> Option<Worker> {
        if self.workers.is_empty() {
            return None;
        }
        match self.strategy {
            WorkerSelectStrategy::RoundRobin => self.select_round_robin(load),
            WorkerSelectStrategy::LeastLoaded => self.select_least_loaded(load),
        }
    }

    fn select_round_robin(&mut self, load: &impl Fn(&str) -> u32) -> Option<Worker> {
        let n = self.workers.len();
        for offset in 0..n {
            let idx = (self.cursor + offset) % n;
            let w = &self.workers[idx];
            if load(&w.id()) < w.capacity {
                self.cursor = (idx + 1) % n;
                return Some(w.clone());
            }
        }
        None
    }

    fn select_least_loaded(&mut self, load: &impl Fn(&str) -> u32) -> Option<Worker> {
        self.workers
            .iter()
            .enumerate()
            .filter(|(_, w)| load(&w.id()) < w.capacity)
            .min_by(|(ia, a), (ib, b)| match load(&a.id()).cmp(&load(&b.id())) {
                Ordering::Equal => ia.cmp(ib),
                other => other,
            })
            .map(|(_, w)| w.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ----- state machine -----

    #[test]
    fn valid_transitions() {
        use TaskState::*;
        assert!(Waiting.can_transition_to(Pending));
        assert!(Waiting.can_transition_to(Failed));
        assert!(Pending.can_transition_to(Running));
        assert!(Pending.can_transition_to(Waiting));
        assert!(Pending.can_transition_to(Failed));
        assert!(Running.can_transition_to(Success));
        assert!(Running.can_transition_to(Failed));
        assert!(Running.can_transition_to(Pending));
    }

    #[test]
    fn invalid_transitions_rejected() {
        use TaskState::*;
        // Cannot skip straight from WAITING to RUNNING.
        assert!(!Waiting.can_transition_to(Running));
        assert!(!Waiting.can_transition_to(Success));
        // Cannot go from PENDING straight to SUCCESS.
        assert!(!Pending.can_transition_to(Success));
        // Terminal states have no outgoing edges.
        assert!(!Success.can_transition_to(Running));
        assert!(!Success.can_transition_to(Failed));
        assert!(!Failed.can_transition_to(Pending));
        assert!(!Failed.can_transition_to(Running));
    }

    #[test]
    fn terminal_flag() {
        assert!(TaskState::Success.is_terminal());
        assert!(TaskState::Failed.is_terminal());
        assert!(!TaskState::Running.is_terminal());
        assert!(!TaskState::Waiting.is_terminal());
    }

    // ----- intervals -----

    #[test]
    fn interval_rejects_empty() {
        assert!(Interval::new(10, 10).is_err());
        assert!(Interval::new(10, 5).is_err());
        assert!(Interval::new(5, 10).is_ok());
    }

    #[test]
    fn interval_overlap_half_open() {
        let a = Interval::new(0, 10).expect("a");
        let b = Interval::new(5, 15).expect("b");
        let c = Interval::new(10, 20).expect("c"); // abuts a
        assert!(a.overlaps(&b));
        assert!(
            !a.overlaps(&c),
            "abutting half-open intervals don't overlap"
        );
    }

    // ----- locks -----

    fn lock(id: &str, task: &str, ds: &str, s: i64, e: i64, ty: LockType, prio: i64) -> TaskLock {
        TaskLock {
            id: id.to_string(),
            task_id: task.to_string(),
            data_source: ds.to_string(),
            interval: Interval::new(s, e).expect("interval"),
            lock_type: ty,
            priority: prio,
            revoked: false,
        }
    }

    #[test]
    fn exclusive_conflicts_with_overlap() {
        let a = lock("1", "t1", "ds", 0, 10, LockType::Exclusive, 0);
        let b = lock("2", "t2", "ds", 5, 15, LockType::Shared, 0);
        assert!(a.conflicts_with(&b));
    }

    #[test]
    fn shared_shared_no_conflict() {
        let a = lock("1", "t1", "ds", 0, 10, LockType::Shared, 0);
        let b = lock("2", "t2", "ds", 5, 15, LockType::Shared, 0);
        assert!(!a.conflicts_with(&b));
    }

    #[test]
    fn different_datasource_no_conflict() {
        let a = lock("1", "t1", "ds1", 0, 10, LockType::Exclusive, 0);
        let b = lock("2", "t2", "ds2", 0, 10, LockType::Exclusive, 0);
        assert!(!a.conflicts_with(&b));
    }

    #[test]
    fn revoked_lock_no_conflict() {
        let a = lock("1", "t1", "ds", 0, 10, LockType::Exclusive, 0);
        let mut b = lock("2", "t2", "ds", 0, 10, LockType::Exclusive, 0);
        b.revoked = true;
        assert!(!a.conflicts_with(&b));
    }

    #[test]
    fn grant_when_no_conflict() {
        let req = lock("0", "t1", "ds", 0, 10, LockType::Exclusive, 5);
        let existing = vec![lock("1", "t2", "ds", 20, 30, LockType::Exclusive, 5)];
        assert_eq!(
            evaluate_lock_request(&req, &existing),
            LockDecision::Granted
        );
    }

    #[test]
    fn blocked_by_equal_priority() {
        let req = lock("0", "t1", "ds", 0, 10, LockType::Exclusive, 5);
        let existing = vec![lock("1", "t2", "ds", 5, 15, LockType::Exclusive, 5)];
        assert_eq!(
            evaluate_lock_request(&req, &existing),
            LockDecision::Blocked
        );
    }

    #[test]
    fn blocked_by_higher_priority() {
        let req = lock("0", "t1", "ds", 0, 10, LockType::Exclusive, 5);
        let existing = vec![lock("1", "t2", "ds", 5, 15, LockType::Exclusive, 9)];
        assert_eq!(
            evaluate_lock_request(&req, &existing),
            LockDecision::Blocked
        );
    }

    #[test]
    fn preempt_lower_priority() {
        let req = lock("0", "t1", "ds", 0, 10, LockType::Exclusive, 9);
        let existing = vec![
            lock("1", "t2", "ds", 5, 15, LockType::Exclusive, 5),
            lock("2", "t3", "ds", 8, 12, LockType::Shared, 1),
        ];
        match evaluate_lock_request(&req, &existing) {
            LockDecision::Preempt(ids) => {
                assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
            }
            other => panic!("expected preempt, got {other:?}"),
        }
    }

    #[test]
    fn own_locks_never_conflict() {
        let req = lock("0", "t1", "ds", 0, 10, LockType::Exclusive, 5);
        let existing = vec![lock("1", "t1", "ds", 5, 15, LockType::Exclusive, 5)];
        assert_eq!(
            evaluate_lock_request(&req, &existing),
            LockDecision::Granted
        );
    }

    // ----- retry -----

    #[test]
    fn retry_budget() {
        let p = RetryPolicy {
            max_attempts: 3,
            base_delay_millis: 1000,
            max_delay_millis: 60_000,
        };
        assert!(p.can_retry(1));
        assert!(p.can_retry(2));
        assert!(!p.can_retry(3), "3 attempts made == budget exhausted");
        assert!(!p.can_retry(4));
    }

    #[test]
    fn backoff_exponential_capped() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay_millis: 1000,
            max_delay_millis: 8000,
        };
        assert_eq!(p.backoff_millis(0), 0);
        assert_eq!(p.backoff_millis(1), 1000);
        assert_eq!(p.backoff_millis(2), 2000);
        assert_eq!(p.backoff_millis(3), 4000);
        assert_eq!(p.backoff_millis(4), 8000);
        assert_eq!(p.backoff_millis(5), 8000, "capped at max");
        // No overflow at huge attempt counts.
        assert_eq!(p.backoff_millis(1_000_000), 8000);
    }

    // ----- worker selection -----

    fn worker(host: &str, port: u16, cap: u32) -> Worker {
        Worker {
            host: host.to_string(),
            port,
            capacity: cap,
        }
    }

    #[test]
    fn round_robin_cycles() {
        let mut sel = WorkerSelector::new(
            vec![worker("a", 1, 10), worker("b", 1, 10), worker("c", 1, 10)],
            WorkerSelectStrategy::RoundRobin,
        );
        let zero = |_: &str| 0u32;
        assert_eq!(sel.select(&zero).expect("w").host, "a");
        assert_eq!(sel.select(&zero).expect("w").host, "b");
        assert_eq!(sel.select(&zero).expect("w").host, "c");
        assert_eq!(sel.select(&zero).expect("w").host, "a");
    }

    #[test]
    fn round_robin_skips_full() {
        let mut sel = WorkerSelector::new(
            vec![worker("a", 1, 1), worker("b", 1, 10)],
            WorkerSelectStrategy::RoundRobin,
        );
        // 'a' is full.
        let load = |id: &str| if id == "a:1" { 1 } else { 0 };
        assert_eq!(sel.select(&load).expect("w").host, "b");
    }

    #[test]
    fn least_loaded_picks_min() {
        let mut sel = WorkerSelector::new(
            vec![worker("a", 1, 10), worker("b", 1, 10), worker("c", 1, 10)],
            WorkerSelectStrategy::LeastLoaded,
        );
        let load = |id: &str| match id {
            "a:1" => 5,
            "b:1" => 2,
            "c:1" => 8,
            _ => 0,
        };
        assert_eq!(sel.select(&load).expect("w").host, "b");
    }

    #[test]
    fn select_none_when_all_full() {
        let mut sel =
            WorkerSelector::new(vec![worker("a", 1, 1)], WorkerSelectStrategy::LeastLoaded);
        let full = |_: &str| 1u32;
        assert!(sel.select(&full).is_none());
    }

    #[test]
    fn deregister_worker() {
        let mut sel = WorkerSelector::new(
            vec![worker("a", 1, 10), worker("b", 1, 10)],
            WorkerSelectStrategy::RoundRobin,
        );
        assert!(sel.is_registered("a:1"));
        assert!(sel.deregister("a:1"));
        assert!(!sel.is_registered("a:1"));
        assert!(!sel.deregister("a:1"), "second deregister is a no-op");
        assert_eq!(sel.len(), 1);
    }

    #[test]
    fn register_replaces_by_id() {
        let mut sel = WorkerSelector::new(vec![], WorkerSelectStrategy::RoundRobin);
        sel.register(worker("a", 1, 5));
        sel.register(worker("a", 1, 20)); // same id, new capacity
        assert_eq!(sel.len(), 1);
    }
}
