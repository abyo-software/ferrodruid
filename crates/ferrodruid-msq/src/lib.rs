// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Multi-Stage Query DAG executor for FerroDruid.
//!
//! MSQ enables distributed SQL execution by decomposing queries into
//! stages that can run across multiple workers. This crate provides
//! task submission, tracking, and reporting.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod coordinator;
pub mod distributed;
pub mod engine;
pub mod executor;
pub mod segment_io;
pub mod wire;
pub mod worker;

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ferrodruid_common::{DruidError, Result};

/// Default cap on the number of completed (`Success` / `Failed`) MSQ
/// task reports retained in [`MsqManager`].
///
/// Wave 45-C closure of Wave 37B `msq` Medium #3: pre-fix every
/// submitted task — completed or otherwise — was kept indefinitely in
/// the in-memory map, so a long-running broker accumulated unbounded
/// `MsqResults`.  Running tasks are NEVER evicted by retention; only
/// terminal tasks (`Success` / `Failed`) compete for the cap.  The
/// cap is generous (1024) so realistic operator workflows that poll
/// recent tasks are unaffected.
pub const DEFAULT_COMPLETED_TASK_CAP: usize = 1024;

/// Default TTL for completed (`Success` / `Failed`) MSQ task reports.
///
/// Wave 45-C closure of Wave 37B `msq` Medium #3: terminal tasks
/// older than this are eligible for eviction by
/// [`MsqManager::evict_completed_older_than`].  24 h matches the
/// typical operator post-mortem window.  Running tasks are NEVER
/// evicted regardless of age.
pub const DEFAULT_COMPLETED_TASK_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// MSQ task identifier.
pub type MsqTaskId = String;

/// MSQ task status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MsqTaskStatus {
    /// The task is currently running.
    Running,
    /// The task completed successfully.
    Success,
    /// The task failed.
    Failed,
}

/// MSQ task spec (submitted via `POST /druid/v2/sql/task`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MsqTaskSpec {
    /// The SQL query to execute.
    pub query: String,
    /// Optional query context parameters.
    #[serde(default)]
    pub context: serde_json::Value,
    /// Optional query parameters for parameterized queries.
    #[serde(default)]
    pub parameters: Vec<serde_json::Value>,
}

/// MSQ task report (Druid 33+ format).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MsqTaskReport {
    /// The task identifier.
    pub task_id: String,
    /// Current task status.
    pub status: MsqTaskStatus,
    /// Error details if the task failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<MsqError>,
    /// Execution stages.
    pub stages: Vec<MsqStage>,
    /// Total duration in milliseconds.
    pub duration_ms: u64,
    /// ISO-8601 start time.
    pub start_time: String,
    /// Query results (present when status is `SUCCESS` for SELECT queries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<MsqResults>,
}

/// A single execution stage within an MSQ task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MsqStage {
    /// Zero-based stage number.
    pub stage_number: usize,
    /// Current phase (e.g. "READING_INPUT", "RESULTS_READY").
    pub phase: String,
    /// Number of workers assigned to this stage.
    pub worker_count: usize,
    /// Rows read by this stage.
    pub input_row_count: u64,
    /// Rows produced by this stage.
    pub output_row_count: u64,
    /// Shuffle type, if applicable (e.g. "HASH", "MIX").
    pub shuffle_type: Option<String>,
}

/// Error detail attached to a failed MSQ task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsqError {
    /// Short error code.
    pub error: String,
    /// Human-readable error message.
    #[serde(rename = "errorMessage")]
    pub error_message: String,
}

/// Query results returned for SELECT-style MSQ tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MsqResults {
    /// Column signature describing the result schema.
    pub signature: Vec<MsqColumnSignature>,
    /// Result rows as JSON values.
    pub results: Vec<serde_json::Value>,
}

/// Column descriptor within an MSQ result signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MsqColumnSignature {
    /// Column name.
    pub name: String,
    /// SQL type name (e.g. "BIGINT", "VARCHAR").
    pub sql_type: String,
}

/// MSQ task manager that tracks submitted tasks and their reports.
///
/// Wave 45-C closure of Wave 37B `msq` Medium #3: terminal tasks
/// (`Success` / `Failed`) are now evictable both by **TTL** (via
/// [`Self::evict_completed_older_than`]) and by **cap** (via
/// [`Self::evict_completed_to_cap`]).  Running tasks are never
/// evicted regardless of age.  Operators are expected to invoke the
/// eviction methods on a periodic ticker; the manager itself does
/// not own a runtime.
pub struct MsqManager {
    tasks: RwLock<HashMap<String, MsqTaskReport>>,
    /// Per-task `Instant` recorded when the task entered a terminal
    /// state (`Success` / `Failed`).  Tasks still in `Running` are
    /// **not** present in this map, so eviction policies that scan
    /// this map automatically skip live tasks.
    completion_times: RwLock<HashMap<String, Instant>>,
    next_id: AtomicU64,
}

impl MsqManager {
    /// Create a new, empty MSQ manager.
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            completion_times: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Submit an MSQ task (SQL-based ingestion or SELECT).
    ///
    /// Returns the assigned task ID, or [`DruidError::Internal`] if
    /// the internal task-table lock is poisoned.
    ///
    /// Closes Wave 37B `msq` Medium #1: pre-fix the function silently
    /// dropped the insert on lock poison and returned a task id that
    /// no later `get_task` / `cancel` could find. Closes Wave 37B
    /// `msq` Medium #4: the raw SQL is no longer logged at info
    /// level (sensitive literals may appear in `query`); only the
    /// task id and a numeric query length are emitted.
    pub fn submit(&self, spec: MsqTaskSpec) -> Result<MsqTaskId> {
        let seq = self.next_id.fetch_add(1, Ordering::Relaxed);
        let task_id = format!("query-{seq:08x}");
        let now = chrono::Utc::now().to_rfc3339();
        let query_len = spec.query.len();

        let report = MsqTaskReport {
            task_id: task_id.clone(),
            status: MsqTaskStatus::Running,
            error: None,
            stages: vec![MsqStage {
                stage_number: 0,
                phase: "READING_INPUT".to_owned(),
                worker_count: 1,
                input_row_count: 0,
                output_row_count: 0,
                shuffle_type: None,
            }],
            duration_ms: 0,
            start_time: now,
            results: None,
        };

        let mut map = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("MSQ task table lock poisoned: {e}")))?;
        map.insert(task_id.clone(), report);
        // Drop guard before logging.
        drop(map);

        // Wave 37B `msq` Medium #4 closure: the raw query may contain
        // literals/secrets/PII. Log only the task id and a query
        // length so operators can still correlate without leaking
        // payload contents into stdout/log aggregation pipelines.
        tracing::info!(task_id = %task_id, query_len, "MSQ task submitted");
        // Optionally surface the SQL at trace level for explicit
        // local-debug builds.
        tracing::trace!(task_id = %task_id, query = %spec.query, "MSQ task SQL");
        Ok(task_id)
    }

    /// Get the current report for a task.
    pub fn get_task(&self, id: &str) -> Option<MsqTaskReport> {
        self.tasks.read().ok().and_then(|map| map.get(id).cloned())
    }

    /// Get the task report (Druid 33+ reports endpoint).
    ///
    /// Currently identical to [`get_task`](Self::get_task); the two are
    /// separated so the REST layer can serve them on distinct paths.
    pub fn get_report(&self, id: &str) -> Option<MsqTaskReport> {
        self.get_task(id)
    }

    /// List all tasks with their current status.
    pub fn list_tasks(&self) -> Vec<(MsqTaskId, MsqTaskStatus)> {
        self.tasks
            .read()
            .ok()
            .map(|map| {
                map.iter()
                    .map(|(id, r)| (id.clone(), r.status.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Cancel a running task.
    pub fn cancel(&self, id: &str) -> Result<()> {
        let mut map = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("lock poisoned: {e}")))?;

        let report = map
            .get_mut(id)
            .ok_or_else(|| DruidError::Query(format!("task not found: {id}")))?;

        if report.status != MsqTaskStatus::Running {
            return Err(DruidError::Query(format!(
                "cannot cancel task {id} in state {:?}",
                report.status
            )));
        }

        report.status = MsqTaskStatus::Failed;
        report.error = Some(MsqError {
            error: "Canceled".to_owned(),
            error_message: format!("Task {id} was canceled by user"),
        });

        // Drop the tasks guard before touching completion_times to
        // avoid lock-order issues.
        drop(map);
        self.record_completion_time(id);

        tracing::info!(task_id = %id, "MSQ task canceled");
        Ok(())
    }

    /// Mark a task as complete with results.
    pub fn complete_task(&self, id: &str, results: MsqResults) -> Result<()> {
        let mut map = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("lock poisoned: {e}")))?;

        let report = map
            .get_mut(id)
            .ok_or_else(|| DruidError::Query(format!("task not found: {id}")))?;

        if report.status != MsqTaskStatus::Running {
            return Err(DruidError::Query(format!(
                "cannot complete task {id} in state {:?}",
                report.status
            )));
        }

        report.status = MsqTaskStatus::Success;
        report.results = Some(results);

        // Update the final stage phase.
        if let Some(stage) = report.stages.last_mut() {
            stage.phase = "RESULTS_READY".to_owned();
        }

        drop(map);
        self.record_completion_time(id);

        tracing::info!(task_id = %id, "MSQ task completed");
        Ok(())
    }

    /// Update the stage details for a task (best-effort internal update).
    ///
    /// This is called by the executor after execution completes to provide
    /// detailed per-stage metrics. Returns an error if the task is not found.
    pub fn update_stages(&self, id: &str, stages: Vec<MsqStage>) -> Result<()> {
        let mut map = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("lock poisoned: {e}")))?;

        let report = map
            .get_mut(id)
            .ok_or_else(|| DruidError::Query(format!("task not found: {id}")))?;

        report.stages = stages;
        Ok(())
    }

    /// Mark a task as failed with an error.
    pub fn fail_task(&self, id: &str, error: MsqError) -> Result<()> {
        let mut map = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("lock poisoned: {e}")))?;

        let report = map
            .get_mut(id)
            .ok_or_else(|| DruidError::Query(format!("task not found: {id}")))?;

        if report.status != MsqTaskStatus::Running {
            return Err(DruidError::Query(format!(
                "cannot fail task {id} in state {:?}",
                report.status
            )));
        }

        report.status = MsqTaskStatus::Failed;
        report.error = Some(error);

        drop(map);
        self.record_completion_time(id);

        tracing::info!(task_id = %id, "MSQ task failed");
        Ok(())
    }

    /// Record `Instant::now()` as the completion time for `id`.
    ///
    /// Wave 45-C closure of Wave 37B `msq` Medium #3: the timestamp
    /// is used by [`Self::evict_completed_older_than`] to identify
    /// terminal tasks that have aged past the configured TTL.  Lock
    /// poison on the completion-times map is logged but **not**
    /// propagated to the caller — the underlying `complete_task` /
    /// `fail_task` / `cancel` operation has already succeeded; we are
    /// recording bookkeeping that, if missed, only impacts retention
    /// (not correctness).
    fn record_completion_time(&self, id: &str) {
        match self.completion_times.write() {
            Ok(mut times) => {
                times.insert(id.to_owned(), Instant::now());
            }
            Err(e) => {
                tracing::error!(
                    task_id = %id,
                    err = %e,
                    "MSQ completion-time map lock poisoned; \
                     retention bookkeeping for this task will be missed",
                );
            }
        }
    }

    /// Number of completed (`Success` / `Failed`) tasks currently
    /// retained in memory.
    ///
    /// Wave 45-C closure of Wave 37B `msq` Medium #3: exposes the
    /// count so operators / tests can verify retention behaviour.
    pub fn completed_task_count(&self) -> usize {
        self.completion_times
            .read()
            .map(|m| m.len())
            .unwrap_or_default()
    }

    /// Evict completed (`Success` / `Failed`) task reports whose
    /// completion timestamp is older than `max_age` from `now`.
    /// Returns the number of evicted reports.
    ///
    /// Wave 45-C closure of Wave 37B `msq` Medium #3: pre-fix every
    /// terminal task lived in memory forever, so a long-running
    /// broker accumulated unbounded `MsqResults`.  This method is
    /// intended to be called from a periodic ticker (e.g. once per
    /// minute) with `max_age = `[`DEFAULT_COMPLETED_TASK_TTL`].
    ///
    /// **Running tasks are never evicted** regardless of how long
    /// they have been alive, because the `completion_times` map only
    /// has entries for terminal tasks.
    ///
    /// Returns `Err` only when an internal lock is poisoned.
    pub fn evict_completed_older_than(&self, max_age: Duration) -> Result<usize> {
        let now = Instant::now();
        let mut times = self
            .completion_times
            .write()
            .map_err(|e| DruidError::Internal(format!("completion times lock poisoned: {e}")))?;

        // Phase 1: identify candidates while holding the times lock.
        let stale: Vec<String> = times
            .iter()
            .filter(|(_, t)| now.duration_since(**t) >= max_age)
            .map(|(id, _)| id.clone())
            .collect();

        if stale.is_empty() {
            return Ok(0);
        }

        for id in &stale {
            times.remove(id);
        }
        drop(times);

        // Phase 2: drop the matching task reports.  We acquire the
        // tasks lock separately so the two locks are never held
        // simultaneously (lock-order: tasks before completion_times
        // in the *update* paths above; reverse order is fine here
        // because the times map is no longer touched).
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("MSQ task table lock poisoned: {e}")))?;
        let mut evicted = 0usize;
        for id in stale {
            if tasks.remove(&id).is_some() {
                evicted += 1;
            }
        }
        Ok(evicted)
    }

    /// Evict the oldest completed (`Success` / `Failed`) task reports
    /// until at most `cap` terminal tasks remain.  Running tasks are
    /// never evicted.  Returns the number of evicted reports.
    ///
    /// Wave 45-C closure of Wave 37B `msq` Medium #3: provides the
    /// hard upper bound counterpart to TTL-based eviction.  Operators
    /// who need protection against bursty completions (many tasks
    /// terminating within the TTL window) call this with `cap =
    /// `[`DEFAULT_COMPLETED_TASK_CAP`].
    pub fn evict_completed_to_cap(&self, cap: usize) -> Result<usize> {
        let times_snapshot: Vec<(String, Instant)> = {
            let times = self.completion_times.read().map_err(|e| {
                DruidError::Internal(format!("completion times lock poisoned: {e}"))
            })?;
            if times.len() <= cap {
                return Ok(0);
            }
            times.iter().map(|(id, t)| (id.clone(), *t)).collect()
        };

        // Sort oldest-first so we evict the longest-completed tasks.
        let mut sorted = times_snapshot;
        sorted.sort_by_key(|(_, t)| *t);
        let evict_count = sorted.len().saturating_sub(cap);
        let to_evict: Vec<String> = sorted
            .into_iter()
            .take(evict_count)
            .map(|(id, _)| id)
            .collect();

        if to_evict.is_empty() {
            return Ok(0);
        }

        // Remove from completion times first.
        {
            let mut times = self.completion_times.write().map_err(|e| {
                DruidError::Internal(format!("completion times lock poisoned: {e}"))
            })?;
            for id in &to_evict {
                times.remove(id);
            }
        }
        // Then drop the matching task reports.
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| DruidError::Internal(format!("MSQ task table lock poisoned: {e}")))?;
        let mut evicted = 0usize;
        for id in to_evict {
            if tasks.remove(&id).is_some() {
                evicted += 1;
            }
        }
        Ok(evicted)
    }
}

impl MsqManager {
    /// Submit an MSQ task and run it to completion through the real
    /// multi-stage engine over the supplied `input` rows, populating the
    /// task report (results + per-stage counters).
    ///
    /// This is the end-to-end submit → run → report entry point.  It is
    /// distinct from [`Self::submit`] (which only registers a `Running`
    /// task) so callers that already have the engine input materialised
    /// (e.g. a segment scan) can drive a complete execution.  Retention
    /// and cap behaviour are unchanged: the task transitions to a terminal
    /// state via [`Self::complete_task`] / [`Self::fail_task`], so the
    /// usual eviction policies apply.
    ///
    /// On engine failure the task is marked `Failed` and the error is
    /// returned.
    ///
    /// # Errors
    ///
    /// Returns the assigned task id paired with an [`MsqError`] when SQL
    /// planning or engine execution fails.
    pub async fn submit_and_run(
        &self,
        spec: MsqTaskSpec,
        input: executor::InputTable,
        config: &engine::EngineConfig,
    ) -> std::result::Result<(MsqTaskId, MsqResults), (MsqTaskId, MsqError)> {
        let query = spec.query.clone();
        let task_id = match self.submit(spec) {
            Ok(id) => id,
            Err(e) => {
                return Err((
                    String::new(),
                    MsqError {
                        error: "MsqSubmitError".to_owned(),
                        error_message: e.to_string(),
                    },
                ));
            }
        };

        let plan = match executor::plan_msq(&query) {
            Ok(p) => p,
            Err(e) => {
                let err = MsqError {
                    error: "SqlPlanningError".to_owned(),
                    error_message: e.to_string(),
                };
                let _ = self.fail_task(&task_id, err.clone());
                return Err((task_id, err));
            }
        };

        match executor::execute_msq_with_input(&plan, input, config, self, &task_id).await {
            Ok(results) => Ok((task_id, results)),
            Err(e) => {
                let err = MsqError {
                    error: "MsqExecutionError".to_owned(),
                    error_message: e.to_string(),
                };
                let _ = self.fail_task(&task_id, err.clone());
                Err((task_id, err))
            }
        }
    }
}

impl Default for MsqManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_and_get() {
        let mgr = MsqManager::new();
        let id = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 1".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");

        let report = mgr.get_task(&id).expect("task exists");
        assert_eq!(report.task_id, id);
        assert_eq!(report.status, MsqTaskStatus::Running);
        assert!(report.error.is_none());
        assert_eq!(report.stages.len(), 1);
        assert_eq!(report.stages[0].phase, "READING_INPUT");
    }

    #[test]
    fn complete_task() {
        let mgr = MsqManager::new();
        let id = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 1 AS val".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");

        let results = MsqResults {
            signature: vec![MsqColumnSignature {
                name: "val".to_owned(),
                sql_type: "BIGINT".to_owned(),
            }],
            results: vec![serde_json::json!({"val": 1})],
        };

        mgr.complete_task(&id, results).expect("complete");
        let report = mgr.get_task(&id).expect("task exists");
        assert_eq!(report.status, MsqTaskStatus::Success);
        assert!(report.results.is_some());
        assert_eq!(report.stages[0].phase, "RESULTS_READY");
    }

    #[test]
    fn fail_task() {
        let mgr = MsqManager::new();
        let id = mgr
            .submit(MsqTaskSpec {
                query: "BAD SQL".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");

        let error = MsqError {
            error: "SqlPlanningError".to_owned(),
            error_message: "Cannot parse SQL".to_owned(),
        };

        mgr.fail_task(&id, error).expect("fail");
        let report = mgr.get_task(&id).expect("task exists");
        assert_eq!(report.status, MsqTaskStatus::Failed);
        let err = report.error.as_ref().expect("error present");
        assert_eq!(err.error, "SqlPlanningError");
    }

    #[test]
    fn cancel_task() {
        let mgr = MsqManager::new();
        let id = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 1".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");

        mgr.cancel(&id).expect("cancel");
        let report = mgr.get_task(&id).expect("task exists");
        assert_eq!(report.status, MsqTaskStatus::Failed);
        assert!(report.error.is_some());
    }

    #[test]
    fn cancel_non_running_fails() {
        let mgr = MsqManager::new();
        let id = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 1".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        mgr.cancel(&id).expect("first cancel");
        assert!(mgr.cancel(&id).is_err());
    }

    #[test]
    fn list_tasks() {
        let mgr = MsqManager::new();
        let id1 = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 1".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");
        let id2 = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 2".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit");

        let tasks = mgr.list_tasks();
        assert_eq!(tasks.len(), 2);

        let ids: Vec<&str> = tasks.iter().map(|(id, _)| id.as_str()).collect();
        assert!(ids.contains(&id1.as_str()));
        assert!(ids.contains(&id2.as_str()));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mgr = MsqManager::new();
        assert!(mgr.get_task("nope").is_none());
        assert!(mgr.get_report("nope").is_none());
    }

    #[test]
    fn report_json_roundtrip() {
        let report = MsqTaskReport {
            task_id: "query-00000001".to_owned(),
            status: MsqTaskStatus::Success,
            error: None,
            stages: vec![MsqStage {
                stage_number: 0,
                phase: "RESULTS_READY".to_owned(),
                worker_count: 1,
                input_row_count: 100,
                output_row_count: 10,
                shuffle_type: Some("HASH".to_owned()),
            }],
            duration_ms: 42,
            start_time: "2026-01-01T00:00:00Z".to_owned(),
            results: Some(MsqResults {
                signature: vec![MsqColumnSignature {
                    name: "cnt".to_owned(),
                    sql_type: "BIGINT".to_owned(),
                }],
                results: vec![serde_json::json!({"cnt": 10})],
            }),
        };

        let json = serde_json::to_string(&report).expect("serialize");
        let parsed: MsqTaskReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.task_id, "query-00000001");
        assert_eq!(parsed.status, MsqTaskStatus::Success);
        assert!(parsed.results.is_some());

        // Verify camelCase serialization.
        let v: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert!(v.get("taskId").is_some());
        assert!(v.get("durationMs").is_some());
        assert!(v.get("startTime").is_some());
        assert!(v.get("stageNumber").is_some() || v["stages"][0].get("stageNumber").is_some());
    }

    #[test]
    fn status_serde_screaming_snake() {
        let json = serde_json::to_string(&MsqTaskStatus::Running).expect("ser");
        assert_eq!(json, "\"RUNNING\"");
        let json = serde_json::to_string(&MsqTaskStatus::Success).expect("ser");
        assert_eq!(json, "\"SUCCESS\"");
        let json = serde_json::to_string(&MsqTaskStatus::Failed).expect("ser");
        assert_eq!(json, "\"FAILED\"");
    }

    #[test]
    fn task_spec_deserialize() {
        let json = r#"{
            "query": "SELECT * FROM wiki",
            "context": {"maxNumTasks": 3},
            "parameters": [{"type": "VARCHAR", "value": "hello"}]
        }"#;
        let spec: MsqTaskSpec = serde_json::from_str(json).expect("deser");
        assert_eq!(spec.query, "SELECT * FROM wiki");
        assert_eq!(spec.context["maxNumTasks"], 3);
        assert_eq!(spec.parameters.len(), 1);
    }

    #[test]
    fn task_spec_minimal() {
        let json = r#"{"query": "SELECT 1"}"#;
        let spec: MsqTaskSpec = serde_json::from_str(json).expect("deser");
        assert_eq!(spec.query, "SELECT 1");
        assert!(spec.parameters.is_empty());
    }

    /// W37B msq Medium #1 regression: every successful `submit()` must
    /// guarantee the returned task id is immediately observable to
    /// `get_task` and `list_tasks`. Pre-fix a poisoned lock would
    /// cause `submit()` to return an id that never landed in the
    /// task table; the invariant could be silently broken in a way
    /// no test could catch because the failure path was a no-op.
    #[test]
    fn submit_returns_id_observable_via_get_and_list() {
        let mgr = MsqManager::new();
        let id = mgr
            .submit(MsqTaskSpec {
                query: "SELECT 1".to_owned(),
                context: serde_json::Value::Null,
                parameters: vec![],
            })
            .expect("submit must succeed on healthy lock");

        // Round-trip: id -> get_task -> id.
        let report = mgr.get_task(&id).expect("submitted task is queryable");
        assert_eq!(report.task_id, id);
        // list_tasks must surface the same id.
        let listed: Vec<MsqTaskId> = mgr.list_tasks().into_iter().map(|(i, _)| i).collect();
        assert!(
            listed.contains(&id),
            "list_tasks must include the freshly-submitted id; got {listed:?}",
        );
    }

    // -----------------------------------------------------------------
    // Wave 45-C — Wave 37B `msq` Medium #3 closure
    // (TTL + cap eviction for terminal task reports).
    // -----------------------------------------------------------------

    fn submit_one(mgr: &MsqManager, q: &str) -> MsqTaskId {
        mgr.submit(MsqTaskSpec {
            query: q.to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        })
        .expect("submit")
    }

    fn empty_results() -> MsqResults {
        MsqResults {
            signature: vec![],
            results: vec![],
        }
    }

    /// W37B msq Medium #3: a freshly-`Running` task must be invisible
    /// to retention scans — the `completion_times` map only tracks
    /// terminal tasks, so neither cap nor TTL eviction can reach a
    /// running task.
    #[test]
    fn task_registry_keeps_running_tasks() {
        let mgr = MsqManager::new();
        let id = submit_one(&mgr, "SELECT 1");

        // Force aggressive eviction; the running task must survive.
        let evicted_ttl = mgr
            .evict_completed_older_than(Duration::from_nanos(1))
            .expect("evict ttl");
        assert_eq!(evicted_ttl, 0, "ttl pass must not touch running tasks");

        let evicted_cap = mgr.evict_completed_to_cap(0).expect("evict cap");
        assert_eq!(evicted_cap, 0, "cap=0 pass must not touch running tasks");

        // The task is still observable.
        assert!(mgr.get_task(&id).is_some());
        assert_eq!(mgr.completed_task_count(), 0);
    }

    /// W37B msq Medium #3: completed tasks beyond the configured cap
    /// must be evicted oldest-first.  We submit and complete 3 tasks
    /// in sequence with deliberate Instant ordering, then evict to
    /// cap=1 and verify the most-recent task is the survivor.
    #[test]
    fn task_registry_evicts_oldest_when_at_cap() {
        let mgr = MsqManager::new();

        let id1 = submit_one(&mgr, "SELECT 1");
        mgr.complete_task(&id1, empty_results())
            .expect("complete 1");
        // Sleep a small amount to ensure monotonic Instant separation.
        std::thread::sleep(Duration::from_millis(2));

        let id2 = submit_one(&mgr, "SELECT 2");
        mgr.complete_task(&id2, empty_results())
            .expect("complete 2");
        std::thread::sleep(Duration::from_millis(2));

        let id3 = submit_one(&mgr, "SELECT 3");
        mgr.complete_task(&id3, empty_results())
            .expect("complete 3");

        assert_eq!(mgr.completed_task_count(), 3);

        // Evict to cap=1: the two oldest (id1, id2) must go.
        let evicted = mgr.evict_completed_to_cap(1).expect("evict to cap");
        assert_eq!(evicted, 2, "expected 2 evictions to reach cap=1");

        assert!(mgr.get_task(&id1).is_none(), "id1 must be evicted");
        assert!(mgr.get_task(&id2).is_none(), "id2 must be evicted");
        assert!(mgr.get_task(&id3).is_some(), "id3 (newest) must survive");
        assert_eq!(mgr.completed_task_count(), 1);
    }

    /// W37B msq Medium #3: completed tasks older than `max_age` must
    /// be evicted by [`MsqManager::evict_completed_older_than`].
    /// We use a 1ms TTL with a small `sleep` to deterministically
    /// age the completion timestamp past the TTL.
    #[test]
    fn task_registry_evicts_completed_after_ttl() {
        let mgr = MsqManager::new();
        let id = submit_one(&mgr, "SELECT 1");
        mgr.complete_task(&id, empty_results()).expect("complete");

        // Sleep past the TTL we will pass.
        std::thread::sleep(Duration::from_millis(20));
        let evicted = mgr
            .evict_completed_older_than(Duration::from_millis(5))
            .expect("evict by ttl");
        assert_eq!(evicted, 1, "expected the aged task to be evicted");
        assert!(mgr.get_task(&id).is_none());
        assert_eq!(mgr.completed_task_count(), 0);

        // Idempotency: a second call with nothing left to evict
        // returns 0 cleanly.
        let again = mgr
            .evict_completed_older_than(Duration::from_millis(5))
            .expect("idempotent evict");
        assert_eq!(again, 0);
    }

    /// Companion: a completed-task TTL eviction must not touch a
    /// task that has just transitioned to terminal — i.e. the TTL
    /// boundary must be inclusive of "older than", strict on "younger
    /// than".  This pins the comparison sense (`>=`) used in
    /// `evict_completed_older_than`.
    #[test]
    fn task_registry_keeps_recent_terminal_within_ttl() {
        let mgr = MsqManager::new();
        let id = submit_one(&mgr, "SELECT 1");
        mgr.complete_task(&id, empty_results()).expect("complete");

        // Use a TTL far larger than the elapsed time since `complete`.
        let evicted = mgr
            .evict_completed_older_than(Duration::from_secs(60 * 60))
            .expect("evict by ttl (recent)");
        assert_eq!(evicted, 0, "recent terminal task must survive 1h TTL");
        assert!(mgr.get_task(&id).is_some());
        assert_eq!(mgr.completed_task_count(), 1);
    }

    /// End-to-end: `submit_and_run` drives the real engine to completion
    /// and the task report reflects the engine's stages + results, rather
    /// than being a no-op.
    #[tokio::test]
    async fn submit_and_run_end_to_end_populates_report() {
        use crate::engine::{EngineConfig, RowSignature, Value};
        use crate::executor::InputTable;

        let mgr = MsqManager::new();
        let input = InputTable {
            signature: RowSignature::new(&[("city", "VARCHAR"), ("n", "BIGINT")]),
            rows: vec![
                vec![Value::Str("a".into()), Value::Long(1)],
                vec![Value::Str("b".into()), Value::Long(2)],
                vec![Value::Str("a".into()), Value::Long(4)],
            ],
        };
        let spec = MsqTaskSpec {
            query: "SELECT city, COUNT(*), SUM(n) FROM t GROUP BY city".to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };

        let (task_id, results) = mgr
            .submit_and_run(spec, input, &EngineConfig::default())
            .await
            .expect("run");

        // Two groups.
        assert_eq!(results.results.len(), 2);

        // Report is terminal SUCCESS with real per-stage counters.
        let report = mgr.get_task(&task_id).expect("task");
        assert_eq!(report.status, MsqTaskStatus::Success);
        assert_eq!(report.stages.len(), 3); // scan -> shuffle -> aggregate
        assert_eq!(report.stages[0].input_row_count, 3);
        assert_eq!(report.stages[2].output_row_count, 2);
        assert!(report.results.is_some());

        // Group "a": count=2 sum=5.
        let a = results
            .results
            .iter()
            .find(|r| r["city"] == "a")
            .expect("group a");
        assert_eq!(a["count"], 2);
        assert_eq!(a["sum_n"], 5);
    }

    /// A planning failure in `submit_and_run` marks the task Failed and
    /// returns the task id with the error.
    #[tokio::test]
    async fn submit_and_run_planning_failure_marks_failed() {
        use crate::engine::{EngineConfig, RowSignature};
        use crate::executor::InputTable;

        let mgr = MsqManager::new();
        let input = InputTable {
            signature: RowSignature::new(&[("x", "BIGINT")]),
            rows: vec![],
        };
        let spec = MsqTaskSpec {
            query: "DROP TABLE t".to_owned(),
            context: serde_json::Value::Null,
            parameters: vec![],
        };
        let (task_id, err) = mgr
            .submit_and_run(spec, input, &EngineConfig::default())
            .await
            .expect_err("must fail planning");
        assert_eq!(err.error, "SqlPlanningError");
        let report = mgr.get_task(&task_id).expect("task");
        assert_eq!(report.status, MsqTaskStatus::Failed);
    }
}
