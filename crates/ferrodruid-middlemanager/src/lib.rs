// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Ingestion task execution via tokio tasks (no JVM fork) for FerroDruid.
//!
//! The [`MiddleManager`] runs ingestion tasks as lightweight tokio tasks,
//! tracking their lifecycle, resource usage, and capacity.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod kafka_task;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

use kafka_task::{
    InMemoryCheckpointStore, InMemoryKafkaSource, KafkaIndexTaskExecutor, KafkaIndexTaskSpec,
    OffsetRange, TopicPartition,
};

/// Default cap on the number of TERMINAL (`Completed` / `Failed`) task
/// handles retained in a [`MiddleManager`].
///
/// DD R11 #1 closure of the DD R10 #1 fix: R10 stopped terminal handles
/// from consuming a capacity slot, but they were still retained in the
/// `running_tasks` map forever, so an unbounded stream of unique fast
/// tasks grew the map without limit (memory leak). Terminal handles now
/// compete for this cap; the oldest (by completion order) are evicted
/// FIFO once the cap is exceeded. Running (non-terminal) tasks are NEVER
/// evicted regardless of this cap. The cap is generous so realistic
/// operator workflows that poll a recently-completed task are
/// unaffected. Mirrors the MSQ crate's `DEFAULT_COMPLETED_TASK_CAP`.
pub const DEFAULT_COMPLETED_TASK_CAP: usize = 1024;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from MiddleManager operations.
#[derive(Debug, Error)]
pub enum MiddleManagerError {
    /// No capacity to accept another task.
    #[error("no capacity: {running}/{max} tasks running")]
    NoCapacity {
        /// Currently running tasks.
        running: usize,
        /// Maximum allowed concurrent tasks.
        max: usize,
    },
    /// Duplicate task ID submitted.
    #[error("duplicate task id: {0}")]
    DuplicateTask(String),
    /// Task not found.
    #[error("task not found: {0}")]
    TaskNotFound(String),
    /// Internal lock error.
    #[error("internal lock error")]
    LockError,
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, MiddleManagerError>;

// ---------------------------------------------------------------------------
// Task status
// ---------------------------------------------------------------------------

/// Status of a running or completed ingestion task.
#[derive(Debug, Clone)]
pub enum TaskRunStatus {
    /// Task is currently running.
    Running,
    /// Task completed successfully.
    Completed {
        /// Total rows ingested.
        rows: u64,
        /// Number of segments produced.
        segments: usize,
    },
    /// Task failed with an error.
    Failed {
        /// Description of the failure.
        error: String,
    },
}

impl TaskRunStatus {
    /// Whether this status is terminal (the worker has finished and the task
    /// no longer occupies a capacity slot). `Completed` and `Failed` are
    /// terminal; `Running` is not.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed { .. } | Self::Failed { .. })
    }
}

// ---------------------------------------------------------------------------
// Task handle
// ---------------------------------------------------------------------------

/// Handle to a running or finished ingestion task.
#[derive(Debug)]
pub struct TaskHandle {
    /// Unique task identifier.
    pub task_id: String,
    /// Target data source.
    pub data_source: String,
    /// Type of ingestion task (e.g. `"kafka"`, `"index_parallel"`).
    pub task_type: String,
    /// Current status.
    pub status: TaskRunStatus,
    /// When the task was submitted.
    pub start_time: DateTime<Utc>,
    /// Rows processed so far (atomically updated by the worker).
    pub rows_processed: Arc<AtomicU64>,
    /// Cancel sender — dropping or sending signals the worker to stop.
    cancel_tx: Option<mpsc::Sender<()>>,
    /// Monotonic sequence assigned when this handle reached a TERMINAL
    /// status, used to evict the oldest terminal handles FIFO once the
    /// retained-history cap is exceeded (DD R11 #1). `None` while the
    /// task is still `Running`; never set for non-terminal tasks so they
    /// are never eligible for eviction.
    terminal_seq: Option<u64>,
}

// ---------------------------------------------------------------------------
// Ingestion specs
// ---------------------------------------------------------------------------

/// Ingestion spec that describes what to ingest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum IngestionSpec {
    /// Kafka supervisor spec (streaming).
    #[serde(rename = "kafka")]
    Kafka(KafkaIngestionSpec),
    /// Native batch index spec.
    #[serde(rename = "index_parallel")]
    IndexParallel(BatchIngestionSpec),
}

impl IngestionSpec {
    /// Return the data source name from the spec.
    pub fn data_source(&self) -> &str {
        match self {
            Self::Kafka(k) => &k.data_source,
            Self::IndexParallel(b) => &b.data_source,
        }
    }

    /// Return a human-readable task type.
    pub fn task_type(&self) -> &str {
        match self {
            Self::Kafka(_) => "kafka",
            Self::IndexParallel(_) => "index_parallel",
        }
    }
}

/// A single partition's assigned offset range within a Kafka task.
///
/// Mirrors Druid's per-partition `SeekableStreamStartSequenceNumbers` /
/// end sequence numbers: `start_offset` is inclusive, `end_offset` is
/// exclusive (and `None` means open-ended — drain to the high-water
/// mark).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionOffsetSpec {
    /// Partition id within the topic.
    pub partition: i32,
    /// First offset to consume (inclusive).
    pub start_offset: i64,
    /// First offset to stop at (exclusive); `None` is open-ended.
    #[serde(default)]
    pub end_offset: Option<i64>,
}

/// Kafka streaming ingestion spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaIngestionSpec {
    /// Target data source.
    pub data_source: String,
    /// Kafka topic to consume.
    pub topic: String,
    /// Kafka consumer properties (bootstrap.servers, etc.).
    pub consumer_properties: HashMap<String, String>,
    /// Column to use as the event timestamp (defaults to `__time`).
    pub timestamp_column: Option<String>,
    /// Dimension column names.
    pub dimensions: Vec<String>,
    /// Metric aggregator specs (JSON objects).
    pub metrics: Vec<serde_json::Value>,
    /// Per-partition offset assignments for this task. When empty the
    /// Kafka arm runs in legacy "wait until cancelled" mode (no real
    /// consumption); when present the task consumes exactly these
    /// partitions and offset ranges from the injected source.
    #[serde(default)]
    pub partitions: Vec<PartitionOffsetSpec>,
    /// Max rows to buffer before flushing a segment (default 5,000,000).
    #[serde(default)]
    pub max_rows_per_segment: Option<usize>,
    /// Checkpoint after this many newly-consumed records (default 1,000;
    /// 0 disables periodic checkpointing).
    #[serde(default)]
    pub checkpoint_every_rows: Option<u64>,
}

/// Native batch ingestion spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchIngestionSpec {
    /// Target data source.
    pub data_source: String,
    /// Where to read input data from.
    pub input_source: InputSource,
    /// Column to use as the event timestamp (defaults to `__time`).
    pub timestamp_column: Option<String>,
    /// Dimension column names.
    pub dimensions: Vec<String>,
    /// Metric aggregator specs (JSON objects).
    pub metrics: Vec<serde_json::Value>,
}

/// Source of input data for batch ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum InputSource {
    /// Read from local filesystem.
    #[serde(rename = "local")]
    Local {
        /// Base directory containing input files.
        base_dir: String,
        /// Optional glob or regex filter for file names.
        filter: Option<String>,
    },
    /// Inline JSON data embedded in the spec.
    #[serde(rename = "inline")]
    Inline {
        /// The rows of data.
        data: Vec<serde_json::Value>,
    },
}

// ---------------------------------------------------------------------------
// MiddleManager
// ---------------------------------------------------------------------------

/// Executes ingestion tasks as lightweight tokio tasks.
pub struct MiddleManager {
    running_tasks: Arc<RwLock<HashMap<String, TaskHandle>>>,
    max_concurrent_tasks: usize,
    /// Record source for Kafka index tasks. When `None`, Kafka specs
    /// with partition assignments fail fast (no source wired); when set,
    /// assigned-partition consumption runs against it. The
    /// [`InMemoryKafkaSource`] stand-in is used in tests and embedded
    /// single-process setups; a real `rdkafka`-backed source would
    /// implement the same [`KafkaSource`] trait.
    kafka_source: Option<Arc<InMemoryKafkaSource>>,
    /// Checkpoint store for Kafka index tasks (resume-from-checkpoint).
    checkpoint_store: Arc<InMemoryCheckpointStore>,
    /// Upper bound on the number of TERMINAL (`Completed` / `Failed`)
    /// task handles retained in `running_tasks` (DD R11 #1). Once
    /// exceeded, the oldest terminal handles are evicted FIFO. Running
    /// tasks are never evicted.
    max_completed_history: usize,
    /// Monotonic counter that assigns each task a completion-order
    /// sequence number when it transitions to a terminal status, so the
    /// oldest terminal handles can be evicted FIFO.
    completion_seq: Arc<AtomicU64>,
    _shutdown_tx: Option<mpsc::Sender<()>>,
}

impl MiddleManager {
    /// Create a new `MiddleManager` with the given concurrency limit.
    ///
    /// No Kafka record source is wired; Kafka tasks carrying partition
    /// assignments will fail until one is supplied via
    /// [`MiddleManager::with_kafka_source`]. Batch tasks and legacy
    /// (assignment-free) Kafka tasks work unchanged.
    pub fn new(max_concurrent_tasks: usize) -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent_tasks,
            kafka_source: None,
            checkpoint_store: Arc::new(InMemoryCheckpointStore::new()),
            max_completed_history: DEFAULT_COMPLETED_TASK_CAP,
            completion_seq: Arc::new(AtomicU64::new(0)),
            _shutdown_tx: None,
        }
    }

    /// Set the cap on retained TERMINAL (`Completed` / `Failed`) task
    /// handles (DD R11 #1). Builder-style; returns `self`. Running tasks
    /// are never evicted regardless of this cap. A cap of `0` retains no
    /// terminal history at all (terminal handles are evicted as soon as
    /// the worker records them).
    #[must_use]
    pub fn with_max_completed_history(mut self, cap: usize) -> Self {
        self.max_completed_history = cap;
        self
    }

    /// Create a `MiddleManager` with an injected Kafka record source so
    /// that Kafka index tasks with partition assignments execute real
    /// (in-memory) consumption + checkpointing.
    pub fn with_kafka_source(
        max_concurrent_tasks: usize,
        source: Arc<InMemoryKafkaSource>,
    ) -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent_tasks,
            kafka_source: Some(source),
            checkpoint_store: Arc::new(InMemoryCheckpointStore::new()),
            max_completed_history: DEFAULT_COMPLETED_TASK_CAP,
            completion_seq: Arc::new(AtomicU64::new(0)),
            _shutdown_tx: None,
        }
    }

    /// Access the checkpoint store (e.g. to inspect persisted offsets).
    #[must_use]
    pub fn checkpoint_store(&self) -> Arc<InMemoryCheckpointStore> {
        Arc::clone(&self.checkpoint_store)
    }

    /// Submit an ingestion task. Returns the task ID on success.
    ///
    /// The task runs as a background tokio task. For `index_parallel` with
    /// inline data the ingestion completes quickly; for `kafka` the task
    /// runs until cancelled.
    pub async fn submit_task(&self, task_id: String, spec: IngestionSpec) -> Result<()> {
        let (cancel_tx, mut cancel_rx) = mpsc::channel::<()>(1);
        let rows_processed = Arc::new(AtomicU64::new(0));
        // Cooperative cancellation flag observed by the (blocking) Kafka
        // executor and by the legacy stub wait.
        let cancel_flag = Arc::new(AtomicBool::new(false));

        let handle = TaskHandle {
            task_id: task_id.clone(),
            data_source: spec.data_source().to_string(),
            task_type: spec.task_type().to_string(),
            status: TaskRunStatus::Running,
            start_time: Utc::now(),
            rows_processed: Arc::clone(&rows_processed),
            cancel_tx: Some(cancel_tx),
            terminal_seq: None,
        };

        // Admission is atomic (DD R11 #2): take the WRITE lock ONCE,
        // re-check capacity + duplicate, and insert the Running handle
        // while still holding the lock. The previous read-then-write
        // split let two concurrent submits both pass the capacity check
        // before either inserted, oversubscribing beyond
        // `max_concurrent_tasks`. The critical section holds the std
        // RwLock with no `.await` inside it; the worker is spawned only
        // AFTER the guard is dropped.
        {
            let mut tasks = self
                .running_tasks
                .write()
                .map_err(|_| MiddleManagerError::LockError)?;
            // Capacity is consumed only by non-terminal (Running) tasks.
            // Terminal handles (Completed/Failed) are retained for status
            // lookup/history but must NOT count against the slot budget,
            // otherwise short tasks that have already finished would
            // permanently exhaust capacity (DD R10 #2).
            let running = tasks.values().filter(|h| !h.status.is_terminal()).count();
            if running >= self.max_concurrent_tasks {
                return Err(MiddleManagerError::NoCapacity {
                    running,
                    max: self.max_concurrent_tasks,
                });
            }
            if tasks.contains_key(&task_id) {
                return Err(MiddleManagerError::DuplicateTask(task_id));
            }
            tasks.insert(task_id.clone(), handle);
        }

        // Spawn the background worker
        let tasks_map = Arc::clone(&self.running_tasks);
        let tid = task_id.clone();
        let rows_counter = Arc::clone(&rows_processed);
        let kafka_source = self.kafka_source.clone();
        let checkpoint_store = Arc::clone(&self.checkpoint_store);
        let completion_seq = Arc::clone(&self.completion_seq);
        let max_completed_history = self.max_completed_history;

        // Bridge the cancel channel to the cancel flag: the first cancel
        // signal flips the flag, which the worker polls.
        let cancel_flag_setter = Arc::clone(&cancel_flag);
        tokio::spawn(async move {
            if cancel_rx.recv().await.is_some() {
                cancel_flag_setter.store(true, Ordering::SeqCst);
            }
        });

        tokio::spawn(async move {
            let result = Self::run_task_inner(
                tid.clone(),
                spec,
                &rows_counter,
                kafka_source,
                checkpoint_store,
                Arc::clone(&cancel_flag),
            )
            .await;

            // Update status
            if let Ok(mut tasks) = tasks_map.write() {
                if let Some(handle) = tasks.get_mut(&tid) {
                    match result {
                        Ok((rows, segments)) => {
                            handle.status = TaskRunStatus::Completed { rows, segments };
                        }
                        Err(e) => {
                            handle.status = TaskRunStatus::Failed {
                                error: e.to_string(),
                            };
                        }
                    }
                    // Clear cancel sender
                    handle.cancel_tx = None;
                    // Stamp the terminal completion order so this handle
                    // can be evicted FIFO once the history cap is
                    // exceeded (DD R11 #1).
                    handle.terminal_seq = Some(completion_seq.fetch_add(1, Ordering::SeqCst));
                }
                // Bound retained terminal-task history: evict the oldest
                // terminal handles (by completion order) beyond the cap.
                // Running (non-terminal) tasks are never evicted.
                Self::evict_terminal_over_cap(&mut tasks, max_completed_history);
            }
        });

        Ok(())
    }

    /// Build a resolved [`KafkaIndexTaskSpec`] from the wire spec.
    fn build_kafka_index_task(task_id: &str, kafka: &KafkaIngestionSpec) -> KafkaIndexTaskSpec {
        let mut assignment: BTreeMap<TopicPartition, OffsetRange> = BTreeMap::new();
        for p in &kafka.partitions {
            let tp = TopicPartition::new(kafka.topic.clone(), p.partition);
            let range = match p.end_offset {
                Some(end) => OffsetRange::bounded(p.start_offset, end),
                None => OffsetRange::open(p.start_offset),
            };
            assignment.insert(tp, range);
        }
        KafkaIndexTaskSpec {
            task_id: task_id.to_owned(),
            data_source: kafka.data_source.clone(),
            timestamp_column: kafka
                .timestamp_column
                .clone()
                .unwrap_or_else(|| "__time".to_owned()),
            dimensions: kafka.dimensions.clone(),
            metrics: kafka.metrics.clone(),
            max_rows_per_segment: kafka.max_rows_per_segment.unwrap_or(5_000_000),
            checkpoint_every_rows: kafka.checkpoint_every_rows.unwrap_or(1_000),
            assignment,
        }
    }

    /// Internal task execution logic.
    async fn run_task_inner(
        task_id: String,
        spec: IngestionSpec,
        rows_counter: &Arc<AtomicU64>,
        kafka_source: Option<Arc<InMemoryKafkaSource>>,
        checkpoint_store: Arc<InMemoryCheckpointStore>,
        cancel_flag: Arc<AtomicBool>,
    ) -> std::result::Result<(u64, usize), String> {
        match spec {
            IngestionSpec::IndexParallel(batch) => {
                // For inline data, count the rows and "ingest" them.
                let row_count = match &batch.input_source {
                    InputSource::Inline { data } => data.len() as u64,
                    InputSource::Local { .. } => {
                        // Stub: local file ingestion would read files here
                        0
                    }
                };
                rows_counter.store(row_count, Ordering::Relaxed);
                tracing::info!(
                    data_source = %batch.data_source,
                    rows = row_count,
                    "batch ingestion completed"
                );
                Ok((row_count, if row_count > 0 { 1 } else { 0 }))
            }
            IngestionSpec::Kafka(kafka) if !kafka.partitions.is_empty() => {
                // Real assigned-partition Kafka ingestion.
                let source = match kafka_source {
                    Some(s) => s,
                    None => {
                        return Err("no Kafka record source wired into the MiddleManager; \
                             construct it with MiddleManager::with_kafka_source"
                            .to_owned());
                    }
                };
                let index_spec = Self::build_kafka_index_task(&task_id, &kafka);
                tracing::info!(
                    data_source = %kafka.data_source,
                    topic = %kafka.topic,
                    partitions = index_spec.assignment.len(),
                    "kafka index task started"
                );

                // The executor is synchronous/CPU-bound: run it on the
                // blocking pool so it does not stall the async runtime.
                let counter = Arc::clone(rows_counter);
                let cancel = Arc::clone(&cancel_flag);
                let result = tokio::task::spawn_blocking(move || {
                    let executor = KafkaIndexTaskExecutor::new(
                        index_spec,
                        source.as_ref(),
                        checkpoint_store.as_ref(),
                    );
                    let is_cancelled = move || cancel.load(Ordering::SeqCst);
                    executor.run(counter.as_ref(), &is_cancelled)
                })
                .await
                .map_err(|e| format!("kafka task join error: {e}"))?;

                match result {
                    Ok(report) => Ok((report.rows_consumed, report.segments)),
                    Err(e) => Err(e.to_string()),
                }
            }
            IngestionSpec::Kafka(kafka) => {
                // Legacy / assignment-free Kafka spec: no partition
                // assignment was supplied, so there is nothing concrete
                // to consume. Wait until cancelled (back-compat path).
                tracing::info!(
                    data_source = %kafka.data_source,
                    topic = %kafka.topic,
                    "kafka ingestion started (no partition assignment — waiting for cancel)"
                );
                while !cancel_flag.load(Ordering::SeqCst) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                let rows = rows_counter.load(Ordering::Relaxed);
                tracing::info!(
                    data_source = %kafka.data_source,
                    rows = rows,
                    "kafka ingestion cancelled"
                );
                Ok((rows, 0))
            }
        }
    }

    /// Evict the oldest TERMINAL task handles (by completion order)
    /// until at most `cap` terminal handles remain (DD R11 #1).
    ///
    /// Running (non-terminal) handles are never evicted and never count
    /// against the cap, so an active task is always preserved. Eviction
    /// is FIFO by `terminal_seq`, the monotonic sequence stamped when a
    /// task reached a terminal status, so the most-recently-completed
    /// tasks survive and remain queryable.
    fn evict_terminal_over_cap(tasks: &mut HashMap<String, TaskHandle>, cap: usize) {
        // Collect (task_id, terminal_seq) for terminal handles only.
        let mut terminals: Vec<(String, u64)> = tasks
            .values()
            .filter_map(|h| h.terminal_seq.map(|seq| (h.task_id.clone(), seq)))
            .collect();
        if terminals.len() <= cap {
            return;
        }
        // Oldest-first by completion sequence.
        terminals.sort_by_key(|(_, seq)| *seq);
        let evict_count = terminals.len() - cap;
        for (id, _) in terminals.into_iter().take(evict_count) {
            tasks.remove(&id);
        }
    }

    /// Get a snapshot of a task's status.
    pub fn get_task_status(&self, task_id: &str) -> Result<(TaskRunStatus, u64)> {
        let tasks = self
            .running_tasks
            .read()
            .map_err(|_| MiddleManagerError::LockError)?;
        match tasks.get(task_id) {
            Some(h) => Ok((h.status.clone(), h.rows_processed.load(Ordering::Relaxed))),
            None => Err(MiddleManagerError::TaskNotFound(task_id.to_string())),
        }
    }

    /// Get all task IDs currently tracked.
    pub fn running_tasks(&self) -> Vec<String> {
        self.running_tasks
            .read()
            .map(|tasks| tasks.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Cancel a running task.
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        let cancel_tx = {
            let tasks = self
                .running_tasks
                .read()
                .map_err(|_| MiddleManagerError::LockError)?;
            match tasks.get(task_id) {
                Some(h) => h.cancel_tx.clone(),
                None => return Err(MiddleManagerError::TaskNotFound(task_id.to_string())),
            }
        };

        if let Some(tx) = cancel_tx {
            // Send cancel signal; ignore error if receiver already dropped
            let _ = tx.send(()).await;
        }
        Ok(())
    }

    /// Get the number of tracked tasks.
    pub fn task_count(&self) -> usize {
        self.running_tasks
            .read()
            .map(|tasks| tasks.len())
            .unwrap_or(0)
    }

    /// Check whether the manager has capacity for another task.
    ///
    /// Capacity is measured against non-terminal (Running) tasks only;
    /// terminal handles are retained for history but do not occupy a slot
    /// (see [`submit_task`](Self::submit_task), DD R10 #2).
    pub fn has_capacity(&self) -> bool {
        let running = self
            .running_tasks
            .read()
            .map(|tasks| tasks.values().filter(|h| !h.status.is_terminal()).count())
            .unwrap_or(self.max_concurrent_tasks);
        running < self.max_concurrent_tasks
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka_task::KafkaCheckpointStore;

    fn inline_batch_spec(ds: &str, rows: usize) -> IngestionSpec {
        let data: Vec<serde_json::Value> = (0..rows)
            .map(|i| {
                serde_json::json!({
                    "__time": format!("2024-01-01T00:00:{:02}Z", i % 60),
                    "dim": format!("val_{i}"),
                    "count": 1
                })
            })
            .collect();

        IngestionSpec::IndexParallel(BatchIngestionSpec {
            data_source: ds.to_string(),
            input_source: InputSource::Inline { data },
            timestamp_column: None,
            dimensions: vec!["dim".to_string()],
            metrics: vec![],
        })
    }

    #[tokio::test]
    async fn submit_and_complete_batch() {
        let mm = MiddleManager::new(4);
        assert!(mm.has_capacity());
        assert_eq!(mm.task_count(), 0);

        mm.submit_task("task-1".into(), inline_batch_spec("ds1", 10))
            .await
            .expect("submit");

        // Wait for the background task to finish
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (status, rows) = mm.get_task_status("task-1").expect("status");
        assert!(matches!(
            status,
            TaskRunStatus::Completed {
                rows: 10,
                segments: 1
            }
        ));
        assert_eq!(rows, 10);
    }

    #[tokio::test]
    async fn capacity_limit() {
        let mm = MiddleManager::new(1);
        // Submit a kafka task that blocks until cancelled
        let kafka_spec = IngestionSpec::Kafka(KafkaIngestionSpec {
            data_source: "ds1".into(),
            topic: "test-topic".into(),
            consumer_properties: HashMap::new(),
            timestamp_column: None,
            dimensions: vec![],
            metrics: vec![],
            partitions: vec![],
            max_rows_per_segment: None,
            checkpoint_every_rows: None,
        });
        mm.submit_task("task-1".into(), kafka_spec.clone())
            .await
            .expect("submit");

        assert!(!mm.has_capacity());

        let err = mm
            .submit_task("task-2".into(), kafka_spec)
            .await
            .unwrap_err();
        assert!(matches!(err, MiddleManagerError::NoCapacity { .. }));

        // Cancel and verify
        mm.cancel_task("task-1").await.expect("cancel");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn duplicate_task_rejected() {
        let mm = MiddleManager::new(4);
        let spec = inline_batch_spec("ds1", 1);
        mm.submit_task("dup".into(), spec.clone())
            .await
            .expect("first");

        let err = mm.submit_task("dup".into(), spec).await.unwrap_err();
        assert!(matches!(err, MiddleManagerError::DuplicateTask(_)));
    }

    #[tokio::test]
    async fn cancel_kafka_task() {
        let mm = MiddleManager::new(4);
        let kafka_spec = IngestionSpec::Kafka(KafkaIngestionSpec {
            data_source: "ds1".into(),
            topic: "t".into(),
            consumer_properties: HashMap::new(),
            timestamp_column: None,
            dimensions: vec![],
            metrics: vec![],
            partitions: vec![],
            max_rows_per_segment: None,
            checkpoint_every_rows: None,
        });
        mm.submit_task("k1".into(), kafka_spec)
            .await
            .expect("submit");

        // Give the task time to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let (status, _) = mm.get_task_status("k1").expect("status");
        assert!(matches!(status, TaskRunStatus::Running));

        mm.cancel_task("k1").await.expect("cancel");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let (status, _) = mm.get_task_status("k1").expect("status");
        assert!(matches!(status, TaskRunStatus::Completed { .. }));
    }

    #[tokio::test]
    async fn task_not_found() {
        let mm = MiddleManager::new(4);
        assert!(matches!(
            mm.get_task_status("nope"),
            Err(MiddleManagerError::TaskNotFound(_))
        ));
        assert!(matches!(
            mm.cancel_task("nope").await,
            Err(MiddleManagerError::TaskNotFound(_))
        ));
    }

    #[tokio::test]
    async fn running_tasks_list() {
        let mm = MiddleManager::new(4);
        let spec = inline_batch_spec("ds1", 5);
        mm.submit_task("a".into(), spec.clone()).await.expect("a");
        mm.submit_task("b".into(), spec).await.expect("b");

        let mut ids = mm.running_tasks();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn ingestion_spec_serde_roundtrip() {
        let spec = IngestionSpec::IndexParallel(BatchIngestionSpec {
            data_source: "wiki".into(),
            input_source: InputSource::Inline {
                data: vec![serde_json::json!({"x": 1})],
            },
            timestamp_column: Some("ts".into()),
            dimensions: vec!["page".into()],
            metrics: vec![serde_json::json!({"type": "count", "name": "count"})],
        });
        let json = serde_json::to_string(&spec).expect("ser");
        let back: IngestionSpec = serde_json::from_str(&json).expect("de");
        assert_eq!(back.data_source(), "wiki");
        assert_eq!(back.task_type(), "index_parallel");
    }

    #[test]
    fn kafka_spec_serde_roundtrip() {
        let spec = IngestionSpec::Kafka(KafkaIngestionSpec {
            data_source: "events".into(),
            topic: "clicks".into(),
            consumer_properties: HashMap::from([(
                "bootstrap.servers".into(),
                "localhost:9092".into(),
            )]),
            timestamp_column: None,
            dimensions: vec!["user".into()],
            metrics: vec![],
            partitions: vec![],
            max_rows_per_segment: None,
            checkpoint_every_rows: None,
        });
        let json = serde_json::to_string(&spec).expect("ser");
        let back: IngestionSpec = serde_json::from_str(&json).expect("de");
        assert_eq!(back.data_source(), "events");
    }

    // -----------------------------------------------------------------
    // Real Kafka index-task execution through the MiddleManager
    // lifecycle (Phase 1.2).
    // -----------------------------------------------------------------

    fn seeded_source(topic: &str, partitions: &[usize]) -> Arc<InMemoryKafkaSource> {
        let mut src = InMemoryKafkaSource::new();
        for (p, &n) in partitions.iter().enumerate() {
            let tp = TopicPartition::new(topic, p as i32);
            src.ensure_partition(tp.clone());
            for o in 0..n {
                let v = serde_json::json!({
                    "__time": 1_700_000_000_000_i64 + o as i64,
                    "dim": format!("p{p}-{o}"),
                });
                src.append_json(tp.clone(), &v).expect("append");
            }
        }
        Arc::new(src)
    }

    fn kafka_task_spec(
        ds: &str,
        topic: &str,
        partitions: Vec<PartitionOffsetSpec>,
    ) -> IngestionSpec {
        IngestionSpec::Kafka(KafkaIngestionSpec {
            data_source: ds.into(),
            topic: topic.into(),
            consumer_properties: HashMap::new(),
            timestamp_column: Some("__time".into()),
            dimensions: vec!["dim".into()],
            metrics: vec![],
            partitions,
            max_rows_per_segment: Some(1_000_000),
            checkpoint_every_rows: Some(4),
        })
    }

    async fn wait_for_completion(mm: &MiddleManager, task_id: &str) -> TaskRunStatus {
        for _ in 0..200 {
            let (status, _) = mm.get_task_status(task_id).expect("status");
            if !matches!(status, TaskRunStatus::Running) {
                return status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("task {task_id} did not finish in time");
    }

    #[tokio::test]
    async fn kafka_index_task_runs_to_completion() {
        let src = seeded_source("wiki", &[10]);
        let mm = MiddleManager::with_kafka_source(4, src);
        let spec = kafka_task_spec(
            "wiki_ds",
            "wiki",
            vec![PartitionOffsetSpec {
                partition: 0,
                start_offset: 0,
                end_offset: Some(10),
            }],
        );
        mm.submit_task("kt-1".into(), spec).await.expect("submit");

        let status = wait_for_completion(&mm, "kt-1").await;
        match status {
            TaskRunStatus::Completed { rows, segments } => {
                assert_eq!(rows, 10);
                assert_eq!(segments, 1);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
        // Checkpoint persisted the final offset.
        let cp = mm
            .checkpoint_store()
            .load("kt-1")
            .expect("load")
            .expect("cp");
        assert_eq!(cp.next_offset(&TopicPartition::new("wiki", 0)), Some(10));
    }

    #[tokio::test]
    async fn terminal_task_frees_capacity_slot() {
        // DD R10 #2: with max_concurrent_tasks == 1, a task that has reached a
        // terminal status must NOT keep its slot occupied. After the first
        // task completes, a second submission must be accepted, and the first
        // task's terminal status must remain queryable for history.
        let src = seeded_source("wiki", &[5]);
        let mm = MiddleManager::with_kafka_source(1, src);
        let spec1 = kafka_task_spec(
            "wiki_ds",
            "wiki",
            vec![PartitionOffsetSpec {
                partition: 0,
                start_offset: 0,
                end_offset: Some(5),
            }],
        );
        mm.submit_task("kt-cap-1".into(), spec1)
            .await
            .expect("submit 1");
        let first = wait_for_completion(&mm, "kt-cap-1").await;
        assert!(first.is_terminal(), "first task should be terminal");

        // Second submission must be ACCEPTED now that the slot is free.
        let spec2 = kafka_task_spec(
            "wiki_ds",
            "wiki",
            vec![PartitionOffsetSpec {
                partition: 0,
                start_offset: 0,
                end_offset: Some(5),
            }],
        );
        mm.submit_task("kt-cap-2".into(), spec2)
            .await
            .expect("second submission should be accepted, not NoCapacity");

        // The first task's terminal status is still queryable (history kept).
        let (status1, _) = mm
            .get_task_status("kt-cap-1")
            .expect("status of first task");
        assert!(
            status1.is_terminal(),
            "first task's terminal status must remain queryable"
        );
    }

    #[tokio::test]
    async fn terminal_history_is_bounded_by_cap() {
        // DD R11 #1: an unbounded stream of unique fast tasks must NOT
        // grow `running_tasks` without limit. With a small history cap,
        // the count of retained TERMINAL handles is bounded by the cap;
        // the oldest terminal handles are evicted FIFO, the most-recent
        // terminal task remains queryable, and capacity is preserved.
        const CAP: usize = 3;
        let mm = MiddleManager::new(1).with_max_completed_history(CAP);

        let total = 20usize;
        for i in 0..total {
            let id = format!("hist-{i}");
            mm.submit_task(id.clone(), inline_batch_spec("ds", 1))
                .await
                .unwrap_or_else(|e| panic!("submit {id}: {e}"));
            // Each inline batch completes promptly; wait for terminal.
            let status = wait_for_completion(&mm, &id).await;
            assert!(status.is_terminal(), "task {id} should be terminal");
        }

        // The total number of retained handles must be bounded by the
        // cap (all tasks here are terminal, so this is the terminal cap).
        let retained = mm.task_count();
        assert!(
            retained <= CAP,
            "retained handles {retained} must be <= cap {CAP}"
        );

        // The most-recent terminal task is still queryable.
        let last_id = format!("hist-{}", total - 1);
        let (last_status, _) = mm
            .get_task_status(&last_id)
            .unwrap_or_else(|e| panic!("most-recent task {last_id} must remain queryable: {e}"));
        assert!(last_status.is_terminal());

        // The oldest task was evicted (FIFO).
        assert!(
            matches!(
                mm.get_task_status("hist-0"),
                Err(MiddleManagerError::TaskNotFound(_))
            ),
            "oldest terminal task must have been evicted"
        );

        // Capacity is still available (terminal handles do not occupy a slot).
        assert!(mm.has_capacity(), "capacity must remain available");
    }

    #[tokio::test]
    async fn concurrent_submit_admits_exactly_one() {
        // DD R11 #2: with max_concurrent_tasks == 1, two concurrent
        // submissions on an empty manager must NOT both be admitted. The
        // previous read-then-write admission split let both pass the
        // capacity check before either inserted, oversubscribing.
        let mm = Arc::new(MiddleManager::new(1));

        // Tasks that stay Running until cancelled (legacy assignment-free
        // Kafka path waits for cancel), so both submissions race for the
        // single slot while both would-be tasks are non-terminal.
        let blocking_spec = || {
            IngestionSpec::Kafka(KafkaIngestionSpec {
                data_source: "ds".into(),
                topic: "t".into(),
                consumer_properties: HashMap::new(),
                timestamp_column: None,
                dimensions: vec![],
                metrics: vec![],
                partitions: vec![],
                max_rows_per_segment: None,
                checkpoint_every_rows: None,
            })
        };

        let mm_a = Arc::clone(&mm);
        let mm_b = Arc::clone(&mm);
        let spec_a = blocking_spec();
        let spec_b = blocking_spec();
        let a = tokio::spawn(async move { mm_a.submit_task("race-a".into(), spec_a).await });
        let b = tokio::spawn(async move { mm_b.submit_task("race-b".into(), spec_b).await });

        let (ra, rb) = tokio::join!(a, b);
        let ra = ra.expect("join a");
        let rb = rb.expect("join b");

        let admitted = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
        let rejected = [&ra, &rb]
            .iter()
            .filter(|r| matches!(r, Err(MiddleManagerError::NoCapacity { .. })))
            .count();
        assert_eq!(
            admitted, 1,
            "exactly one concurrent submission must be admitted, got {admitted} (a={ra:?}, b={rb:?})"
        );
        assert_eq!(
            rejected, 1,
            "exactly one concurrent submission must be rejected with NoCapacity"
        );

        // Clean up: cancel whichever task was admitted.
        for id in ["race-a", "race-b"] {
            let _ = mm.cancel_task(id).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn kafka_index_task_consumes_only_assigned_partitions() {
        // Two partitions of 10; task assigned only partition 1, offsets [3, 8).
        let src = seeded_source("orders", &[10, 10]);
        let mm = MiddleManager::with_kafka_source(4, src);
        let spec = kafka_task_spec(
            "orders_ds",
            "orders",
            vec![PartitionOffsetSpec {
                partition: 1,
                start_offset: 3,
                end_offset: Some(8),
            }],
        );
        mm.submit_task("kt-2".into(), spec).await.expect("submit");
        let status = wait_for_completion(&mm, "kt-2").await;
        match status {
            TaskRunStatus::Completed { rows, .. } => assert_eq!(rows, 5),
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn kafka_index_task_resume_does_not_reconsume() {
        let src = seeded_source("logs", &[10]);
        let mm = MiddleManager::with_kafka_source(4, Arc::clone(&src));
        let make = || {
            kafka_task_spec(
                "logs_ds",
                "logs",
                vec![PartitionOffsetSpec {
                    partition: 0,
                    start_offset: 0,
                    end_offset: Some(10),
                }],
            )
        };
        // First run consumes 10.
        mm.submit_task("resume-task".into(), make())
            .await
            .expect("submit1");
        let s1 = wait_for_completion(&mm, "resume-task").await;
        assert!(matches!(s1, TaskRunStatus::Completed { rows: 10, .. }));

        // A second MiddleManager sharing the SAME checkpoint store would
        // be ideal, but here we reuse the same store by re-submitting a
        // distinct task id that loads the prior checkpoint. Instead, run
        // the executor path directly against the shared store to prove
        // resume: re-submit with the same task id is rejected as a dup,
        // so we assert resume at the executor layer.
        let store = mm.checkpoint_store();
        let counter = std::sync::atomic::AtomicU64::new(0);
        let mut assignment = BTreeMap::new();
        assignment.insert(TopicPartition::new("logs", 0), OffsetRange::bounded(0, 10));
        let resume_spec = KafkaIndexTaskSpec {
            task_id: "resume-task".into(),
            data_source: "logs_ds".into(),
            timestamp_column: "__time".into(),
            dimensions: vec!["dim".into()],
            metrics: vec![],
            max_rows_per_segment: 1_000_000,
            checkpoint_every_rows: 0,
            assignment,
        };
        let exec = KafkaIndexTaskExecutor::new(resume_spec, src.as_ref(), store.as_ref());
        let report = exec.run(&counter, &|| false).expect("resume run");
        assert_eq!(
            report.rows_consumed, 0,
            "resume must not re-consume checkpointed rows"
        );
    }

    #[tokio::test]
    async fn kafka_index_task_failure_is_reported() {
        // No source wired → assigned-partition Kafka task must fail, not hang.
        let mm = MiddleManager::new(4);
        let spec = kafka_task_spec(
            "x",
            "x",
            vec![PartitionOffsetSpec {
                partition: 0,
                start_offset: 0,
                end_offset: Some(1),
            }],
        );
        mm.submit_task("kt-fail".into(), spec)
            .await
            .expect("submit");
        let status = wait_for_completion(&mm, "kt-fail").await;
        match status {
            TaskRunStatus::Failed { error } => {
                assert!(error.contains("no Kafka record source"), "error={error}");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn kafka_index_task_counts_against_capacity() {
        // Capacity 1: a running Kafka task (open-ended) blocks a second.
        let src = seeded_source("c", &[1]);
        let mm = MiddleManager::with_kafka_source(1, src);
        // Open-ended partition with one slow consumer won't return
        // immediately because the executor drains then completes; to keep
        // it occupying the slot we instead submit two tasks back-to-back
        // and assert the second is rejected before the first finishes.
        let spec_a = kafka_task_spec(
            "c_ds",
            "c",
            vec![PartitionOffsetSpec {
                partition: 0,
                start_offset: 0,
                end_offset: None,
            }],
        );
        mm.submit_task("cap-a".into(), spec_a).await.expect("a");
        // Immediately (before the background task can finish) the slot is
        // taken.
        let spec_b = kafka_task_spec(
            "c_ds",
            "c",
            vec![PartitionOffsetSpec {
                partition: 0,
                start_offset: 0,
                end_offset: None,
            }],
        );
        let err = mm.submit_task("cap-b".into(), spec_b).await.unwrap_err();
        assert!(matches!(err, MiddleManagerError::NoCapacity { .. }));
    }
}
