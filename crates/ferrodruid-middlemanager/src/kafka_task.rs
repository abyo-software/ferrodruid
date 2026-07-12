// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Real Kafka indexing-task execution for the MiddleManager.
//!
//! A [`KafkaIndexTaskExecutor`] consumes a set of assigned
//! `(topic, partition) -> [start, end)` offset ranges from a pluggable
//! [`KafkaSource`], parses each record as JSON, builds segments via the
//! batch ingester, and checkpoints consumed offsets into a
//! [`KafkaCheckpointStore`] so a restarted task resumes from the last
//! checkpoint instead of re-consuming.
//!
//! The MiddleManager crate cannot depend on `ferrodruid-ingest-kafka`
//! (that crate already depends on the MiddleManager, so the reverse
//! edge would be a cycle). The partition/offset/checkpoint model here is
//! therefore an intentionally small, self-contained mirror of the one in
//! `ferrodruid-ingest-kafka`, sufficient to drive task execution through
//! the MiddleManager lifecycle. See the crate-level limitations note.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ferrodruid_ingest_batch::BatchIngester;
use serde::{Deserialize, Serialize};

/// A `(topic, partition)` identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
    /// Kafka topic name.
    pub topic: String,
    /// Partition id within the topic.
    pub partition: i32,
}

impl TopicPartition {
    /// Construct a new `(topic, partition)` pair.
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Half-open offset range `[start, end)` for one partition. A `None`
/// end means the range is open-ended (drain to the high-water mark).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffsetRange {
    /// First offset to consume (inclusive).
    pub start: i64,
    /// First offset to stop at (exclusive); `None` is open-ended.
    pub end: Option<i64>,
}

impl OffsetRange {
    /// A bounded range `[start, end)`.
    pub fn bounded(start: i64, end: i64) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }

    /// An open-ended range from `start`.
    pub fn open(start: i64) -> Self {
        Self { start, end: None }
    }

    /// Whether `offset` falls inside this range.
    #[must_use]
    pub fn contains(&self, offset: i64) -> bool {
        offset >= self.start && self.end.is_none_or(|e| offset < e)
    }

    /// Whether `next_offset` has reached the bounded end.
    #[must_use]
    pub fn is_complete_at(&self, next_offset: i64) -> bool {
        self.end.is_some_and(|e| next_offset >= e)
    }
}

/// A record fetched from a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRecord {
    /// Partition the record came from.
    pub partition: TopicPartition,
    /// Offset of this record within its partition.
    pub offset: i64,
    /// Raw record payload (JSON bytes).
    pub payload: Vec<u8>,
}

/// Errors from a [`KafkaSource`].
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// The requested partition does not exist.
    #[error("unknown partition: {0:?}")]
    UnknownPartition(TopicPartition),
    /// Underlying transport failure.
    #[error("source transport error: {0}")]
    Transport(String),
}

/// A source that can be polled for records on a partition from an offset.
pub trait KafkaSource: Send + Sync {
    /// Next offset to be produced for `partition` (one past the last).
    fn high_watermark(&self, partition: &TopicPartition) -> Result<i64, SourceError>;

    /// Poll up to `max_records` records with offset `>= from`, ascending.
    fn poll(
        &self,
        partition: &TopicPartition,
        from: i64,
        max_records: usize,
    ) -> Result<Vec<SourceRecord>, SourceError>;
}

/// In-memory multi-partition Kafka stand-in for tests and embedded use.
#[derive(Debug, Clone, Default)]
pub struct InMemoryKafkaSource {
    partitions: BTreeMap<TopicPartition, Vec<Vec<u8>>>,
}

impl InMemoryKafkaSource {
    /// Create an empty source.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure a partition exists.
    pub fn ensure_partition(&mut self, tp: TopicPartition) {
        self.partitions.entry(tp).or_default();
    }

    /// Append a raw record; returns the assigned offset.
    pub fn append(&mut self, tp: TopicPartition, payload: Vec<u8>) -> i64 {
        let log = self.partitions.entry(tp).or_default();
        let offset = log.len() as i64;
        log.push(payload);
        offset
    }

    /// Append a JSON value as a record payload; returns the offset.
    pub fn append_json(
        &mut self,
        tp: TopicPartition,
        value: &serde_json::Value,
    ) -> Result<i64, SourceError> {
        let payload = serde_json::to_vec(value)
            .map_err(|e| SourceError::Transport(format!("serialize: {e}")))?;
        Ok(self.append(tp, payload))
    }
}

impl KafkaSource for InMemoryKafkaSource {
    fn high_watermark(&self, partition: &TopicPartition) -> Result<i64, SourceError> {
        self.partitions
            .get(partition)
            .map(|l| l.len() as i64)
            .ok_or_else(|| SourceError::UnknownPartition(partition.clone()))
    }

    fn poll(
        &self,
        partition: &TopicPartition,
        from: i64,
        max_records: usize,
    ) -> Result<Vec<SourceRecord>, SourceError> {
        let log = self
            .partitions
            .get(partition)
            .ok_or_else(|| SourceError::UnknownPartition(partition.clone()))?;
        if max_records == 0 {
            return Ok(Vec::new());
        }
        let start = from.max(0);
        let mut out = Vec::new();
        let mut offset = start;
        while (offset as usize) < log.len() && out.len() < max_records {
            out.push(SourceRecord {
                partition: partition.clone(),
                offset,
                payload: log[offset as usize].clone(),
            });
            offset += 1;
        }
        Ok(out)
    }
}

/// Per-partition next-offset checkpoint for one task.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// Next offset to consume, per partition.
    pub next_offsets: BTreeMap<TopicPartition, i64>,
}

impl Checkpoint {
    /// Record `next_offset` for `tp` (monotonic; never rewinds).
    pub fn record(&mut self, tp: TopicPartition, next_offset: i64) {
        let entry = self.next_offsets.entry(tp).or_insert(next_offset);
        if next_offset > *entry {
            *entry = next_offset;
        }
    }

    /// The recorded next offset for `tp`, if any.
    #[must_use]
    pub fn next_offset(&self, tp: &TopicPartition) -> Option<i64> {
        self.next_offsets.get(tp).copied()
    }
}

/// Errors from a [`KafkaCheckpointStore`].
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// Backing store failure.
    #[error("checkpoint store error: {0}")]
    Store(String),
}

/// Persistence backend for task checkpoints.
pub trait KafkaCheckpointStore: Send + Sync {
    /// Persist `checkpoint` for `task_id` (last-write-wins).
    fn save(&self, task_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError>;

    /// Load the last checkpoint for `task_id`, or `None`.
    fn load(&self, task_id: &str) -> Result<Option<Checkpoint>, CheckpointError>;
}

/// In-memory [`KafkaCheckpointStore`]; clones share state via `Arc`.
#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    inner: Arc<Mutex<BTreeMap<String, Checkpoint>>>,
}

impl InMemoryCheckpointStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl KafkaCheckpointStore for InMemoryCheckpointStore {
    fn save(&self, task_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let mut g = self
            .inner
            .lock()
            .map_err(|_| CheckpointError::Store("mutex poisoned".to_owned()))?;
        g.insert(task_id.to_owned(), checkpoint.clone());
        Ok(())
    }

    fn load(&self, task_id: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let g = self
            .inner
            .lock()
            .map_err(|_| CheckpointError::Store("mutex poisoned".to_owned()))?;
        Ok(g.get(task_id).cloned())
    }
}

/// Maximum bytes accepted from a single record before JSON parsing.
const MAX_RECORD_PAYLOAD_BYTES: usize = 1_048_576;
/// Poll batch size when draining a partition.
const POLL_BATCH: usize = 256;

/// A fully-resolved Kafka indexing task ready for execution.
#[derive(Debug, Clone)]
pub struct KafkaIndexTaskSpec {
    /// Unique task identifier (also the checkpoint key).
    pub task_id: String,
    /// Target data source for produced segments.
    pub data_source: String,
    /// Column holding the event timestamp.
    pub timestamp_column: String,
    /// Dimension column names to extract.
    pub dimensions: Vec<String>,
    /// Metric aggregator specs.
    pub metrics: Vec<serde_json::Value>,
    /// Max rows to buffer before flushing a segment.
    pub max_rows_per_segment: usize,
    /// Checkpoint after this many newly-consumed records (0 = only final).
    pub checkpoint_every_rows: u64,
    /// Assigned partition/offset ranges.
    pub assignment: BTreeMap<TopicPartition, OffsetRange>,
}

/// Outcome of running a [`KafkaIndexTaskExecutor`].
#[derive(Debug, Default)]
pub struct ExecutionReport {
    /// Total records consumed.
    pub rows_consumed: u64,
    /// Number of segments produced.
    pub segments: usize,
    /// Final next-offsets per partition.
    pub final_offsets: BTreeMap<TopicPartition, i64>,
}

/// Errors from executing a Kafka indexing task.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    /// A record payload exceeded the size cap.
    #[error("record payload too large: {0} bytes")]
    PayloadTooLarge(usize),
    /// A record payload was not valid JSON.
    #[error("record parse error at {partition:?}@{offset}: {message}")]
    Parse {
        /// Partition the bad record came from.
        partition: TopicPartition,
        /// Offset of the bad record.
        offset: i64,
        /// Underlying parse error.
        message: String,
    },
    /// The record source failed.
    #[error("source error: {0}")]
    Source(#[from] SourceError),
    /// Segment construction failed.
    #[error("segment build error: {0}")]
    SegmentBuild(String),
    /// Checkpoint persistence failed.
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
    /// The task was cancelled before completion.
    #[error("task cancelled")]
    Cancelled,
}

/// Executes a [`KafkaIndexTaskSpec`] against a source + checkpoint store.
pub struct KafkaIndexTaskExecutor<'a, S: KafkaSource, C: KafkaCheckpointStore> {
    spec: KafkaIndexTaskSpec,
    source: &'a S,
    checkpoints: &'a C,
}

impl<'a, S: KafkaSource, C: KafkaCheckpointStore> KafkaIndexTaskExecutor<'a, S, C> {
    /// Create an executor.
    pub fn new(spec: KafkaIndexTaskSpec, source: &'a S, checkpoints: &'a C) -> Self {
        Self {
            spec,
            source,
            checkpoints,
        }
    }

    /// Run the task to completion, reporting consumed rows and offsets.
    ///
    /// `rows_counter` is updated as rows are consumed so the
    /// MiddleManager can surface live progress. `is_cancelled` is polled
    /// between partitions/batches; when it returns `true` the task stops
    /// early with [`ExecutionError::Cancelled`] after flushing buffered
    /// rows and persisting a checkpoint (so a resumed task continues
    /// cleanly).
    pub fn run(
        &self,
        rows_counter: &AtomicU64,
        is_cancelled: &dyn Fn() -> bool,
    ) -> Result<ExecutionReport, ExecutionError> {
        let mut checkpoint = self
            .checkpoints
            .load(&self.spec.task_id)
            .map_err(|e| ExecutionError::Checkpoint(e.to_string()))?
            .unwrap_or_default();

        let mut report = ExecutionReport::default();
        let mut buffer: Vec<serde_json::Value> = Vec::new();
        let mut rows_since_checkpoint: u64 = 0;
        let mut cancelled = false;

        'outer: for (tp, range) in self.spec.assignment.iter() {
            let high = self.source.high_watermark(tp)?;
            let mut offset = match checkpoint.next_offset(tp) {
                Some(c) => c.max(range.start),
                None => range.start,
            };

            loop {
                if is_cancelled() {
                    cancelled = true;
                    break 'outer;
                }
                if range.is_complete_at(offset) || offset >= high {
                    break;
                }
                let batch = self.source.poll(tp, offset, POLL_BATCH)?;
                if batch.is_empty() {
                    break;
                }
                for rec in batch {
                    // Sparse-offset handling (DD R10 #3). A source may return a
                    // first record below `start` (skip and advance) or
                    // at/beyond the exclusive `end` (compaction/retention/txn
                    // gaps leave the next live record past the window). Never
                    // re-poll the same offset forever: advance past below-start
                    // records, and when a record is at/beyond `end`, pin
                    // `offset` to the range end and exit this partition.
                    if rec.offset < range.start {
                        offset = rec.offset + 1;
                        continue;
                    }
                    if !range.contains(rec.offset) {
                        if let Some(end) = range.end {
                            offset = offset.max(end);
                            checkpoint.record(tp.clone(), offset);
                        }
                        break;
                    }
                    let value = self.parse_record(tp, rec.offset, &rec.payload)?;
                    buffer.push(value);
                    report.rows_consumed += 1;
                    rows_counter.fetch_add(1, Ordering::Relaxed);
                    rows_since_checkpoint += 1;
                    offset = rec.offset + 1;
                    checkpoint.record(tp.clone(), offset);

                    if buffer.len() >= self.spec.max_rows_per_segment {
                        self.flush(&mut buffer, &mut report)?;
                    }
                    if self.spec.checkpoint_every_rows > 0
                        && rows_since_checkpoint >= self.spec.checkpoint_every_rows
                    {
                        self.persist(&checkpoint)?;
                        rows_since_checkpoint = 0;
                    }
                }
            }
        }

        if !buffer.is_empty() {
            self.flush(&mut buffer, &mut report)?;
        }
        self.persist(&checkpoint)?;
        report.final_offsets = checkpoint.next_offsets.clone();

        if cancelled {
            return Err(ExecutionError::Cancelled);
        }
        Ok(report)
    }

    fn parse_record(
        &self,
        tp: &TopicPartition,
        offset: i64,
        payload: &[u8],
    ) -> Result<serde_json::Value, ExecutionError> {
        if payload.len() > MAX_RECORD_PAYLOAD_BYTES {
            return Err(ExecutionError::PayloadTooLarge(payload.len()));
        }
        serde_json::from_slice(payload).map_err(|e| ExecutionError::Parse {
            partition: tp.clone(),
            offset,
            message: e.to_string(),
        })
    }

    fn flush(
        &self,
        buffer: &mut Vec<serde_json::Value>,
        report: &mut ExecutionReport,
    ) -> Result<(), ExecutionError> {
        if buffer.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(buffer);
        let ingester = BatchIngester::new(
            self.spec.data_source.clone(),
            self.spec.timestamp_column.clone(),
            self.spec.dimensions.clone(),
            self.spec.metrics.clone(),
        );
        ingester
            .ingest(rows)
            .map_err(|e| ExecutionError::SegmentBuild(e.to_string()))?;
        report.segments += 1;
        Ok(())
    }

    fn persist(&self, checkpoint: &Checkpoint) -> Result<(), ExecutionError> {
        self.checkpoints
            .save(&self.spec.task_id, checkpoint)
            .map_err(|e| ExecutionError::Checkpoint(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(p: i32) -> TopicPartition {
        TopicPartition::new("events", p)
    }

    fn source_with(counts: &[usize]) -> InMemoryKafkaSource {
        let mut src = InMemoryKafkaSource::new();
        for (p, &n) in counts.iter().enumerate() {
            let part = tp(p as i32);
            src.ensure_partition(part.clone());
            for o in 0..n {
                let v = serde_json::json!({
                    "__time": 1_700_000_000_000_i64 + o as i64,
                    "dim": format!("v{o}"),
                });
                src.append_json(part.clone(), &v).expect("append");
            }
        }
        src
    }

    fn spec(assignment: BTreeMap<TopicPartition, OffsetRange>) -> KafkaIndexTaskSpec {
        KafkaIndexTaskSpec {
            task_id: "t1".to_owned(),
            data_source: "events".to_owned(),
            timestamp_column: "__time".to_owned(),
            dimensions: vec!["dim".to_owned()],
            metrics: vec![],
            max_rows_per_segment: 1_000_000,
            checkpoint_every_rows: 2,
            assignment,
        }
    }

    fn never_cancel() -> impl Fn() -> bool {
        || false
    }

    #[test]
    fn consumes_only_assigned_range() {
        let src = source_with(&[10, 10, 10]);
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(1), OffsetRange::bounded(2, 7));
        let exec = KafkaIndexTaskExecutor::new(spec(a), &src, &store);
        let counter = AtomicU64::new(0);
        let report = exec.run(&counter, &never_cancel()).expect("run");
        assert_eq!(report.rows_consumed, 5);
        assert_eq!(counter.load(Ordering::Relaxed), 5);
        assert_eq!(report.final_offsets.get(&tp(1)), Some(&7));
        assert!(!report.final_offsets.contains_key(&tp(0)));
    }

    /// Finite source whose records sit at fixed offsets regardless of `from`,
    /// modelling compaction/retention/txn gaps. A bounded `poll` counter
    /// detects a re-poll spin (a buggy consumer would call poll forever).
    struct SparseSource {
        records: Vec<(TopicPartition, i64, Vec<u8>)>,
        high: i64,
        poll_calls: AtomicU64,
    }

    impl KafkaSource for SparseSource {
        fn high_watermark(&self, _partition: &TopicPartition) -> Result<i64, SourceError> {
            Ok(self.high)
        }
        fn poll(
            &self,
            partition: &TopicPartition,
            from: i64,
            max_records: usize,
        ) -> Result<Vec<SourceRecord>, SourceError> {
            let calls = self.poll_calls.fetch_add(1, Ordering::Relaxed) + 1;
            assert!(
                calls < 1000,
                "poll spun without making progress (DD R10 #3)"
            );
            let mut out = Vec::new();
            for (tp, off, payload) in &self.records {
                if tp == partition && *off >= from {
                    out.push(SourceRecord {
                        partition: tp.clone(),
                        offset: *off,
                        payload: payload.clone(),
                    });
                    if out.len() >= max_records {
                        break;
                    }
                }
            }
            Ok(out)
        }
    }

    #[test]
    fn sparse_first_record_beyond_end_terminates_without_spin() {
        // DD R10 #3: assigned [0,10) but the first live record is at offset 12
        // (everything below compacted away). Must terminate, consuming zero
        // in-range records, not spin re-polling offset 0.
        let payload =
            serde_json::to_vec(&serde_json::json!({"__time": 1_700_000_000_000_i64, "dim": "x"}))
                .expect("payload");
        let src = SparseSource {
            records: vec![(tp(0), 12, payload)],
            high: 20,
            poll_calls: AtomicU64::new(0),
        };
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(0), OffsetRange::bounded(0, 10));
        let exec = KafkaIndexTaskExecutor::new(spec(a), &src, &store);
        let counter = AtomicU64::new(0);
        let report = exec
            .run(&counter, &never_cancel())
            .expect("run must terminate, not hang");
        assert_eq!(report.rows_consumed, 0);
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&10));
    }

    #[test]
    fn sparse_below_start_skipped_then_in_range_consumed() {
        // DD R10 #3: below-start records skipped (advancing), in-range consumed.
        let mk = |s: &str| {
            serde_json::to_vec(&serde_json::json!({"__time": 1_700_000_000_000_i64, "dim": s}))
                .expect("payload")
        };
        let src = SparseSource {
            records: vec![
                (tp(0), 1, mk("below")),
                (tp(0), 5, mk("in")),
                (tp(0), 6, mk("in")),
                (tp(0), 11, mk("beyond")),
            ],
            high: 20,
            poll_calls: AtomicU64::new(0),
        };
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(0), OffsetRange::bounded(5, 10));
        let exec = KafkaIndexTaskExecutor::new(spec(a), &src, &store);
        let counter = AtomicU64::new(0);
        let report = exec.run(&counter, &never_cancel()).expect("run terminates");
        assert_eq!(report.rows_consumed, 2);
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&10));
    }

    #[test]
    fn checkpoint_and_resume_no_reconsume() {
        let src = source_with(&[10]);
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(0), OffsetRange::bounded(0, 10));

        let c1 = AtomicU64::new(0);
        let r1 = KafkaIndexTaskExecutor::new(spec(a.clone()), &src, &store)
            .run(&c1, &never_cancel())
            .expect("run1");
        assert_eq!(r1.rows_consumed, 10);
        assert_eq!(
            store
                .load("t1")
                .expect("load")
                .expect("cp")
                .next_offset(&tp(0)),
            Some(10)
        );

        let c2 = AtomicU64::new(0);
        let r2 = KafkaIndexTaskExecutor::new(spec(a), &src, &store)
            .run(&c2, &never_cancel())
            .expect("run2");
        assert_eq!(r2.rows_consumed, 0, "resume must not re-consume");
    }

    #[test]
    fn open_range_drains_then_cancel_stops() {
        let src = source_with(&[5]);
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(0), OffsetRange::open(0));
        let exec = KafkaIndexTaskExecutor::new(spec(a), &src, &store);
        let counter = AtomicU64::new(0);
        // Cancel immediately: should stop with Cancelled, having consumed nothing.
        let err = exec.run(&counter, &|| true).unwrap_err();
        assert!(matches!(err, ExecutionError::Cancelled));
    }

    #[test]
    fn bad_json_fails() {
        let mut src = source_with(&[1]);
        src.append(tp(0), b"not json".to_vec());
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(0), OffsetRange::bounded(0, 2));
        let exec = KafkaIndexTaskExecutor::new(spec(a), &src, &store);
        let counter = AtomicU64::new(0);
        let err = exec.run(&counter, &never_cancel()).unwrap_err();
        assert!(matches!(err, ExecutionError::Parse { offset: 1, .. }));
    }

    #[test]
    fn segments_split_on_max_rows() {
        let src = source_with(&[10]);
        let store = InMemoryCheckpointStore::new();
        let mut a = BTreeMap::new();
        a.insert(tp(0), OffsetRange::bounded(0, 10));
        let mut s = spec(a);
        s.max_rows_per_segment = 4;
        let exec = KafkaIndexTaskExecutor::new(s, &src, &store);
        let counter = AtomicU64::new(0);
        let report = exec.run(&counter, &never_cancel()).expect("run");
        assert_eq!(report.segments, 3);
    }
}
