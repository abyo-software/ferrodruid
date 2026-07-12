// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `KafkaIndexTask`: a Druid-compatible Kafka indexing-task peon.
//!
//! Given a [`KafkaIndexTaskSpec`] (data schema + a
//! [`PartitionAssignment`] of `(topic, partition) -> [start, end)`
//! offset ranges), the task consumes **only** its assigned partitions
//! between the start and end offsets, parses each record as JSON,
//! buffers rows, and builds segments via the batch ingester.
//!
//! Progress is checkpointed periodically (every `checkpoint_every_rows`
//! consumed records) into a [`CheckpointStore`]; a restarted task with
//! the same `task_id` resumes from the last recorded offsets rather than
//! re-consuming records it has already processed.

use std::collections::BTreeMap;

use ferrodruid_ingest_batch::{BatchIngester, IngestedSegment};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::consumer::check_record_bounds;
use crate::partitions::{OffsetRange, PartitionAssignment, TopicPartition};
use crate::source::{PartitionedRecordSource, SourceError};

/// How a partition's start offset should be resolved when there is no
/// checkpoint and no explicit start offset is desired.
const DEFAULT_POLL_BATCH: usize = 256;

/// Specification handed to a single Kafka indexing task.
///
/// This is the in-crate analogue of Druid's `KafkaIndexTask` payload:
/// the data schema needed to build segments plus the partition/offset
/// assignment that bounds what the task consumes.
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
    /// Metric aggregator specs (JSON objects with a `"name"` field).
    pub metrics: Vec<serde_json::Value>,
    /// Maximum rows to buffer before flushing a segment.
    pub max_rows_per_segment: usize,
    /// Checkpoint after this many newly-consumed records (0 disables
    /// periodic checkpointing; a final checkpoint is still written when
    /// the task completes).
    pub checkpoint_every_rows: u64,
    /// Partition/offset assignment bounding what this task consumes.
    pub assignment: PartitionAssignment,
}

impl KafkaIndexTaskSpec {
    /// Construct a spec with sensible checkpoint defaults.
    pub fn new(
        task_id: impl Into<String>,
        data_source: impl Into<String>,
        timestamp_column: impl Into<String>,
        dimensions: Vec<String>,
        assignment: PartitionAssignment,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            data_source: data_source.into(),
            timestamp_column: timestamp_column.into(),
            dimensions,
            metrics: Vec::new(),
            max_rows_per_segment: 5_000_000,
            checkpoint_every_rows: 1_000,
            assignment,
        }
    }
}

/// Outcome of running a [`KafkaIndexTask`] to completion.
#[derive(Debug, Default)]
pub struct IndexTaskReport {
    /// Total records consumed across all assigned partitions.
    pub rows_consumed: u64,
    /// Segments produced (one per flush).
    pub segments: Vec<IngestedSegment>,
    /// Final next-offsets per partition (also persisted as a checkpoint).
    pub final_offsets: BTreeMap<TopicPartition, i64>,
}

impl IndexTaskReport {
    /// Number of segments produced.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

/// Errors from running a Kafka indexing task.
#[derive(Debug, thiserror::Error)]
pub enum IndexTaskError {
    /// A record payload failed the size/depth safety bounds.
    #[error("record bounds rejected: {0}")]
    RecordBounds(String),
    /// A record payload was not valid JSON.
    #[error("record parse error on {partition:?}@{offset}: {message}")]
    Parse {
        /// Partition the bad record came from.
        partition: TopicPartition,
        /// Offset of the bad record.
        offset: i64,
        /// Underlying parse error.
        message: String,
    },
    /// The underlying record source failed.
    #[error("source error: {0}")]
    Source(#[from] SourceError),
    /// Segment construction failed.
    #[error("segment build error: {0}")]
    SegmentBuild(String),
    /// Checkpoint persistence failed.
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
}

/// Behaviour when a record payload fails parsing/bounds checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadRecordPolicy {
    /// Abort the whole task on the first bad record.
    Fail,
    /// Skip the bad record and continue (count it but do not buffer it).
    Skip,
}

/// A runnable Kafka indexing task bound to a record source and a
/// checkpoint store.
pub struct KafkaIndexTask<'a, S: PartitionedRecordSource, C: CheckpointStore> {
    spec: KafkaIndexTaskSpec,
    source: &'a S,
    checkpoints: &'a C,
    bad_record_policy: BadRecordPolicy,
}

impl<'a, S: PartitionedRecordSource, C: CheckpointStore> KafkaIndexTask<'a, S, C> {
    /// Create a task that fails on the first malformed record.
    pub fn new(spec: KafkaIndexTaskSpec, source: &'a S, checkpoints: &'a C) -> Self {
        Self {
            spec,
            source,
            checkpoints,
            bad_record_policy: BadRecordPolicy::Fail,
        }
    }

    /// Set the bad-record policy (builder style).
    #[must_use]
    pub fn with_bad_record_policy(mut self, policy: BadRecordPolicy) -> Self {
        self.bad_record_policy = policy;
        self
    }

    /// Resolve the offset this task should begin consuming `tp` from:
    /// the checkpointed next-offset if present (and at/after the
    /// assigned start), otherwise the assigned start offset. This is the
    /// resume-from-checkpoint guarantee — already-consumed records are
    /// not re-consumed.
    fn resume_offset(&self, tp: &TopicPartition, range: &OffsetRange, cp: &Checkpoint) -> i64 {
        match cp.next_offset(tp) {
            Some(checkpointed) => checkpointed.max(range.start),
            None => range.start,
        }
    }

    /// Run the task to completion: consume every assigned partition from
    /// its resume offset up to (but not including) its end offset,
    /// flushing segments as buffers fill and checkpointing periodically.
    ///
    /// For open-ended ranges (`end == None`) the task drains the
    /// partition up to its current high-water mark and then stops — a
    /// single batch pass — which is the right semantics for a bounded
    /// run; a long-running streaming loop is the MiddleManager's job.
    pub fn run(&self) -> Result<IndexTaskReport, IndexTaskError> {
        // Load prior progress (resume support).
        let mut checkpoint = self
            .checkpoints
            .load(&self.spec.task_id)
            .map_err(|e| IndexTaskError::Checkpoint(e.to_string()))?
            .unwrap_or_default();

        let mut report = IndexTaskReport::default();
        let mut buffer: Vec<serde_json::Value> = Vec::new();
        let mut rows_since_checkpoint: u64 = 0;

        for (tp, range) in self.spec.assignment.ranges.iter() {
            let high = self.source.high_watermark(tp)?;
            let mut offset = self.resume_offset(tp, range, &checkpoint);

            'partition: loop {
                // Stop conditions: reached bounded end, or drained the log.
                if range.is_complete_at(offset) || offset >= high {
                    break;
                }
                let batch = self.source.poll(tp, offset, DEFAULT_POLL_BATCH)?;
                if batch.is_empty() {
                    break;
                }
                for rec in batch {
                    // Sparse-offset handling. A source may return a first
                    // record whose offset is BELOW `start` (skip and advance)
                    // or AT/BEYOND the exclusive `end` (compaction, retention,
                    // or transaction filtering can leave gaps so the next live
                    // record sits past the assigned window). We must never
                    // re-poll the same offset forever (DD R10 #3): if the
                    // record is below `start`, advance past it; if it is
                    // at/beyond `end`, the partition is done — pin `offset` to
                    // the range end and exit the OUTER loop.
                    if rec.offset < range.start {
                        // Below the window: skip but advance so the next poll
                        // makes progress.
                        offset = rec.offset + 1;
                        continue;
                    }
                    if !range.contains(rec.offset) {
                        // At/beyond the exclusive end: stop this partition.
                        if let Some(end) = range.end {
                            offset = offset.max(end);
                            checkpoint.record(tp.clone(), offset);
                        }
                        break 'partition;
                    }
                    match self.parse_record(&rec.payload) {
                        Ok(value) => {
                            buffer.push(value);
                            report.rows_consumed += 1;
                            rows_since_checkpoint += 1;
                        }
                        Err(e) => match self.bad_record_policy {
                            BadRecordPolicy::Fail => return Err(self.map_bad(tp, rec.offset, e)),
                            BadRecordPolicy::Skip => {
                                tracing::warn!(
                                    task = %self.spec.task_id,
                                    partition = ?tp,
                                    offset = rec.offset,
                                    "skipping malformed record",
                                );
                            }
                        },
                    }

                    // Advance: the next offset to consume is one past this record.
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

        // Final flush + checkpoint.
        if !buffer.is_empty() {
            self.flush(&mut buffer, &mut report)?;
        }
        self.persist(&checkpoint)?;
        report.final_offsets = checkpoint.next_offsets.clone();
        Ok(report)
    }

    fn parse_record(&self, payload: &[u8]) -> Result<serde_json::Value, BadRecord> {
        check_record_bounds(payload).map_err(|e| BadRecord::Bounds(e.to_string()))?;
        serde_json::from_slice(payload).map_err(|e| BadRecord::Parse(e.to_string()))
    }

    fn map_bad(&self, tp: &TopicPartition, offset: i64, bad: BadRecord) -> IndexTaskError {
        match bad {
            BadRecord::Bounds(m) => IndexTaskError::RecordBounds(m),
            BadRecord::Parse(m) => IndexTaskError::Parse {
                partition: tp.clone(),
                offset,
                message: m,
            },
        }
    }

    fn flush(
        &self,
        buffer: &mut Vec<serde_json::Value>,
        report: &mut IndexTaskReport,
    ) -> Result<(), IndexTaskError> {
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
        let segment = ingester
            .ingest(rows)
            .map_err(|e| IndexTaskError::SegmentBuild(e.to_string()))?;
        report.segments.push(segment);
        Ok(())
    }

    fn persist(&self, checkpoint: &Checkpoint) -> Result<(), IndexTaskError> {
        self.checkpoints
            .save(&self.spec.task_id, checkpoint)
            .map_err(|e| IndexTaskError::Checkpoint(e.to_string()))
    }
}

/// Internal classification of a record-level failure.
enum BadRecord {
    Bounds(String),
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::InMemoryCheckpointStore;
    use crate::source::{InMemoryKafkaSource, SourceRecord};

    fn tp(p: i32) -> TopicPartition {
        TopicPartition::new("events", p)
    }

    /// Build a source with `counts[p]` rows on partition `p`, each row a
    /// JSON object `{"__time": base+offset, "dim": "p<p>-o<offset>"}`.
    fn source_with(counts: &[usize]) -> InMemoryKafkaSource {
        let mut src = InMemoryKafkaSource::new();
        for (p, &n) in counts.iter().enumerate() {
            let part = tp(p as i32);
            src.ensure_partition(part.clone());
            for o in 0..n {
                let v = serde_json::json!({
                    "__time": 1_700_000_000_000_i64 + o as i64,
                    "dim": format!("p{p}-o{o}"),
                });
                src.append_json(part.clone(), &v, Some(o as i64))
                    .expect("append");
            }
        }
        src
    }

    fn spec_for(assignment: PartitionAssignment) -> KafkaIndexTaskSpec {
        let mut s = KafkaIndexTaskSpec::new(
            "task-1",
            "events",
            "__time",
            vec!["dim".to_owned()],
            assignment,
        );
        s.max_rows_per_segment = 1_000_000;
        s.checkpoint_every_rows = 2;
        s
    }

    #[test]
    fn consumes_only_assigned_partition_and_range() {
        // 3 partitions of 10 rows; task is assigned only p1 offsets [2,7).
        let src = source_with(&[10, 10, 10]);
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(1), OffsetRange::bounded(2, 7))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);

        let report = task.run().expect("run");
        // Offsets 2,3,4,5,6 = 5 rows.
        assert_eq!(report.rows_consumed, 5);
        assert_eq!(report.final_offsets.get(&tp(1)), Some(&7));
        // No other partition touched.
        assert!(!report.final_offsets.contains_key(&tp(0)));
        assert!(!report.final_offsets.contains_key(&tp(2)));
        // One segment of 5 rows.
        assert_eq!(report.segment_count(), 1);
        assert_eq!(report.segments[0].num_rows, 5);
    }

    /// A deterministic, finite source whose first record for a partition is
    /// returned at a fixed offset regardless of the requested `from` — and
    /// where that record may sit AT/BEYOND the assigned range end (modelling
    /// compaction/retention/txn-filtering gaps). The `poll_calls` counter is
    /// used to detect a re-poll spin: a buggy consumer that never advances
    /// would call `poll` an unbounded number of times.
    struct SparseSource {
        /// (partition, offset, payload) tuples, ascending by offset.
        records: Vec<(TopicPartition, i64, Vec<u8>)>,
        high: i64,
        poll_calls: std::cell::Cell<usize>,
    }

    impl PartitionedRecordSource for SparseSource {
        fn high_watermark(&self, _partition: &TopicPartition) -> Result<i64, SourceError> {
            Ok(self.high)
        }
        fn low_watermark(&self, _partition: &TopicPartition) -> Result<i64, SourceError> {
            Ok(0)
        }
        fn poll(
            &self,
            partition: &TopicPartition,
            from: i64,
            max_records: usize,
        ) -> Result<Vec<SourceRecord>, SourceError> {
            // Guard against runaway spinning: if the consumer fails to make
            // progress it would call poll forever. Bound it deterministically.
            let calls = self.poll_calls.get() + 1;
            self.poll_calls.set(calls);
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
                        timestamp_ms: Some(*off),
                    });
                    if out.len() >= max_records {
                        break;
                    }
                }
            }
            Ok(out)
        }
        fn partitions_for(&self, _topic: &str) -> Vec<TopicPartition> {
            vec![tp(0)]
        }
    }

    #[test]
    fn sparse_first_record_beyond_end_terminates_without_spin() {
        // DD R10 #3: assigned [0,10) but the partition's first live record is
        // at offset 12 (everything below was compacted/retained away). The
        // task must terminate, consuming zero in-range records, not spin
        // re-polling offset 0.
        let payload = serde_json::to_vec(&serde_json::json!({
            "__time": 1_700_000_000_000_i64,
            "dim": "beyond",
        }))
        .expect("payload");
        let src = SparseSource {
            records: vec![(tp(0), 12, payload)],
            high: 20,
            poll_calls: std::cell::Cell::new(0),
        };
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(0, 10))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let report = task.run().expect("run must terminate, not hang");
        assert_eq!(report.rows_consumed, 0, "no in-range records to consume");
        // Offset pinned to the range end so a resume does not re-scan.
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&10));
    }

    #[test]
    fn sparse_records_below_start_are_skipped_then_in_range_consumed() {
        // DD R10 #3: records exist below `start` and within range. The
        // below-start records must be skipped (advancing offset) and the
        // in-range ones consumed, without spinning.
        let mk = |s: &str| {
            serde_json::to_vec(&serde_json::json!({
                "__time": 1_700_000_000_000_i64,
                "dim": s,
            }))
            .expect("payload")
        };
        let src = SparseSource {
            records: vec![
                (tp(0), 1, mk("below")),
                (tp(0), 2, mk("below")),
                (tp(0), 5, mk("in")),
                (tp(0), 6, mk("in")),
                (tp(0), 11, mk("beyond")),
            ],
            high: 20,
            poll_calls: std::cell::Cell::new(0),
        };
        let store = InMemoryCheckpointStore::new();
        // Range [5,10): only offsets 5 and 6 should be consumed.
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(5, 10))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let report = task.run().expect("run must terminate");
        assert_eq!(report.rows_consumed, 2);
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&10));
    }

    #[test]
    fn consumes_multiple_assigned_partitions() {
        let src = source_with(&[10, 10]);
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([
            (tp(0), OffsetRange::bounded(0, 4)),
            (tp(1), OffsetRange::bounded(5, 10)),
        ]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let report = task.run().expect("run");
        // p0: 0..4 = 4 rows; p1: 5..10 = 5 rows.
        assert_eq!(report.rows_consumed, 9);
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&4));
        assert_eq!(report.final_offsets.get(&tp(1)), Some(&10));
    }

    #[test]
    fn open_ended_range_drains_to_high_watermark() {
        let src = source_with(&[6]);
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::open(0))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let report = task.run().expect("run");
        assert_eq!(report.rows_consumed, 6);
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&6));
    }

    #[test]
    fn checkpoint_records_offsets() {
        let src = source_with(&[5]);
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(0, 5))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        task.run().expect("run");

        let cp = store.load("task-1").expect("load").expect("some");
        assert_eq!(cp.next_offset(&tp(0)), Some(5));
    }

    #[test]
    fn resume_from_checkpoint_does_not_reconsume() {
        let src = source_with(&[10]);
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(0, 10))]);

        // First run consumes everything.
        let first = KafkaIndexTask::new(spec_for(assignment.clone()), &src, &store);
        let r1 = first.run().expect("run1");
        assert_eq!(r1.rows_consumed, 10);

        // Second run with the SAME task_id resumes from offset 10 and
        // consumes nothing more.
        let second = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let r2 = second.run().expect("run2");
        assert_eq!(r2.rows_consumed, 0, "must not re-consume checkpointed rows");
        assert_eq!(r2.segment_count(), 0);
    }

    #[test]
    fn resume_after_partial_progress() {
        // Pre-seed a checkpoint at offset 4; assigned range is [0,10).
        let src = source_with(&[10]);
        let store = InMemoryCheckpointStore::new();
        let mut cp = Checkpoint::empty();
        cp.record(tp(0), 4);
        store.save("task-1", &cp).expect("seed checkpoint");

        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(0, 10))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let report = task.run().expect("run");
        // Offsets 4..10 = 6 rows.
        assert_eq!(report.rows_consumed, 6);
    }

    #[test]
    fn checkpoint_below_start_does_not_rewind() {
        // Checkpoint at 1 but assigned start is 5 → must start at 5.
        let src = source_with(&[10]);
        let store = InMemoryCheckpointStore::new();
        let mut cp = Checkpoint::empty();
        cp.record(tp(0), 1);
        store.save("task-1", &cp).expect("seed");
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(5, 10))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let report = task.run().expect("run");
        // 5..10 = 5 rows.
        assert_eq!(report.rows_consumed, 5);
    }

    #[test]
    fn bad_record_fail_policy_aborts() {
        let mut src = source_with(&[2]);
        // Append a malformed record at offset 2.
        src.append(tp(0), b"not json".to_vec(), None);
        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(0, 3))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store);
        let err = task.run().unwrap_err();
        assert!(matches!(err, IndexTaskError::Parse { offset: 2, .. }));
    }

    #[test]
    fn bad_record_skip_policy_continues() {
        let mut src = source_with(&[2]);
        src.append(tp(0), b"not json".to_vec(), None);
        // valid row after the bad one.
        let v = serde_json::json!({"__time": 1_700_000_000_000_i64, "dim": "ok"});
        src.append_json(tp(0), &v, None).expect("append");

        let store = InMemoryCheckpointStore::new();
        let assignment = PartitionAssignment::from_ranges([(tp(0), OffsetRange::bounded(0, 4))]);
        let task = KafkaIndexTask::new(spec_for(assignment), &src, &store)
            .with_bad_record_policy(BadRecordPolicy::Skip);
        let report = task.run().expect("run");
        // 4 records seen, 1 skipped → 3 buffered rows.
        assert_eq!(report.rows_consumed, 3);
        // Checkpoint still advances past the bad record.
        assert_eq!(report.final_offsets.get(&tp(0)), Some(&4));
    }

    #[test]
    fn segment_flush_splits_on_max_rows() {
        let src = source_with(&[10]);
        let store = InMemoryCheckpointStore::new();
        let mut spec = spec_for(PartitionAssignment::from_ranges([(
            tp(0),
            OffsetRange::bounded(0, 10),
        )]));
        spec.max_rows_per_segment = 4;
        let task = KafkaIndexTask::new(spec, &src, &store);
        let report = task.run().expect("run");
        // 10 rows / 4 per segment → flushes at 4, 8, then final 2 = 3 segments.
        assert_eq!(report.segment_count(), 3);
        assert_eq!(report.segments[0].num_rows, 4);
        assert_eq!(report.segments[1].num_rows, 4);
        assert_eq!(report.segments[2].num_rows, 2);
    }
}
