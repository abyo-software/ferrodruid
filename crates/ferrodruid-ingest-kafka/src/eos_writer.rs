// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Exactly-once segment writer with on-disk offset tracking.
//!
//! Wave V1-A CL-1 closure. Closes the at-least-once gap of
//! [`crate::consumer::KafkaConsumerTask::run`] for the integration-test
//! scenarios mandated by `ROADMAP.md` CL-1 closure bar:
//!
//! 1. **Atomic publish.** A segment is written to a `pending/` directory,
//!    then atomically renamed to `published/`. A crash mid-write leaves
//!    no `published/<id>/` entry and the partial pending dir is ignored
//!    on the next start.
//! 2. **Embedded offsets.** Each published segment carries an
//!    `offsets.json` sidecar declaring the half-open `(start, next)`
//!    offset span it covers per partition. The resume offset for a
//!    partition is the max `next` across all published segments. This
//!    is the *durable* checkpoint — no separate file can drift out of
//!    sync with the segment set.
//! 3. **Replay on restart.** A consumer rebuilt with the same
//!    `base_dir` and the same Kafka consumer-group will `seek` to the
//!    resume offsets and continue, with no duplicate or missing rows
//!    in the *published* segment set even if the prior process was
//!    killed mid-flush.
//!
//! This module is intentionally I/O-independent of `rdkafka` so the
//! exactly-once invariants are unit-testable without a broker (the
//! `kafka_eos_real` integration test layers `rdkafka` on top in the
//! `kafka-io`-feature-gated test crate).

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use ferrodruid_ingest_batch::{BatchIngester, IngestedSegment};
use ferrodruid_segment::writer::write_segment_v9;
use serde::{Deserialize, Serialize};

use crate::partitions::TopicPartition;

/// Half-open offset span `[start, next)` covered by one published segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffsetSpan {
    /// First offset included in the segment (inclusive).
    pub start: i64,
    /// First offset NOT included in the segment (exclusive). This is also
    /// the value used as the resume offset for this partition.
    pub next: i64,
}

impl OffsetSpan {
    /// Construct a span covering offsets `[start, next)`.
    #[must_use]
    pub const fn new(start: i64, next: i64) -> Self {
        Self { start, next }
    }
}

/// One per-partition entry inside an `offsets.json` sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OffsetEntry {
    topic: String,
    partition: i32,
    start: i64,
    next: i64,
}

/// Contents of a segment's `offsets.json` sidecar file.
///
/// `task_id` is the logical owner of the segment — a single physical
/// `base_dir` may host multiple tasks with disjoint partition sets
/// (e.g. the rebalance-test scenario). Resume-offset computation
/// filters by `task_id` so an unrelated task's segments do not perturb
/// the answer.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OffsetSidecar {
    task_id: String,
    spans: Vec<OffsetEntry>,
}

/// A published segment as discovered on disk.
#[derive(Debug, Clone)]
pub struct PublishedSegment {
    /// Directory containing the segment files (under `published/`).
    pub dir: PathBuf,
    /// Task that produced the segment.
    pub task_id: String,
    /// Per-partition offset spans covered by the segment.
    pub spans: BTreeMap<TopicPartition, OffsetSpan>,
}

/// Errors from [`EosSegmentWriter`].
#[derive(Debug, thiserror::Error)]
pub enum EosError {
    /// Filesystem I/O failure (create_dir, rename, write, …).
    #[error("eos i/o error at {path}: {source}")]
    Io {
        /// Filesystem path the operation was acting on.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// JSON serialisation or deserialisation failure.
    #[error("eos serde error: {0}")]
    Serde(String),
    /// Segment build (BatchIngester) failure.
    #[error("eos segment build error: {0}")]
    Build(String),
    /// `publish` was called with an empty row buffer.
    #[error("eos publish refused: row buffer is empty")]
    EmptyBuffer,
}

/// One in-progress batch on its way to becoming a published segment.
pub struct PendingBatch {
    /// Rows buffered for the segment, in arrival order.
    pub rows: Vec<serde_json::Value>,
    /// Per-partition `[start, next)` span the rows came from.
    pub spans: BTreeMap<TopicPartition, OffsetSpan>,
}

impl PendingBatch {
    /// Construct an empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            spans: BTreeMap::new(),
        }
    }

    /// Whether the batch has any buffered rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Buffered row count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Record one consumed row and extend the partition's span to cover
    /// `offset`.  The span `next` advances to `offset + 1` so a
    /// subsequent crash + resume starts at the right place.
    pub fn record(&mut self, partition: TopicPartition, offset: i64, row: serde_json::Value) {
        self.rows.push(row);
        let entry = self
            .spans
            .entry(partition)
            .or_insert(OffsetSpan::new(offset, offset + 1));
        if offset < entry.start {
            entry.start = offset;
        }
        if offset + 1 > entry.next {
            entry.next = offset + 1;
        }
    }
}

impl Default for PendingBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Coordinator that publishes Kafka segments atomically with the
/// offset checkpoint each segment covers.
///
/// Directory layout under `base_dir`:
///
/// ```text
/// base_dir/
///   pending/                  # in-flight writes; ignored on restart
///     <segment_id>/
///   published/                # durable, atomically-renamed final state
///     <segment_id>/
///       meta.smoosh           # segment v9 files (BatchIngester output)
///       …
///       offsets.json          # OffsetSidecar — per-partition (start,next)
/// ```
pub struct EosSegmentWriter {
    base_dir: PathBuf,
}

impl EosSegmentWriter {
    /// Open or create a writer rooted at `base_dir`.
    ///
    /// Idempotent: creates `pending/` and `published/` if absent, and
    /// purges any leftover `pending/` entries from a prior crashed run
    /// so they cannot be confused with valid published segments.
    pub fn new(base_dir: impl Into<PathBuf>) -> Result<Self, EosError> {
        let base_dir = base_dir.into();
        let pending = base_dir.join("pending");
        let published = base_dir.join("published");
        fs::create_dir_all(&pending).map_err(|e| EosError::Io {
            path: pending.clone(),
            source: e,
        })?;
        fs::create_dir_all(&published).map_err(|e| EosError::Io {
            path: published,
            source: e,
        })?;

        // Sweep stale pending entries — anything under pending/ from a
        // prior crashed run is by definition unpublished and must be
        // discarded so it cannot leak into list_published().
        if let Ok(rd) = fs::read_dir(&pending) {
            for entry in rd.flatten() {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
        Ok(Self { base_dir })
    }

    /// Root directory passed to [`Self::new`].
    #[must_use]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Publish `batch` as a single segment under `segment_id`.
    ///
    /// Atomicity contract:
    /// 1. Write segment files + `offsets.json` into `pending/<id>/`.
    /// 2. fsync the directory's files.
    /// 3. Atomically `rename` `pending/<id>/` → `published/<id>/`.
    ///
    /// A crash between (1) and (3) leaves only `pending/<id>/`, which
    /// [`Self::new`] clears on the next start — the segment is treated
    /// as if it was never produced and Kafka offsets must NOT have been
    /// committed for it (caller responsibility).
    pub fn publish(
        &self,
        task_id: &str,
        segment_id: &str,
        batch: PendingBatch,
        ingester: &BatchIngester,
    ) -> Result<(PublishedSegment, IngestedSegment), EosError> {
        if batch.is_empty() {
            return Err(EosError::EmptyBuffer);
        }
        let pending_dir = self.base_dir.join("pending").join(segment_id);
        let published_dir = self.base_dir.join("published").join(segment_id);

        // If a leftover pending dir for this id exists (re-publish after
        // a prior crash for the same id), remove it first so write_segment_v9
        // sees a clean target.
        let _ = fs::remove_dir_all(&pending_dir);
        fs::create_dir_all(&pending_dir).map_err(|e| EosError::Io {
            path: pending_dir.clone(),
            source: e,
        })?;

        let ingested = ingester
            .ingest(batch.rows)
            .map_err(|e| EosError::Build(e.to_string()))?;
        write_segment_v9(&ingested.segment_data, &pending_dir)
            .map_err(|e| EosError::Build(e.to_string()))?;

        let sidecar = OffsetSidecar {
            task_id: task_id.to_owned(),
            spans: batch
                .spans
                .iter()
                .map(|(tp, span)| OffsetEntry {
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                    start: span.start,
                    next: span.next,
                })
                .collect(),
        };
        let sidecar_path = pending_dir.join("offsets.json");
        let mut f = fs::File::create(&sidecar_path).map_err(|e| EosError::Io {
            path: sidecar_path.clone(),
            source: e,
        })?;
        let json =
            serde_json::to_vec_pretty(&sidecar).map_err(|e| EosError::Serde(e.to_string()))?;
        f.write_all(&json).map_err(|e| EosError::Io {
            path: sidecar_path.clone(),
            source: e,
        })?;
        f.sync_all().map_err(|e| EosError::Io {
            path: sidecar_path.clone(),
            source: e,
        })?;
        drop(f);

        // Atomic publish: rename leaves either the old state or the new
        // state visible to a concurrent reader / restarted process.
        if published_dir.exists() {
            // Idempotency: a repeated publish for the same id (e.g. a
            // resume that re-emits the same span) is a no-op on the
            // published side. Discard the freshly-built pending dir.
            let _ = fs::remove_dir_all(&pending_dir);
        } else {
            fs::rename(&pending_dir, &published_dir).map_err(|e| EosError::Io {
                path: published_dir.clone(),
                source: e,
            })?;
        }

        Ok((
            PublishedSegment {
                dir: published_dir,
                task_id: task_id.to_owned(),
                spans: batch.spans,
            },
            ingested,
        ))
    }

    /// Enumerate published segments under `base_dir`. Entries without a
    /// readable `offsets.json` sidecar are skipped — they did not pass
    /// the publish-time invariant and are treated as corrupted.
    pub fn list_published(&self) -> Result<Vec<PublishedSegment>, EosError> {
        let dir = self.base_dir.join("published");
        let mut out = Vec::new();
        let rd = fs::read_dir(&dir).map_err(|e| EosError::Io {
            path: dir.clone(),
            source: e,
        })?;
        for entry in rd.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let sidecar_path = path.join("offsets.json");
            let bytes = match fs::read(&sidecar_path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let sidecar: OffsetSidecar = match serde_json::from_slice(&bytes) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let spans = sidecar
                .spans
                .into_iter()
                .map(|e| {
                    (
                        TopicPartition::new(e.topic, e.partition),
                        OffsetSpan::new(e.start, e.next),
                    )
                })
                .collect();
            out.push(PublishedSegment {
                dir: path,
                task_id: sidecar.task_id,
                spans,
            });
        }
        Ok(out)
    }

    /// Compute the resume offset for every partition this `task_id`
    /// has previously published: the max `next` across all matching
    /// published segments. Partitions with no published history are
    /// absent from the returned map.
    pub fn resume_offsets(&self, task_id: &str) -> Result<BTreeMap<TopicPartition, i64>, EosError> {
        let mut out: BTreeMap<TopicPartition, i64> = BTreeMap::new();
        for seg in self.list_published()? {
            if seg.task_id != task_id {
                continue;
            }
            for (tp, span) in seg.spans {
                let cur = out.entry(tp).or_insert(span.next);
                if span.next > *cur {
                    *cur = span.next;
                }
            }
        }
        Ok(out)
    }

    /// All resume offsets across all task ids that have published under
    /// `base_dir`. Used for cross-task rebalance assertions where the
    /// caller wants to see "what is the next offset for partition P,
    /// regardless of which task last wrote it?".
    pub fn resume_offsets_all_tasks(&self) -> Result<BTreeMap<TopicPartition, i64>, EosError> {
        let mut out: BTreeMap<TopicPartition, i64> = BTreeMap::new();
        for seg in self.list_published()? {
            for (tp, span) in seg.spans {
                let cur = out.entry(tp).or_insert(span.next);
                if span.next > *cur {
                    *cur = span.next;
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_segment::segment::SegmentData;
    use tempfile::TempDir;

    fn tp(p: i32) -> TopicPartition {
        TopicPartition::new("events", p)
    }

    fn make_row(time: i64, dim: &str) -> serde_json::Value {
        serde_json::json!({ "__time": time, "dim": dim })
    }

    fn ingester() -> BatchIngester {
        BatchIngester::new(
            "events".to_owned(),
            "__time".to_owned(),
            vec!["dim".to_owned()],
            vec![],
        )
    }

    fn new_writer() -> (TempDir, EosSegmentWriter) {
        let dir = TempDir::new().expect("tempdir");
        let w = EosSegmentWriter::new(dir.path()).expect("writer");
        (dir, w)
    }

    #[test]
    fn pending_batch_records_span() {
        let mut batch = PendingBatch::new();
        batch.record(tp(0), 10, make_row(1_700_000_000_000, "a"));
        batch.record(tp(0), 11, make_row(1_700_000_000_001, "b"));
        batch.record(tp(1), 4, make_row(1_700_000_000_002, "c"));

        assert_eq!(batch.len(), 3);
        assert_eq!(batch.spans.get(&tp(0)), Some(&OffsetSpan::new(10, 12)));
        assert_eq!(batch.spans.get(&tp(1)), Some(&OffsetSpan::new(4, 5)));
    }

    #[test]
    fn publish_then_list_roundtrips() {
        let (_tmp, w) = new_writer();
        let mut batch = PendingBatch::new();
        for o in 0..5 {
            batch.record(tp(0), o, make_row(1_700_000_000_000 + o, "x"));
        }
        let (pub_seg, ingested) = w
            .publish("task-1", "seg-0", batch, &ingester())
            .expect("publish");
        assert_eq!(ingested.num_rows, 5);

        // Segment dir on disk and round-trippable as v9.
        assert!(pub_seg.dir.join("meta.smoosh").exists());
        let segment = SegmentData::open(&pub_seg.dir).expect("open segment");
        assert_eq!(segment.num_rows(), 5);

        // Listing surfaces the segment with the right span.
        let listed = w.list_published().expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].task_id, "task-1");
        assert_eq!(listed[0].spans.get(&tp(0)), Some(&OffsetSpan::new(0, 5)));
    }

    #[test]
    fn empty_publish_refused() {
        let (_tmp, w) = new_writer();
        let err = w
            .publish("t", "seg-0", PendingBatch::new(), &ingester())
            .expect_err("must refuse");
        assert!(matches!(err, EosError::EmptyBuffer));
    }

    #[test]
    fn resume_offsets_takes_max_next_per_partition() {
        let (_tmp, w) = new_writer();
        // First segment covers p0 [0,3).
        let mut b1 = PendingBatch::new();
        for o in 0..3 {
            b1.record(tp(0), o, make_row(1_700_000_000_000 + o, "x"));
        }
        w.publish("t1", "seg-0", b1, &ingester()).expect("p1");

        // Second segment covers p0 [3,7) and p1 [10,12).
        let mut b2 = PendingBatch::new();
        for o in 3..7 {
            b2.record(tp(0), o, make_row(1_700_000_000_000 + o, "y"));
        }
        for o in 10..12 {
            b2.record(tp(1), o, make_row(1_700_000_000_100 + o, "z"));
        }
        w.publish("t1", "seg-1", b2, &ingester()).expect("p2");

        let resume = w.resume_offsets("t1").expect("resume");
        assert_eq!(resume.get(&tp(0)), Some(&7));
        assert_eq!(resume.get(&tp(1)), Some(&12));
    }

    #[test]
    fn resume_offsets_filters_by_task_id() {
        let (_tmp, w) = new_writer();
        // Task A publishes p0 [0,5).
        let mut a = PendingBatch::new();
        for o in 0..5 {
            a.record(tp(0), o, make_row(1_700_000_000_000 + o, "a"));
        }
        w.publish("task-A", "seg-a", a, &ingester()).expect("pa");

        // Task B publishes p1 [0,3).
        let mut b = PendingBatch::new();
        for o in 0..3 {
            b.record(tp(1), o, make_row(1_700_000_000_100 + o, "b"));
        }
        w.publish("task-B", "seg-b", b, &ingester()).expect("pb");

        let a_resume = w.resume_offsets("task-A").expect("a");
        assert_eq!(a_resume.get(&tp(0)), Some(&5));
        assert!(!a_resume.contains_key(&tp(1)));

        let b_resume = w.resume_offsets("task-B").expect("b");
        assert_eq!(b_resume.get(&tp(1)), Some(&3));
        assert!(!b_resume.contains_key(&tp(0)));

        // Cross-task view sees both.
        let all = w.resume_offsets_all_tasks().expect("all");
        assert_eq!(all.get(&tp(0)), Some(&5));
        assert_eq!(all.get(&tp(1)), Some(&3));
    }

    #[test]
    fn new_purges_leftover_pending() {
        let tmp = TempDir::new().expect("tempdir");
        // Simulate a prior crashed publish: create pending/seg-crash/
        // with a half-written file. A subsequent EosSegmentWriter::new
        // must remove it so list_published() never sees it.
        let crashed = tmp.path().join("pending").join("seg-crash");
        fs::create_dir_all(&crashed).expect("mk crash");
        fs::write(crashed.join("partial.bin"), b"abc").expect("seed");

        let w = EosSegmentWriter::new(tmp.path()).expect("writer");
        assert!(!tmp.path().join("pending").join("seg-crash").exists());
        assert!(w.list_published().expect("list").is_empty());
    }

    #[test]
    fn republish_same_id_is_idempotent() {
        let (_tmp, w) = new_writer();
        let mut b = PendingBatch::new();
        for o in 0..3 {
            b.record(tp(0), o, make_row(1_700_000_000_000 + o, "x"));
        }
        w.publish("t", "seg-0", b, &ingester()).expect("first");

        // A duplicate publish (e.g. retry that didn't observe success)
        // must not corrupt the existing published segment.
        let mut b2 = PendingBatch::new();
        for o in 0..3 {
            b2.record(tp(0), o, make_row(1_700_000_000_000 + o, "x"));
        }
        w.publish("t", "seg-0", b2, &ingester()).expect("retry");

        let listed = w.list_published().expect("list");
        assert_eq!(listed.len(), 1);
        // Pending dir cleaned up after the no-op retry.
        assert!(
            !w.base_dir().join("pending").join("seg-0").exists(),
            "pending dir must be cleaned up after idempotent retry"
        );
    }
}
