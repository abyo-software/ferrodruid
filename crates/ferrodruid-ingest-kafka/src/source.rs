// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Pluggable record source for Kafka indexing tasks.
//!
//! Real Kafka I/O lives behind the `kafka-io` feature (see
//! [`crate::consumer`]). For unit testing partition assignment,
//! checkpointing and resume — and for single-process / embedded use —
//! this module provides a [`PartitionedRecordSource`] trait plus an
//! [`InMemoryKafkaSource`] that models a topic as a set of partitions,
//! each an append-only log of records addressed by 0-based offset.
//!
//! The trait is a deliberately small "poll one partition from an
//! offset" surface so that a real `rdkafka`-backed implementation could
//! be slotted in without changing the indexing-task logic.

use std::collections::BTreeMap;

use crate::partitions::TopicPartition;

/// A single record fetched from a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRecord {
    /// Partition the record was read from.
    pub partition: TopicPartition,
    /// Offset of this record within its partition.
    pub offset: i64,
    /// Raw record payload (typically JSON bytes).
    pub payload: Vec<u8>,
    /// Optional broker-assigned timestamp in epoch millis.
    pub timestamp_ms: Option<i64>,
}

/// Errors from a [`PartitionedRecordSource`].
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// The requested partition does not exist in the source.
    #[error("unknown partition: {0:?}")]
    UnknownPartition(TopicPartition),
    /// Underlying transport / client failure.
    #[error("source transport error: {0}")]
    Transport(String),
}

/// A source that can be polled for records on a specific partition
/// starting at a given offset.
///
/// `poll` returns up to `max_records` records with offset `>= from`,
/// in ascending offset order. An empty result means no more records are
/// currently available at or after `from` (caller decides whether to
/// retry, stop, or treat it as end-of-stream for a bounded range).
pub trait PartitionedRecordSource {
    /// The current high-water mark (next offset to be produced) for a
    /// partition — i.e. one past the last existing record. Used to
    /// resolve "latest" start positions and to detect end-of-log.
    fn high_watermark(&self, partition: &TopicPartition) -> Result<i64, SourceError>;

    /// The earliest available offset for a partition.
    fn low_watermark(&self, partition: &TopicPartition) -> Result<i64, SourceError>;

    /// Poll up to `max_records` records from `partition` with offset
    /// `>= from`, in ascending offset order.
    fn poll(
        &self,
        partition: &TopicPartition,
        from: i64,
        max_records: usize,
    ) -> Result<Vec<SourceRecord>, SourceError>;

    /// The set of partitions currently present for `topic`, sorted by
    /// partition id. Used by the supervisor to detect partition-count
    /// changes.
    fn partitions_for(&self, topic: &str) -> Vec<TopicPartition>;
}

/// In-memory append-only log for one partition.
#[derive(Debug, Clone, Default)]
struct PartitionLog {
    /// Records in offset order. The offset of `records[i]` is
    /// `base_offset + i`.
    records: Vec<(Option<i64>, Vec<u8>)>,
    /// Offset of `records[0]` (allows modelling log truncation; 0 here).
    base_offset: i64,
}

impl PartitionLog {
    fn low(&self) -> i64 {
        self.base_offset
    }

    fn high(&self) -> i64 {
        self.base_offset + self.records.len() as i64
    }
}

/// An in-memory multi-partition topic store implementing
/// [`PartitionedRecordSource`].
///
/// Records are appended per partition and addressed by contiguous
/// 0-based offsets. This is the test-driven Kafka stand-in used to
/// exercise assignment, checkpoint and resume without a broker.
#[derive(Debug, Clone, Default)]
pub struct InMemoryKafkaSource {
    partitions: BTreeMap<TopicPartition, PartitionLog>,
}

impl InMemoryKafkaSource {
    /// Create an empty source.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensure a partition exists (no-op if already present).
    pub fn ensure_partition(&mut self, tp: TopicPartition) {
        self.partitions.entry(tp).or_default();
    }

    /// Append a record to a partition, creating the partition if needed.
    /// Returns the offset assigned to the appended record.
    pub fn append(
        &mut self,
        tp: TopicPartition,
        payload: Vec<u8>,
        timestamp_ms: Option<i64>,
    ) -> i64 {
        let log = self.partitions.entry(tp).or_default();
        let offset = log.high();
        log.records.push((timestamp_ms, payload));
        offset
    }

    /// Convenience: append a JSON value as a record's payload.
    ///
    /// Returns the assigned offset, or a [`SourceError::Transport`] if
    /// the value cannot be serialized (practically unreachable for
    /// owned `serde_json::Value`).
    pub fn append_json(
        &mut self,
        tp: TopicPartition,
        value: &serde_json::Value,
        timestamp_ms: Option<i64>,
    ) -> Result<i64, SourceError> {
        let payload = serde_json::to_vec(value)
            .map_err(|e| SourceError::Transport(format!("serialize record: {e}")))?;
        Ok(self.append(tp, payload, timestamp_ms))
    }

    fn log(&self, tp: &TopicPartition) -> Result<&PartitionLog, SourceError> {
        self.partitions
            .get(tp)
            .ok_or_else(|| SourceError::UnknownPartition(tp.clone()))
    }
}

impl PartitionedRecordSource for InMemoryKafkaSource {
    fn high_watermark(&self, partition: &TopicPartition) -> Result<i64, SourceError> {
        Ok(self.log(partition)?.high())
    }

    fn low_watermark(&self, partition: &TopicPartition) -> Result<i64, SourceError> {
        Ok(self.log(partition)?.low())
    }

    fn poll(
        &self,
        partition: &TopicPartition,
        from: i64,
        max_records: usize,
    ) -> Result<Vec<SourceRecord>, SourceError> {
        let log = self.log(partition)?;
        if max_records == 0 {
            return Ok(Vec::new());
        }
        let start = from.max(log.base_offset);
        let mut out = Vec::new();
        let mut offset = start;
        while offset < log.high() && out.len() < max_records {
            let idx = (offset - log.base_offset) as usize;
            let (ts, payload) = &log.records[idx];
            out.push(SourceRecord {
                partition: partition.clone(),
                offset,
                payload: payload.clone(),
                timestamp_ms: *ts,
            });
            offset += 1;
        }
        Ok(out)
    }

    fn partitions_for(&self, topic: &str) -> Vec<TopicPartition> {
        self.partitions
            .keys()
            .filter(|tp| tp.topic == topic)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(p: i32) -> TopicPartition {
        TopicPartition::new("t", p)
    }

    fn seed() -> InMemoryKafkaSource {
        let mut src = InMemoryKafkaSource::new();
        for i in 0..5 {
            let v = serde_json::json!({"i": i});
            src.append_json(tp(0), &v, Some(1000 + i)).expect("append");
        }
        for i in 0..3 {
            let v = serde_json::json!({"j": i});
            src.append_json(tp(1), &v, Some(2000 + i)).expect("append");
        }
        src
    }

    #[test]
    fn watermarks() {
        let src = seed();
        assert_eq!(src.low_watermark(&tp(0)).expect("low"), 0);
        assert_eq!(src.high_watermark(&tp(0)).expect("high"), 5);
        assert_eq!(src.high_watermark(&tp(1)).expect("high"), 3);
    }

    #[test]
    fn poll_returns_contiguous_offsets() {
        let src = seed();
        let recs = src.poll(&tp(0), 0, 100).expect("poll");
        assert_eq!(recs.len(), 5);
        let offsets: Vec<i64> = recs.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
        assert_eq!(recs[0].partition, tp(0));
    }

    #[test]
    fn poll_respects_from_and_limit() {
        let src = seed();
        let recs = src.poll(&tp(0), 2, 2).expect("poll");
        let offsets: Vec<i64> = recs.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![2, 3]);
    }

    #[test]
    fn poll_past_high_is_empty() {
        let src = seed();
        assert!(src.poll(&tp(0), 5, 10).expect("poll").is_empty());
        assert!(src.poll(&tp(0), 99, 10).expect("poll").is_empty());
    }

    #[test]
    fn poll_unknown_partition_errors() {
        let src = seed();
        let err = src.poll(&tp(9), 0, 1).unwrap_err();
        assert!(matches!(err, SourceError::UnknownPartition(_)));
    }

    #[test]
    fn partitions_for_topic_is_sorted() {
        let mut src = seed();
        src.append(TopicPartition::new("other", 0), b"x".to_vec(), None);
        let parts = src.partitions_for("t");
        assert_eq!(parts, vec![tp(0), tp(1)]);
        assert_eq!(src.partitions_for("other").len(), 1);
        assert!(src.partitions_for("missing").is_empty());
    }
}
