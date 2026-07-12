// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Partition assignment primitives for Kafka indexing tasks.
//!
//! Models Druid's `SeekableStreamStartSequenceNumbers` /
//! `SeekableStreamEndSequenceNumbers` for the Kafka case: a task is
//! assigned a set of `(topic, partition)` pairs, each with a start
//! offset (inclusive) and an optional end offset (exclusive). When the
//! end offset is `None` the assignment is open-ended (the supervisor
//! drives task hand-off by other means, e.g. a duration cap).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A `(topic, partition)` identifier.
///
/// Ordering is by topic name then partition id so that assignments and
/// checkpoints serialize deterministically.
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

/// Half-open offset range `[start, end)` for one partition.
///
/// Mirrors Druid's sequence-number semantics: `start` is the first
/// offset the task must consume (inclusive), `end` is the first offset
/// the task must **not** consume (exclusive). A `None` end means the
/// range is open-ended.
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

    /// An open-ended range starting at `start`.
    pub fn open(start: i64) -> Self {
        Self { start, end: None }
    }

    /// Returns `true` when `offset` falls inside this range (i.e. should
    /// be consumed): `offset >= start` and, if bounded, `offset < end`.
    #[must_use]
    pub fn contains(&self, offset: i64) -> bool {
        if offset < self.start {
            return false;
        }
        match self.end {
            Some(end) => offset < end,
            None => true,
        }
    }

    /// Returns `true` when `offset` has reached or passed the (bounded)
    /// end of this range. Open-ended ranges are never complete.
    #[must_use]
    pub fn is_complete_at(&self, next_offset: i64) -> bool {
        match self.end {
            Some(end) => next_offset >= end,
            None => false,
        }
    }
}

/// One `(partition, range)` entry in the JSON wire form of a
/// [`PartitionAssignment`]. JSON object keys must be strings, so the
/// assignment serializes as a list of these entries rather than a map.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AssignmentEntry {
    #[serde(flatten)]
    partition: TopicPartition,
    #[serde(flatten)]
    range: OffsetRange,
}

/// The full set of partition assignments handed to a single Kafka
/// indexing task. Equivalent to Druid's `KafkaIndexTaskIOConfig`
/// start/end sequence numbers combined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    /// Per-partition offset ranges, keyed for deterministic ordering.
    pub ranges: BTreeMap<TopicPartition, OffsetRange>,
}

impl Serialize for PartitionAssignment {
    fn serialize<Sr: serde::Serializer>(&self, serializer: Sr) -> Result<Sr::Ok, Sr::Error> {
        let entries: Vec<AssignmentEntry> = self
            .ranges
            .iter()
            .map(|(partition, range)| AssignmentEntry {
                partition: partition.clone(),
                range: range.clone(),
            })
            .collect();
        entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PartitionAssignment {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<AssignmentEntry>::deserialize(deserializer)?;
        let ranges = entries
            .into_iter()
            .map(|e| (e.partition, e.range))
            .collect();
        Ok(Self { ranges })
    }
}

impl PartitionAssignment {
    /// Create an empty assignment.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            ranges: BTreeMap::new(),
        }
    }

    /// Create an assignment from `(partition, range)` pairs.
    pub fn from_ranges(ranges: impl IntoIterator<Item = (TopicPartition, OffsetRange)>) -> Self {
        Self {
            ranges: ranges.into_iter().collect(),
        }
    }

    /// Insert or replace the range for a partition.
    pub fn assign(&mut self, tp: TopicPartition, range: OffsetRange) {
        self.ranges.insert(tp, range);
    }

    /// Number of assigned partitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Whether the assignment is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Whether this task is assigned the given partition.
    #[must_use]
    pub fn owns(&self, tp: &TopicPartition) -> bool {
        self.ranges.contains_key(tp)
    }

    /// The assigned partitions, sorted.
    pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition> {
        self.ranges.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_range_contains_bounded() {
        let r = OffsetRange::bounded(10, 20);
        assert!(!r.contains(9));
        assert!(r.contains(10));
        assert!(r.contains(19));
        assert!(!r.contains(20));
        assert!(!r.contains(100));
    }

    #[test]
    fn offset_range_contains_open() {
        let r = OffsetRange::open(5);
        assert!(!r.contains(4));
        assert!(r.contains(5));
        assert!(r.contains(1_000_000));
    }

    #[test]
    fn offset_range_complete_at() {
        let bounded = OffsetRange::bounded(0, 10);
        assert!(!bounded.is_complete_at(9));
        assert!(bounded.is_complete_at(10));
        assert!(bounded.is_complete_at(11));

        let open = OffsetRange::open(0);
        assert!(!open.is_complete_at(1_000_000));
    }

    #[test]
    fn assignment_ownership_and_ordering() {
        let mut a = PartitionAssignment::empty();
        assert!(a.is_empty());
        a.assign(TopicPartition::new("t", 2), OffsetRange::bounded(0, 5));
        a.assign(TopicPartition::new("t", 0), OffsetRange::open(0));
        a.assign(TopicPartition::new("a", 7), OffsetRange::bounded(1, 2));

        assert_eq!(a.len(), 3);
        assert!(a.owns(&TopicPartition::new("t", 0)));
        assert!(!a.owns(&TopicPartition::new("t", 9)));

        // BTreeMap iteration is sorted: ("a",7) < ("t",0) < ("t",2).
        let order: Vec<_> = a.partitions().cloned().collect();
        assert_eq!(
            order,
            vec![
                TopicPartition::new("a", 7),
                TopicPartition::new("t", 0),
                TopicPartition::new("t", 2),
            ]
        );
    }

    #[test]
    fn assignment_serde_roundtrip() {
        let a = PartitionAssignment::from_ranges([
            (
                TopicPartition::new("topic", 0),
                OffsetRange::bounded(0, 100),
            ),
            (TopicPartition::new("topic", 1), OffsetRange::open(50)),
        ]);
        let json = serde_json::to_string(&a).expect("ser");
        let back: PartitionAssignment = serde_json::from_str(&json).expect("de");
        assert_eq!(a, back);
    }
}
