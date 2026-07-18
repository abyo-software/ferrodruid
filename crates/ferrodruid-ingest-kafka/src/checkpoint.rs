// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Checkpoint persistence for Kafka indexing tasks.
//!
//! A checkpoint records, per assigned `(topic, partition)`, the offset
//! of the **next** record the task should consume. On restart a task
//! loads its last checkpoint and resumes from there rather than
//! re-consuming records it has already processed.
//!
//! Persistence is abstracted behind the [`CheckpointStore`] trait so
//! callers can supply their own backing store (metadata DB, deep
//! storage, …) without this crate depending on the metadata crate. An
//! [`InMemoryCheckpointStore`] is provided for tests and single-process
//! use.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::partitions::TopicPartition;

/// One `(partition, next_offset)` entry in the JSON wire form of a
/// [`Checkpoint`]. JSON object keys must be strings, so the checkpoint
/// serializes as a list of these entries rather than a map.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointEntry {
    #[serde(flatten)]
    partition: TopicPartition,
    next_offset: i64,
}

/// A point-in-time record of consumed progress for one task.
///
/// `next_offsets[tp]` is the offset of the next record to consume for
/// partition `tp`. A partition absent from the map has no recorded
/// progress and resumes from its assigned start offset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// Next offset to consume, per partition.
    pub next_offsets: BTreeMap<TopicPartition, i64>,
}

impl Serialize for Checkpoint {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let entries: Vec<CheckpointEntry> = self
            .next_offsets
            .iter()
            .map(|(partition, &next_offset)| CheckpointEntry {
                partition: partition.clone(),
                next_offset,
            })
            .collect();
        entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Checkpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<CheckpointEntry>::deserialize(deserializer)?;
        let next_offsets = entries
            .into_iter()
            .map(|e| (e.partition, e.next_offset))
            .collect();
        Ok(Self { next_offsets })
    }
}

impl Checkpoint {
    /// An empty checkpoint (no recorded progress).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Record that `next_offset` is the next offset to consume for `tp`.
    ///
    /// Recording is monotonic: a lower offset never overwrites a higher
    /// one, so an out-of-order update cannot rewind progress.
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

    /// Whether any progress has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.next_offsets.is_empty()
    }
}

/// Errors from a [`CheckpointStore`].
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// The backing store failed (I/O, lock poisoning, serialization, …).
    #[error("checkpoint store error: {0}")]
    Store(String),
}

/// Persistence backend for task checkpoints.
///
/// Implementations must be safe to share across threads. `save` is
/// expected to be last-write-wins for a given `task_id`.
pub trait CheckpointStore: Send + Sync {
    /// Persist `checkpoint` for `task_id`, replacing any prior value.
    fn save(&self, task_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError>;

    /// Load the last checkpoint for `task_id`, or `None` if none exists.
    fn load(&self, task_id: &str) -> Result<Option<Checkpoint>, CheckpointError>;
}

/// In-memory [`CheckpointStore`] for tests and single-process use.
///
/// Clones share the same underlying map (via `Arc`), so a checkpoint
/// saved through one clone is visible through another — this models the
/// "restart the task, reload from the store" flow without external
/// infrastructure.
#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    inner: Arc<Mutex<BTreeMap<String, Checkpoint>>>,
}

impl InMemoryCheckpointStore {
    /// Create an empty in-memory checkpoint store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn save(&self, task_id: &str, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| CheckpointError::Store("checkpoint store mutex poisoned".to_owned()))?;
        guard.insert(task_id.to_owned(), checkpoint.clone());
        Ok(())
    }

    fn load(&self, task_id: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| CheckpointError::Store("checkpoint store mutex poisoned".to_owned()))?;
        Ok(guard.get(task_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(p: i32) -> TopicPartition {
        TopicPartition::new("topic", p)
    }

    #[test]
    fn checkpoint_record_is_monotonic() {
        let mut cp = Checkpoint::empty();
        assert!(cp.is_empty());
        cp.record(tp(0), 10);
        cp.record(tp(0), 25);
        // Lower update must not rewind.
        cp.record(tp(0), 5);
        assert_eq!(cp.next_offset(&tp(0)), Some(25));
        assert_eq!(cp.next_offset(&tp(1)), None);
    }

    #[test]
    fn store_save_load_roundtrip() {
        let store = InMemoryCheckpointStore::new();
        assert_eq!(store.load("t1").expect("load"), None);

        let mut cp = Checkpoint::empty();
        cp.record(tp(0), 42);
        cp.record(tp(1), 7);
        store.save("t1", &cp).expect("save");

        let loaded = store.load("t1").expect("load").expect("some");
        assert_eq!(loaded, cp);
        // Unrelated task id is unaffected.
        assert_eq!(store.load("other").expect("load"), None);
    }

    #[test]
    fn store_clones_share_state() {
        let store = InMemoryCheckpointStore::new();
        let clone = store.clone();
        let mut cp = Checkpoint::empty();
        cp.record(tp(0), 1);
        store.save("x", &cp).expect("save");
        // Visible through the clone — models task restart reload.
        assert_eq!(clone.load("x").expect("load"), Some(cp));
    }

    #[test]
    fn checkpoint_serde_roundtrip() {
        let mut cp = Checkpoint::empty();
        cp.record(tp(0), 100);
        cp.record(tp(3), 200);
        let json = serde_json::to_string(&cp).expect("ser");
        let back: Checkpoint = serde_json::from_str(&json).expect("de");
        assert_eq!(cp, back);
    }
}
