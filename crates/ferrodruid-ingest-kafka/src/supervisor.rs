// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kafka supervisor task-planning logic.
//!
//! The supervisor owns one topic and decides how to split that topic's
//! partitions across a target number of indexing tasks. It does **not**
//! dispatch tasks over the network here — it *produces* a set of
//! [`crate::partitions::PartitionAssignment`]s (one per task) that an
//! Overlord (or a MiddleManager directly) can turn into running
//! [`crate::index_task::KafkaIndexTask`]s. Network dispatch is a
//! follow-up.
//!
//! Two responsibilities are modelled:
//!
//! * [`SupervisorPlanner::generate_tasks`] — split `partition ->
//!   start_offset` across `task_count` tasks (round-robin by partition
//!   id, mirroring Druid's `taskCount` fan-out), each task assigned an
//!   open-ended range from its start offset.
//! * [`SupervisorPlanner::detect_partition_change`] — compare a known
//!   partition set against a freshly-observed one and report
//!   added/removed partitions so the supervisor can re-plan.

use std::collections::{BTreeMap, BTreeSet};

use crate::partitions::{OffsetRange, PartitionAssignment, TopicPartition};

/// Lifecycle state of a supervisor's task-planning loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerState {
    /// Actively planning and (notionally) issuing tasks.
    Running,
    /// Paused — `generate_tasks` yields no work.
    Suspended,
}

/// The difference between a previously-known partition set and a newly
/// observed one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionChange {
    /// Partitions present now but not before.
    pub added: Vec<TopicPartition>,
    /// Partitions present before but not now.
    pub removed: Vec<TopicPartition>,
}

impl PartitionChange {
    /// Whether anything changed.
    #[must_use]
    pub fn is_changed(&self) -> bool {
        !self.added.is_empty() || !self.removed.is_empty()
    }
}

/// Plans indexing-task assignments for a single topic.
#[derive(Debug, Clone)]
pub struct SupervisorPlanner {
    /// Topic this supervisor owns.
    pub topic: String,
    /// Current planner state.
    pub state: PlannerState,
}

impl SupervisorPlanner {
    /// Create a running planner for `topic`.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            state: PlannerState::Running,
        }
    }

    /// Suspend planning (subsequent `generate_tasks` returns no work).
    pub fn suspend(&mut self) {
        self.state = PlannerState::Suspended;
    }

    /// Resume planning.
    pub fn resume(&mut self) {
        self.state = PlannerState::Running;
    }

    /// Split partitions across `task_count` indexing tasks.
    ///
    /// `partition_offsets` maps each `(topic, partition)` to the offset
    /// the next task for that partition should start at (e.g. the
    /// supervisor's last committed offset, or the low/high watermark for
    /// a fresh start). Each produced task is assigned an **open-ended**
    /// range from that start offset.
    ///
    /// Assignment is round-robin by sorted partition order, so task `k`
    /// receives partitions at sorted indices `k, k+task_count, …`. This
    /// matches Druid's behaviour where `taskCount` is capped at the
    /// partition count (a task is never created with zero partitions):
    /// the returned vector has length `min(task_count, partitions)` when
    /// there is at least one partition.
    ///
    /// Returns an empty vector when suspended, when `task_count == 0`,
    /// or when there are no partitions.
    #[must_use]
    pub fn generate_tasks(
        &self,
        partition_offsets: &BTreeMap<TopicPartition, i64>,
        task_count: usize,
    ) -> Vec<PartitionAssignment> {
        if self.state == PlannerState::Suspended || task_count == 0 {
            return Vec::new();
        }
        // Only consider partitions for this topic, in sorted order.
        let parts: Vec<(&TopicPartition, &i64)> = partition_offsets
            .iter()
            .filter(|(tp, _)| tp.topic == self.topic)
            .collect();
        if parts.is_empty() {
            return Vec::new();
        }

        let effective = task_count.min(parts.len());
        let mut tasks: Vec<PartitionAssignment> = (0..effective)
            .map(|_| PartitionAssignment::empty())
            .collect();

        for (idx, (tp, start)) in parts.iter().enumerate() {
            let slot = idx % effective;
            tasks[slot].assign((*tp).clone(), OffsetRange::open(**start));
        }
        tasks
    }

    /// Compare a previously-known partition set with a freshly-observed
    /// one and report what changed. Only partitions for this planner's
    /// topic are considered.
    #[must_use]
    pub fn detect_partition_change(
        &self,
        known: &BTreeSet<TopicPartition>,
        observed: &BTreeSet<TopicPartition>,
    ) -> PartitionChange {
        let in_topic = |tp: &&TopicPartition| tp.topic == self.topic;
        let added = observed
            .iter()
            .filter(in_topic)
            .filter(|tp| !known.contains(*tp))
            .cloned()
            .collect();
        let removed = known
            .iter()
            .filter(in_topic)
            .filter(|tp| !observed.contains(*tp))
            .cloned()
            .collect();
        PartitionChange { added, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(p: i32) -> TopicPartition {
        TopicPartition::new("orders", p)
    }

    fn offsets(n: i32) -> BTreeMap<TopicPartition, i64> {
        (0..n).map(|p| (tp(p), 0_i64)).collect()
    }

    fn total_partitions(tasks: &[PartitionAssignment]) -> usize {
        tasks.iter().map(PartitionAssignment::len).sum()
    }

    #[test]
    fn generate_one_task_gets_all_partitions() {
        let planner = SupervisorPlanner::new("orders");
        let tasks = planner.generate_tasks(&offsets(4), 1);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].len(), 4);
    }

    #[test]
    fn generate_splits_evenly() {
        let planner = SupervisorPlanner::new("orders");
        let tasks = planner.generate_tasks(&offsets(6), 3);
        assert_eq!(tasks.len(), 3);
        // 6 partitions / 3 tasks = 2 each.
        for t in &tasks {
            assert_eq!(t.len(), 2);
        }
        assert_eq!(total_partitions(&tasks), 6);
    }

    #[test]
    fn generate_splits_unevenly_round_robin() {
        let planner = SupervisorPlanner::new("orders");
        let tasks = planner.generate_tasks(&offsets(7), 3);
        assert_eq!(tasks.len(), 3);
        // 7 partitions round-robin over 3 → 3,2,2.
        let mut sizes: Vec<usize> = tasks.iter().map(PartitionAssignment::len).collect();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![2, 2, 3]);
        assert_eq!(total_partitions(&tasks), 7);
        // Every partition assigned exactly once (no overlap / no gap).
        let mut all: Vec<TopicPartition> =
            tasks.iter().flat_map(|t| t.partitions().cloned()).collect();
        all.sort();
        assert_eq!(all, (0..7).map(tp).collect::<Vec<_>>());
    }

    #[test]
    fn task_count_capped_at_partition_count() {
        let planner = SupervisorPlanner::new("orders");
        // 2 partitions, ask for 5 tasks → only 2 tasks.
        let tasks = planner.generate_tasks(&offsets(2), 5);
        assert_eq!(tasks.len(), 2);
        for t in &tasks {
            assert_eq!(t.len(), 1);
        }
    }

    #[test]
    fn generate_uses_start_offsets() {
        let planner = SupervisorPlanner::new("orders");
        let mut offs = BTreeMap::new();
        offs.insert(tp(0), 100);
        offs.insert(tp(1), 250);
        let tasks = planner.generate_tasks(&offs, 2);
        assert_eq!(tasks.len(), 2);
        // task 0 → partition 0 @100, task 1 → partition 1 @250.
        assert_eq!(tasks[0].ranges.get(&tp(0)), Some(&OffsetRange::open(100)));
        assert_eq!(tasks[1].ranges.get(&tp(1)), Some(&OffsetRange::open(250)));
    }

    #[test]
    fn zero_task_count_yields_nothing() {
        let planner = SupervisorPlanner::new("orders");
        assert!(planner.generate_tasks(&offsets(4), 0).is_empty());
    }

    #[test]
    fn no_partitions_yields_nothing() {
        let planner = SupervisorPlanner::new("orders");
        assert!(planner.generate_tasks(&BTreeMap::new(), 3).is_empty());
    }

    #[test]
    fn suspended_planner_yields_nothing() {
        let mut planner = SupervisorPlanner::new("orders");
        planner.suspend();
        assert_eq!(planner.state, PlannerState::Suspended);
        assert!(planner.generate_tasks(&offsets(4), 2).is_empty());
        planner.resume();
        assert_eq!(planner.generate_tasks(&offsets(4), 2).len(), 2);
    }

    #[test]
    fn generate_ignores_other_topics() {
        let planner = SupervisorPlanner::new("orders");
        let mut offs = offsets(2);
        offs.insert(TopicPartition::new("other", 0), 0);
        let tasks = planner.generate_tasks(&offs, 4);
        // Only the 2 "orders" partitions count.
        assert_eq!(tasks.len(), 2);
        assert_eq!(total_partitions(&tasks), 2);
    }

    #[test]
    fn detect_partition_growth() {
        let planner = SupervisorPlanner::new("orders");
        let known: BTreeSet<_> = (0..2).map(tp).collect();
        let observed: BTreeSet<_> = (0..4).map(tp).collect();
        let change = planner.detect_partition_change(&known, &observed);
        assert!(change.is_changed());
        assert_eq!(change.added, vec![tp(2), tp(3)]);
        assert!(change.removed.is_empty());
    }

    #[test]
    fn detect_partition_removal() {
        let planner = SupervisorPlanner::new("orders");
        let known: BTreeSet<_> = (0..4).map(tp).collect();
        let observed: BTreeSet<_> = (0..2).map(tp).collect();
        let change = planner.detect_partition_change(&known, &observed);
        assert_eq!(change.removed, vec![tp(2), tp(3)]);
        assert!(change.added.is_empty());
    }

    #[test]
    fn detect_no_change() {
        let planner = SupervisorPlanner::new("orders");
        let set: BTreeSet<_> = (0..3).map(tp).collect();
        let change = planner.detect_partition_change(&set, &set);
        assert!(!change.is_changed());
    }

    #[test]
    fn detect_ignores_other_topics() {
        let planner = SupervisorPlanner::new("orders");
        let mut known: BTreeSet<_> = (0..2).map(tp).collect();
        let mut observed: BTreeSet<_> = (0..2).map(tp).collect();
        // A change on a different topic must not register.
        known.insert(TopicPartition::new("other", 0));
        observed.insert(TopicPartition::new("other", 9));
        let change = planner.detect_partition_change(&known, &observed);
        assert!(!change.is_changed());
    }
}
