// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Postgres/MySQL/SQLite metadata store via sqlx for FerroDruid.
//!
//! Phase 1 targets SQLite as the backend, with a Druid-compatible schema
//! covering segments, rules, supervisors, config, audit, task logs, and
//! task locks.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod store;

pub use store::{
    MetadataStore, PublishLock, SegmentMetadataRow, SupervisorRow, TaskLockRow, TaskRow,
};
