// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Postgres/MySQL/SQLite metadata store via sqlx for FerroDruid.
//!
//! One Druid-compatible schema (segments, rules, supervisors, config,
//! audit, task logs, task locks) over three interchangeable backends:
//! SQLite (the default — file or in-memory), PostgreSQL and MySQL.
//! Select a backend with [`MetadataStore::connect`] and a URI
//! (`postgres://…`, `mysql://…`, `sqlite://<path>`, a bare path, or
//! `:memory:`); behavior is identical across backends and the store
//! test suite runs parametrized over all three.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod ddl;
mod dialect;
mod exec;
mod store;
mod uri;

pub use store::{
    MetadataStore, PublishLock, SegmentMetadataRow, SupervisorRow, TaskLockRow, TaskRow,
};
pub use uri::{MetadataUri, parse_metadata_uri, redact_metadata_uri};
