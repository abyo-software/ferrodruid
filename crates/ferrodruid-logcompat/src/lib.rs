// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `ferrodruid-logcompat` — static compatibility classification of Apache
//! Druid broker request logs.
//!
//! `ferro-logcompat` statically classifies the queries in a Druid request
//! log — it does **not** run anything and needs **no data**. (For live
//! verification against a running FerroDruid, use `ferrodruid-compat-check`
//! instead.)
//!
//! The pipeline is:
//!
//! 1. [`input`] — parse each log line of a Druid *file-based* request log
//!    (TSV lines whose fields include a JSON query object, or plain
//!    one-JSON-object-per-line logs) into a SQL string or a native query
//!    JSON value. Emitter-format logs are detected and skipped cleanly.
//! 2. [`shape`] — strip literal values (filter constants, interval bounds,
//!    limits) to a canonical *query shape*, so identical workloads with
//!    different constants group together and the report never contains
//!    literal values.
//! 3. [`classify`] — run each distinct shape's exemplar query through
//!    FerroDruid's **existing** parse + plan path (`parse_druid_sql` +
//!    `plan_sql` for SQL, the native-query deserializers for native)
//!    without executing anything, and bucket it as `supported`,
//!    `fail-closed` (recognized but deliberately rejected, with the
//!    reason) or `unsupported` (parse/plan error, with the reason).
//! 4. [`report`] — aggregate into a Markdown or JSON compatibility report
//!    weighted by shape frequency.
//!
//! Everything runs locally on the machine that invokes the binary; the
//! tool performs no network I/O of any kind.

pub mod classify;
pub mod input;
pub mod report;
pub mod shape;
