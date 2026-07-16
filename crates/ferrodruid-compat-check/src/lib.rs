// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `ferro-compat-check` — self-serve Druid-SQL compatibility checker.
//!
//! Point the bundled `ferro-compat-check` binary at a RUNNING
//! FerroDruid endpoint and it verifies, without needing an Apache
//! Druid cluster, that the endpoint reproduces KNOWN-CORRECT Apache
//! Druid behavior:
//!
//! ```text
//! ferro-compat-check --url http://host:8888 [--auth user:pass] \
//!     [--datasource PREFIX] [--section null|grains|rollup|aggregates|superset|ping|all] \
//!     [--json] [--cleanup]
//! ```
//!
//! The battery ingests three tiny inline fixtures (zero setup), then
//! runs ~40 SQL probes whose expected values are lifted verbatim from
//! the live Druid ⇄ FerroDruid diff-harness evidence
//! (`crates/ferrodruid-rest/tests/druid_diff_test.rs`,
//! `tests/druid-compat/RESULTS_*`; Druid 30.0.1-36.0.0, re-run
//! 2026-07-11). Sections mirror the harness: null semantics, Superset
//! time grains, ingestion-time rollup, base aggregates,
//! INFORMATION_SCHEMA introspection, and a `SELECT 1` ping.
//!
//! Honesty contract: surfaces that are KNOWN to diverge between the
//! engines by construction (EXPLAIN plan bodies, the
//! `week_ending_saturday` grain) are marked informational — they are
//! recorded but never asserted, and known-unsupported features
//! (JOIN / CTE execution, CL-4-R8) are not probed at all. See
//! `docs/design/compatibility-modes.md` for the full stance.
//!
//! The library half (this crate) is the pure probe/assertion engine;
//! it is unit-tested with canned JSON responses so `cargo test`
//! validates the classification logic without any server.

pub mod catalog;
pub mod probe;
pub mod report;
pub mod runner;
