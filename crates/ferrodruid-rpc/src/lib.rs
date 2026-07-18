// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Cross-role HTTP RPC contracts for FerroDruid v1.0.
//!
//! Wave 39.HH (W3) landed the first two cross-role HTTP wires
//! (router→broker SQL forward, overlord→middleManager task
//! dispatch). Wave 40.LL (W4) closes the remaining two flows
//! (broker→historical query scatter, coordinator→historical
//! segment-load / drop / status), so all four cross-role flows in
//! the v1.0 architecture are now wired.
//!
//! This crate ships:
//!
//! 1. **Wire types** (`SqlQuery`, `SqlResponse`, `TaskAssignment`,
//!    `TaskStatus`, `SegmentQuery`, `SegmentQueryResponse`,
//!    `SegmentLoadCommand`, `SegmentDropCommand`, `LoadStatusReport`,
//!    etc.) shared between the client and server side of a
//!    cross-role call.
//! 2. **Client traits** ([`BrokerClient`], [`MiddleManagerClient`],
//!    [`HistoricalClient`]) used by the *caller* side of a cross-role
//!    flow. Today's router uses [`BrokerClient`]; the overlord uses
//!    [`MiddleManagerClient`]; the broker (for query scatter) and the
//!    coordinator (for segment placement) both use
//!    [`HistoricalClient`].
//! 3. **Mock implementations** for unit tests so the caller logic can
//!    be exercised without spinning up a real HTTP server.
//! 4. **Real HTTP implementations** ([`HttpBrokerClient`],
//!    [`HttpMiddleManagerClient`], [`HttpHistoricalClient`]) backed
//!    by `reqwest`, plus axum `Router` factories
//!    ([`broker_server::router`], [`mm_server::router`],
//!    [`historical_server::router`]) the *callee* binary mounts on
//!    its HTTP server.
//!
//! The four cross-role flows wired by Wave 39.HH (W3) + Wave 40.LL
//! (W4) — **all 4/4 of the v1.0 cross-role architecture**:
//!
//! | caller       | callee         | endpoint                                       |
//! |--------------|----------------|------------------------------------------------|
//! | router       | broker         | `POST /druid/v2/sql` (Druid-aligned)           |
//! | router       | broker         | `GET /druid/v2/info`                           |
//! | overlord     | middleManager  | `POST /druid/v1/middlemanager/task`            |
//! | overlord     | middleManager  | `GET /druid/v1/middlemanager/task/{id}/status` |
//! | broker       | historical     | `POST /druid/v2/native` (per-segment scatter)  |
//! | coordinator  | historical     | `POST /druid/v1/historical/load`               |
//! | coordinator  | historical     | `POST /druid/v1/historical/drop`               |
//! | coordinator  | historical     | `GET /druid/v1/historical/loadstatus`          |
//!
//! ## Honest scope (Wave 40.LL)
//!
//! - The broker → historical scatter is **wired but stub-executed**:
//!   the historical's `POST /druid/v2/native` handler echoes the query
//!   string back as a single-row response. Real per-segment query
//!   execution (loading the segment, running the query against it)
//!   stays in the single-binary path until W5.
//! - The coordinator → historical load command is **wired but
//!   stub-executed**: the historical accepts the command, marks the
//!   segment `Loading`, and a tokio timer flips it to `Loaded`. No
//!   actual deep-storage fetch happens; W5 wires
//!   `ferrodruid-deep-storage`.
//! - There is **no authentication, mTLS, or retry** between roles in
//!   this wave; the deferred-to-W5 hardening list lives in
//!   `docs/v1.0-roadmap.md`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod broker_client;
pub mod broker_server;
pub mod cross_role_server;
pub mod cross_role_startup;
pub mod cross_role_tls;
mod error;
pub mod historical_client;
pub mod historical_server;
pub mod mm_client;
pub mod mm_server;
pub mod native_query;
pub mod sql_bridge;
pub mod types;

pub use broker_client::{BrokerClient, HttpBrokerClient, MockBrokerClient};
pub use cross_role_server::{CrossRoleListener, CrossRoleServeError, serve_cross_role};
pub use cross_role_startup::CrossRoleStartup;
pub use cross_role_tls::{
    CrossRoleMtlsMode, CrossRoleTlsConfig, CrossRoleTlsError,
    build_client as build_cross_role_client,
    build_server_acceptor as build_cross_role_server_acceptor, load_from_dir, validate,
};
pub use error::RpcError;
pub use historical_client::{HistoricalClient, HttpHistoricalClient, MockHistoricalClient};
pub use mm_client::{HttpMiddleManagerClient, MiddleManagerClient, MockMiddleManagerClient};
pub use native_query::{
    Aggregation, EqualsFilter, GroupBySpec, HavingClause, NativeQuery, NativeQueryResult, ScanSpec,
    SortDirection, SortSpec, TimeseriesBucket, TimeseriesSpec, TopNSpec, merge_group_by,
    merge_scan, merge_timeseries, merge_top_n,
};
pub use sql_bridge::{
    BridgedQuery, TranslateError, TranslateResult, default_schema_for_sql, translate_query,
    translate_sql,
};
pub use types::{
    BrokerInfo, LoadStatusReport, SegmentDropCommand, SegmentLoadCommand, SegmentLoadState,
    SegmentQuery, SegmentQueryResponse, SqlQuery, SqlResponse, SqlResultFormat, TaskAssignment,
    TaskKind, TaskState, TaskStatus, TierHint,
};
