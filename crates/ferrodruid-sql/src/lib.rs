// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid SQL dialect parser and query planner (Calcite-compatible).
//!
//! This crate provides:
//! - A SQL parser that recognises Druid-specific functions (`TIME_FLOOR`,
//!   `APPROX_COUNT_DISTINCT`, etc.) on top of standard SQL
//! - A query planner that converts parsed SQL into Druid Native Queries
//! - SQL type mapping between Druid column types and SQL types

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod functions;
pub mod parser;
pub mod planner;
pub mod types;

pub use functions::{
    FrameBound, FrameMode, WindowFrame, WindowFunction, WindowFunctionType, is_window_function,
};
pub use parser::{DruidSqlStatement, SelectQuery, parse_druid_sql};
pub use planner::{
    ColumnSchema, DataSourceSchema, PlannedJoin, PlannedQuery, PlannerOptions, plan_sql,
    plan_sql_with_options,
};
pub use types::{OutputColumn, SqlType};
