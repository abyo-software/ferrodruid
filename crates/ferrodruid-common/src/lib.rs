// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Shared types, errors, and configuration for FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod config;
pub mod error;
pub mod memory;
pub mod null_mode;
pub mod tpch;
pub mod types;

// Re-export commonly used items at crate root for convenience.
pub use error::{DruidError, Result};
pub use null_mode::legacy_null_mode;
