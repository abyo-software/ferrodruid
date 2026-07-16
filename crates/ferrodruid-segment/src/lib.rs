// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid Segment file format v9/FDX reader and writer.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod column;
pub(crate) mod druid_native;
pub mod fdx;
pub mod null_generation;
pub mod segment;
pub mod smoosh;
pub mod v9;
pub mod writer;

// Re-export the primary entry points.
pub use column::{ColumnData, ColumnDescriptor, StringColumnData};
pub use fdx::{write_segment_fdx, write_segment_fdx_to_memory};
pub use null_generation::{
    ColumnNullGeneration, NullGenerationReport, NullHandling, check_null_generation,
    classify_column, classify_segment,
};
pub use segment::{Interval, SegmentData, SegmentDataBuilder};
pub use smoosh::SmooshReader;
pub use writer::{write_segment_v9, write_segment_v9_to_memory};
