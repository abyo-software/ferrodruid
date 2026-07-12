// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Post-aggregator types — re-exported from lib.rs where `PostAggregatorSpec` is defined.
//!
//! This module exists solely to satisfy the module layout requirement. The actual
//! `PostAggregatorSpec` enum, its serde impls, and `evaluate()` live in `lib.rs`
//! so that they can reference the same `value_to_f64` helper used by the trait.

// All post-aggregator logic is in lib.rs. This module is intentionally empty
// because PostAggregatorSpec is defined in lib.rs for cohesion with the
// Aggregator trait and helper functions.
