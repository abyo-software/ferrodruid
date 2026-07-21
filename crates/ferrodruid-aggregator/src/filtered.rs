// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Filtered aggregator — wraps any aggregator with a filter predicate.

use std::any::Any;

use crate::Aggregator;

/// Wraps an inner aggregator whose input rows are pre-filtered by the query
/// engine.
///
/// The filter is stored as an opaque JSON value (this crate cannot depend on
/// the query crate's `FilterSpec`).  Per-row filter evaluation is the query
/// engine's responsibility: its feed helpers evaluate the filter against each
/// row and only feed matching rows, so this wrapper itself delegates
/// unconditionally.
#[derive(Debug, Clone)]
pub struct FilteredAggregator {
    filter: serde_json::Value,
    inner: Box<dyn Aggregator>,
}

impl FilteredAggregator {
    /// Creates a new filtered aggregator wrapping `inner` with the given filter.
    pub fn new(filter: serde_json::Value, inner: Box<dyn Aggregator>) -> Self {
        Self { filter, inner }
    }

    /// Returns a reference to the stored filter definition.
    pub fn filter(&self) -> &serde_json::Value {
        &self.filter
    }

    /// Returns the wrapped inner aggregator.
    ///
    /// Multi-shard exact union (2026-07-11): lets the partial-emission
    /// probe ([`crate::exact_cardinality_partial`]) reach a
    /// `CardinalityAggregator` through the `filtered` wrapper the E16
    /// exact `COUNT(DISTINCT)` SQL lowering produces.
    pub fn inner(&self) -> &dyn Aggregator {
        self.inner.as_ref()
    }
}

impl Aggregator for FilteredAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        // Always delegate: the query engine evaluates the filter per row and
        // only feeds matching rows (see the query crate's feed helpers).
        self.inner.aggregate(value);
    }

    fn aggregate_multi(&mut self, values: &[Option<&serde_json::Value>]) {
        // Forward to the inner aggregator's OWN aggregate_multi — the trait
        // default would decompose the row into per-value aggregate() calls,
        // silently degrading a byRow multi-field cardinality tuple into
        // per-field value counting (codex-review r3, 2026-07-11).
        self.inner.aggregate_multi(values);
    }

    fn get(&self) -> serde_json::Value {
        self.inner.get()
    }

    fn saturation(&self) -> Option<crate::AggregatorSaturation> {
        // Fail-closed (2026-07-11): a saturation report from the wrapped
        // aggregator (e.g. the E16 exact COUNT(DISTINCT) lowering wraps
        // `cardinality` in a not-null `filtered`) must survive the wrapper,
        // otherwise a saturated filtered-cardinality would finalize to a
        // silently under-counted scalar.
        self.inner.saturation()
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        self.inner.merge(other);
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(Self {
            filter: self.filter.clone(),
            inner: self.inner.clone_box(),
        })
    }

    fn as_any(&self) -> Option<&dyn Any> {
        // Lets `exact_cardinality_partial` downcast the wrapper and walk
        // into the inner aggregator (see `Self::inner`).
        Some(self)
    }
}
