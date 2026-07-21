// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Count aggregator — counts the number of rows processed.

use crate::Aggregator;

/// Counts the number of rows. Every row increments the counter regardless of value.
#[derive(Debug, Clone)]
pub struct CountAggregator {
    count: i64,
}

impl CountAggregator {
    /// Creates a new count aggregator initialized to zero.
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl Aggregator for CountAggregator {
    fn aggregate(&mut self, _value: Option<&serde_json::Value>) {
        self.count += 1;
    }

    fn get(&self) -> serde_json::Value {
        serde_json::Value::Number(serde_json::Number::from(self.count))
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        if let Some(n) = other.get().as_i64() {
            self.count += n;
        }
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}
