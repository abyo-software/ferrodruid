// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! First/Last aggregators — track the first or last value by timestamp.

use crate::Aggregator;

// ---------------------------------------------------------------------------
// Macro for numeric first/last
// ---------------------------------------------------------------------------

macro_rules! define_first_last_numeric {
    (
        $first_name:ident, $first_doc:expr,
        $last_name:ident, $last_doc:expr,
        $ty:ty, $json_extract:ident, $to_json:expr
    ) => {
        #[doc = $first_doc]
        #[derive(Debug, Clone)]
        pub struct $first_name {
            timestamp: i64,
            value: Option<$ty>,
        }

        impl $first_name {
            /// Creates a new first-value aggregator.
            pub fn new() -> Self {
                Self {
                    timestamp: i64::MAX,
                    value: None,
                }
            }

            /// Aggregates a value with an explicit timestamp.
            pub fn aggregate_with_time(&mut self, ts: i64, value: Option<&serde_json::Value>) {
                if let Some(v) = value.and_then(|v| v.$json_extract()) {
                    if ts < self.timestamp {
                        self.timestamp = ts;
                        #[allow(clippy::cast_possible_truncation)]
                        {
                            self.value = Some(v as $ty);
                        }
                    }
                }
            }
        }

        impl Default for $first_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl Aggregator for $first_name {
            fn aggregate(&mut self, value: Option<&serde_json::Value>) {
                // Without explicit timestamp, use insertion order (decrementing counter).
                let ts = self
                    .timestamp
                    .saturating_sub(1)
                    .min(if self.value.is_none() {
                        i64::MAX
                    } else {
                        self.timestamp
                    });
                Self::aggregate_with_time(self, ts, value);
            }

            fn aggregate_with_time(&mut self, timestamp: i64, value: Option<&serde_json::Value>) {
                Self::aggregate_with_time(self, timestamp, value);
            }

            fn get(&self) -> serde_json::Value {
                match self.value {
                    Some(v) =>
                    {
                        #[allow(clippy::redundant_closure_call)]
                        ($to_json)(v)
                    }
                    None => serde_json::Value::Null,
                }
            }

            fn merge(&mut self, other: &dyn Aggregator) {
                // For merge, we accept the other's value if it exists (simple merge).
                let other_val = other.get();
                if !other_val.is_null() {
                    if self.value.is_none() {
                        if let Some(v) = other_val.$json_extract() {
                            #[allow(clippy::cast_possible_truncation)]
                            {
                                self.value = Some(v as $ty);
                            }
                        }
                    }
                }
            }

            fn reset(&mut self) {
                self.timestamp = i64::MAX;
                self.value = None;
            }

            fn clone_box(&self) -> Box<dyn Aggregator> {
                Box::new(self.clone())
            }
        }

        #[doc = $last_doc]
        #[derive(Debug, Clone)]
        pub struct $last_name {
            timestamp: i64,
            value: Option<$ty>,
        }

        impl $last_name {
            /// Creates a new last-value aggregator.
            pub fn new() -> Self {
                Self {
                    timestamp: i64::MIN,
                    value: None,
                }
            }

            /// Aggregates a value with an explicit timestamp.
            pub fn aggregate_with_time(&mut self, ts: i64, value: Option<&serde_json::Value>) {
                if let Some(v) = value.and_then(|v| v.$json_extract()) {
                    if ts >= self.timestamp {
                        self.timestamp = ts;
                        #[allow(clippy::cast_possible_truncation)]
                        {
                            self.value = Some(v as $ty);
                        }
                    }
                }
            }
        }

        impl Default for $last_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl Aggregator for $last_name {
            fn aggregate(&mut self, value: Option<&serde_json::Value>) {
                let ts = self
                    .timestamp
                    .saturating_add(1)
                    .max(if self.value.is_none() {
                        i64::MIN
                    } else {
                        self.timestamp
                    });
                Self::aggregate_with_time(self, ts, value);
            }

            fn aggregate_with_time(&mut self, timestamp: i64, value: Option<&serde_json::Value>) {
                Self::aggregate_with_time(self, timestamp, value);
            }

            fn get(&self) -> serde_json::Value {
                match self.value {
                    Some(v) =>
                    {
                        #[allow(clippy::redundant_closure_call)]
                        ($to_json)(v)
                    }
                    None => serde_json::Value::Null,
                }
            }

            fn merge(&mut self, other: &dyn Aggregator) {
                let other_val = other.get();
                if !other_val.is_null() {
                    if let Some(v) = other_val.$json_extract() {
                        #[allow(clippy::cast_possible_truncation)]
                        {
                            self.value = Some(v as $ty);
                        }
                    }
                }
            }

            fn reset(&mut self) {
                self.timestamp = i64::MIN;
                self.value = None;
            }

            fn clone_box(&self) -> Box<dyn Aggregator> {
                Box::new(self.clone())
            }
        }
    };
}

fn i64_to_json(v: i64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(v))
}

fn f64_to_json(v: f64) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or(serde_json::Value::Null)
}

fn f32_to_json(v: f32) -> serde_json::Value {
    serde_json::to_value(f64::from(v)).unwrap_or(serde_json::Value::Null)
}

define_first_last_numeric!(
    LongFirstAggregator,
    "Returns the first long (i64) value by timestamp.",
    LongLastAggregator,
    "Returns the last long (i64) value by timestamp.",
    i64,
    as_i64,
    i64_to_json
);

define_first_last_numeric!(
    DoubleFirstAggregator,
    "Returns the first double (f64) value by timestamp.",
    DoubleLastAggregator,
    "Returns the last double (f64) value by timestamp.",
    f64,
    as_f64,
    f64_to_json
);

define_first_last_numeric!(
    FloatFirstAggregator,
    "Returns the first float (f32) value by timestamp.",
    FloatLastAggregator,
    "Returns the last float (f32) value by timestamp.",
    f32,
    as_f64,
    f32_to_json
);

// ---------------------------------------------------------------------------
// StringFirst / StringLast
// ---------------------------------------------------------------------------

/// Returns the first string value by timestamp, truncated to `max_string_bytes`.
#[derive(Debug, Clone)]
pub struct StringFirstAggregator {
    timestamp: i64,
    value: Option<String>,
    max_bytes: usize,
}

impl StringFirstAggregator {
    /// Creates a new string-first aggregator with the given byte limit.
    pub fn new(max_string_bytes: usize) -> Self {
        Self {
            timestamp: i64::MAX,
            value: None,
            max_bytes: max_string_bytes,
        }
    }

    /// Aggregates a value with an explicit timestamp.
    pub fn aggregate_with_time(&mut self, ts: i64, value: Option<&serde_json::Value>) {
        if let Some(s) = value
            .and_then(|v| v.as_str())
            .filter(|_| ts < self.timestamp)
        {
            self.timestamp = ts;
            self.value = Some(truncate_string(s, self.max_bytes));
        }
    }
}

impl Aggregator for StringFirstAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let ts = if self.value.is_none() {
            i64::MAX
        } else {
            self.timestamp.saturating_sub(1)
        };
        Self::aggregate_with_time(self, ts, value);
    }

    fn aggregate_with_time(&mut self, timestamp: i64, value: Option<&serde_json::Value>) {
        Self::aggregate_with_time(self, timestamp, value);
    }

    fn get(&self) -> serde_json::Value {
        match &self.value {
            Some(s) => serde_json::Value::String(s.clone()),
            None => serde_json::Value::Null,
        }
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        let other_val = other.get();
        if let Some(s) = other_val.as_str().filter(|_| self.value.is_none()) {
            self.value = Some(truncate_string(s, self.max_bytes));
        }
    }

    fn reset(&mut self) {
        self.timestamp = i64::MAX;
        self.value = None;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

/// Returns the last string value by timestamp, truncated to `max_string_bytes`.
#[derive(Debug, Clone)]
pub struct StringLastAggregator {
    timestamp: i64,
    value: Option<String>,
    max_bytes: usize,
}

impl StringLastAggregator {
    /// Creates a new string-last aggregator with the given byte limit.
    pub fn new(max_string_bytes: usize) -> Self {
        Self {
            timestamp: i64::MIN,
            value: None,
            max_bytes: max_string_bytes,
        }
    }

    /// Aggregates a value with an explicit timestamp.
    pub fn aggregate_with_time(&mut self, ts: i64, value: Option<&serde_json::Value>) {
        if let Some(s) = value
            .and_then(|v| v.as_str())
            .filter(|_| ts >= self.timestamp)
        {
            self.timestamp = ts;
            self.value = Some(truncate_string(s, self.max_bytes));
        }
    }
}

impl Aggregator for StringLastAggregator {
    fn aggregate(&mut self, value: Option<&serde_json::Value>) {
        let ts = if self.value.is_none() {
            i64::MIN
        } else {
            self.timestamp.saturating_add(1)
        };
        Self::aggregate_with_time(self, ts, value);
    }

    fn aggregate_with_time(&mut self, timestamp: i64, value: Option<&serde_json::Value>) {
        Self::aggregate_with_time(self, timestamp, value);
    }

    fn get(&self) -> serde_json::Value {
        match &self.value {
            Some(s) => serde_json::Value::String(s.clone()),
            None => serde_json::Value::Null,
        }
    }

    fn merge(&mut self, other: &dyn Aggregator) {
        let other_val = other.get();
        if let Some(s) = other_val.as_str() {
            self.value = Some(truncate_string(s, self.max_bytes));
        }
    }

    fn reset(&mut self) {
        self.timestamp = i64::MIN;
        self.value = None;
    }

    fn clone_box(&self) -> Box<dyn Aggregator> {
        Box::new(self.clone())
    }
}

/// Truncates a string to at most `max_bytes` bytes, respecting UTF-8 boundaries.
fn truncate_string(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    // Find the largest char boundary <= max_bytes
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Aggregator;
    use serde_json::json;

    /// Pin (Druid 35 default null handling, measured 2026-07-11): a
    /// first/last that has seen NO non-null input reports SQL null. Unlike
    /// min/max (which needed a `seen` flag), these already hold
    /// `Option<value>` and were null-correct on empty — this test pins that
    /// contract against regression.
    #[test]
    fn all_null_input_first_last_report_null() {
        let aggs: Vec<Box<dyn Aggregator>> = vec![
            Box::new(LongFirstAggregator::new()),
            Box::new(LongLastAggregator::new()),
            Box::new(DoubleFirstAggregator::new()),
            Box::new(DoubleLastAggregator::new()),
            Box::new(FloatFirstAggregator::new()),
            Box::new(FloatLastAggregator::new()),
            Box::new(StringFirstAggregator::new(1024)),
            Box::new(StringLastAggregator::new(1024)),
        ];
        for mut agg in aggs {
            assert_eq!(
                agg.get(),
                serde_json::Value::Null,
                "empty first/last must be SQL null"
            );
            agg.aggregate(None);
            agg.aggregate(Some(&serde_json::Value::Null));
            assert_eq!(
                agg.get(),
                serde_json::Value::Null,
                "a first/last with no non-null input must stay SQL null"
            );
        }
    }

    /// A real input flips first/last from null; reset() returns to null.
    #[test]
    fn first_last_value_then_reset_returns_to_null() {
        let mut first = LongFirstAggregator::new();
        first.aggregate_with_time(100, Some(&json!(7)));
        first.aggregate_with_time(50, Some(&json!(3)));
        assert_eq!(first.get(), json!(3));
        first.reset();
        assert_eq!(first.get(), serde_json::Value::Null);

        let mut last = StringLastAggregator::new(1024);
        last.aggregate_with_time(1, Some(&json!("a")));
        last.aggregate_with_time(2, Some(&json!("b")));
        assert_eq!(last.get(), json!("b"));
        last.reset();
        assert_eq!(last.get(), serde_json::Value::Null);
    }
}
