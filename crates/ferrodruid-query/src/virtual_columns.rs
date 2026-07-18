// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Virtual-column support shared across scan / groupBy / timeseries / topN.
//!
//! A virtual column defines a new, derived column computed by an expression
//! over real columns.  Once resolved, the derived name can be referenced
//! anywhere a real column name is referenced: in `dimensions`, in an
//! aggregator's `fieldName`, and in `filter`.
//!
//! ## Wire shape
//!
//! ```json
//! "virtualColumns": [
//!   {"type": "expression", "name": "derived", "expression": "price * qty"}
//! ]
//! ```
//!
//! Only `type == "expression"` is supported.  Any other virtual-column type is
//! rejected at compile time with [`DruidError::Query`] rather than silently
//! producing a missing column.
//!
//! ## Resolution model
//!
//! Because aggregators and filters read column values out of the row map
//! produced by [`crate::helpers::build_row`] / per-row column extraction, the
//! cleanest integration is to *materialise* each virtual column into the row
//! map (and, for aggregator field access, into a lightweight per-segment
//! synthesized column).  This module provides:
//!
//! * [`VirtualColumns::compile`] — parse + compile the spec list once at plan
//!   time (surfacing malformed expressions as errors).
//! * [`VirtualColumns::augment_row`] — given a base row map, insert every
//!   virtual column's computed value.  Virtual columns are evaluated in
//!   declaration order and may reference earlier virtual columns.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use ferrodruid_common::error::{DruidError, Result};
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::ColumnData;

use crate::expr::Expr;

// ---------------------------------------------------------------------------
// Spec (deserialized)
// ---------------------------------------------------------------------------

/// A single virtual-column specification as it appears on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum VirtualColumnSpec {
    /// An expression virtual column.
    #[serde(rename = "expression")]
    Expression {
        /// The output column name.
        name: String,
        /// The expression computing the column's value.
        expression: String,
        /// Optional declared output type (advisory; ignored for evaluation).
        #[serde(default, rename = "outputType")]
        output_type: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Compiled form
// ---------------------------------------------------------------------------

/// A compiled virtual column: a name plus its parsed expression.
#[derive(Debug, Clone)]
struct CompiledVirtualColumn {
    name: String,
    expr: Expr,
}

/// A compiled, ready-to-evaluate set of virtual columns.
#[derive(Debug, Clone, Default)]
pub struct VirtualColumns {
    columns: Vec<CompiledVirtualColumn>,
}

impl VirtualColumns {
    /// Compile a list of virtual-column specs.  Returns
    /// [`DruidError::Query`] if any expression is malformed or any spec uses
    /// an unsupported virtual-column type (only `expression` is supported).
    ///
    /// `None` / an empty list compile to an empty (no-op) set.
    pub fn compile(specs: &Option<Vec<VirtualColumnSpec>>) -> Result<Self> {
        let Some(specs) = specs else {
            return Ok(Self::default());
        };
        let mut columns = Vec::with_capacity(specs.len());
        for spec in specs {
            match spec {
                VirtualColumnSpec::Expression {
                    name, expression, ..
                } => {
                    let expr = Expr::compile(expression).map_err(|e| {
                        DruidError::Query(format!(
                            "virtual column '{name}' has an invalid expression: {e}"
                        ))
                    })?;
                    columns.push(CompiledVirtualColumn {
                        name: name.clone(),
                        expr,
                    });
                }
            }
        }
        Ok(Self { columns })
    }

    /// Returns true when there are no virtual columns to apply.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Iterate the output names of the compiled virtual columns, in
    /// declaration order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|c| c.name.as_str())
    }

    /// Fail loud when any virtual-column expression references a genuine
    /// multi-value (`StringMulti`) segment column (compat-11 MV fail-loud
    /// guard).
    ///
    /// The expression mini-language has no element-wise MV semantics yet:
    /// pre-fix, an MV row's array was silently stringified into JSON text
    /// (`["a","b"]` → the scalar `"[\"a\",\"b\"]"`) by `Value::from_json`
    /// and the expression computed corrupt results.  Until element-wise MV
    /// expressions land, a query whose virtual column references an MV
    /// column is rejected at PLAN time with a clear [`DruidError::Query`]
    /// naming the column — it errors once, never corrupts per row.
    ///
    /// A reference to an EARLIER virtual column of the same name is not a
    /// segment-column read (the derived scalar shadows it in the row map)
    /// and is exempt.  Call this right after [`Self::compile`] wherever a
    /// segment is in scope.
    ///
    /// # Errors
    ///
    /// Returns [`DruidError::Query`] naming the first multi-value column
    /// referenced by any virtual-column expression.
    pub fn ensure_no_multi_value_refs(&self, segment: &SegmentData) -> Result<()> {
        // Names derived by an earlier virtual column shadow same-named
        // segment columns for every LATER expression (declaration order).
        let mut derived: Vec<&str> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let mut refs: Vec<&str> = Vec::new();
            col.expr.collect_referenced_columns(&mut refs);
            for name in refs {
                if derived.contains(&name) {
                    continue;
                }
                if matches!(segment.columns.get(name), Some(ColumnData::StringMulti(_))) {
                    return Err(DruidError::Query(format!(
                        "expression over a multi-value dimension `{name}` is not supported \
                         yet (element-wise MV expressions are a follow-on; virtual column \
                         '{vc}')",
                        vc = col.name
                    )));
                }
            }
            derived.push(&col.name);
        }
        Ok(())
    }

    /// Insert every virtual column's computed value into `row` in declaration
    /// order.  Later virtual columns may reference earlier ones (they read
    /// from the same, progressively-augmented map).
    ///
    /// A no-op when [`Self::is_empty`].
    pub fn augment_row(&self, row: &mut HashMap<String, serde_json::Value>) {
        for col in &self.columns {
            let value = col.expr.eval(row).to_json();
            row.insert(col.name.clone(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn specs(json: serde_json::Value) -> Option<Vec<VirtualColumnSpec>> {
        Some(serde_json::from_value(json).expect("deser virtual columns"))
    }

    #[test]
    fn compile_and_augment() {
        let vc = VirtualColumns::compile(&specs(json!([
            {"type": "expression", "name": "total", "expression": "price * qty"}
        ])))
        .expect("compile");
        let mut row: HashMap<String, serde_json::Value> = [
            ("price".to_string(), json!(3)),
            ("qty".to_string(), json!(4)),
        ]
        .into_iter()
        .collect();
        vc.augment_row(&mut row);
        assert_eq!(row.get("total"), Some(&json!(12)));
    }

    #[test]
    fn later_column_references_earlier() {
        let vc = VirtualColumns::compile(&specs(json!([
            {"type": "expression", "name": "a2", "expression": "a + 1"},
            {"type": "expression", "name": "a4", "expression": "a2 * 2"}
        ])))
        .expect("compile");
        let mut row: HashMap<String, serde_json::Value> =
            [("a".to_string(), json!(3))].into_iter().collect();
        vc.augment_row(&mut row);
        assert_eq!(row.get("a2"), Some(&json!(4)));
        assert_eq!(row.get("a4"), Some(&json!(8)));
    }

    #[test]
    fn malformed_expression_rejected() {
        let err = VirtualColumns::compile(&specs(json!([
            {"type": "expression", "name": "bad", "expression": "a + "}
        ])))
        .expect_err("must reject");
        match err {
            DruidError::Query(msg) => assert!(msg.contains("invalid expression"), "{msg}"),
            other => panic!("expected Query error, got {other:?}"),
        }
    }

    #[test]
    fn empty_is_noop() {
        let vc = VirtualColumns::compile(&None).expect("compile");
        assert!(vc.is_empty());
        let mut row: HashMap<String, serde_json::Value> =
            [("x".to_string(), json!(1))].into_iter().collect();
        vc.augment_row(&mut row);
        assert_eq!(row.len(), 1);
    }
}
