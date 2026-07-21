// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL type mapping between Druid column types and SQL value types.

use serde::{Deserialize, Serialize};

use ferrodruid_common::types::ColumnType;

// ---------------------------------------------------------------------------
// SqlType — SQL-level type classification
// ---------------------------------------------------------------------------

/// SQL-level data type for Druid SQL columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlType {
    /// A 64-bit signed integer (`BIGINT`).
    Bigint,
    /// A 64-bit floating-point number (`DOUBLE`).
    Double,
    /// A 32-bit floating-point number (`FLOAT`).
    Float,
    /// A variable-length character string (`VARCHAR`).
    Varchar,
    /// A timestamp (`TIMESTAMP`).
    Timestamp,
    /// A boolean value (`BOOLEAN`).
    Boolean,
    /// An opaque complex type (e.g. sketch objects).
    Other(String),
}

impl SqlType {
    /// Convert a Druid [`ColumnType`] to the SQL equivalent.
    pub fn from_druid(ct: &ColumnType) -> Self {
        match ct {
            ColumnType::Long => Self::Bigint,
            ColumnType::Float => Self::Float,
            ColumnType::Double => Self::Double,
            ColumnType::String => Self::Varchar,
            ColumnType::Complex(name) => Self::Other(name.clone()),
        }
    }

    /// Convert this SQL type to the Druid [`ColumnType`].
    pub fn to_druid(&self) -> ColumnType {
        match self {
            Self::Bigint | Self::Timestamp => ColumnType::Long,
            Self::Float => ColumnType::Float,
            Self::Double => ColumnType::Double,
            Self::Varchar | Self::Boolean => ColumnType::String,
            Self::Other(name) => ColumnType::Complex(name.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// OutputColumn — describes one result column
// ---------------------------------------------------------------------------

/// Describes a single output column from a planned SQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputColumn {
    /// The column name as it appears in the result set.
    pub name: String,
    /// The SQL type of this column.
    pub sql_type: SqlType,
    /// The native result-row key this column reads from, when it differs
    /// from `name`. Native SCAN rows have no `outputName` concept, so an
    /// aliased scan projection (`SELECT site_id AS s`) keeps its raw column
    /// key in the native row; the SQL wire projection reads `source` and
    /// emits under `name` (codex QA r12). `None` means `name` is the native
    /// key (all aggregate paths, which carry aliases via `outputName`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

// ---------------------------------------------------------------------------
// legacy_string_cell — W-B shared SQL-wire null-string rendering
// ---------------------------------------------------------------------------

/// W-B legacy null mode: Druid's SQL layer renders a NULL in a
/// STRING-typed (`VARCHAR`) output column as `""` — the merged ''/null
/// value's SQL-wire face (oracle `select_all_rows.json` +
/// `group_strcol.json` render `""` where the NATIVE surfaces render JSON
/// null for the same rows; both are pinned).  Every other type, and ANSI
/// mode, pass through unchanged.
///
/// This is the ONE shared canonicalization point for BOTH SQL
/// formatters: the single-binary REST path (`ferrodruid-rest`) and the
/// role-split broker path (`ferrodruid-rpc::sql_bridge`) — a copy per
/// formatter is exactly the divergence species W-B exists to close.
#[must_use]
pub fn legacy_string_cell(col: &OutputColumn, v: serde_json::Value) -> serde_json::Value {
    if v.is_null()
        && matches!(col.sql_type, SqlType::Varchar)
        && ferrodruid_common::legacy_null_mode()
    {
        return serde_json::Value::String(String::new());
    }
    v
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_long() {
        let ct = ColumnType::Long;
        let st = SqlType::from_druid(&ct);
        assert_eq!(st, SqlType::Bigint);
        assert_eq!(st.to_druid(), ColumnType::Long);
    }

    #[test]
    fn round_trip_string() {
        let ct = ColumnType::String;
        let st = SqlType::from_druid(&ct);
        assert_eq!(st, SqlType::Varchar);
    }

    #[test]
    fn round_trip_double() {
        let ct = ColumnType::Double;
        let st = SqlType::from_druid(&ct);
        assert_eq!(st, SqlType::Double);
        assert_eq!(st.to_druid(), ColumnType::Double);
    }

    #[test]
    fn complex_type() {
        let ct = ColumnType::Complex("hyperUnique".to_string());
        let st = SqlType::from_druid(&ct);
        assert_eq!(st, SqlType::Other("hyperUnique".to_string()));
    }

    #[test]
    fn timestamp_to_druid() {
        let st = SqlType::Timestamp;
        assert_eq!(st.to_druid(), ColumnType::Long);
    }
}
