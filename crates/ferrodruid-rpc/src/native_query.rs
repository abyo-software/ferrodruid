// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Wave 41.OO + 42.RR native query subset.
//!
//! For Wave 5/6 of the v1.0 plan we ship a small, focused subset of
//! Druid's native query JSON shape so the cross-role wire can carry
//! real query bodies. Four query types are supported here:
//!
//! - [`NativeQuery::Timeseries`] — time-bucketed aggregate over a
//!   metric column (count or sum). Wave 41.OO.
//! - [`NativeQuery::Scan`] — row-by-row dump with optional column
//!   projection, optional row limit, and optional equality filter.
//!   Wave 41.OO.
//! - [`NativeQuery::GroupBy`] — group rows by N dimensions, fold via
//!   N aggregations, optional `having` predicate + sort + limit.
//!   Wave 42.RR.
//! - [`NativeQuery::TopN`] — top-N rows over a single dimension
//!   ranked by a single metric. Wave 42.RR.
//!
//! Other native query shapes (`search`, `segmentMetadata`,
//! `dataSourceMetadata`, `timeBoundary`, full SQL) live in the richer
//! [`ferrodruid-query`](../../ferrodruid_query) crate used by the
//! single-binary path; wiring them through the cross-role HTTP
//! boundary remains deferred.
//!
//! ## Why this lives in `ferrodruid-rpc` and not `ferrodruid-query`
//!
//! The role-split path keeps the historical / broker binaries small
//! and dependency-light. Pulling in the full `ferrodruid-query` crate
//! (which transitively pulls in `ferrodruid-segment`,
//! `ferrodruid-aggregator`, `ferrodruid-bitmap`, `ferrodruid-dict`,
//! ...) would defeat the role-split goal of independent deployability.
//! The Wave 41.OO subset is a tiny, hand-written executor that operates
//! directly against [`Segment`] artifacts.

use std::collections::HashMap;

use ferrodruid_common::null_mode::{LegacyColumnKind, legacy_canonical_cell};
use ferrodruid_deep_storage::{ColumnType as WireColumnType, Segment};
use serde::{Deserialize, Serialize};

/// Top-level native-query envelope carried over the broker→historical
/// HTTP wire. Tagged by the `queryType` JSON field, mirroring Druid's
/// native query shape so an existing Druid client targeting this
/// surface gets a recognisable response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "queryType", rename_all = "camelCase")]
pub enum NativeQuery {
    /// Time-bucketed aggregate over a metric column.
    Timeseries(TimeseriesSpec),
    /// Row-by-row dump with optional projection / limit / filter.
    Scan(ScanSpec),
    /// Group rows by N dimensions, fold via N aggregations.
    GroupBy(GroupBySpec),
    /// Top-N over a single dimension ranked by one metric.
    TopN(TopNSpec),
}

/// Time-bucketed aggregate.
///
/// Buckets are keyed by `timestamp_ms / granularity_ms * granularity_ms`
/// — i.e. floor-aligned to the granularity. The empty-bucket fill rule
/// is **not** applied (deferred to W6) so a query over a sparse
/// interval returns only the buckets that have at least one matching
/// row.  The ONE exception is the W-B legacy empty-RESULT bucket, which
/// is synthesized AFTER the broker merge (never per segment) by
/// [`legacy_fill_empty_timeseries`], anchored via [`Self::intervals`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimeseriesSpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// Granularity in milliseconds. `0` is treated as "single bucket".
    /// Druid's `all` granularity maps to `0`; `minute` to `60_000`;
    /// `hour` to `3_600_000`; `day` to `86_400_000`.
    pub granularity_ms: i64,
    /// Aggregations to compute, in projection order. The result for
    /// each bucket carries one entry per aggregation, keyed by the
    /// aggregation's `name`.
    pub aggregations: Vec<Aggregation>,
    /// Optional equality filter. When present, only rows whose
    /// `dimension == value` participate in the bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
    /// Query intervals as ISO-8601 `start/end` strings (W-B H2).
    ///
    /// Carried so the BROKER can anchor the legacy empty-result bucket
    /// to the query interval start after the scatter merge
    /// ([`legacy_fill_empty_timeseries`]) exactly like the single-binary
    /// empty-bucket fill.  The per-segment wire executor does **not**
    /// row-filter on these (pre-existing wire scope — the broker
    /// scatters only to segments it routed).  An empty list is the
    /// historical wire spelling and is skipped during serialization, so
    /// wire bytes are unchanged for every pre-existing producer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intervals: Vec<String>,
}

/// Row-by-row dump.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScanSpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// Optional column projection. When `None` or empty, every column
    /// in the row is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Optional row limit. When `None`, every matching row is
    /// returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Optional equality filter. Same semantics as for
    /// [`TimeseriesSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
}

/// Wave 42.RR `groupBy` query spec.
///
/// Rows are grouped by the tuple of dimension values pulled from each
/// row at the columns named in `dimensions`. For each group, the
/// per-aggregation result is folded over the rows that fell into it.
/// An optional [`HavingClause`] filters groups *after* aggregation,
/// optional [`SortSpec`] entries order the surviving groups, and an
/// optional `limit` truncates the head of the sorted list.
///
/// The Wave 42.RR `groupBy` is intentionally simple: no extraction
/// functions, no pre-aggregation filter beyond the W5 equality filter,
/// no nested groupBy. Druid's full surface lands in a later wave; this
/// subset is what the cross-role demo needs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GroupBySpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// Columns to group by. Each dimension READS the physical column
    /// named by [`DimensionRef::name`] and the result row carries one
    /// entry per dimension keyed by its EMITTED name (the output alias
    /// when one exists). Order matters because it controls the default
    /// sort key tie-break order. Each entry optionally carries the
    /// consuming `outputType` (see [`DimensionRef`]); a bare name keeps
    /// the historical wire spelling.
    pub dimensions: Vec<DimensionRef>,
    /// Aggregations to compute per group, in projection order.
    pub aggregations: Vec<Aggregation>,
    /// Optional equality filter applied *before* grouping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
    /// Optional `having` predicate evaluated against each post-fold
    /// group. Groups for which the predicate is false are dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<HavingClause>,
    /// Optional sort. When `None` or empty, the result order is
    /// stable but unspecified beyond "groups appear in encounter
    /// order"; tests should always pass an explicit sort when
    /// asserting on order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<SortSpec>>,
    /// Optional limit on the number of post-sort groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Wave 42.RR `topN` query spec.
///
/// `topN` is the cardinality-friendly degenerate of `groupBy`: a
/// single dimension, ranked by a single metric. The result is the
/// top `threshold` rows ordered descending by `metric` (numeric
/// types) — Druid's "high" sort. We intentionally omit the
/// "low" / "alphaNumeric" / "lexicographic" variants the full Druid
/// surface ships; this subset is enough for the cross-role demo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopNSpec {
    /// Datasource the query targets.
    pub data_source: String,
    /// The single dimension to group by, optionally carrying its
    /// consuming `outputType` (see [`DimensionRef`]).
    pub dimension: DimensionRef,
    /// Aggregations to compute per group. The `metric` column must
    /// match one of these aggregation names.
    pub aggregations: Vec<Aggregation>,
    /// Aggregation name to rank by. Higher = better (Druid's "high"
    /// sort). When the metric isn't numeric the row is sorted as 0.
    pub metric: String,
    /// Maximum number of result rows to keep after the rank-and-cap.
    pub threshold: usize,
    /// Optional pre-grouping equality filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<EqualsFilter>,
}

/// Wave 42.RR `having` predicate. We support equality + the four
/// standard numeric inequalities. Combinators (AND / OR / NOT) and
/// extraction functions are deferred.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum HavingClause {
    /// `aggregation == value` (numeric).
    Equal {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation > value`.
    GreaterThan {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation >= value`.
    GreaterThanOrEqual {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation < value`.
    LessThan {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
    /// `aggregation <= value`.
    LessThanOrEqual {
        /// Aggregation name to evaluate.
        aggregation: String,
        /// Numeric literal.
        value: f64,
    },
}

impl HavingClause {
    /// True iff the result row satisfies the predicate.
    #[must_use]
    pub fn matches(&self, row: &serde_json::Map<String, serde_json::Value>) -> bool {
        let (key, value, op) = match self {
            HavingClause::Equal { aggregation, value } => (aggregation, *value, HavingOp::Eq),
            HavingClause::GreaterThan { aggregation, value } => (aggregation, *value, HavingOp::Gt),
            HavingClause::GreaterThanOrEqual { aggregation, value } => {
                (aggregation, *value, HavingOp::Ge)
            }
            HavingClause::LessThan { aggregation, value } => (aggregation, *value, HavingOp::Lt),
            HavingClause::LessThanOrEqual { aggregation, value } => {
                (aggregation, *value, HavingOp::Le)
            }
        };
        let actual = row.get(key).and_then(serde_json::Value::as_f64);
        match actual {
            Some(a) => match op {
                HavingOp::Eq => (a - value).abs() < f64::EPSILON,
                HavingOp::Gt => a > value,
                HavingOp::Ge => a >= value,
                HavingOp::Lt => a < value,
                HavingOp::Le => a <= value,
            },
            None => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum HavingOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

/// Sort specification for a [`GroupBySpec`].
///
/// `dimension` selects the result-row key (either a dimension name or
/// an aggregation name); `direction` controls ascending vs descending.
/// Numeric columns sort numerically; everything else sorts as a
/// string. Missing keys sort as the smallest possible value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SortSpec {
    /// Result-row key to sort on.
    pub dimension: String,
    /// Sort direction.
    #[serde(default)]
    pub direction: SortDirection,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    /// Ascending order.
    #[default]
    Ascending,
    /// Descending order.
    Descending,
}

/// Consuming output kind of one wire grouping dimension — the
/// `DimensionSpec.outputType` the SQL bridge threads through to the
/// role-split executor (W-B role-split legacy-null divergence fix).
///
/// Under the legacy latch a grouping column ABSENT from the segment
/// header canonicalizes by this DECLARED kind (LONG → `0`,
/// DOUBLE/FLOAT → `0.0`) exactly like the single-binary coercion
/// (`ferrodruid-query`'s `coerce_group_key_to_output_type`), so both
/// paths emit the IDENTICAL group key and absent rows merge with
/// real-zero rows.  Serialized in Druid's UPPERCASE spelling.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum DimensionOutputType {
    /// 64-bit integer — an absent column legacy-defaults to `0`.
    Long,
    /// 64-bit float — an absent column legacy-defaults to `0.0`.
    Double,
    /// 32-bit float — legacy-defaults like [`Self::Double`] (`0.0`),
    /// mirroring the single-binary FLOAT coercion of a null key.
    Float,
    /// String — identical to an unspecified `outputType` (the
    /// header-kind / String fallback); representable so the Druid
    /// object spelling round-trips losslessly.
    String,
}

/// One wire grouping dimension: the PHYSICAL segment column the
/// executor READS per row, the OPTIONAL output alias it EMITS the group
/// key under, and the OPTIONAL consuming [`DimensionOutputType`]
/// declared by the query's `DimensionSpec`.
///
/// The input (read) and output (emit) names are SEPARATE (role-split
/// aliasing fix): `SELECT y AS alias … GROUP BY y` plans a
/// `DimensionSpec` reading `y` and emitting `alias` — collapsing the
/// two onto the alias made the executor read a NONEXISTENT column, so
/// under the legacy latch every row canonicalized to the missing-column
/// default and all real groups silently merged into one.
///
/// Wire shape (backward compatible): a name-only dimension serializes
/// as the historical BARE STRING (`"page"`) — byte-identical to the
/// pre-fix wire — while a typed and/or aliased dimension serializes as
/// the Druid-style object
/// `{"dimension": "m", "outputName": "alias", "outputType": "LONG"}`
/// (each optional field only when present; without an alias the bytes
/// are identical to the pre-fix object form).  Deserialization accepts
/// both spellings (plus an optional `"type": "default"` tag); any other
/// `type` fails loudly rather than silently dropping its semantics, and
/// an alias equal to the physical name normalizes to "no alias" so the
/// round-trip stays byte-identical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DimensionRef {
    /// PHYSICAL dimension (segment column) name — what the executor
    /// READS from each row, and the name whose header-declared kind
    /// keys the legacy canonicalization.
    pub name: String,
    /// Output alias, ONLY when it differs from the physical name —
    /// what the executor EMITS as the group key / result column.
    /// `None` means output == input (the no-alias common case), which
    /// keeps the wire bytes identical to the historical spelling.
    pub output_name: Option<String>,
    /// Declared consuming output kind, when the originating
    /// `DimensionSpec` carried a non-STRING `outputType`.  `None`
    /// keeps the historical header-kind / String fallback (the
    /// ANSI-parity default).
    pub output_type: Option<DimensionOutputType>,
}

impl DimensionRef {
    /// The result-row key / column label this dimension EMITS: the
    /// alias when one exists, else the physical name.
    #[must_use]
    pub fn emitted_name(&self) -> &str {
        self.output_name.as_deref().unwrap_or(&self.name)
    }
}

impl From<String> for DimensionRef {
    fn from(name: String) -> Self {
        Self {
            name,
            output_name: None,
            output_type: None,
        }
    }
}

impl From<&str> for DimensionRef {
    fn from(name: &str) -> Self {
        Self {
            name: name.to_string(),
            output_name: None,
            output_type: None,
        }
    }
}

impl Serialize for DimensionRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match (&self.output_name, self.output_type) {
            // Name-only: the historical bare-string wire spelling,
            // byte-identical to the pre-fix representation.
            (None, None) => serializer.serialize_str(&self.name),
            (output_name, output_type) => {
                use serde::ser::SerializeStruct;
                let len =
                    1 + usize::from(output_name.is_some()) + usize::from(output_type.is_some());
                let mut s = serializer.serialize_struct("DimensionRef", len)?;
                s.serialize_field("dimension", &self.name)?;
                if let Some(alias) = output_name {
                    s.serialize_field("outputName", alias)?;
                }
                if let Some(output_type) = output_type {
                    s.serialize_field("outputType", &output_type)?;
                }
                s.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for DimensionRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Bare(String),
            Object {
                #[serde(default, rename = "type")]
                kind: Option<String>,
                dimension: String,
                #[serde(default, rename = "outputName", alias = "output_name")]
                output_name: Option<String>,
                #[serde(default, rename = "outputType", alias = "output_type")]
                output_type: Option<DimensionOutputType>,
            },
        }
        match Wire::deserialize(deserializer)? {
            Wire::Bare(name) => Ok(Self {
                name,
                output_name: None,
                output_type: None,
            }),
            Wire::Object {
                kind,
                dimension,
                output_name,
                output_type,
            } => {
                // Fail loud on non-default DimensionSpec types — the
                // wire executor has no extraction / filtered-dimension
                // machinery, and silently ignoring an `extractionFn`
                // would return wrong-but-plausible groups.
                if let Some(k) = kind
                    && k != "default"
                {
                    return Err(serde::de::Error::custom(format!(
                        "unsupported wire dimension type {k:?} (only \"default\")"
                    )));
                }
                // The alias rides SEPARATELY from the physical column
                // (the executor READS `dimension`, EMITS the alias); an
                // alias equal to the physical name normalizes to "no
                // alias" so the round-trip stays byte-identical to the
                // no-alias wire spelling.
                let output_name = output_name.filter(|alias| *alias != dimension);
                Ok(Self {
                    name: dimension,
                    output_name,
                    output_type,
                })
            }
        }
    }
}

/// Single equality predicate `dimension == value`. The Wave 41.OO
/// surface intentionally only supports equality; richer filters
/// (range, IN, NOT, AND/OR) are deferred.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EqualsFilter {
    /// Column name to match on.
    pub dimension: String,
    /// Expected literal value (string-compared, mirroring Druid's
    /// dimension-aware equality).
    pub value: String,
}

impl EqualsFilter {
    /// True iff `row[self.dimension]` stringifies to `self.value`.
    ///
    /// Schema-blind convenience form: identical to
    /// [`Self::matches_with_kind`] with [`LegacyColumnKind::String`]
    /// (the historical behavior, and Druid's semantics for a column no
    /// schema declares).  The executors resolve the segment header's
    /// declared kind and call [`Self::matches_with_kind`] instead.
    #[must_use]
    pub fn matches(&self, row: &serde_json::Map<String, serde_json::Value>) -> bool {
        self.matches_with_kind(row, LegacyColumnKind::String)
    }

    /// Kind-aware equality match (W-B H2).
    ///
    /// Under the legacy latch the row cell is first canonicalized
    /// through the SHARED legacy read rule
    /// ([`legacy_canonical_cell`], keyed on the segment header's
    /// declared column type — see [`legacy_kind`]), exactly like the
    /// single-binary `column_value_at` read, and then compared:
    ///
    /// * a NUMERIC null/missing cell reads as the coerced default
    ///   `0`/`0.0` and compares **numerically** against the literal
    ///   (`"0"` matches `0.0`; exact-i64 equality via the single-binary
    ///   rule `ferrodruid_query::filter::legacy_number_equals_literal`,
    ///   never a lossy f64 round-trip);
    /// * a STRING null/missing/`""` cell is the ONE merged null value,
    ///   matched by the `""` literal (H4) — the same equivalence the
    ///   single-binary Selector arm applies.
    ///
    /// ANSI behavior (latch off) is byte-identical to the historical
    /// `matches`: string/number/bool stringify-compare, null/missing
    /// never match, arrays/objects never match a scalar literal.
    #[must_use]
    pub fn matches_with_kind(
        &self,
        row: &serde_json::Map<String, serde_json::Value>,
        kind: LegacyColumnKind,
    ) -> bool {
        let cell = row.get(&self.dimension);
        if ferrodruid_common::legacy_null_mode() {
            return match legacy_canonical_cell(kind, cell) {
                serde_json::Value::Null => self.value.is_empty(),
                serde_json::Value::String(s) => s == self.value,
                serde_json::Value::Number(n) => {
                    ferrodruid_query::filter::legacy_number_equals_literal(&n, &self.value)
                }
                serde_json::Value::Bool(b) => b.to_string() == self.value,
                // Arrays / objects never match a scalar literal.
                _ => false,
            };
        }
        match cell {
            Some(serde_json::Value::String(s)) => *s == self.value,
            Some(serde_json::Value::Number(n)) => n.to_string() == self.value,
            Some(serde_json::Value::Bool(b)) => b.to_string() == self.value,
            Some(serde_json::Value::Null) | None => false,
            // Arrays / objects never match a scalar literal in Wave
            // 41.OO. Druid would route these via dedicated typed
            // filters; we deliberately reject them here so the
            // semantics stay narrow and predictable.
            Some(_) => false,
        }
    }
}

/// Druid-aligned aggregation spec. Wave 41.OO covers the two
/// aggregations the cross-role demo touches: row-count and metric sum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Aggregation {
    /// Row count. The `field_name` is ignored.
    Count {
        /// Result key.
        name: String,
    },
    /// Sum of a numeric column over the bucket. Non-numeric / missing
    /// values are skipped.
    LongSum {
        /// Result key.
        name: String,
        /// Source column.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
    /// Sum of a numeric column as a `f64`. Non-numeric / missing
    /// values are skipped.
    DoubleSum {
        /// Result key.
        name: String,
        /// Source column.
        #[serde(rename = "fieldName")]
        field_name: String,
    },
}

impl Aggregation {
    /// Result-map key the aggregation writes to.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Aggregation::Count { name }
            | Aggregation::LongSum { name, .. }
            | Aggregation::DoubleSum { name, .. } => name,
        }
    }
}

/// Result of a native-query execution against a single segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "queryType", rename_all = "camelCase")]
pub enum NativeQueryResult {
    /// Bucketed timeseries result. Buckets are sorted ascending.
    Timeseries(Vec<TimeseriesBucket>),
    /// Scan row dump.
    Scan(Vec<serde_json::Map<String, serde_json::Value>>),
    /// `groupBy` row dump. Each row carries one entry per dimension
    /// keyed by the dimension's column name plus one entry per
    /// aggregation keyed by the aggregation's `name`.
    GroupBy(Vec<serde_json::Map<String, serde_json::Value>>),
    /// `topN` row dump. Identical wire shape to `GroupBy` but ranked
    /// and capped to a small `threshold`.
    TopN(Vec<serde_json::Map<String, serde_json::Value>>),
}

/// One bucket of a timeseries result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeseriesBucket {
    /// Bucket-floor timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Aggregation results for this bucket, keyed by aggregation name.
    pub result: serde_json::Map<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

impl NativeQuery {
    /// Execute the query against a single in-memory segment.
    ///
    /// Wave 41.OO does not validate the segment's `data_source` against
    /// the query's `data_source` — the broker is expected to scatter
    /// only to historicals that hold the relevant segment. The
    /// historical's segment store may filter by `data_source` before
    /// calling this, but execution itself is segment-local.
    #[must_use]
    pub fn execute(&self, segment: &Segment) -> NativeQueryResult {
        match self {
            NativeQuery::Timeseries(spec) => {
                NativeQueryResult::Timeseries(execute_timeseries(spec, segment))
            }
            NativeQuery::Scan(spec) => NativeQueryResult::Scan(execute_scan(spec, segment)),
            NativeQuery::GroupBy(spec) => {
                NativeQueryResult::GroupBy(execute_group_by(spec, segment))
            }
            NativeQuery::TopN(spec) => NativeQueryResult::TopN(execute_top_n(spec, segment)),
        }
    }
}

/// Merge two timeseries result vectors by bucket timestamp.
///
/// Rows with identical `timestamp_ms` have their aggregation entries
/// summed. The result is sorted ascending by timestamp.
///
/// This is the per-broker reducer used to combine per-historical
/// scatter responses into a single tier-wide answer.
#[must_use]
pub fn merge_timeseries(
    parts: Vec<Vec<TimeseriesBucket>>,
    aggs: &[Aggregation],
) -> Vec<TimeseriesBucket> {
    let mut by_ts: HashMap<i64, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for part in parts {
        for bucket in part {
            let entry = by_ts.entry(bucket.timestamp_ms).or_default();
            for agg in aggs {
                let key = agg.name();
                let incoming = bucket
                    .result
                    .get(key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let prev = entry
                    .get(key)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let total = prev + incoming;
                let value = match agg {
                    Aggregation::DoubleSum { .. } => json_number(total),
                    _ => {
                        // Count / LongSum: store as integer when integral.
                        if total.fract() == 0.0 && total.is_finite() {
                            serde_json::Value::Number(serde_json::Number::from(total as i64))
                        } else {
                            json_number(total)
                        }
                    }
                };
                entry.insert(key.to_string(), value);
            }
        }
    }
    let mut keys: Vec<i64> = by_ts.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter()
        .map(|ts| TimeseriesBucket {
            timestamp_ms: ts,
            result: by_ts.remove(&ts).unwrap_or_default(),
        })
        .collect()
}

/// W-B H2 — legacy empty-RESULT timeseries synthesis, applied AFTER the
/// broker merge (never per segment).
///
/// Under the legacy latch, Druid's timeseries emits ONE bucket even when
/// no row matched, carrying every aggregator's legacy no-input value
/// (count → 0, longSum → 0, doubleSum → 0.0) and anchored to the query
/// interval start — exactly what the single-binary empty-bucket fill
/// produces (`ferrodruid-query::timeseries` anchors
/// `bucket_timestamp(interval_start)`; granularity `all` floors to epoch
/// 0 there, matching [`bucket_floor`] with `granularity_ms == 0` here).
///
/// Synthesizing this per segment (the earlier R3 shape) was wrong twice
/// over: a PARTIALLY-empty scatter (some shards match) grew a spurious
/// epoch-0 bucket next to the real buckets, and an ALL-empty scatter
/// anchored at epoch 0 instead of the interval start.  The rule lives
/// here instead: when the MERGED result is empty (all shards empty) and
/// the latch is on, emit one bucket per distinct interval-start bucket
/// floor (the single-binary fill emits one per interval; SQL always
/// carries exactly one).  Absent / unparseable intervals anchor at epoch
/// 0 — the single-binary DEFAULT SQL interval start.  ANSI mode returns
/// `merged` unchanged (the historical `[]`), and a non-empty merge is
/// always passed through untouched (real buckets only).
#[must_use]
pub fn legacy_fill_empty_timeseries(
    merged: Vec<TimeseriesBucket>,
    spec: &TimeseriesSpec,
) -> Vec<TimeseriesBucket> {
    if !merged.is_empty() || !ferrodruid_common::legacy_null_mode() {
        return merged;
    }
    let mut result = serde_json::Map::with_capacity(spec.aggregations.len());
    for agg in &spec.aggregations {
        result.insert(agg.name().to_string(), agg_value(agg, 0.0));
    }
    let mut starts: Vec<i64> = spec
        .intervals
        .iter()
        .filter_map(|interval| interval_start_millis(interval))
        .map(|start| bucket_floor(start, spec.granularity_ms))
        .collect();
    starts.sort_unstable();
    starts.dedup();
    if starts.is_empty() {
        starts.push(0);
    }
    starts
        .into_iter()
        .map(|timestamp_ms| TimeseriesBucket {
            timestamp_ms,
            result: result.clone(),
        })
        .collect()
}

/// Parse the START of one ISO-8601 `start/end` interval string into
/// epoch milliseconds.  Returns `None` for malformed strings so the
/// caller can fall back to the epoch-0 anchor instead of erroring a
/// query that already produced a (merged, empty) answer.
fn interval_start_millis(interval: &str) -> Option<i64> {
    let start = interval.split('/').next()?;
    ferrodruid_query::parse_iso_millis(start)
}

/// Merge two scan result vectors by concatenation, applying the
/// supplied per-broker limit. The merge is order-preserving with
/// respect to the input fragment order.
#[must_use]
pub fn merge_scan(
    parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>>,
    limit: Option<usize>,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for part in parts {
        for row in part {
            if let Some(cap) = limit
                && out.len() >= cap
            {
                return out;
            }
            out.push(row);
        }
    }
    out
}

fn execute_timeseries(spec: &TimeseriesSpec, segment: &Segment) -> Vec<TimeseriesBucket> {
    let mut buckets: HashMap<i64, BucketState> = HashMap::new();
    let filter_kind = filter_column_kind(segment, spec.filter.as_ref());

    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches_with_kind(row, filter_kind)
        {
            continue;
        }
        let ts = row
            .get("__time")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let key = bucket_floor(ts, spec.granularity_ms);
        let state = buckets
            .entry(key)
            .or_insert_with(|| BucketState::new(&spec.aggregations));
        state.feed(row, &spec.aggregations);
    }

    // W-B legacy null mode (H4, re-sited by the H2 role-split fix): an
    // EMPTY match set returns NO buckets here.  The legacy empty-RESULT
    // bucket (count → 0, longSum → 0, doubleSum → 0.0 at the query
    // interval start) is a BROKER synthesis applied AFTER the scatter
    // merge — [`legacy_fill_empty_timeseries`] — because a per-segment
    // synthesis planted a spurious epoch-0 bucket next to the real
    // buckets whenever only SOME shards were empty, and anchored the
    // all-shards-empty bucket at epoch 0 instead of the interval start
    // (both diverging from the single-binary empty-bucket fill).  ANSI
    // keeps the historical empty result (`[]`) byte-identical on both
    // tiers.

    let mut keys: Vec<i64> = buckets.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter()
        .map(|ts| {
            let state = buckets
                .remove(&ts)
                .unwrap_or_else(|| BucketState::new(&spec.aggregations));
            TimeseriesBucket {
                timestamp_ms: ts,
                result: state.finish(&spec.aggregations),
            }
        })
        .collect()
}

/// Merge two groupBy result vectors by dimension tuple.
///
/// Per-segment fragments are grouped by the dimensions key tuple
/// (taken from `dimensions` in spec order); aggregations are folded
/// across fragments using the same sum-fold rule as `merge_timeseries`.
/// The `having` predicate, sort, and limit are then applied to the
/// merged set so they always see the cluster-wide totals (not the
/// per-segment partial folds).
#[must_use]
pub fn merge_group_by(
    parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>>,
    spec: &GroupBySpec,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    // Key by dimension-tuple (stringified) to keep it Hash-able.
    let mut groups: HashMap<Vec<String>, serde_json::Map<String, serde_json::Value>> =
        HashMap::new();
    let mut order: Vec<Vec<String>> = Vec::new();
    for part in parts {
        for row in part {
            // Per-segment result rows carry the dimension under its
            // OUTPUT (emitted) name — the alias when one exists.
            let key: Vec<String> = spec
                .dimensions
                .iter()
                .map(|d| stringify_dim_value(row.get(d.emitted_name())))
                .collect();
            let entry = groups.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                let mut m = serde_json::Map::new();
                for d in &spec.dimensions {
                    if let Some(v) = row.get(d.emitted_name()) {
                        m.insert(d.emitted_name().to_string(), v.clone());
                    }
                }
                m
            });
            for agg in &spec.aggregations {
                let k = agg.name();
                let prev = entry
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let incoming = row
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let total = prev + incoming;
                entry.insert(k.to_string(), agg_value(agg, total));
            }
        }
    }
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = order
        .into_iter()
        .filter_map(|k| groups.remove(&k))
        .collect();
    apply_having_sort_limit(&mut out, spec);
    out
}

/// Merge two topN result vectors by dimension value.
///
/// Same fold rule as [`merge_group_by`] but with a single dimension,
/// then ranked descending by the `metric` aggregation and capped at
/// `threshold`.
#[must_use]
pub fn merge_top_n(
    parts: Vec<Vec<serde_json::Map<String, serde_json::Value>>>,
    spec: &TopNSpec,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut groups: HashMap<String, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for part in parts {
        for row in part {
            // Per-segment result rows carry the dimension under its
            // OUTPUT (emitted) name — the alias when one exists.
            let key = stringify_dim_value(row.get(spec.dimension.emitted_name()));
            let entry = groups.entry(key).or_insert_with(|| {
                let mut m = serde_json::Map::new();
                if let Some(v) = row.get(spec.dimension.emitted_name()) {
                    m.insert(spec.dimension.emitted_name().to_string(), v.clone());
                }
                m
            });
            for agg in &spec.aggregations {
                let k = agg.name();
                let prev = entry
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let incoming = row
                    .get(k)
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let total = prev + incoming;
                entry.insert(k.to_string(), agg_value(agg, total));
            }
        }
    }
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = groups.into_values().collect();
    rank_top_n(&mut out, spec);
    out
}

fn execute_scan(
    spec: &ScanSpec,
    segment: &Segment,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    let legacy = ferrodruid_common::legacy_null_mode();
    let filter_kind = filter_column_kind(segment, spec.filter.as_ref());
    // W-B legacy null mode (H2): per-projected-column (declared?, kind)
    // resolved once against the segment header.
    let projection_meta: Option<Vec<(bool, LegacyColumnKind)>> = match &spec.columns {
        Some(cols) if legacy && !cols.is_empty() => Some(
            cols.iter()
                .map(|c| (header_declares(segment, c), legacy_kind(segment, c)))
                .collect(),
        ),
        _ => None,
    };
    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches_with_kind(row, filter_kind)
        {
            continue;
        }
        let projected = match &spec.columns {
            Some(cols) if !cols.is_empty() => {
                let mut m = serde_json::Map::with_capacity(cols.len());
                if let Some(meta) = &projection_meta {
                    // W-B legacy (H2): a projected column the header
                    // DECLARES emits its canonical legacy read even when
                    // the row omits it (numeric default 0/0.0, canonical
                    // null for strings — the shared `legacy_canonical_cell`
                    // rule the single-binary `column_value_at` mirrors);
                    // a column absent from header AND row stays omitted,
                    // matching the single-binary scan's treatment of a
                    // physically-absent column.
                    for (c, (declared, kind)) in cols.iter().zip(meta) {
                        match row.get(c) {
                            None if !declared => {}
                            cell => {
                                m.insert(c.clone(), legacy_canonical_cell(*kind, cell));
                            }
                        }
                    }
                } else {
                    for c in cols {
                        if let Some(v) = row.get(c) {
                            m.insert(c.clone(), v.clone());
                        }
                    }
                }
                m
            }
            _ => {
                if legacy {
                    // W-B legacy (H2): the full-row scan emits every
                    // header-declared column (header order) at its
                    // canonical legacy read — a row omitting a declared
                    // field still shows the typed default — then any
                    // undeclared row extras, string-canonicalized.
                    let mut m =
                        serde_json::Map::with_capacity(segment.header.columns.len() + row.len());
                    for cs in &segment.header.columns {
                        m.insert(
                            cs.name.clone(),
                            legacy_canonical_cell(wire_kind(cs.typ), row.get(&cs.name)),
                        );
                    }
                    for (k, v) in row {
                        if !header_declares(segment, k) {
                            m.insert(
                                k.clone(),
                                legacy_canonical_cell(LegacyColumnKind::String, Some(v)),
                            );
                        }
                    }
                    m
                } else {
                    row.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                }
            }
        };
        out.push(projected);
        if let Some(cap) = spec.limit
            && out.len() >= cap
        {
            break;
        }
    }
    out
}

fn execute_group_by(
    spec: &GroupBySpec,
    segment: &Segment,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut groups: HashMap<Vec<String>, GroupState> = HashMap::new();
    let mut order: Vec<Vec<String>> = Vec::new();
    let filter_kind = filter_column_kind(segment, spec.filter.as_ref());
    // W-B legacy null mode (H2): per-dimension declared kinds, resolved
    // once against the segment header (with the dimension's declared
    // `outputType` overriding for header-absent columns).
    let dim_kinds: Vec<LegacyColumnKind> = spec
        .dimensions
        .iter()
        .map(|d| dim_legacy_kind(segment, d))
        .collect();

    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches_with_kind(row, filter_kind)
        {
            continue;
        }
        let key: Vec<String> = spec
            .dimensions
            .iter()
            .zip(&dim_kinds)
            .map(|(d, kind)| dim_group_key(row, &d.name, *kind))
            .collect();
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            GroupState::new(spec, &dim_kinds, row)
        });
        entry.feed(row, &spec.aggregations);
    }

    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = order
        .into_iter()
        .filter_map(|k| groups.remove(&k).map(|g| g.finish(&spec.aggregations)))
        .collect();
    apply_having_sort_limit(&mut out, spec);
    out
}

fn execute_top_n(
    spec: &TopNSpec,
    segment: &Segment,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let mut groups: HashMap<String, GroupState> = HashMap::new();
    let dim_keys = std::slice::from_ref(&spec.dimension);
    let filter_kind = filter_column_kind(segment, spec.filter.as_ref());
    // W-B legacy null mode (H2): single-dimension declared kind (with
    // the declared `outputType` overriding for a header-absent column).
    let dim_kind = [dim_legacy_kind(segment, &spec.dimension)];

    for row in &segment.rows {
        if let Some(filter) = &spec.filter
            && !filter.matches_with_kind(row, filter_kind)
        {
            continue;
        }
        let key = dim_group_key(row, &spec.dimension.name, dim_kind[0]);
        let entry = groups
            .entry(key)
            .or_insert_with(|| GroupState::new_for_dims(dim_keys, &dim_kind, row));
        entry.feed(row, &spec.aggregations);
    }

    let mut out: Vec<serde_json::Map<String, serde_json::Value>> = groups
        .into_values()
        .map(|g| g.finish(&spec.aggregations))
        .collect();
    rank_top_n(&mut out, spec);
    out
}

fn apply_having_sort_limit(
    rows: &mut Vec<serde_json::Map<String, serde_json::Value>>,
    spec: &GroupBySpec,
) {
    if let Some(having) = &spec.having {
        rows.retain(|r| having.matches(r));
    }
    if let Some(sort) = &spec.sort
        && !sort.is_empty()
    {
        rows.sort_by(|a, b| compare_rows(a, b, sort));
    }
    if let Some(limit) = spec.limit
        && rows.len() > limit
    {
        rows.truncate(limit);
    }
}

fn rank_top_n(rows: &mut Vec<serde_json::Map<String, serde_json::Value>>, spec: &TopNSpec) {
    rows.sort_by(|a, b| {
        let av = a
            .get(&spec.metric)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let bv = b
            .get(&spec.metric)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        // Descending by metric (Druid "high" sort).
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });
    if rows.len() > spec.threshold {
        rows.truncate(spec.threshold);
    }
}

fn compare_rows(
    a: &serde_json::Map<String, serde_json::Value>,
    b: &serde_json::Map<String, serde_json::Value>,
    sort: &[SortSpec],
) -> std::cmp::Ordering {
    for s in sort {
        let av = a.get(&s.dimension);
        let bv = b.get(&s.dimension);
        let ord = compare_json(av, bv);
        if ord != std::cmp::Ordering::Equal {
            return match s.direction {
                SortDirection::Ascending => ord,
                SortDirection::Descending => ord.reverse(),
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_json(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        // Missing keys sort smallest.
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(av), Some(bv)) => {
            // Prefer numeric compare when both sides parse as numbers.
            if let (Some(an), Some(bn)) = (av.as_f64(), bv.as_f64()) {
                an.partial_cmp(&bn).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                stringify_dim_value(Some(av)).cmp(&stringify_dim_value(Some(bv)))
            }
        }
    }
}

/// Resolved [`LegacyColumnKind`] of one wire column (W-B H2).
///
/// The single-binary path funnels every row read through ONE
/// schema-aware canonicalization point
/// (`ferrodruid-query::helpers::column_value_at`, keyed on the physical
/// column type).  This executor reads raw JSON-Lines rows instead, so
/// the JSON-Lines segment HEADER is the schema authority here, and
/// every legacy read routes through the same shared rule
/// ([`legacy_canonical_cell`]) keyed on the header-declared type.  A
/// column absent from the header reads as a null STRING column —
/// Druid's missing-column semantics, and exactly how the single-binary
/// path treats a physically-absent column — so the two paths
/// legacy-default identically.
fn legacy_kind(segment: &Segment, name: &str) -> LegacyColumnKind {
    segment
        .header
        .columns
        .iter()
        .find(|c| c.name == name)
        .map_or(LegacyColumnKind::String, |c| wire_kind(c.typ))
}

/// Resolved [`LegacyColumnKind`] of one GROUPING dimension (W-B
/// role-split legacy-null divergence fix).
///
/// Keyed on the INPUT (physical) column name — `DimensionRef.name` —
/// never the output alias: the legacy absent-column defaulting must
/// follow the REAL column the executor reads.  A column the header
/// DECLARES keys by its header kind — the schema authority, unchanged.
/// A header-ABSENT column keys by the
/// dimension's DECLARED [`DimensionOutputType`] when the wire carries
/// one, threading the consuming `outputType` into the shared
/// canonicalizer ([`legacy_canonical_cell`]) so the legacy default is
/// the single-binary one (LONG → `0`, DOUBLE/FLOAT → `0.0` — exactly
/// `coerce_group_key_to_output_type`'s latched null-key coercion) and
/// absent rows merge with real-zero rows.  Without a declared
/// `outputType` the historical String fallback (Druid's missing-column
/// semantics) is preserved — the ANSI-parity default.
fn dim_legacy_kind(segment: &Segment, dim: &DimensionRef) -> LegacyColumnKind {
    if header_declares(segment, &dim.name) {
        legacy_kind(segment, &dim.name)
    } else {
        dim.output_type
            .map_or(LegacyColumnKind::String, output_type_kind)
    }
}

/// Map a declared [`DimensionOutputType`] onto the shared
/// [`LegacyColumnKind`].  FLOAT canonicalizes like DOUBLE (`0.0`
/// default — the single-binary FLOAT coercion of a null key emits the
/// same `0.0`); STRING is the identity fallback.
fn output_type_kind(output_type: DimensionOutputType) -> LegacyColumnKind {
    match output_type {
        DimensionOutputType::Long => LegacyColumnKind::Long,
        DimensionOutputType::Double | DimensionOutputType::Float => LegacyColumnKind::Double,
        DimensionOutputType::String => LegacyColumnKind::String,
    }
}

/// Map a wire header column type onto the shared [`LegacyColumnKind`].
/// `Json` is an opaque pass-through dimension and canonicalizes like a
/// string (missing/null → canonical null; present values unchanged).
fn wire_kind(typ: WireColumnType) -> LegacyColumnKind {
    match typ {
        WireColumnType::Long => LegacyColumnKind::Long,
        WireColumnType::Double => LegacyColumnKind::Double,
        WireColumnType::String | WireColumnType::Json => LegacyColumnKind::String,
    }
}

/// True when the segment header declares `name`.
fn header_declares(segment: &Segment, name: &str) -> bool {
    segment.header.columns.iter().any(|c| c.name == name)
}

/// The declared kind of an equality filter's column, or the STRING
/// default when the query carries no filter (the value is then unused).
fn filter_column_kind(segment: &Segment, filter: Option<&EqualsFilter>) -> LegacyColumnKind {
    filter.map_or(LegacyColumnKind::String, |f| {
        legacy_kind(segment, &f.dimension)
    })
}

/// The grouping key string for one dimension of one row (W-B H2).
///
/// Under the legacy latch the cell is canonicalized through the shared
/// rule first, so a null/missing NUMERIC cell keys as `"0"`/`"0.0"` and
/// merges with the stored-default rows — matching the single-binary
/// `GroupKey` composition through `column_value_at` — and a
/// null/missing/`''` STRING cell keys as the ONE merged null group.
/// ANSI keys are unchanged.
fn dim_group_key(
    row: &serde_json::Map<String, serde_json::Value>,
    name: &str,
    kind: LegacyColumnKind,
) -> String {
    if ferrodruid_common::legacy_null_mode() {
        stringify_dim_value(Some(&legacy_canonical_cell(kind, row.get(name))))
    } else {
        stringify_dim_value(row.get(name))
    }
}

fn stringify_dim_value(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::Bool(b)) => b.to_string(),
        Some(serde_json::Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn agg_value(agg: &Aggregation, v: f64) -> serde_json::Value {
    match agg {
        Aggregation::DoubleSum { .. } => json_number(v),
        _ => {
            if v.fract() == 0.0 && v.is_finite() {
                serde_json::Value::Number(serde_json::Number::from(v as i64))
            } else {
                json_number(v)
            }
        }
    }
}

#[derive(Debug)]
struct GroupState {
    /// Row holding the dimension values (cloned at first encounter).
    base: serde_json::Map<String, serde_json::Value>,
    /// One slot per aggregation, in spec order.
    slots: Vec<f64>,
}

impl GroupState {
    fn new(
        spec: &GroupBySpec,
        kinds: &[LegacyColumnKind],
        row: &serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        Self::new_for_dims(spec.dimensions.as_slice(), kinds, row)
    }

    /// `kinds` is the per-dimension [`LegacyColumnKind`] slice aligned
    /// with `dims` (resolved once per query by the executors); it is
    /// only consulted under the legacy latch.
    fn new_for_dims(
        dims: &[DimensionRef],
        kinds: &[LegacyColumnKind],
        row: &serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        let mut base = serde_json::Map::with_capacity(dims.len());
        if ferrodruid_common::legacy_null_mode() {
            // W-B legacy (H2): the emitted dimension value is the
            // canonical legacy read — including for null/missing cells
            // (numeric default 0/0.0, canonical null for strings) — so
            // the result row always carries the dimension key, exactly
            // like the single-binary `GroupKey::to_json` emission.
            // READ by the physical (input) name, EMIT under the output
            // name (the alias when one exists).
            for (d, kind) in dims.iter().zip(kinds) {
                base.insert(
                    d.emitted_name().to_string(),
                    legacy_canonical_cell(*kind, row.get(&d.name)),
                );
            }
        } else {
            for d in dims {
                if let Some(v) = row.get(&d.name) {
                    base.insert(d.emitted_name().to_string(), v.clone());
                }
            }
        }
        Self {
            base,
            slots: Vec::new(),
        }
    }

    fn feed(&mut self, row: &serde_json::Map<String, serde_json::Value>, aggs: &[Aggregation]) {
        if self.slots.len() != aggs.len() {
            self.slots = vec![0.0; aggs.len()];
        }
        for (idx, agg) in aggs.iter().enumerate() {
            match agg {
                Aggregation::Count { .. } => {
                    self.slots[idx] += 1.0;
                }
                Aggregation::LongSum { field_name, .. }
                | Aggregation::DoubleSum { field_name, .. } => {
                    if let Some(v) = row.get(field_name).and_then(serde_json::Value::as_f64) {
                        self.slots[idx] += v;
                    }
                }
            }
        }
    }

    fn finish(self, aggs: &[Aggregation]) -> serde_json::Map<String, serde_json::Value> {
        let mut out = self.base;
        for (idx, agg) in aggs.iter().enumerate() {
            let v = self.slots.get(idx).copied().unwrap_or(0.0);
            out.insert(agg.name().to_string(), agg_value(agg, v));
        }
        out
    }
}

#[derive(Debug)]
struct BucketState {
    /// One slot per aggregation, in spec order.
    slots: Vec<f64>,
}

impl BucketState {
    fn new(aggs: &[Aggregation]) -> Self {
        Self {
            slots: vec![0.0; aggs.len()],
        }
    }

    fn feed(&mut self, row: &serde_json::Map<String, serde_json::Value>, aggs: &[Aggregation]) {
        for (idx, agg) in aggs.iter().enumerate() {
            match agg {
                Aggregation::Count { .. } => {
                    self.slots[idx] += 1.0;
                }
                Aggregation::LongSum { field_name, .. }
                | Aggregation::DoubleSum { field_name, .. } => {
                    if let Some(v) = row.get(field_name).and_then(serde_json::Value::as_f64) {
                        self.slots[idx] += v;
                    }
                }
            }
        }
    }

    fn finish(self, aggs: &[Aggregation]) -> serde_json::Map<String, serde_json::Value> {
        let mut out = serde_json::Map::with_capacity(aggs.len());
        for (idx, agg) in aggs.iter().enumerate() {
            let v = self.slots[idx];
            let value = match agg {
                Aggregation::DoubleSum { .. } => json_number(v),
                _ => {
                    if v.fract() == 0.0 && v.is_finite() {
                        serde_json::Value::Number(serde_json::Number::from(v as i64))
                    } else {
                        json_number(v)
                    }
                }
            };
            out.insert(agg.name().to_string(), value);
        }
        out
    }
}

fn bucket_floor(ts: i64, granularity_ms: i64) -> i64 {
    if granularity_ms <= 0 {
        // `all` granularity: every row goes into the timestamp=0 bucket.
        return 0;
    }
    // Saturating to avoid overflow on pathological inputs.
    let g = granularity_ms;
    if ts >= 0 {
        (ts / g) * g
    } else {
        // Floor for negative timestamps.
        let q = ts / g;
        let r = ts % g;
        if r == 0 { q * g } else { (q - 1) * g }
    }
}

fn json_number(v: f64) -> serde_json::Value {
    serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_deep_storage::Segment;

    fn fixture() -> Segment {
        let text = r#"{"segmentId":"wiki_v0_0","dataSource":"wikipedia","columns":[{"name":"__time","type":"long"},{"name":"page","type":"string"},{"name":"count","type":"long"}]}
{"__time":1714694400000,"page":"home","count":3}
{"__time":1714694460000,"page":"about","count":1}
{"__time":1714694520000,"page":"home","count":2}
{"__time":1714694580000,"page":"home","count":5}
"#;
        Segment::parse_jsonl(text).expect("parse fixture")
    }

    #[test]
    fn timeseries_count_all_granularity() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 0,
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            intervals: Vec::new(),
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Timeseries(buckets) => {
                assert_eq!(buckets.len(), 1);
                assert_eq!(buckets[0].timestamp_ms, 0);
                assert_eq!(buckets[0].result.get("rows"), Some(&serde_json::json!(4)));
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    /// ANSI (this unit-test binary never latches legacy, so the
    /// process-global latch freezes on the `false` default): an empty
    /// match set keeps the historical `[]` — per segment AND through the
    /// broker's post-merge legacy fill, which must be a no-op.
    #[test]
    fn ansi_empty_timeseries_stays_empty_and_legacy_fill_is_noop() {
        let spec = TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 3_600_000,
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "no-such-page".into(),
            }),
            intervals: vec!["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z".into()],
        };
        let result = NativeQuery::Timeseries(spec.clone()).execute(&fixture());
        let NativeQueryResult::Timeseries(buckets) = result else {
            panic!("expected timeseries");
        };
        assert!(buckets.is_empty(), "ANSI empty match returns no buckets");
        let filled = legacy_fill_empty_timeseries(buckets, &spec);
        assert!(
            filled.is_empty(),
            "ANSI mode: the legacy empty-result fill must be a no-op ([]): {filled:?}"
        );
    }

    /// The post-merge legacy fill NEVER touches a non-empty merge — real
    /// buckets pass through untouched regardless of mode.
    #[test]
    fn legacy_fill_passes_non_empty_merge_through_unchanged() {
        let spec = TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 3_600_000,
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            intervals: vec!["2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z".into()],
        };
        let mut result = serde_json::Map::new();
        result.insert("rows".to_string(), serde_json::json!(4));
        let merged = vec![TimeseriesBucket {
            timestamp_ms: 1_704_067_200_000,
            result,
        }];
        let filled = legacy_fill_empty_timeseries(merged.clone(), &spec);
        assert_eq!(filled, merged, "non-empty merges pass through untouched");
    }

    #[test]
    fn timeseries_long_sum_with_filter() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 0,
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
            intervals: Vec::new(),
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Timeseries(buckets) => {
                // 3 + 2 + 5 = 10 (only "home" rows).
                assert_eq!(buckets[0].result.get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn timeseries_minute_granularity_yields_per_minute_buckets() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 60_000,
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            intervals: Vec::new(),
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Timeseries(buckets) => {
                assert_eq!(buckets.len(), 4);
                for b in &buckets {
                    assert_eq!(b.result.get("rows"), Some(&serde_json::json!(1)));
                }
                // Sorted ascending.
                let mut prev = i64::MIN;
                for b in &buckets {
                    assert!(b.timestamp_ms >= prev);
                    prev = b.timestamp_ms;
                }
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn scan_returns_full_rows_when_no_projection() {
        let q = NativeQuery::Scan(ScanSpec {
            data_source: "wikipedia".into(),
            columns: None,
            limit: None,
            filter: None,
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 4);
                assert!(rows[0].contains_key("__time"));
                assert!(rows[0].contains_key("page"));
                assert!(rows[0].contains_key("count"));
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_projects_columns_and_applies_limit() {
        let q = NativeQuery::Scan(ScanSpec {
            data_source: "wikipedia".into(),
            columns: Some(vec!["page".into()]),
            limit: Some(2),
            filter: None,
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 2);
                for r in &rows {
                    assert_eq!(r.len(), 1);
                    assert!(r.contains_key("page"));
                }
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_filter_skips_non_matching_rows() {
        let q = NativeQuery::Scan(ScanSpec {
            data_source: "wikipedia".into(),
            columns: None,
            limit: None,
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "about".into(),
            }),
        });
        let result = q.execute(&fixture());
        match result {
            NativeQueryResult::Scan(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("about")));
            }
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn merge_timeseries_sums_buckets_by_timestamp() {
        let aggs = vec![Aggregation::Count {
            name: "rows".into(),
        }];
        let part_a = vec![
            TimeseriesBucket {
                timestamp_ms: 1714694400000,
                result: serde_json::json!({"rows": 2})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
            TimeseriesBucket {
                timestamp_ms: 1714694460000,
                result: serde_json::json!({"rows": 1})
                    .as_object()
                    .expect("obj")
                    .clone(),
            },
        ];
        let part_b = vec![TimeseriesBucket {
            timestamp_ms: 1714694400000,
            result: serde_json::json!({"rows": 5})
                .as_object()
                .expect("obj")
                .clone(),
        }];

        let merged = merge_timeseries(vec![part_a, part_b], &aggs);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].timestamp_ms, 1714694400000);
        assert_eq!(merged[0].result.get("rows"), Some(&serde_json::json!(7)));
        assert_eq!(merged[1].timestamp_ms, 1714694460000);
        assert_eq!(merged[1].result.get("rows"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn merge_scan_concatenates_and_caps() {
        let part_a = vec![
            serde_json::json!({"page": "home"})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let part_b = vec![
            serde_json::json!({"page": "about"})
                .as_object()
                .expect("obj")
                .clone(),
            serde_json::json!({"page": "kb"})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let merged = merge_scan(vec![part_a, part_b], Some(2));
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get("page"), Some(&serde_json::json!("home")));
        assert_eq!(merged[1].get("page"), Some(&serde_json::json!("about")));
    }

    #[test]
    fn native_query_round_trips_via_serde_json() {
        let q = NativeQuery::Timeseries(TimeseriesSpec {
            data_source: "wikipedia".into(),
            granularity_ms: 60_000,
            aggregations: vec![
                Aggregation::Count {
                    name: "rows".into(),
                },
                Aggregation::LongSum {
                    name: "sum_count".into(),
                    field_name: "count".into(),
                },
            ],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
            intervals: Vec::new(),
        });
        let s = serde_json::to_string(&q).expect("ser");
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, q);
        assert!(s.contains("\"queryType\":\"timeseries\""), "{s}");
    }

    #[test]
    fn bucket_floor_negative_timestamp_floors_correctly() {
        assert_eq!(bucket_floor(-1, 1000), -1000);
        assert_eq!(bucket_floor(-1000, 1000), -1000);
        assert_eq!(bucket_floor(-1001, 1000), -2000);
    }

    // ---------------------------------------------------------------
    // Wave 42.RR — groupBy + topN executor / merge tests
    // ---------------------------------------------------------------

    #[test]
    fn group_by_single_dimension_sums_metric_per_group() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "page".into(),
                direction: SortDirection::Ascending,
            }]),
            limit: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("about")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(1)));
                assert_eq!(rows[1].get("page"), Some(&serde_json::json!("home")));
                // 3 + 2 + 5 = 10
                assert_eq!(rows[1].get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_having_filters_post_aggregation() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: Some(HavingClause::GreaterThan {
                aggregation: "total".into(),
                value: 5.0,
            }),
            sort: None,
            limit: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_sort_descending_by_metric_with_limit() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "rows".into(),
                direction: SortDirection::Descending,
            }]),
            limit: Some(1),
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("rows"), Some(&serde_json::json!(3)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_filter_skips_rows_pre_aggregation() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: Some(EqualsFilter {
                dimension: "page".into(),
                value: "home".into(),
            }),
            having: None,
            sort: None,
            limit: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::GroupBy(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("rows"), Some(&serde_json::json!(3)));
            }
            other => panic!("expected groupBy, got {other:?}"),
        }
    }

    #[test]
    fn top_n_ranks_descending_by_metric_with_threshold() {
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            metric: "total".into(),
            threshold: 1,
            filter: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::TopN(rows) => {
                assert_eq!(rows.len(), 1);
                // home wins: 3+2+5=10 vs about=1.
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("total"), Some(&serde_json::json!(10)));
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    #[test]
    fn top_n_returns_all_groups_when_threshold_exceeds_cardinality() {
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            metric: "rows".into(),
            threshold: 100,
            filter: None,
        });
        match q.execute(&fixture()) {
            NativeQueryResult::TopN(rows) => {
                assert_eq!(rows.len(), 2);
                // First row is the higher-count "home".
                assert_eq!(rows[0].get("page"), Some(&serde_json::json!("home")));
                assert_eq!(rows[0].get("rows"), Some(&serde_json::json!(3)));
                assert_eq!(rows[1].get("page"), Some(&serde_json::json!("about")));
                assert_eq!(rows[1].get("rows"), Some(&serde_json::json!(1)));
            }
            other => panic!("expected topN, got {other:?}"),
        }
    }

    #[test]
    fn merge_group_by_combines_per_segment_partial_folds() {
        let spec = GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            filter: None,
            having: None,
            sort: Some(vec![SortSpec {
                dimension: "page".into(),
                direction: SortDirection::Ascending,
            }]),
            limit: None,
        };
        let part_a = vec![
            serde_json::json!({"page":"home","total":3})
                .as_object()
                .expect("obj")
                .clone(),
            serde_json::json!({"page":"about","total":1})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let part_b = vec![
            serde_json::json!({"page":"home","total":7})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        let merged = merge_group_by(vec![part_a, part_b], &spec);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get("page"), Some(&serde_json::json!("about")));
        assert_eq!(merged[0].get("total"), Some(&serde_json::json!(1)));
        assert_eq!(merged[1].get("page"), Some(&serde_json::json!("home")));
        // 3 + 7 = 10 cluster-wide.
        assert_eq!(merged[1].get("total"), Some(&serde_json::json!(10)));
    }

    #[test]
    fn merge_top_n_re_ranks_after_combining_segments() {
        let spec = TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::LongSum {
                name: "total".into(),
                field_name: "count".into(),
            }],
            metric: "total".into(),
            threshold: 2,
            filter: None,
        };
        // Per-segment: a=5, b=4 → segment 1 winner is "a".
        let part_a = vec![
            serde_json::json!({"page":"a","total":5})
                .as_object()
                .expect("obj")
                .clone(),
            serde_json::json!({"page":"b","total":4})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        // Per-segment: b=10 → segment 2 contributes 10 to "b".
        let part_b = vec![
            serde_json::json!({"page":"b","total":10})
                .as_object()
                .expect("obj")
                .clone(),
        ];
        // Cluster-wide: b=14, a=5 → ordering flips.
        let merged = merge_top_n(vec![part_a, part_b], &spec);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get("page"), Some(&serde_json::json!("b")));
        assert_eq!(merged[0].get("total"), Some(&serde_json::json!(14)));
        assert_eq!(merged[1].get("page"), Some(&serde_json::json!("a")));
        assert_eq!(merged[1].get("total"), Some(&serde_json::json!(5)));
    }

    #[test]
    fn group_by_round_trips_via_serde_json() {
        let q = NativeQuery::GroupBy(GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            having: Some(HavingClause::GreaterThanOrEqual {
                aggregation: "rows".into(),
                value: 2.0,
            }),
            sort: Some(vec![SortSpec {
                dimension: "rows".into(),
                direction: SortDirection::Descending,
            }]),
            limit: Some(5),
        });
        let s = serde_json::to_string(&q).expect("ser");
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, q);
        assert!(s.contains("\"queryType\":\"groupBy\""), "{s}");
    }

    // ---------------------------------------------------------------
    // W-B role-split legacy-null fix — DimensionRef wire shape
    // ---------------------------------------------------------------

    /// A name-only dimension serializes as the historical BARE STRING —
    /// the pre-fix wire bytes — and round-trips.
    #[test]
    fn dimension_ref_name_only_keeps_bare_string_wire_bytes() {
        let spec = GroupBySpec {
            data_source: "wikipedia".into(),
            dimensions: vec!["page".into()],
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            filter: None,
            having: None,
            sort: None,
            limit: None,
        };
        let s = serde_json::to_string(&NativeQuery::GroupBy(spec.clone())).expect("ser");
        assert!(
            s.contains(r#""dimensions":["page"]"#),
            "name-only dimensions must keep the bare-string wire spelling: {s}"
        );
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, NativeQuery::GroupBy(spec));
    }

    /// A typed (no-alias) dimension serializes as the Druid-style
    /// object — byte-identical to the pre-aliasing-fix wire — and
    /// round-trips with its `outputType` intact.
    #[test]
    fn dimension_ref_with_output_type_round_trips_object_form() {
        let dim = DimensionRef {
            name: "m".into(),
            output_name: None,
            output_type: Some(DimensionOutputType::Long),
        };
        let s = serde_json::to_string(&dim).expect("ser");
        assert_eq!(s, r#"{"dimension":"m","outputType":"LONG"}"#);
        let back: DimensionRef = serde_json::from_str(&s).expect("de");
        assert_eq!(back, dim);
    }

    /// An ALIASED dimension carries the physical (read) name AND the
    /// output alias on the wire, round-tripping both — with and without
    /// an `outputType`.
    #[test]
    fn dimension_ref_with_alias_round_trips_both_names() {
        let dim = DimensionRef {
            name: "y".into(),
            output_name: Some("alias".into()),
            output_type: Some(DimensionOutputType::Long),
        };
        let s = serde_json::to_string(&dim).expect("ser");
        assert_eq!(
            s,
            r#"{"dimension":"y","outputName":"alias","outputType":"LONG"}"#
        );
        let back: DimensionRef = serde_json::from_str(&s).expect("de");
        assert_eq!(back, dim);
        assert_eq!(back.emitted_name(), "alias");

        let untyped = DimensionRef {
            name: "s".into(),
            output_name: Some("label".into()),
            output_type: None,
        };
        let s = serde_json::to_string(&untyped).expect("ser");
        assert_eq!(s, r#"{"dimension":"s","outputName":"label"}"#);
        let back: DimensionRef = serde_json::from_str(&s).expect("de");
        assert_eq!(back, untyped);
        assert_eq!(back.emitted_name(), "label");
    }

    /// The Druid `"type": "default"` tagged spelling is accepted (an
    /// alias equal to the physical name normalizes to "no alias" so the
    /// round-trip stays byte-identical); a DISTINCT alias is carried
    /// through; non-default types still fail loud (the wire executor
    /// has no extraction machinery).
    #[test]
    fn dimension_ref_accepts_default_tag_and_aliases_rejects_extraction() {
        let tagged: DimensionRef = serde_json::from_str(
            r#"{"type":"default","dimension":"m","outputName":"m","outputType":"DOUBLE"}"#,
        )
        .expect("default-tagged spelling parses");
        assert_eq!(
            tagged,
            DimensionRef {
                name: "m".into(),
                output_name: None,
                output_type: Some(DimensionOutputType::Double),
            },
            "an alias equal to the physical name normalizes to None"
        );

        let aliased: DimensionRef =
            serde_json::from_str(r#"{"type":"default","dimension":"m","outputName":"alias"}"#)
                .expect("a distinct outputName alias parses");
        assert_eq!(
            aliased,
            DimensionRef {
                name: "m".into(),
                output_name: Some("alias".into()),
                output_type: None,
            },
            "a distinct alias must ride the wire (read `m`, emit `alias`)"
        );

        assert!(
            serde_json::from_str::<DimensionRef>(
                r#"{"type":"extraction","dimension":"m","extractionFn":{"type":"substring","index":0}}"#
            )
            .is_err(),
            "non-default dimension types must fail loud, never silently degrade"
        );
    }

    #[test]
    fn top_n_round_trips_via_serde_json() {
        let q = NativeQuery::TopN(TopNSpec {
            data_source: "wikipedia".into(),
            dimension: "page".into(),
            aggregations: vec![Aggregation::Count {
                name: "rows".into(),
            }],
            metric: "rows".into(),
            threshold: 10,
            filter: None,
        });
        let s = serde_json::to_string(&q).expect("ser");
        let back: NativeQuery = serde_json::from_str(&s).expect("de");
        assert_eq!(back, q);
        assert!(s.contains("\"queryType\":\"topN\""), "{s}");
    }
}
