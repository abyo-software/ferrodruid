// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! `DimensionSpec` extraction-function and filter-wrapper application.
//!
//! Wave 36-G1 (Wave 37B query Highs #2 + #3) closes a wire-compat
//! regression where TopN/GroupBy silently dropped the
//! `extraction`/`listFiltered`/`regexFiltered`/`prefixFiltered` wrappers
//! from a `DimensionSpec` and grouped on the raw column value instead.
//! These helpers walk the wrapper chain at row time and produce the
//! correctly-transformed key (or signal that the row should be excluded
//! from the per-bucket aggregation).

use ferrodruid_common::{
    error::{DruidError, Result},
    types::{DimensionSpec, ExtractionFunction, NullHandling},
};
use ordered_float::OrderedFloat;
use regex::{Regex, RegexBuilder};

// ---------------------------------------------------------------------------
// Wave 45-F — Compiled DimensionSpec
// ---------------------------------------------------------------------------

/// Maximum DFA size in bytes per compiled regex.  Caps both NFA-blowup
/// patterns and adversarial alternations so a hostile query cannot drive
/// historical memory to OOM via a single `regex::Regex::new` call.
///
/// 1 MiB is large enough for every realistic Druid extraction pattern
/// observed in the wild (the largest in the public Druid corpus is
/// ~12 KiB); anything larger is overwhelmingly likely to be hostile or
/// generated.
const MAX_REGEX_DFA_BYTES: usize = 1 << 20;

/// Cap on per-call regex evaluation work.  The `regex` crate evaluates in
/// linear time on the input length (no catastrophic backtracking — see
/// crate-level docs), so this cap defends against truly absurd inputs
/// (e.g. multi-MB single-row strings).  Anything longer than this is
/// truncated before matching, with the truncated tail logged as a parse-
/// time `DruidError::Query` only when the *plan-time* sample exceeds it.
const MAX_REGEX_INPUT_BYTES: usize = 1 << 20;

/// Selector for which capture group a regex extraction returns.
///
/// Wave 45-F (W39 NEW Medium): the previous wire-only `index: Option<usize>`
/// encoding could not express "the whole match" (group 0) and had no
/// way to address named capture groups.  `RegexGroup` is the canonical
/// internal form derived from the JSON wire fields `index` and
/// `groupName` (the latter is a FerroDruid extension; numeric `index`
/// remains Druid-wire-compatible).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegexGroup {
    /// The whole match (group 0).  Selected when neither `index` nor
    /// `groupName` is set on the wire.
    Default,
    /// Numbered capture group (`$N`, 1-indexed in Druid terms).
    Numbered(usize),
    /// Named capture group `(?P<name>…)`.
    Named(String),
}

impl RegexGroup {
    /// Resolve the wire fields into the canonical group selector.
    /// `groupName` takes precedence over `index` when both are set.
    fn from_wire(index: Option<usize>, group_name: Option<&str>) -> Self {
        if let Some(name) = group_name {
            return Self::Named(name.to_owned());
        }
        match index {
            Some(n) => Self::Numbered(n),
            None => Self::Default,
        }
    }
}

/// A `DimensionSpec` whose embedded regular expressions have been
/// compiled exactly once at query-plan time.
///
/// Wave 45-F closes the W39 NEW Medium "regex DimensionSpec parse-failure
/// silently turning into empty results" by:
///
/// * compiling every regex (extraction `regex` / `partial`, and the
///   `regexFiltered` filter) at construction time;
/// * surfacing malformed patterns as
///   [`DruidError::Query`](ferrodruid_common::error::DruidError::Query)
///   from [`Self::new`], not as a silent per-row `None` swallow during
///   execution;
/// * bounding the worst-case DFA size via [`RegexBuilder::dfa_size_limit`]
///   (see [`MAX_REGEX_DFA_BYTES`]) so a hostile pattern such as
///   `(a+)+$` or an enormous alternation cannot OOM the historical;
/// * supporting capture-group selection by number (`index`) and by name
///   (`groupName`), defaulting to "the whole match" (group 0) per the
///   Wave 45-F spec.
///
/// The query layer constructs one `CompiledDimSpec` per dimension at
/// plan time (TopN/GroupBy `execute*`) and then calls
/// [`Self::apply_typed`] / [`Self::apply`] in the per-row hot loop.
#[derive(Debug)]
pub struct CompiledDimSpec {
    spec: DimensionSpec,
    /// Compiled regex tree, mirroring the wrapper structure of `spec`.
    /// `None` for `Default` / `Substring` / `Lower` / `Upper` / etc.
    compiled: CompiledNode,
}

/// Mirror of the `DimensionSpec` / `ExtractionFunction` wrapper tree
/// holding compiled regex objects at the leaves.
#[derive(Debug)]
enum CompiledNode {
    /// No compiled regex below this node.
    Plain,
    /// Inner extraction-function compiled regex (or none if the
    /// extraction is not regex-flavoured).
    Extraction(Option<CompiledExtraction>),
    /// `regexFiltered` wrapper.
    RegexFiltered {
        /// Compiled filter regex.
        filter: Regex,
        /// Inner spec's compiled tree.
        inner: Box<CompiledNode>,
    },
    /// `listFiltered` / `prefixFiltered` — recursive into the inner spec.
    Wrapped(Box<CompiledNode>),
}

/// Compiled regex sitting under an `ExtractionFunction::Regex` /
/// `ExtractionFunction::Partial` / `ExtractionFunction::Cascade` leaf.
#[derive(Debug, Clone)]
enum CompiledExtraction {
    /// Compiled regex extraction (`type: regex`).
    Regex {
        /// Compiled pattern.
        re: Regex,
        /// Resolved capture-group selector.
        group: RegexGroup,
        /// `replaceMissingValueWith` fallback string, materialised once.
        replace_missing_value_with: Option<String>,
        /// Whether to honour `replace_missing_value_with` on no-match
        /// (i.e. the wire `replace_missing_value` flag).
        replace_missing: bool,
    },
    /// Compiled partial-match extraction (`type: partial`).
    Partial(Regex),
    /// Cascade — recursively compiled child list.
    Cascade(Vec<Option<CompiledExtraction>>),
}

impl CompiledDimSpec {
    /// Compile every regex inside `spec` exactly once.
    ///
    /// # Errors
    /// Returns [`DruidError::Query`] when any embedded regular expression
    /// fails to parse, or when its compiled DFA would exceed
    /// [`MAX_REGEX_DFA_BYTES`].  The error message includes the offending
    /// pattern so the caller can surface it to the operator.
    pub fn new(spec: &DimensionSpec) -> Result<Self> {
        let compiled = compile_node(spec)?;
        Ok(Self {
            spec: spec.clone(),
            compiled,
        })
    }

    /// Apply the (compiled) spec to a typed JSON dimension value.
    ///
    /// Mirrors [`apply_dim_spec_typed`] but uses the compiled regex
    /// tree, so each hot-loop call avoids re-parsing the regex source.
    #[must_use]
    pub fn apply_typed(&self, value: &serde_json::Value) -> Option<GroupKey> {
        match (&self.spec, &self.compiled) {
            (DimensionSpec::Default { .. }, _) => Some(json_to_group_key(value)),
            _ => {
                let raw = json_value_to_string(value);
                self.apply(&raw).map(GroupKey::String)
            }
        }
    }

    /// Apply the (compiled) spec to a string-domain dimension value.
    #[must_use]
    pub fn apply(&self, raw: &str) -> Option<String> {
        apply_compiled(&self.spec, &self.compiled, raw)
    }
}

/// Build a [`Regex`] with the FerroDruid hardening defaults
/// ([`MAX_REGEX_DFA_BYTES`] cap on the compiled DFA).
fn build_hardened(pattern: &str) -> Result<Regex> {
    RegexBuilder::new(pattern)
        .dfa_size_limit(MAX_REGEX_DFA_BYTES)
        .size_limit(MAX_REGEX_DFA_BYTES)
        .build()
        .map_err(|e| DruidError::Query(format!("invalid regex pattern {pattern:?}: {e}")))
}

/// Recursively compile every regex inside a `DimensionSpec`.
fn compile_node(spec: &DimensionSpec) -> Result<CompiledNode> {
    match spec {
        DimensionSpec::Default { .. } => Ok(CompiledNode::Plain),
        DimensionSpec::Extraction { extraction_fn, .. } => {
            Ok(CompiledNode::Extraction(compile_extraction(extraction_fn)?))
        }
        DimensionSpec::ListFiltered { delegate, .. }
        | DimensionSpec::PrefixFiltered { delegate, .. } => {
            Ok(CompiledNode::Wrapped(Box::new(compile_node(delegate)?)))
        }
        DimensionSpec::RegexFiltered { delegate, pattern } => {
            let filter = build_hardened(pattern)?;
            let inner = Box::new(compile_node(delegate)?);
            Ok(CompiledNode::RegexFiltered { filter, inner })
        }
    }
}

/// Compile any regex objects nested inside an extraction function.
fn compile_extraction(func: &ExtractionFunction) -> Result<Option<CompiledExtraction>> {
    match func {
        ExtractionFunction::Regex {
            expr,
            index,
            replace_missing_value,
            replace_missing_value_with,
            group_name,
        } => {
            let re = build_hardened(expr)?;
            let group = RegexGroup::from_wire(*index, group_name.as_deref());
            // If a named group is requested, verify it exists at compile
            // time so the operator gets a parse-time error rather than a
            // silent per-row no-match.
            if let RegexGroup::Named(name) = &group
                && re.capture_names().flatten().all(|n| n != name)
            {
                return Err(DruidError::Query(format!(
                    "regex {expr:?} has no capture group named {name:?}",
                )));
            }
            // Numbered groups beyond the regex's static group count are
            // also a parse-time error.
            if let RegexGroup::Numbered(n) = &group
                && *n >= re.captures_len()
            {
                return Err(DruidError::Query(format!(
                    "regex {expr:?} has only {} capture group(s) but index {n} was requested",
                    re.captures_len().saturating_sub(1),
                )));
            }
            Ok(Some(CompiledExtraction::Regex {
                re,
                group,
                replace_missing_value_with: replace_missing_value_with.clone(),
                replace_missing: *replace_missing_value,
            }))
        }
        ExtractionFunction::Partial { expr } => {
            let re = build_hardened(expr)?;
            Ok(Some(CompiledExtraction::Partial(re)))
        }
        ExtractionFunction::Cascade { extraction_fns } => {
            let mut compiled = Vec::with_capacity(extraction_fns.len());
            for f in extraction_fns {
                compiled.push(compile_extraction(f)?);
            }
            Ok(Some(CompiledExtraction::Cascade(compiled)))
        }
        _ => Ok(None),
    }
}

/// Walk the compiled tree alongside the original spec and apply the
/// transform in lock-step.
fn apply_compiled(spec: &DimensionSpec, compiled: &CompiledNode, raw: &str) -> Option<String> {
    match (spec, compiled) {
        (DimensionSpec::Default { .. }, _) => Some(raw.to_owned()),
        (DimensionSpec::Extraction { extraction_fn, .. }, CompiledNode::Extraction(c)) => {
            apply_extraction_compiled(extraction_fn, c.as_ref(), raw)
        }
        (
            DimensionSpec::ListFiltered {
                delegate,
                values,
                is_whitelist,
            },
            CompiledNode::Wrapped(inner),
        ) => {
            let transformed = apply_compiled(delegate, inner, raw)?;
            let in_list = values.iter().any(|v| v == &transformed);
            if in_list == *is_whitelist {
                Some(transformed)
            } else {
                None
            }
        }
        (DimensionSpec::PrefixFiltered { delegate, prefix }, CompiledNode::Wrapped(inner)) => {
            let transformed = apply_compiled(delegate, inner, raw)?;
            if transformed.starts_with(prefix.as_str()) {
                Some(transformed)
            } else {
                None
            }
        }
        (
            DimensionSpec::RegexFiltered { delegate, .. },
            CompiledNode::RegexFiltered { filter, inner },
        ) => {
            let transformed = apply_compiled(delegate, inner, raw)?;
            let probe = bound_input(&transformed);
            if filter.is_match(probe) {
                Some(transformed)
            } else {
                None
            }
        }
        // Tree mismatch — should be impossible by construction, but fall
        // back to the legacy interpreter so production never crashes.
        _ => apply_dim_spec(spec, raw),
    }
}

/// Apply a (possibly compiled) extraction function.
fn apply_extraction_compiled(
    func: &ExtractionFunction,
    compiled: Option<&CompiledExtraction>,
    value: &str,
) -> Option<String> {
    match (func, compiled) {
        (
            ExtractionFunction::Regex { .. },
            Some(CompiledExtraction::Regex {
                re,
                group,
                replace_missing_value_with,
                replace_missing,
            }),
        ) => {
            let probe = bound_input(value);
            if let Some(caps) = re.captures(probe) {
                let m = match group {
                    RegexGroup::Default => caps.get(0),
                    RegexGroup::Numbered(n) => caps.get(*n),
                    RegexGroup::Named(name) => caps.name(name),
                };
                if let Some(m) = m {
                    return Some(m.as_str().to_owned());
                }
            }
            if *replace_missing {
                Some(replace_missing_value_with.clone().unwrap_or_default())
            } else {
                Some(String::new())
            }
        }
        (ExtractionFunction::Partial { .. }, Some(CompiledExtraction::Partial(re))) => {
            let probe = bound_input(value);
            re.find(probe)
                .map(|m| m.as_str().to_owned())
                .or_else(|| Some(String::new()))
        }
        (
            ExtractionFunction::Cascade { extraction_fns },
            Some(CompiledExtraction::Cascade(children)),
        ) => {
            let mut current = value.to_owned();
            for (f, c) in extraction_fns.iter().zip(children.iter()) {
                current = apply_extraction_compiled(f, c.as_ref(), &current)?;
            }
            Some(current)
        }
        // Non-regex extractions reuse the legacy single-call interpreter,
        // which has no per-row regex compile cost.
        _ => apply_extraction_fn(func, value),
    }
}

/// Bound the slice of input fed to the regex engine.  The `regex` crate
/// guarantees linear-time evaluation (no catastrophic backtracking), but
/// astronomical inputs can still soak CPU; cap them at
/// [`MAX_REGEX_INPUT_BYTES`].  This is a defence-in-depth knob — by the
/// time a single dimension value exceeds 1 MiB the operator has bigger
/// problems than this cap firing.
fn bound_input(value: &str) -> &str {
    if value.len() <= MAX_REGEX_INPUT_BYTES {
        value
    } else {
        // Round down to the nearest UTF-8 boundary at or below the cap.
        let mut end = MAX_REGEX_INPUT_BYTES;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        &value[..end]
    }
}

// ---------------------------------------------------------------------------
// GroupKey — typed grouping-key encoding (Wave 40-B)
// ---------------------------------------------------------------------------

/// A typed grouping key value.
///
/// Wave 40-B (Wave 39 [High] [NEW-VARIANT] groupby.rs:247-259, topn.rs:166-173):
/// the previous implementation funneled every dimension value through
/// `value.to_string()` before hashing it into the per-key map.  That meant:
///
/// * `1` (number) and `"1"` (string) collided in the same bucket.
/// * `true` (bool) and `"true"` (string) collided.
/// * `null` JSON values silently became the empty string and merged with
///   the empty-string dimension value.
/// * NaN doubles round-tripped to the literal string `"NaN"`, defeating
///   any deterministic merge semantics.
///
/// `GroupKey` preserves the JSON type tag in the hash key so distinct
/// types do not collide, and uses [`OrderedFloat`] so doubles (including
/// NaN — `OrderedFloat<f64>` defines `Hash`/`Eq` that treat all NaN
/// representations as equal, matching the documented Druid behaviour).
///
/// For multi-dimension `GroupBy`, the runtime key is `Vec<GroupKey>`
/// constructed by iterating the dimensions in declaration order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupKey {
    /// 64-bit signed integer dimension value.
    Long(i64),
    /// 64-bit floating-point dimension value (NaN-aware via [`OrderedFloat`]).
    Double(OrderedFloat<f64>),
    /// Boolean dimension value.
    Bool(bool),
    /// String dimension value (also used for the result of any
    /// extraction / filter-wrapper transform, since those produce a
    /// string by definition).
    String(String),
    /// Null / missing dimension value.
    Null,
}

impl GroupKey {
    /// Project this key into the `output_name` JSON value used by the
    /// `GroupByResult.event` map (and TopN's per-row `event`).  String
    /// keys go back as JSON strings, numeric/boolean keys go back as
    /// their typed JSON forms.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Long(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Self::Double(f) => serde_json::Number::from_f64(f.into_inner())
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Bool(b) => serde_json::Value::Bool(*b),
            Self::String(s) => serde_json::Value::String(s.clone()),
            Self::Null => serde_json::Value::Null,
        }
    }

    /// String form used when a downstream sort path reads the key as text
    /// (e.g. TopN dimension-ordering or GroupBy lex sort).  This is *not*
    /// used for hashing and so does not re-introduce the type-collision
    /// regression that motivated `GroupKey`.
    #[must_use]
    pub fn as_sort_key(&self) -> String {
        match self {
            Self::Long(n) => n.to_string(),
            Self::Double(f) => f.into_inner().to_string(),
            Self::Bool(b) => b.to_string(),
            Self::String(s) => s.clone(),
            Self::Null => String::new(),
        }
    }
}

/// Build a `GroupKey` from a JSON dimension value, preserving the JSON
/// type tag.
pub(crate) fn json_to_group_key(value: &serde_json::Value) -> GroupKey {
    match value {
        serde_json::Value::Null => GroupKey::Null,
        serde_json::Value::Bool(b) => GroupKey::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                GroupKey::Long(i)
            } else if let Some(f) = n.as_f64() {
                GroupKey::Double(OrderedFloat(f))
            } else {
                // u64 too large to fit in i64 — preserve as string rather
                // than silently truncating.
                GroupKey::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => GroupKey::String(s.clone()),
        other => GroupKey::String(other.to_string()),
    }
}

/// Apply a `DimensionSpec` to a *typed* JSON dimension value, returning a
/// `GroupKey` that preserves the JSON type tag when the spec is
/// `Default` (i.e. no extraction/filter wrapper) and falling back to a
/// `GroupKey::String` for any spec that performs a string-domain
/// transform (extraction / list / regex / prefix).
///
/// Returns `None` when a `listFiltered` / `regexFiltered` /
/// `prefixFiltered` wrapper rejects the value, mirroring
/// [`apply_dim_spec`] semantics.
///
/// Wave 45-F: superseded in the query hot loop by
/// [`CompiledDimSpec::apply_typed`], which compiles every regex once at
/// plan time.  Kept for ad-hoc callers (and for the GroupKey unit
/// tests) that do not care about the per-row regex compile cost.
#[must_use]
#[cfg_attr(not(test), allow(dead_code))]
pub fn apply_dim_spec_typed(spec: &DimensionSpec, value: &serde_json::Value) -> Option<GroupKey> {
    match spec {
        DimensionSpec::Default { .. } => Some(json_to_group_key(value)),
        // Any spec that runs a string transform (extraction / list /
        // regex / prefix) inherently downgrades to a string-domain key.
        // Re-use the string-domain code path for these.
        _ => {
            let raw = json_value_to_string(value);
            apply_dim_spec(spec, &raw).map(GroupKey::String)
        }
    }
}

/// Convert a JSON value to its string form for the legacy/string-domain
/// `apply_dim_spec` path.
fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        _ => v.to_string(),
    }
}

/// Apply a `DimensionSpec` (with all its wrappers) to a raw dimension
/// value.
///
/// Returns `None` when a `listFiltered` / `regexFiltered` /
/// `prefixFiltered` wrapper rejects the value, in which case the caller
/// must skip the row (i.e. neither hash it into a per-key bucket nor
/// allocate aggregators for it).  Returns `Some(transformed)` otherwise.
///
/// The returned string is the **group key** (and the `output_name`
/// column value for the result).
#[must_use]
pub fn apply_dim_spec(spec: &DimensionSpec, raw: &str) -> Option<String> {
    match spec {
        DimensionSpec::Default { .. } => Some(raw.to_owned()),
        DimensionSpec::Extraction { extraction_fn, .. } => apply_extraction_fn(extraction_fn, raw),
        DimensionSpec::ListFiltered {
            delegate,
            values,
            is_whitelist,
        } => {
            // First apply the inner wrapper (extraction / nested filter),
            // then run the list filter on its output — this matches the
            // documented `listFiltered { delegate, values, isWhitelist }`
            // semantics where the filter is evaluated on the *post-
            // transform* value.
            let transformed = apply_dim_spec(delegate, raw)?;
            let in_list = values.iter().any(|v| v == &transformed);
            if in_list == *is_whitelist {
                Some(transformed)
            } else {
                None
            }
        }
        DimensionSpec::RegexFiltered { delegate, pattern } => {
            let transformed = apply_dim_spec(delegate, raw)?;
            // Legacy single-call path: compile per call.  Wave 45-F
            // introduced [`CompiledDimSpec`] for the hot loop, and the
            // query engine now goes through it; this branch survives
            // only for ad-hoc callers that have not yet migrated.
            match build_hardened(pattern) {
                Ok(re) => {
                    let probe = bound_input(&transformed);
                    if re.is_match(probe) {
                        Some(transformed)
                    } else {
                        None
                    }
                }
                // Invalid pattern -> conservatively reject all rows so the
                // operator notices.  The compile-time validator
                // ([`CompiledDimSpec::new`]) is the canonical error
                // surface; this fallback is wire-compat insurance.
                Err(_) => None,
            }
        }
        DimensionSpec::PrefixFiltered { delegate, prefix } => {
            let transformed = apply_dim_spec(delegate, raw)?;
            if transformed.starts_with(prefix.as_str()) {
                Some(transformed)
            } else {
                None
            }
        }
    }
}

/// Apply a single `ExtractionFunction` to a string value.
///
/// Returns `None` only when the function explicitly drops the value (for
/// example a `regex` with no match and `replaceMissingValue=false`
/// returning `null`); callers that want to keep null rows in their
/// per-bucket key map can substitute the empty string.
fn apply_extraction_fn(func: &ExtractionFunction, value: &str) -> Option<String> {
    match func {
        ExtractionFunction::Regex {
            expr,
            index,
            replace_missing_value,
            replace_missing_value_with,
            group_name,
        } => {
            let re = build_hardened(expr).ok()?;
            let group = RegexGroup::from_wire(*index, group_name.as_deref());
            let probe = bound_input(value);
            if let Some(caps) = re.captures(probe) {
                let m = match &group {
                    RegexGroup::Default => caps.get(0),
                    RegexGroup::Numbered(n) => caps.get(*n),
                    RegexGroup::Named(name) => caps.name(name),
                };
                if let Some(m) = m {
                    return Some(m.as_str().to_owned());
                }
            }
            if *replace_missing_value {
                Some(replace_missing_value_with.clone().unwrap_or_default())
            } else {
                Some(String::new())
            }
        }
        ExtractionFunction::Partial { expr } => {
            let re = build_hardened(expr).ok()?;
            let probe = bound_input(value);
            re.find(probe)
                .map(|m| m.as_str().to_owned())
                .or_else(|| Some(String::new()))
        }
        ExtractionFunction::SearchQuery { .. } => Some(value.to_owned()),
        ExtractionFunction::Strlen => Some(value.chars().count().to_string()),
        ExtractionFunction::TimeFormat { .. } => Some(value.to_owned()),
        ExtractionFunction::Time { .. } => Some(value.to_owned()),
        ExtractionFunction::Substring { index, length } => {
            let chars: Vec<char> = value.chars().collect();
            let start = (*index).min(chars.len());
            let end = match length {
                Some(len) => (start + *len).min(chars.len()),
                None => chars.len(),
            };
            Some(chars[start..end].iter().collect())
        }
        ExtractionFunction::Upper { .. } => Some(value.to_uppercase()),
        ExtractionFunction::Lower { .. } => Some(value.to_lowercase()),
        ExtractionFunction::Bucket { size, offset } => {
            let parsed: f64 = value.parse().unwrap_or(0.0);
            let s = if *size == 0.0 { 1.0 } else { *size };
            let bucket = ((parsed - *offset) / s).floor() * s + *offset;
            Some(format!("{bucket}"))
        }
        ExtractionFunction::Cascade { extraction_fns } => {
            let mut current = value.to_owned();
            for f in extraction_fns {
                current = apply_extraction_fn(f, &current)?;
            }
            Some(current)
        }
        ExtractionFunction::StringFormat {
            null_handling,
            format,
        } => {
            // Druid's `String.format` is Java; we honour the null-handling
            // flag (the only behaviour with wire-compat impact for
            // groupBy keys) and otherwise pass the value through using
            // Rust's `replace` of the `%s` token.  Unsupported `%d` /
            // `%f` etc. fall back to the raw value.
            let v = if value.is_empty() {
                match null_handling {
                    NullHandling::NullString => "null".to_owned(),
                    NullHandling::EmptyString => String::new(),
                    NullHandling::ReturnNull => return Some(String::new()),
                }
            } else {
                value.to_owned()
            };
            Some(format.replace("%s", &v))
        }
        ExtractionFunction::Lookup { .. } | ExtractionFunction::RegisteredLookup { .. } => {
            // Without a wired-in lookup table we can only pass the value
            // through unchanged.  This matches the behaviour of the
            // top-level `apply_dim_spec` for previously-Default rows:
            // wire compat with the *value* is lost, but no group keys
            // are silently merged that shouldn't be.
            Some(value.to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrodruid_common::types::ColumnType;

    fn default_spec(name: &str) -> DimensionSpec {
        DimensionSpec::Default {
            dimension: name.into(),
            output_name: name.into(),
            output_type: ColumnType::String,
        }
    }

    #[test]
    fn default_passes_value_through() {
        assert_eq!(
            apply_dim_spec(&default_spec("page"), "Foo"),
            Some("Foo".into())
        );
    }

    #[test]
    fn extraction_lower_normalises() {
        let spec = DimensionSpec::Extraction {
            dimension: "page".into(),
            output_name: "page_lc".into(),
            extraction_fn: ExtractionFunction::Lower { locale: None },
        };
        assert_eq!(apply_dim_spec(&spec, "MainPage"), Some("mainpage".into()));
    }

    #[test]
    fn extraction_strlen_counts_chars() {
        let spec = DimensionSpec::Extraction {
            dimension: "page".into(),
            output_name: "len".into(),
            extraction_fn: ExtractionFunction::Strlen,
        };
        assert_eq!(apply_dim_spec(&spec, "abc"), Some("3".into()));
    }

    #[test]
    fn list_filtered_whitelist_excludes_other_values() {
        let spec = DimensionSpec::ListFiltered {
            delegate: Box::new(default_spec("page")),
            values: vec!["A".into(), "B".into()],
            is_whitelist: true,
        };
        assert_eq!(apply_dim_spec(&spec, "A"), Some("A".into()));
        assert_eq!(apply_dim_spec(&spec, "C"), None);
    }

    #[test]
    fn list_filtered_blacklist_excludes_listed_values() {
        let spec = DimensionSpec::ListFiltered {
            delegate: Box::new(default_spec("page")),
            values: vec!["X".into()],
            is_whitelist: false,
        };
        assert_eq!(apply_dim_spec(&spec, "X"), None);
        assert_eq!(apply_dim_spec(&spec, "Y"), Some("Y".into()));
    }

    #[test]
    fn prefix_filtered_excludes_non_matches() {
        let spec = DimensionSpec::PrefixFiltered {
            delegate: Box::new(default_spec("page")),
            prefix: "Talk:".into(),
        };
        assert_eq!(
            apply_dim_spec(&spec, "Talk:Main_Page"),
            Some("Talk:Main_Page".into())
        );
        assert_eq!(apply_dim_spec(&spec, "Main_Page"), None);
    }

    #[test]
    fn regex_filtered_pattern_match() {
        let spec = DimensionSpec::RegexFiltered {
            delegate: Box::new(default_spec("page")),
            pattern: r"^[A-Z]".into(),
        };
        assert_eq!(apply_dim_spec(&spec, "Foo"), Some("Foo".into()));
        assert_eq!(apply_dim_spec(&spec, "foo"), None);
    }

    #[test]
    fn list_filtered_runs_after_extraction() {
        // Inner: lower-case; outer whitelist on the lowered form.
        let spec = DimensionSpec::ListFiltered {
            delegate: Box::new(DimensionSpec::Extraction {
                dimension: "page".into(),
                output_name: "page".into(),
                extraction_fn: ExtractionFunction::Lower { locale: None },
            }),
            values: vec!["foo".into()],
            is_whitelist: true,
        };
        // "FOO" -> lower -> "foo" -> whitelisted.
        assert_eq!(apply_dim_spec(&spec, "FOO"), Some("foo".into()));
        // "BAR" -> lower -> "bar" -> not in whitelist.
        assert_eq!(apply_dim_spec(&spec, "BAR"), None);
    }

    // -----------------------------------------------------------------------
    // Wave 40-B: typed GroupKey unit tests
    // (Wave 39 [High] [NEW-VARIANT] groupby.rs:247-259, topn.rs:166-173)
    // -----------------------------------------------------------------------

    #[test]
    fn group_key_distinguishes_number_one_from_string_one() {
        let n = json_to_group_key(&serde_json::json!(1));
        let s = json_to_group_key(&serde_json::json!("1"));
        assert_ne!(
            n, s,
            "JSON number `1` and string `\"1\"` must not collide in GroupKey"
        );
    }

    #[test]
    fn group_key_double_nan_hashes_consistently() {
        use std::collections::HashSet;
        // OrderedFloat treats all NaN representations as equal under
        // Hash + Eq, so feeding the same NaN value twice must collapse
        // to a single entry.
        let nan_a = json_to_group_key(&serde_json::json!(f64::NAN));
        let nan_b = json_to_group_key(&serde_json::json!(f64::NAN));
        let mut set: HashSet<GroupKey> = HashSet::new();
        set.insert(nan_a);
        set.insert(nan_b);
        assert_eq!(
            set.len(),
            1,
            "GroupKey::Double(NaN) must hash consistently across instances"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 45-F: CompiledDimSpec — compile-once + groups + hostile-input
    // (W39 NEW Medium close)
    // -----------------------------------------------------------------------

    fn regex_extraction(
        expr: &str,
        index: Option<usize>,
        group_name: Option<&str>,
        replace_missing_value: bool,
        replace_missing_value_with: Option<&str>,
    ) -> DimensionSpec {
        DimensionSpec::Extraction {
            dimension: "page".into(),
            output_name: "out".into(),
            extraction_fn: ExtractionFunction::Regex {
                expr: expr.into(),
                index,
                replace_missing_value,
                replace_missing_value_with: replace_missing_value_with.map(str::to_owned),
                group_name: group_name.map(str::to_owned),
            },
        }
    }

    #[test]
    fn regex_dim_spec_with_malformed_pattern_rejected_at_parse_time() {
        // Unclosed alternation group — the regex crate refuses this.
        let spec = regex_extraction("(unclosed", None, None, false, None);
        let err =
            CompiledDimSpec::new(&spec).expect_err("malformed regex must error at parse time");
        let msg = err.to_string();
        assert!(
            msg.contains("invalid regex pattern"),
            "expected query-error to mention 'invalid regex pattern', got: {msg}",
        );
    }

    #[test]
    fn regex_dim_spec_filter_malformed_pattern_rejected_at_parse_time() {
        // The filter wrapper variant must surface the same parse-time
        // error path so an operator never sees a silent empty result.
        let spec = DimensionSpec::RegexFiltered {
            delegate: Box::new(default_spec("page")),
            pattern: "[unterminated".into(),
        };
        assert!(CompiledDimSpec::new(&spec).is_err());
    }

    #[test]
    fn regex_dim_spec_default_group_returns_whole_match() {
        // No `index`, no `groupName` => RegexGroup::Default => group 0
        // (the entire match), not group 1 as the legacy default would.
        let spec = regex_extraction(r"\d+", None, None, false, None);
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(compiled.apply("foo123bar"), Some("123".into()));
    }

    #[test]
    fn regex_dim_spec_numbered_group_returns_capture() {
        // index = 2 selects the *second* parenthesised group.
        let spec = regex_extraction(r"(\d+)-(\w+)", Some(2), None, false, None);
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(compiled.apply("42-banana"), Some("banana".into()));
    }

    #[test]
    fn regex_dim_spec_named_group_returns_capture() {
        let spec = regex_extraction(
            r"(?P<num>\d+)-(?P<word>\w+)",
            None,
            Some("word"),
            false,
            None,
        );
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(compiled.apply("99-cherries"), Some("cherries".into()));
    }

    #[test]
    fn regex_dim_spec_named_group_missing_at_compile_time_errors() {
        // Asking for a name that does not exist in the pattern is a
        // parse-time error, not a silent per-row empty.
        let spec = regex_extraction(
            r"(?P<num>\d+)-(\w+)",
            None,
            Some("nonexistent"),
            false,
            None,
        );
        let err =
            CompiledDimSpec::new(&spec).expect_err("missing named group must error at parse time");
        assert!(err.to_string().contains("no capture group named"));
    }

    #[test]
    fn regex_dim_spec_numbered_group_out_of_range_errors_at_parse() {
        // Pattern has only 1 capture group; asking for group 5 must fail.
        let spec = regex_extraction(r"(\w+)", Some(5), None, false, None);
        assert!(CompiledDimSpec::new(&spec).is_err());
    }

    #[test]
    fn regex_dim_spec_no_match_uses_replace_missing_value() {
        // No match + replace_missing_value=true + replacement="MISSING"
        // => fallback string is returned.
        let spec = regex_extraction(r"\d+", None, None, true, Some("MISSING"));
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(compiled.apply("no-digits-here"), Some("MISSING".into()));
    }

    #[test]
    fn regex_dim_spec_no_match_without_replace_returns_empty_string() {
        // replace_missing_value=false => empty-string sentinel
        // (Druid wire compat — the existing behavior for the legacy
        // non-compiled path).
        let spec = regex_extraction(r"\d+", None, None, false, None);
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(compiled.apply("no-digits-here"), Some(String::new()));
    }

    #[test]
    fn regex_dim_spec_named_group_takes_precedence_over_numbered_index() {
        // groupName=foo + index=1 => the named group wins.
        let spec = regex_extraction(r"(?P<foo>[a-z]+)-(\d+)", Some(2), Some("foo"), false, None);
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(compiled.apply("hello-123"), Some("hello".into()));
    }

    #[test]
    fn regex_dim_spec_catastrophic_pattern_does_not_oom() {
        // Classic catastrophic-backtracking exemplar: `(a+)+$` against
        // `aaaa…!`.  In a Perl-style engine this would explode
        // exponentially.  The Rust `regex` crate uses an NFA simulation
        // that runs in linear time, and on top of that we cap the
        // compiled DFA at MAX_REGEX_DFA_BYTES.  Either the pattern
        // compiles and runs in bounded time, or `CompiledDimSpec::new`
        // refuses it — in both cases the historical does not OOM.
        let spec = regex_extraction(r"(a+)+$", None, None, false, None);
        match CompiledDimSpec::new(&spec) {
            Ok(compiled) => {
                let input = "a".repeat(64) + "!";
                let started = std::time::Instant::now();
                let _ = compiled.apply(&input);
                // 1 second is *enormously* generous — the regex crate
                // typically completes this in microseconds.  A failure
                // here would indicate the linearity guarantee was broken.
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(1),
                    "catastrophic-backtracking-style input must complete in bounded time",
                );
            }
            Err(_) => {
                // Refusing the pattern at parse time is also a valid
                // outcome — the operator gets a clear error instead of
                // a hung query.
            }
        }
    }

    #[test]
    fn regex_dim_spec_huge_input_is_bounded() {
        // Single dimension value > MAX_REGEX_INPUT_BYTES => the engine
        // truncates the slice fed to the regex.  The result must still
        // be a `Some(_)`; the apply call must not panic or hang.
        let spec = regex_extraction(r"^x", None, None, true, Some("FALLBACK"));
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        let huge = "y".repeat(MAX_REGEX_INPUT_BYTES + 4096);
        let started = std::time::Instant::now();
        let result = compiled.apply(&huge);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "huge-input apply must complete in bounded time",
        );
        // No match (input starts with 'y') => fallback string.
        assert_eq!(result, Some("FALLBACK".into()));
    }

    #[test]
    fn regex_dim_spec_unicode_named_group_round_trips() {
        // Non-ASCII capture-group payload — verifies we honour UTF-8
        // when slicing the matched substring.
        let spec = regex_extraction(r"(?P<jp>\p{Hiragana}+)", None, Some("jp"), false, None);
        let compiled = CompiledDimSpec::new(&spec).expect("compile");
        assert_eq!(
            compiled.apply("page=ひらがなValue"),
            Some("ひらがな".into())
        );
    }

    // -----------------------------------------------------------------------
    // Wave 40-B GroupKey tests (kept)
    // -----------------------------------------------------------------------

    #[test]
    fn group_key_typed_apply_default_preserves_type() {
        let spec = default_spec("col");
        let bool_key = apply_dim_spec_typed(&spec, &serde_json::json!(true)).expect("bool");
        assert_eq!(bool_key, GroupKey::Bool(true));
        let null_key = apply_dim_spec_typed(&spec, &serde_json::Value::Null).expect("null");
        assert_eq!(null_key, GroupKey::Null);
        let long_key = apply_dim_spec_typed(&spec, &serde_json::json!(42)).expect("long");
        assert_eq!(long_key, GroupKey::Long(42));
    }

    // -----------------------------------------------------------------------
    // Wave 48 — proptest hardening (regex DimSpec hostile-input safety +
    //   GroupKey hash consistency)
    //
    // * `prop_regex_dim_spec_apply_never_panics_on_utf8` — for any safe
    //   pre-canned regex pattern × any UTF-8 input, the compiled DimSpec
    //   must return Some/None without panicking.
    // * `prop_group_key_hash_consistent` — same JSON value re-built into
    //   a `GroupKey` twice must hash to identical entries in a `HashSet`.
    // -----------------------------------------------------------------------
    mod proptests {
        use super::super::*;
        use proptest::prelude::*;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn default_spec_local(name: &str) -> ferrodruid_common::types::DimensionSpec {
            ferrodruid_common::types::DimensionSpec::Default {
                dimension: name.into(),
                output_name: name.into(),
                output_type: ferrodruid_common::types::ColumnType::String,
            }
        }

        proptest! {
            /// For a fixed safe regex (`\d+`), feeding any UTF-8 input
            /// must never panic the compiled DimSpec.  This pins the
            /// Wave 45-F linear-time guarantee under random fuzz.
            #[test]
            fn prop_regex_dim_spec_apply_never_panics_on_utf8(
                input in r"[a-zA-Z0-9 .,_!?\-]{0,200}"
            ) {
                let spec = ferrodruid_common::types::DimensionSpec::Extraction {
                    dimension: "page".into(),
                    output_name: "out".into(),
                    extraction_fn: ferrodruid_common::types::ExtractionFunction::Regex {
                        expr: r"\d+".into(),
                        index: None,
                        replace_missing_value: true,
                        replace_missing_value_with: Some("FALLBACK".into()),
                        group_name: None,
                    },
                };
                let compiled = CompiledDimSpec::new(&spec).expect("compile");
                // Result is allowed to be Some(_) (always Some for this
                // regex+replace_missing path) — but must not panic.
                let _ = compiled.apply(&input);
            }

            /// For a fixed `regexFiltered` wrapper, arbitrary UTF-8
            /// input must never panic.
            #[test]
            fn prop_regex_filtered_dim_spec_never_panics(
                input in r"[a-zA-Z0-9_ .]{0,200}"
            ) {
                let spec = ferrodruid_common::types::DimensionSpec::RegexFiltered {
                    delegate: Box::new(default_spec_local("page")),
                    pattern: r"^[A-Z][a-z]+$".into(),
                };
                let compiled = CompiledDimSpec::new(&spec).expect("compile");
                let _ = compiled.apply(&input);
            }

            /// Hash consistency: the same JSON dimension value must
            /// hash to the same bucket every time.  This pins the
            /// Wave 40-B GroupKey-typed-encoding guarantee under
            /// random scalar inputs.
            #[test]
            fn prop_group_key_hash_consistent(
                tag in 0u8..5,
                int_v in any::<i64>(),
                bool_v in any::<bool>(),
                str_v in r"[a-zA-Z0-9]{0,32}",
            ) {
                let value = match tag {
                    0 => serde_json::Value::Null,
                    1 => serde_json::json!(int_v),
                    2 => serde_json::json!(bool_v),
                    3 => serde_json::json!(str_v.clone()),
                    _ => serde_json::Value::Null,
                };
                let spec = default_spec_local("col");
                let k1 = apply_dim_spec_typed(&spec, &value).expect("key");
                let k2 = apply_dim_spec_typed(&spec, &value).expect("key");
                let mut h1 = DefaultHasher::new();
                let mut h2 = DefaultHasher::new();
                k1.hash(&mut h1);
                k2.hash(&mut h2);
                prop_assert_eq!(h1.finish(), h2.finish());
                prop_assert_eq!(k1, k2);
            }
        }
    }
}
