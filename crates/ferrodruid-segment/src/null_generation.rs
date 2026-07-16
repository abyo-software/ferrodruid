// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Legacy null-handling **generation detection** for segments read from disk.
//!
//! Apache Druid changed its default null handling in 28.0: before that
//! (`useDefaultValueForNull=true`, the default on <= 27 and an opt-in
//! kept by some 28-31 clusters), SQL NULLs were **coerced at ingest** to `""`
//! (strings) / `0` (numerics), so the written segment carries no null
//! markers at all. A modern (SQL-compatible-null) segment instead records
//! nulls explicitly: nullable string columns carry a leading null dictionary
//! entry plus a null-row bitmap, and nullable numeric columns are stored with
//! an explicit null encoding (in FerroDruid's in-memory model: NaN-null
//! `DOUBLE`, see [`crate::column`]).
//!
//! This module classifies each column of a decoded segment as either
//! **confirmed-modern** (explicit null markers present — a legacy writer
//! never produces them) or **unconfirmed** (no null markers present), and
//! flags the *legacy-null-consistent signature*: values equal to the legacy
//! coercion defaults (`""` / `0`) in an unconfirmed column.
//!
//! # Honest limitations (read before trusting the classification)
//!
//! The detection is a **heuristic** and can only ever say "could not confirm
//! modern null handling" — it can **never prove** a segment is legacy:
//!
//! * A legacy null-coerced string column is **byte-identical** to a modern
//!   column that genuinely contains empty strings and no NULLs. Flagging the
//!   latter is a **false positive** inherent to the signal, not a bug.
//! * A legacy null-coerced `0` is likewise indistinguishable from a genuine
//!   zero. Zeros are extremely common in real data, so the numeric signature
//!   is *weaker evidence* than the string one — expect false positives on
//!   almost any real datasource that has a `0` anywhere.
//! * Conversely, a legacy segment whose data happened to contain **no nulls
//!   at ingest** produces no signature at all (a **false negative**) — but
//!   for such data legacy and modern semantics agree, so no divergence is
//!   being missed.
//! * The classifier sees only the decoded column values; it does not know
//!   which cluster or FerroDruid writer produced the segment. A
//!   FerroDruid-written segment with genuine `""`/`0` values classifies
//!   exactly the same way.
//! * NaN is FerroDruid's in-band numeric null marker, and a **genuine** NaN
//!   arriving through a non-JSON ingest path is indistinguishable from it
//!   (documented in [`crate::column`]). A column containing such a genuine
//!   NaN classifies as confirmed-modern, suppressing any zero signature in
//!   the same column — an additional (rare) false-negative path.
//!
//! `__time` is exempt: it is required and non-nullable in every Druid
//! generation, so an epoch-zero timestamp is not a null sentinel.
//!
//! See `docs/design/compatibility-modes.md` §2 row 1 — this module is the
//! S-sized *defensive detection* subset; the actual legacy-null
//! compatibility MODE (answering with legacy semantics) is a separate,
//! program-sized effort and is intentionally NOT implemented here.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use ferrodruid_common::error::{DruidError, Result};

use crate::column::{ColumnData, is_null_double, is_null_float};
use crate::segment::SegmentData;

// ---------------------------------------------------------------------------
// Classification types
// ---------------------------------------------------------------------------

/// Per-column null-handling classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullHandling {
    /// The column carries explicit modern null markers (null-row bitmap on a
    /// string column, NaN-null encoding on a numeric column). A legacy
    /// (`useDefaultValueForNull=true`) writer never produces these.
    ConfirmedModern,
    /// The column carries no null markers. This is what BOTH a modern
    /// null-free column and a legacy null-coerced column look like — the two
    /// cannot be distinguished at the segment level.
    Unconfirmed,
}

/// Classification of one column, plus whether it exhibits the
/// legacy-null-consistent signature.
#[derive(Debug, Clone)]
pub struct ColumnNullGeneration {
    /// Column name.
    pub column: String,
    /// Null-handling classification.
    pub handling: NullHandling,
    /// `true` when the column is [`NullHandling::Unconfirmed`] AND contains
    /// values equal to the legacy coercion defaults (`""` for strings, `0`
    /// for numerics) — i.e. exactly what a legacy Druid would have written
    /// for NULL input. See the module docs for why this can never prove
    /// legacy provenance.
    pub legacy_signature: bool,
}

/// Per-segment null-generation report (one entry per column, `__time`
/// excluded, sorted by column name for deterministic messages).
#[derive(Debug, Clone)]
pub struct NullGenerationReport {
    /// Per-column classifications.
    pub columns: Vec<ColumnNullGeneration>,
}

impl NullGenerationReport {
    /// Names of the columns that exhibit the legacy-null-consistent
    /// signature (the warn-worthy subset of the unconfirmed columns).
    #[must_use]
    pub fn legacy_consistent_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.legacy_signature)
            .map(|c| c.column.as_str())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Classifier
// ---------------------------------------------------------------------------

/// Classify a single column's null handling from its decoded values.
///
/// See the module docs for the exact signal and its inherent
/// false-positive/false-negative behavior.
#[must_use]
pub fn classify_column(name: &str, column: &ColumnData) -> ColumnNullGeneration {
    let (handling, legacy_signature) = match column {
        ColumnData::String(s) => {
            if s.null_rows().is_some() {
                // A trailing null-row bitmap is written only by a modern
                // (SQL-null) writer — upstream Druid's leading null
                // dictionary entry decodes to exactly this layout.
                (NullHandling::ConfirmedModern, false)
            } else {
                // No null markers. The legacy signature is a `""` value that a
                // row actually references: the sorted dictionary puts `""` at
                // ordinal 0 (an O(1) probe), but an unused dictionary entry is
                // legal, so we also confirm at least one row encodes ordinal 0
                // — otherwise a `["", "x"]` dictionary whose rows are all `"x"`
                // would be flagged despite containing no empty string.
                let empty_present =
                    s.dictionary.get(0) == Some("") && s.encoded_values.contains(&0);
                (NullHandling::Unconfirmed, empty_present)
            }
        }
        // A LONG column has no possible in-band null encoding (null-bearing
        // long-typed columns are stored as NaN-null DOUBLE, see
        // `crate::column`), so it can never be confirmed modern; a `0` is
        // exactly what legacy coercion writes for NULL.
        ColumnData::Long(v) => (NullHandling::Unconfirmed, v.contains(&0)),
        ColumnData::Double(v) => {
            if v.iter().copied().any(is_null_double) {
                (NullHandling::ConfirmedModern, false)
            } else {
                (NullHandling::Unconfirmed, v.contains(&0.0))
            }
        }
        ColumnData::Float(v) => {
            if v.iter().copied().any(is_null_float) {
                (NullHandling::ConfirmedModern, false)
            } else {
                (NullHandling::Unconfirmed, v.contains(&0.0))
            }
        }
        // Complex (sketch) columns carry no null convention this detector
        // understands; stay honest and report them unconfirmed, without a
        // signature to warn about.
        ColumnData::Complex(_) => (NullHandling::Unconfirmed, false),
    };
    ColumnNullGeneration {
        column: name.to_string(),
        handling,
        legacy_signature,
    }
}

/// Classify every column of a decoded segment.
///
/// `__time` is skipped: it is required and non-nullable in every Druid
/// generation, so its zeros (epoch 1970) are never null sentinels.
#[must_use]
pub fn classify_segment(segment: &SegmentData) -> NullGenerationReport {
    let mut names: Vec<&String> = segment.columns.keys().filter(|n| *n != "__time").collect();
    names.sort();
    let columns = names
        .into_iter()
        .map(|name| classify_column(name, &segment.columns[name]))
        .collect();
    NullGenerationReport { columns }
}

// ---------------------------------------------------------------------------
// Warn-once / strict-fail enforcement
// ---------------------------------------------------------------------------

/// Hard cap on the number of datasource fingerprints retained by the
/// warning dedupe. Real deployments have tens-to-hundreds of datasources;
/// the cap only exists so that pathological datasource churn cannot grow the
/// process-global set without bound. Beyond the cap the warning is
/// suppressed for NEW datasources (bounded memory wins over a best-effort
/// advisory); already-registered datasources stay deduped.
const MAX_WARNED_DATASOURCES: usize = 65_536;

/// Process-global set of datasource **fingerprints** for which the
/// legacy-null warning has already been emitted (the warning is once per
/// **datasource**, not per segment or per row). Fixed-size 64-bit hashes are
/// stored instead of owned names so arbitrarily long (potentially
/// caller-controlled) datasource names cannot retain unbounded memory; a
/// 64-bit collision merely suppresses one advisory warning.
fn warned_datasources() -> &'static Mutex<HashSet<u64>> {
    static WARNED: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    WARNED.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Fixed-size dedupe fingerprint of a datasource name.
fn datasource_fingerprint(datasource: &str) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    datasource.hash(&mut hasher);
    hasher.finish()
}

/// Dedupe core over an explicit set (unit-testable without the process
/// global): returns `true` exactly once per datasource — the call that
/// should emit the warning — and `false` for repeats or once the set is at
/// [`MAX_WARNED_DATASOURCES`].
fn mark_warned_in(set: &mut HashSet<u64>, datasource: &str) -> bool {
    let fingerprint = datasource_fingerprint(datasource);
    if set.contains(&fingerprint) {
        return false;
    }
    if set.len() >= MAX_WARNED_DATASOURCES {
        // Fail OPEN once the dedupe set is full: keep warning (noisy) rather
        // than silently suppressing every future datasource. Suppressing would
        // let datasource-name churn silence the advisory process-wide, which is
        // worse than a repeated warning for an advisory-only signal. We do not
        // insert, so the set stays bounded. (Fingerprint collisions can still
        // suppress one warning for a distinct datasource — acceptable for an
        // advisory; the strict-mode fail-loud path does not depend on dedupe.)
        return true;
    }
    set.insert(fingerprint)
}

/// Record that `datasource` has been warned about (process-global dedupe).
fn mark_warned(datasource: &str) -> bool {
    let mut set = match warned_datasources().lock() {
        Ok(guard) => guard,
        // A poisoned lock only means another thread panicked while holding
        // it; the set itself is still usable and losing a dedupe entry is
        // harmless (worst case: one extra warning).
        Err(poisoned) => poisoned.into_inner(),
    };
    mark_warned_in(&mut set, datasource)
}

/// Check a decoded segment's null-generation signature at load time.
///
/// * No legacy-consistent columns: returns `Ok(())` silently.
/// * Legacy-consistent columns present, `strict == false` (the default,
///   `strict_null_generation` in `DruidConfig`): emits a LOUD
///   `tracing::warn!` **once per datasource** and returns `Ok(())`.
/// * Legacy-consistent columns present, `strict == true`: returns a
///   fail-loud [`DruidError::Segment`] so an operator can gate
///   ingestion/serving of unconfirmed-null segments.
///
/// The warning wording is deliberately honest: the signature cannot prove
/// the segment is legacy (see the module docs), only that modern null
/// handling could not be confirmed.
pub fn check_null_generation(datasource: &str, segment: &SegmentData, strict: bool) -> Result<()> {
    let report = classify_segment(segment);
    let legacy = report.legacy_consistent_columns();
    if legacy.is_empty() {
        return Ok(());
    }
    let columns = legacy.join(", ");
    if strict {
        return Err(DruidError::Segment(format!(
            "strict_null_generation: datasource `{datasource}`: column(s) [{columns}] contain \
             legacy-null-consistent values (`\"\"` strings / `0` numerics) but carry no null \
             markers (no null dictionary entry, no null-row bitmap, no null encoding) — cannot \
             confirm this data was written with modern (SQL-compatible) null handling. If it was \
             written by a legacy Druid (useDefaultValueForNull=true, the default on <= 27), \
             NULL-sensitive results (COUNT(col), AVG/SUM over nullable columns, IS NULL, boolean \
             predicates) will DIFFER from the source cluster. NOTE: this heuristic cannot \
             distinguish legacy-coerced nulls from genuine empty-string/zero data; set \
             strict_null_generation = false to load such segments with a warning instead"
        )));
    }
    if mark_warned(datasource) {
        tracing::warn!(
            datasource,
            columns = %columns,
            "NULL GENERATION UNCONFIRMED: datasource `{datasource}`: column(s) [{columns}] \
             contain legacy-null-consistent values (`\"\"` strings / `0` numerics) and carry no \
             null markers. If this data was written by a legacy (useDefaultValueForNull=true) \
             Druid, NULL-sensitive results (COUNT(col), AVG, SUM over nullable columns, IS NULL, \
             boolean predicates) will DIFFER from the source cluster — FerroDruid answers with \
             modern SQL-null semantics. This heuristic cannot prove legacy provenance (genuine \
             empty-string/zero data looks identical); set strict_null_generation = true to \
             refuse unconfirmed segments instead. This warning is emitted once per datasource."
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentDataBuilder;

    fn classification<'a>(
        report: &'a NullGenerationReport,
        column: &str,
    ) -> &'a ColumnNullGeneration {
        report
            .columns
            .iter()
            .find(|c| c.column == column)
            .unwrap_or_else(|| panic!("column {column} missing from report"))
    }

    /// The legacy signature: a string column containing `""` with neither a
    /// null dictionary entry nor a null-row bitmap must classify as
    /// Unconfirmed WITH the legacy signature.
    #[test]
    fn legacy_signature_string_column_flagged() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2, 3])
            .add_string_column(
                "city",
                vec![String::new(), "osaka".to_string(), "tokyo".to_string()],
            )
            .build()
            .expect("build");
        let report = classify_segment(&segment);
        let city = classification(&report, "city");
        assert_eq!(city.handling, NullHandling::Unconfirmed);
        assert!(city.legacy_signature, "\"\" without null markers must flag");
        assert_eq!(report.legacy_consistent_columns(), vec!["city"]);
    }

    /// A modern null-bearing string column (trailing null-row bitmap) must
    /// classify as ConfirmedModern — even though its dictionary also
    /// contains `""` (both a genuine `""` row and the NULL placeholder).
    #[test]
    fn modern_null_marked_string_is_confirmed() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2, 3])
            .add_string_column_nullable(
                "city",
                vec![None, Some("osaka".to_string()), Some(String::new())],
            )
            .build()
            .expect("build");
        let report = classify_segment(&segment);
        let city = classification(&report, "city");
        assert_eq!(city.handling, NullHandling::ConfirmedModern);
        assert!(!city.legacy_signature);
        assert!(report.legacy_consistent_columns().is_empty());
    }

    /// A string column with neither `""` nor null markers is Unconfirmed
    /// (a legacy writer whose input had no nulls looks the same) but does
    /// NOT carry the legacy signature — nothing to warn about.
    #[test]
    fn plain_string_column_unconfirmed_but_unflagged() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2])
            .add_string_column("city", vec!["osaka".to_string(), "tokyo".to_string()])
            .build()
            .expect("build");
        let report = classify_segment(&segment);
        let city = classification(&report, "city");
        assert_eq!(city.handling, NullHandling::Unconfirmed);
        assert!(!city.legacy_signature);
        assert!(report.legacy_consistent_columns().is_empty());
    }

    /// Numeric analogue: a LONG column containing `0` has no possible null
    /// encoding (nullable longs are stored as NaN-null DOUBLE), so it is
    /// Unconfirmed with the legacy signature. A zero-free numeric column is
    /// Unconfirmed without it.
    #[test]
    fn numeric_zero_without_null_encoding_flagged() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2])
            .add_long_column("added", true, vec![0, 5])
            .add_double_column("delta", true, vec![1.5, 2.5])
            .build()
            .expect("build");
        let report = classify_segment(&segment);
        let added = classification(&report, "added");
        assert_eq!(added.handling, NullHandling::Unconfirmed);
        assert!(added.legacy_signature, "0 without null encoding must flag");
        let delta = classification(&report, "delta");
        assert_eq!(delta.handling, NullHandling::Unconfirmed);
        assert!(!delta.legacy_signature);
        assert_eq!(report.legacy_consistent_columns(), vec!["added"]);
    }

    /// A NaN-null numeric column (modern nullable encoding) must classify
    /// as ConfirmedModern even when it also contains zeros.
    #[test]
    fn nan_null_numeric_is_confirmed_modern() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2, 3])
            .add_double_column_nullable("added", true, vec![None, Some(0.0), Some(2.0)])
            .build()
            .expect("build");
        let report = classify_segment(&segment);
        let added = classification(&report, "added");
        assert_eq!(added.handling, NullHandling::ConfirmedModern);
        assert!(!added.legacy_signature);
    }

    /// Float columns follow the same NaN-null rule (direct classifier call:
    /// the builder has no float helper).
    #[test]
    fn float_column_classification() {
        let flagged = classify_column("f", &ColumnData::Float(vec![0.0, 1.0]));
        assert_eq!(flagged.handling, NullHandling::Unconfirmed);
        assert!(flagged.legacy_signature);
        let confirmed = classify_column("f", &ColumnData::Float(vec![f32::NAN, 0.0]));
        assert_eq!(confirmed.handling, NullHandling::ConfirmedModern);
        assert!(!confirmed.legacy_signature);
    }

    /// `__time` is required + non-nullable in every Druid generation: an
    /// epoch-zero timestamp must never be treated as a null sentinel.
    #[test]
    fn time_column_is_exempt() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![0, 1])
            .add_string_column("city", vec!["osaka".to_string(), "tokyo".to_string()])
            .build()
            .expect("build");
        let report = classify_segment(&segment);
        assert!(
            report.columns.iter().all(|c| c.column != "__time"),
            "__time must not appear in the report"
        );
        assert!(report.legacy_consistent_columns().is_empty());
    }

    /// Strict mode: the warning becomes a fail-loud `DruidError` naming the
    /// datasource, the columns, and the honest "cannot confirm" framing.
    #[test]
    fn strict_mode_fails_loud_with_honest_message() {
        let segment = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1])
            .add_string_column("city", vec![String::new()])
            .build()
            .expect("build");
        let err = check_null_generation("wiki_strict_test", &segment, true)
            .expect_err("strict mode must fail on the legacy signature");
        let msg = err.to_string();
        assert!(msg.contains("wiki_strict_test"), "names datasource: {msg}");
        assert!(msg.contains("city"), "names the column: {msg}");
        assert!(
            msg.contains("cannot confirm") || msg.contains("could not confirm"),
            "honest unconfirmed framing, never 'is legacy': {msg}"
        );
        assert!(
            msg.contains("strict_null_generation"),
            "names the config knob so the operator can act: {msg}"
        );
    }

    /// Non-strict mode: legacy-signature segments load fine (warn only), and
    /// segments without the signature pass strict mode too.
    #[test]
    fn non_strict_loads_and_clean_segments_pass_strict() {
        let legacy_ish = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1])
            .add_string_column("city", vec![String::new()])
            .build()
            .expect("build");
        check_null_generation("wiki_warn_test", &legacy_ish, false)
            .expect("non-strict must load with a warning only");

        let modern = SegmentDataBuilder::new()
            .add_timestamp_column(vec![1, 2])
            .add_string_column_nullable("city", vec![None, Some("osaka".to_string())])
            .build()
            .expect("build");
        check_null_generation("wiki_modern_test", &modern, true)
            .expect("confirmed-modern segments must pass strict mode");
    }

    /// The warning dedupe is once per datasource (pure core over a local
    /// set so the assertion cannot be perturbed by other tests touching the
    /// process-global set), and the process-global wrapper agrees.
    #[test]
    fn warn_dedupe_is_once_per_datasource() {
        let mut set = std::collections::HashSet::new();
        assert!(mark_warned_in(&mut set, "alpha"));
        assert!(!mark_warned_in(&mut set, "alpha"));
        assert!(mark_warned_in(&mut set, "beta"));

        assert!(mark_warned("dedupe_test_ds_global"));
        assert!(!mark_warned("dedupe_test_ds_global"));
    }

    /// The dedupe set is bounded AND fails open: at the cap, memory stops
    /// growing, a NEW datasource still WARNS (returns true) rather than being
    /// silently suppressed forever, and known datasources stay deduped.
    #[test]
    fn warn_dedupe_set_is_bounded_and_fails_open() {
        let mut set: std::collections::HashSet<u64> = (0..MAX_WARNED_DATASOURCES)
            .map(|i| datasource_fingerprint(&format!("ds{i}")))
            .collect();
        assert_eq!(set.len(), MAX_WARNED_DATASOURCES, "no test-name collision");
        assert!(
            !set.contains(&datasource_fingerprint("one_past_the_cap")),
            "probe must not collide with the prefill, or the cap branch is untested"
        );
        // Fail open: a new datasource past the cap still warns...
        assert!(mark_warned_in(&mut set, "one_past_the_cap"));
        // ...but the set does not grow (memory stays bounded).
        assert_eq!(set.len(), MAX_WARNED_DATASOURCES, "set must not grow");
        // A known datasource is still deduped (found in the set → no warn).
        assert!(!mark_warned_in(&mut set, "ds0"), "known ds stays deduped");
    }
}
