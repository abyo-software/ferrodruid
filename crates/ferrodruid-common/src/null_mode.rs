// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Process-global **legacy null mode** latch (`useDefaultValueForNull`).
//!
//! Apache Druid <= 27 defaulted to *legacy* null handling
//! (`druid.generic.useDefaultValueForNull=true`; the opt-in property is
//! honored through Druid 31 and hard-removed in 32): SQL NULL does not
//! exist as a distinct value — a missing/null STRING is identical to
//! `""`, and a missing/null NUMERIC is identical to `0`/`0.0`.  Data
//! written by such a cluster carries **no null markers at all**, and
//! every query answers over the coerced default values.
//!
//! FerroDruid's W-B compatibility mode reproduces those semantics behind
//! ONE process-global flag, mirroring Druid's own static
//! `NullHandling.replaceWithDefault()` design:
//!
//! * The flag is **latched once at startup** from configuration
//!   ([`crate::config::DruidConfig::use_default_value_for_null`], the
//!   `--use-default-value-for-null` CLI flag, or
//!   `FERRODRUID_USE_DEFAULT_VALUE_FOR_NULL`) via
//!   [`init_legacy_null_mode`]; it can never change mid-flight, so query
//!   paths may cache it freely.
//! * **Default is `false`** — modern SQL-compatible (ANSI) null
//!   semantics, byte-for-byte identical to FerroDruid before this module
//!   existed.  When the latch is never initialised, every read reports
//!   ANSI mode.
//! * There is deliberately **no per-query knob**: Druid itself never had
//!   one (the property was cluster-global), and threading a per-query
//!   value through every null-branch call site would ripple signatures
//!   across the workspace for a migration-compat mode.
//!
//! Read-side legacy-*detection* of segments
//! (`ferrodruid_segment::null_generation`) is a SEPARATE, independent
//! concern: it classifies what a segment's bytes look like and is not
//! consulted by — and does not consult — this semantics flag.
//!
//! The oracle basis for every behavioral branch gated on this flag is
//! recorded in `tests/segment-compat/RESULTS_v150_wb_legacy_null.md`
//! (Druid 27.0.0 legacy vs Druid 31.0.2 ANSI, measured 2026-07-20/21).

use std::sync::OnceLock;

/// The process-global latch.  `None` (never observed) reads as ANSI —
/// and the first read latches that ANSI default permanently (see
/// [`legacy_null_mode`]).
static LEGACY_NULL_MODE: OnceLock<bool> = OnceLock::new();

/// Latch the process-global legacy-null-mode flag.
///
/// The **first observation** wins (`OnceLock` set-once) — whether that
/// observation was an earlier `init` call or a plain
/// [`legacy_null_mode`] read (which freezes the ANSI default).  Later
/// calls are no-ops.  Returns the flag's *effective* value after the
/// call so a startup path can detect a conflicting earlier latch:
///
/// ```
/// let effective = ferrodruid_common::null_mode::init_legacy_null_mode(true);
/// assert!(effective, "first init wins");
/// // A later, conflicting init does not change the latch:
/// assert!(ferrodruid_common::null_mode::init_legacy_null_mode(false));
/// ```
pub fn init_legacy_null_mode(enabled: bool) -> bool {
    *LEGACY_NULL_MODE.get_or_init(|| enabled)
}

/// Startup latch for SERVING binaries: latch the flag and **fail loudly
/// on conflict** instead of silently keeping the earlier value.
///
/// Every role binary (single-binary, broker, historical, coordinator,
/// router, overlord, middleManager) must call this BEFORE binding its
/// listener / serving any query or ingest traffic, so the process-global
/// is frozen before the first query-path read can observe it.
///
/// Returns the latched value on success (`requested`), and logs the
/// Druid-style startup WARN when legacy mode is enabled.
///
/// # Errors
///
/// Returns a human-readable error when the process already latched (or
/// froze via an early [`legacy_null_mode`] read) a DIFFERENT value —
/// the flag is immutable from first observation, so the binary must
/// refuse to start rather than serve half-ANSI/half-legacy answers.
pub fn init_legacy_null_mode_serve(requested: bool) -> Result<bool, String> {
    let effective = init_legacy_null_mode(requested);
    if effective != requested {
        return Err(format!(
            "legacy-null-mode latch conflict: requested \
             use_default_value_for_null={requested} but the process already \
             observed {effective}; the flag is immutable from first \
             observation, so it must be latched at startup before serving"
        ));
    }
    if effective {
        tracing::warn!(
            "useDefaultValueForNull=true (LEGACY null mode): null strings == '' \
             and null numerics == 0, matching Apache Druid <= 27 defaults. This \
             is a migration compatibility mode; modern SQL-compatible (ANSI) \
             null handling is the recommended default."
        );
    }
    Ok(effective)
}

/// Whether the process runs Druid-legacy (`useDefaultValueForNull=true`)
/// null semantics.
///
/// `false` (the default, including when [`init_legacy_null_mode`] was
/// never called) = modern SQL-compatible (ANSI) null handling — the
/// long-standing FerroDruid behavior, unchanged byte-for-byte.
///
/// The flag is **immutable from first observation** (H3): a read on an
/// un-initialised latch FREEZES the ANSI default, so a later init with a
/// different value is a detectable no-op instead of a mid-process flip —
/// no two readers can ever disagree.  Serving binaries must therefore
/// latch via [`init_legacy_null_mode_serve`] at startup, before anything
/// can read.
#[must_use]
#[inline]
pub fn legacy_null_mode() -> bool {
    // get_or_init (not get + default): the FIRST observation latches, so
    // the answer this reader hands out can never be contradicted later.
    *LEGACY_NULL_MODE.get_or_init(|| false)
}

// ---------------------------------------------------------------------------
// W-B shared legacy read-canonicalization (single-binary ⇄ role-split)
// ---------------------------------------------------------------------------

/// Declared kind of a column for W-B legacy read-canonicalization.
///
/// This is the SHARED type key both query paths resolve before calling
/// [`legacy_canonical_cell`]:
///
/// * the single-binary path derives it from the physical
///   `ferrodruid-segment` column (or, for a physically-absent column,
///   from the consuming aggregator / dimension `outputType` — mirroring
///   Druid, where a missing column read through a *numeric* value
///   selector yields the numeric default while every *dimension*-flavored
///   read yields the null string);
/// * the role-split executor (`ferrodruid-rpc::native_query`) derives it
///   from the JSON-Lines segment header's declared column type, with a
///   column absent from the header reading as a null STRING column
///   (Druid's missing-column semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyColumnKind {
    /// 64-bit integer column — legacy null/missing default `0`.
    Long,
    /// Floating-point column — legacy null/missing default `0.0`.
    Double,
    /// String (or unknown / opaque) column — legacy null/missing/`''`
    /// reads as the ONE merged canonical JSON null.
    String,
}

/// The W-B legacy canonical read of one cell — the ONE
/// `(column kind, raw cell)` → canonical-JSON rule both the
/// single-binary read path (`ferrodruid-query::helpers::column_value_at`
/// mirrors it over typed columns; asserted identical by the shared
/// parity tests) and the role-split executor
/// (`ferrodruid-rpc::native_query`) apply, so the two paths can never
/// diverge on a legacy answer:
///
/// * numeric ([`LegacyColumnKind::Long`] / [`LegacyColumnKind::Double`])
///   null or missing cells read as the coerced default `0` / `0.0` — a
///   legacy-written segment stores those literal defaults, so an absent
///   or null cell is indistinguishable from a stored default;
/// * string ([`LegacyColumnKind::String`]) null, missing, or `""` cells
///   read as canonical JSON null (the merged `''`/null value; Druid 27
///   renders BOTH as null in every native surface);
/// * any other present value passes through unchanged (a cell whose JSON
///   type contradicts the declared kind is malformed input and is
///   deliberately NOT coerced here).
///
/// **Legacy mode only**: callers gate on [`legacy_null_mode`] — ANSI
/// reads are pass-through and stay byte-identical on their historical
/// per-site code paths.  The function itself is pure (latch-independent)
/// so the shared-rule tests can exercise it from any process.
#[must_use]
pub fn legacy_canonical_cell(
    kind: LegacyColumnKind,
    cell: Option<&serde_json::Value>,
) -> serde_json::Value {
    match kind {
        LegacyColumnKind::Long => match cell {
            None | Some(serde_json::Value::Null) => {
                serde_json::Value::Number(serde_json::Number::from(0))
            }
            Some(v) => v.clone(),
        },
        LegacyColumnKind::Double => match cell {
            None | Some(serde_json::Value::Null) => serde_json::Number::from_f64(0.0)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Some(v) => v.clone(),
        },
        LegacyColumnKind::String => match cell {
            None | Some(serde_json::Value::Null) => serde_json::Value::Null,
            Some(serde_json::Value::String(s)) if s.is_empty() => serde_json::Value::Null,
            Some(v) => v.clone(),
        },
    }
}
