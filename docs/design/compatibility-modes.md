<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# Compatibility modes — design (NOT implemented)

**Status: DESIGN ONLY** — with one exception: the S-sized *defensive
detection* subset of §2 row 1 is now built (see "Built: legacy-null
detection" below). The modes themselves remain unimplemented.

This document specifies a
`compatibility_mode` configuration surface for absorbing *generation*
differences between Apache Druid eras, catalogs the differences a mode must
cover (from the vendor's legacy-generation assessment, retained in the
vendor evidence pack),
and estimates the effort per difference so implementation can be triggered
when a prospect's Druid generation is confirmed and a given mode becomes a
requirement. It folds in the §4 legacy-null effort estimate.

FerroDruid today implements **one** world: the modern (Druid 28+ default)
semantics — ANSI SQL null handling, strict booleans, approximate-by-default
distinct — verified live against Druid 30–36. A "compatibility mode" is only
needed for a prospect on a **legacy** generation (≤ 27, or a 28–31 cluster
that kept the legacy opt-ins) whose data/queries depend on the old behavior.

## 1. Configuration surface

A single generation key selects a coherent bundle of era semantics:

```
compatibility_mode: druid-0.22     # one of: none (default, = modern 28+),
                                    #   druid-0.22 | druid-0.20 | druid-26 | ...
```

Rules:
- **`none` (default)** = the current modern behavior; no legacy code paths
  active. This is what ships and is verified.
- A generation value (e.g. `druid-0.22`) flips the whole **semantic bundle**
  for that era at once (null/boolean handling, and any other era-specific
  behavior below), so an operator sets ONE key rather than N flags.
- **Individual flags still win when explicitly set.** The bundle sets
  defaults; an explicit query-context / config flag (e.g.
  `useApproximateCountDistinct`, a future `useDefaultValueForNull`) overrides
  the bundle's value for that one knob. Precedence: explicit flag > mode
  bundle > modern default.
- The mode is a **server/datasource-level** setting (it changes ingestion +
  query semantics that must be consistent for a datasource), NOT a per-query
  knob — a per-query null-mode would make a datasource's own rows
  self-inconsistent.

## 2. Generation-difference catalog (what a mode must absorb)

Derived from the A-2 assessment. The **only load-bearing** semantic
difference for realistic legacy prospects is #1 (null/boolean); the rest are
narrower or already handled.

| # | Difference | Modern (28+, built) | Legacy (≤27) a mode must provide |
|---|---|---|---|
| 1 | **NULL + boolean handling** | ANSI: NULL is a distinct value; `COUNT(col)` = non-null; `IS NULL` ≠ `''`; SUM/AVG of all-NULL → NULL; strict boolean 1/0 | `useDefaultValueForNull=true`: NULL coerced to `0`/`''`; `COUNT(col)` counts coerced rows; `IS NULL` matches `''`; SUM/AVG treats NULL as 0; non-strict booleans |
| 2 | Scan-query `legacy` mode | removed in 31; FerroDruid emits modern scan shape | ≤30 `legacy:true` scan result envelope (different column framing) |
| 3 | Error envelope / HTTP codes | `DruidException` era (400/504) | ≤0.20 clients expecting 500-class — cosmetic; migration-note only |
| 4 | MSQ statements API | `POST /druid/v2/sql/task` served | 27/28 `POST /druid/v2/sql/statements` route absent (separate feature, not a "mode") |
| 5 | Nested columns write compat | not written (Phase 2) | n/a for ≤26 (they can't read 28-nested anyway) |

Only #1 (and marginally #2) are things a `compatibility_mode` bundle would
carry. #4 is a missing route (its own feature), #3 is documentation, #5 is
out of era-scope.

### Built: legacy-null detection (defensive subset of #1, 2026-07-12)

`ferrodruid_segment::null_generation` now classifies every decoded column
as **Confirmed-modern** (explicit null markers present: null-row bitmap /
NaN-null encoding — a `useDefaultValueForNull=true` writer never produces
them) or **Unconfirmed**, and flags the legacy-null-CONSISTENT signature
(`""` in a marker-less string dictionary, `0` in a marker-less numeric
column; `__time` exempt). `check_null_generation()` emits a loud warning
**once per datasource**, and the `strict_null_generation` config knob
(default off) turns it into a fail-loud error to gate ingestion/serving.

**Honest limits:** this is a heuristic that can only say "could not confirm
modern null handling", never "this is legacy" — modern data with genuine
empty strings / zeros is byte-identical (false positives, especially for
numeric `0`), and a legacy segment whose input had no nulls shows no
signature at all (a false negative with no semantic divergence). It detects
and warns only; the legacy-null *semantics* are the SEPARATE W-B mode below.

### Built: legacy null MODE (#1's null half, v1.5.0 W-B, 2026-07-21)

The `useDefaultValueForNull` **semantics mode** now exists: a
startup-latched process-global flag (`ferrodruid_common::null_mode`;
TOML `useDefaultValueForNull` / `FERRODRUID_USE_DEFAULT_VALUE_FOR_NULL` /
`serve --use-default-value-for-null`, default off = ANSI) that answers
with Druid <= 27 legacy null semantics at ingest (coerced `""`/`0`, no
null markers written) and at query time (aggregation null-as-0,
`IS NULL`↔`''` filter equivalence, merged group keys, empty-set
aggregate sentinels, SQL-wire `""` rendering for string nulls).
Oracle-verified cell-for-cell against Druid 27.0.0 (legacy) and 31.0.2
(ANSI) — see `tests/segment-compat/RESULTS_v150_wb_legacy_null.md` for
the diff-battery, the fast-path gating, and the honest limitations
(legacy **booleans**, the scan `legacy:true` envelope, and the
`compatibility_mode` bundle plumbing remain unbuilt; the detection
module above stays an independent concern).

**Runtime enforcement:** the single-binary `serve` command accepts
`--strict-null-generation` (or
`FERRODRUID_STRICT_NULL_GENERATION=true`) and passes it to the Historical
segment store. Datasource-aware loads and atomic replacements run
`check_null_generation()` before publishing either segment data or routing
metadata. The legacy two-step load API remains available for compatibility,
and strict mode checks its segment before cache insertion using the segment ID
as the diagnostic label. A clean segment stays default-deny until
`set_segment_datasource`; that call repeats the check with the real datasource
name and rejects unknown segment IDs.

## 3. Effort estimate (closes the §4 gap)

Sizing: **S** ≈ ≤ ~1 day, **M** ≈ ~2–5 days, **L** ≈ ~1–2 weeks, each
including tests + a live diff-harness leg against a legacy Druid container.

| Difference | Effort | Where the work lands | Query-result impact if unbuilt |
|---|---|---|---|
| **#1 legacy null/boolean mode** (`useDefaultValueForNull`-equivalent) | **L** | ingestion (store coerced `0`/`''` instead of NULL), executor (aggregation null-as-0, `IS NULL`↔`''` filter equivalence), SQL planner (boolean strictness), wire formatting — mirrors the 2026-07-11 null-semantics program in reverse, across the SAME four layers, so it is a program-sized effort, not a flag | **High**: on a legacy dataset, `COUNT(col)`, `SUM`/`AVG` over nullable columns, `IS NULL` filters, and boolean predicates all return DIFFERENT numbers than the customer's Druid — silent wrong answers, not errors |
| #1a — just the aggregation null-as-0 subset (no ingest coercion, no boolean) | **M** | executor + planner only; assumes data ingested with the legacy coercion already, or accept ingest-side divergence | Medium: fixes SUM/AVG/COUNT but not `IS NULL`↔`''` or booleans |
| #2 scan `legacy:true` envelope | **S** | scan result serialization (a shape toggle) | Low: only clients pinned to the ≤30 scan shape |
| #4 `/sql/statements` MSQ route | **M** | new REST route + async statement lifecycle (separate from modes) | n/a unless the prospect uses that API |
| The `compatibility_mode` bundle plumbing itself | **S** | config parse + a mode→flags resolver + precedence | — |

**Bottom line for a legacy prospect:** a faithful legacy-null mode is an
**L** (program-sized, four layers) — the biggest single item in any
"extended-life" engagement. A partial **M** (#1a) covers the most common
aggregation divergence but leaves `IS NULL`/boolean gaps. The mode plumbing
(#bundle) is trivial; the semantics are the cost.

## 4. Implementation trigger

Build a mode ONLY when both hold:
1. the prospect's Druid **generation is confirmed** ≤ 27 (or a 28–31 cluster
   that kept `useDefaultValueForNull=true`), AND
2. a data audit shows their queries actually depend on the legacy behavior
   (nullable numeric columns aggregated, `IS NULL` used as `=''`, boolean
   predicates) — i.e. an ANSI-migration acceptance is NOT viable for them.

Absent (2), the cheaper path is **customer-accepted ANSI migration** (Druid
itself publishes a migration guide for exactly the 28.0 default change) —
no FerroDruid code, just a documented data audit. The first trigger is a
prospect confirming their Druid version (and legacy null-handling settings).

## 5. Explicitly out of scope

Per-query null mode (datasource self-consistency); implementing #1 or any
mode speculatively before a confirmed generation + deal condition; the
`/sql/statements` route (tracked separately, not a "mode").
