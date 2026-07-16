# ferro-logcompat — Druid request-log compatibility report

> Naming note: the D-1 work order called this document `compat-check.md`,
> but that name collides with the existing `ferrodruid-compat-check` crate
> (the *live* probe battery that runs against a running FerroDruid). To
> keep the two tools from being conflated — a confusion that has happened
> before — this doc lives at `docs/logcompat.md` and the tool is named
> `ferro-logcompat`.

ferro-logcompat statically classifies the queries in a Druid request log —
it does NOT run anything and needs no data. (For live verification against
a running FerroDruid, use `ferrodruid-compat-check` instead.)

## Who this is for

A prospect running Apache Druid can gauge FerroDruid compatibility
**without installing anything near their cluster**: build (or receive)
the single static `ferro-logcompat` binary, run it on a broker request
log on their own machine, and share only the generated report. The
default report contains no literal values from the queries and the tool
performs no network I/O (see "Privacy" below).

## Quick start

1. Enable Druid's file request logger (broker/router
   `common.runtime.properties`):

   ```properties
   druid.request.logging.type=file
   druid.request.logging.dir=var/druid/request-logs
   ```

2. Let production traffic accumulate (a day of logs is ideal), then run:

   ```bash
   ferro-logcompat var/druid/request-logs/2026-07-11.log --out report.md
   ferro-logcompat var/druid/request-logs/2026-07-11.log --json --out report.json
   ```

3. Send back `report.md` / `report.json`.

Flags: `<logfile>` or `--stdin`; `--json`; `--out <path>`; `--top <N>`
(default 20); `--no-redact` (opt-in verbatim shapes — see Privacy).

## How it classifies (Phase 1: static, data-free)

Each log line is parsed into a SQL string or a native query JSON object
(both layouts of the Druid *file* request logger are handled; *emitter*
format is detected and reported as unsupported input instead of
crashing). Queries are **shape-normalized** — literals stripped to `?`,
identical shapes grouped and counted — and each distinct shape's exemplar
is pushed through FerroDruid's *existing* query front-end, without
executing anything:

* SQL → `ferrodruid_sql::parse_druid_sql` then `ferrodruid_sql::plan_sql`
  against a synthetic schema (the planner resolves unknown columns as
  `VARCHAR`, so no segments are needed). `SELECT 1`-style constant
  selects and `INFORMATION_SCHEMA.{SCHEMATA,TABLES,COLUMNS}` are
  recognized as the broker's direct/virtual-table paths.
* Native → the same `ferrodruid_query::DruidQuery` serde deserializers
  the `/druid/v2` endpoint uses.

Buckets:

* `supported` — parses + plans (plan-through counts as supported in
  Phase 1; results are **not** compared).
* `fail-closed` — recognized but deliberately rejected, with the reason
  (e.g. `FULL OUTER JOIN`, `WITH RECURSIVE`, JavaScript aggregators,
  `sys.*` system tables).
* `unsupported` — parse/plan/deserialize error, with the reason.

Because Druid logs its own *re-serialization* of each query rather than
the client's original bytes, the tool inverts known log-only artifacts
back to wire form before classifying (verified against a real Druid
35.0.1 log): interval spec objects (`LegacySegmentSpec`), uppercase /
object granularities, `restrict` datasource wrappers,
`LegacyDimensionSpec` / `LegacyTopNMetricSpec` objects, `dimensionOrder`
and search-`sort` object forms. Two record classes are excluded from the
percentages (but still listed) because FerroDruid never receives them on
the wire: segment-pinned broker→data-node fan-out sub-queries, and
Calcite-generated natives that duplicate a SQL line (context
`sqlQueryId`).

## Privacy

* Local only: no network I/O of any kind; input file → report file.
* No data: nothing is executed and no segments are read; the tool never
  sees table contents.
* No literals in the default report: filter constants, interval bounds,
  limits, and string/numeric literals are masked to `?` before grouping;
  native masking is default-deny (unknown value-bearing keys are masked);
  error text in `reason` fields passes through the same masking.
  Structural text (table/column/function names) is retained.
* `--no-redact` opt-in includes each shape's first-seen query verbatim
  (its literals included) — still only query text, never data.

## Verified against a real Druid 35 log

`crates/ferrodruid-logcompat/tests/fixtures/druid35-request-log-2026-07-11.log`
is a verbatim Druid 35.0.1 file request log (micro-quickstart,
`wikipedia_compat` fixture, diff-harness battery + Superset-style SQL +
intentionally-incompatible probes). The committed integration test
(`tests/real_druid_log.rs`) asserts the classification of that log
end-to-end; the generated sample reports are at
`tests/druid-compat/RESULTS_logcompat_druid35_2026-07-11.{md,json}`:
208 log lines → 115 client records (56 fan-out + 37 SQL-lowering records
excluded), 51 distinct client shapes → 92.2% of shapes / 91.3% of records
supported, with the FULL OUTER JOIN / WITH RECURSIVE / JavaScript
aggregator probes fail-closed with correct reasons and the Druid-26+
`equals` filter surfaced as a genuine native-wire gap.

## Limitations / next

* Emitter-format request logs are detected but not parsed (file logger
  only for now).
* `supported` = parses + plans; result correctness is not compared.
  **Phase 2 (not built): replay-diff** — re-execute supported shapes
  against a FerroDruid and byte-compare with Druid.
* Native coverage equals FerroDruid's deserializers: findings like the
  `equals` filter gap are honest wire gaps, not tool artifacts.
* All-in-one Druid deployments log the same query at several tiers
  (router + broker); shape grouping absorbs this, but record counts
  reflect log lines, not unique client requests.
