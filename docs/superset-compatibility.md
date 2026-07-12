<!-- SPDX-License-Identifier: BUSL-1.1 -->
# Apache Superset ⇄ FerroDruid compatibility (pydruid layer)

**Status:** S-1 (pydruid smoke) + S-2 (query diff surface) + **S-3 (Superset UI
end-to-end)** all complete.
**Last verified:** 2026-07-11, FerroDruid `v1.0.0` single-binary. S-1/S-2 were
verified against a 10-row `wikipedia_compat` dataset; **S-3 drove the real
Apache Superset 4.1.4 UI** against FerroDruid serving a `telemetry` dataset
(1,152 rows: `site_id` / `device_id` / `metric_name` / `value` / `status`),
screenshot-verified in the live UI (2026-07-12 re-shoot; screenshots
retained in the vendor evidence pack).
Reproduce the pydruid smoke with `tests/superset-compat/pydruid_smoke.py` (see
bottom); the FerroDruid↔Druid deep-diff over the Superset shapes is Section 6 of
`crates/ferrodruid-rest/tests/druid_diff_test.rs`.

## S-3 — Superset UI end-to-end (verified in the real UI, screenshots saved)

Driven with Playwright against Apache Superset 4.1.4 (docker) + FerroDruid
single-binary. Every step of the deal-demo flow works:

| Step | Result | Evidence |
|------|--------|----------|
| Add Database → **Test Connection** | ✅ HTTP 200, **natively** (no ORM bypass; the `SELECT 1` fix) | `01-test-connection.png`, `02-database-connected.png` |
| Register dataset → **column auto-sync** | ✅ all columns + types (`__time`→TIMESTAMP, dims→VARCHAR, metrics→FLOAT) | `03-dataset-column-sync.png` |
| **Time-series Line chart** (Hour grain) | ✅ renders with a real time axis | `04-timeseries-line-chart.png` |
| **SQL Lab** ad-hoc query | ✅ `WHERE`+`GROUP BY`+`ORDER BY`, 8 rows | `05-sql-lab.png` |

Four additional FerroDruid fixes were needed to make the dataset-create + chart
path work, all found by driving the live UI:

7. **`has_table` existence probe (S-3)** — Superset's dataset creation runs
   `SELECT COUNT(*) > 0 AS exists_ FROM INFORMATION_SCHEMA.TABLES WHERE …` (an
   aggregate wrapped in a comparison the planner can't project). Evaluated via
   `infoschema::try_existence_check`. *(commit `feat(rest): has_table…`.)*
8. **Schema-qualified table names (S-3)** — Superset emits `FROM "druid"."t"`;
   the default `druid.` datasource-schema prefix is now stripped so it resolves
   to `t`. *(commit `feat(sql): preserve …`.)*
9. **SELECT column order (S-3)** — `serde_json` was sorting result keys
   alphabetically, but Superset/pydruid map columns to the SELECT list
   *positionally*, silently swapping them (a time-series chart plotted the
   timestamp as the metric). Enabled `preserve_order` so rows emit in projection
   order. **This was the root cause of the initial chart mis-render.**
10. **Timestamp wire shape (S-3)** — confirmed the time-grain column stays an
    ISO-8601 string (Druid's `/druid/v2/sql` object-format shape); an interim
    epoch-millis experiment was reverted after review as it diverged from Druid
    (the mis-render was #9, not the format).

Apache Superset reaches Druid through **pydruid**, three ways: the **DBAPI**
(SQL Lab / query execution), the **SQLAlchemy dialect** (`druid://…` — engine
ping + schema introspection for dataset sync), and the legacy **PyDruid native
client** (`/druid/v2` timeseries/topN/groupBy). All three are exercised here.

## Result matrix — pydruid 0.6.9 (the version Superset 6.x bundles)

Python 3.11 (matches Superset container runtimes), SQLAlchemy 2.0.51.

| Surface | Check | Result | Notes |
|---------|-------|:------:|-------|
| DBAPI | `connect` → `SELECT 1` | ✅ | do_ping-style constant SELECT |
| DBAPI | `SELECT COUNT(*)` | ✅ | 10 rows |
| DBAPI | `GROUP BY` (chart shape) | ✅ | 4 language groups |
| SQLAlchemy | connect + `SELECT 1` (ping) | ✅ | **Test Connection works natively** |
| SQLAlchemy | `SELECT` via dialect | ✅ | |
| SQLAlchemy | `get_table_names()` | ✅ | `['wikipedia_compat']` (INFORMATION_SCHEMA.TABLES) |
| SQLAlchemy | `get_columns()` | ✅ | 11 columns w/ JDBC types (INFORMATION_SCHEMA.COLUMNS) |
| Native | `timeseries` | ✅ | `total_added=1375` |
| Native | `topN` | ✅ | en/fr/de/it ranked |
| Native | `groupBy` | ✅ | per-language sums |

**10 / 10 pass.** Every path works: DBAPI query execution, SQLAlchemy ping +
SELECT + **schema introspection** (dataset discovery + column sync), and all
native queries.

## Fixes landed during S-1 (were blocking Superset)

1. **`SELECT 1` (do_ping)** — FerroDruid used to reject any FROM-less SELECT, so
   Superset's connection health check failed and Wave 33 had to insert the
   datasource via an ORM bypass. Now answered natively (single synthetic row).
   *(commit: `feat(sql): support constant FROM-less SELECT`.)*
2. **Whitespace-run cap** — pydruid's `get_columns` query (and Superset-generated
   SQL generally) indents continuation lines to 20+ spaces; the anti-DoS
   `MAX_SQL_TOKEN_WHITESPACE_RUN=16` cap rejected them (`run of 20 consecutive
   whitespace bytes`). Raised to 64 without re-opening the fuzz artifact it
   guarded (now caught by the bracket-opens cap). *(commit: `fix(sql): raise SQL
   whitespace-run cap`.)* This unblocks **all** indented BI SQL, not just
   introspection.
3. **INFORMATION_SCHEMA introspection** — `SCHEMATA`/`TABLES`/`COLUMNS` used to
   return `[]` (treated as empty datasources), so `get_table_names()` /
   `get_columns()` returned nothing. Now materialised on demand from live
   segment metadata and run through the normal planner + executor. **Dataset
   auto-discovery and column auto-sync now work.** *(commit: `feat(rest): serve
   INFORMATION_SCHEMA…`.)*
4. **`TIME_FLOOR` time-series (S-2)** — Superset time charts issue
   `SELECT TIME_FLOOR(__time,'PT..') AS t, <agg> GROUP BY 1`. The result dropped
   the bucket timestamp (no `t` column) and leaked empty buckets. Now the SQL
   formatter surfaces the bucket under the alias and the planner sets
   `skipEmptyBuckets` (SQL GROUP BY has no empty groups). *(commit: `feat(sql):
   Superset time-series…`.)*
5. **`DATE_TRUNC('unit', expr)` (S-2)** — was "Unknown function"; now lowered to
   `TIME_FLOOR` (second…year). Superset uses it for some time grains.
6. **`EXPLAIN PLAN FOR <query>` (S-2)** — Druid/Calcite SQL-Lab syntax the
   underlying sqlparser rejected; now rewritten to standard `EXPLAIN`.

## Remaining gaps

### Gap #1 (CLOSED 2026-07-11) — INFORMATION_SCHEMA introspection

Was: the metadata tables returned `[]`. Now served from live segment metadata
(`crates/ferrodruid-rest/src/infoschema.rs`): `SCHEMATA` → the fixed
`druid`/`INFORMATION_SCHEMA`/`sys` schemas; `TABLES` → one row per datasource;
`COLUMNS` → one row per (datasource, column) with the Druid→JDBC type mapping
(BIGINT −5, DOUBLE 8, FLOAT 6, VARCHAR 12, TIMESTAMP 93 for `__time`). Each
virtual table is built as a throwaway segment the planner + executor run the
SELECT against, so projection / WHERE / aggregates all work. Verified via
pydruid (`get_table_names` / `get_columns` / `get_schema_names`) **and** a REST
integration test (`information_schema_introspection_end_to_end`).

Still open (minor, not required for dataset sync): `sys.segments` / `sys.servers`
are not yet populated; `has_table`'s `SELECT COUNT(*) > 0` shape is best-effort.

### Gap #2 — pydruid 0.5.x not installable on modern Python (pydruid-side, not FerroDruid)

`pydruid==0.5.11` installs a **broken wheel layout** (its `db/`, `client/`,
`utils/` modules land at top level, not under the `pydruid` namespace), so
`import pydruid` fails on modern pip/Python 3.11. This is a pydruid packaging
defect, independent of FerroDruid. The wire surface pydruid 0.5.x would exercise
(native `/druid/v2` + DBAPI) is a subset of what 0.6.9 already covers and
passes, so there is no FerroDruid-side action. Real 2019–2021 Superset
deployments installed 0.5.x with the contemporaneous setuptools that packaged it
correctly; if a customer environment pins it, they run it from a working install.

### Note — native client on Python ≥ 3.12

pydruid 0.6.9's native client calls `urllib.request.urlopen(cafile=…)`, and
`cafile` was removed in Python 3.12+. On Python 3.14 the native tests raise
`TypeError: urlopen() got an unexpected keyword argument 'cafile'`. This is a
pydruid/Python-version issue, not FerroDruid — the native `/druid/v2` endpoint
answers correct results to direct HTTP and to pydruid on Python 3.11. Superset
6.x runs on Python 3.9–3.11, so this does not affect the target environment.

## Outward-usable claims (measured)

- "Superset connects to FerroDruid and runs SQL Lab queries and native
  timeseries/topN/groupBy — verified with pydruid 0.6.9." ✅
- "Superset **Test Connection** succeeds against FerroDruid natively." ✅ (after
  the `SELECT 1` fix)
- "Superset **auto-discovers FerroDruid tables and syncs their columns** — no
  manual dataset definition." ✅ (after the INFORMATION_SCHEMA fix; verified with
  pydruid 0.6.9 `get_table_names` / `get_columns`, 10/10 in the matrix above)
- "Point Superset at FerroDruid and your existing dashboards work: **connect,
  register a dataset, build a time-series chart, and run SQL Lab — verified
  end-to-end in the Superset UI.**" ✅ (S-3, screenshot-verified in the
  live UI)
- "Superset's `ORDER BY <column-alias>` resolves — sorting a chart or SQL-Lab
  result by a SELECT alias of a grouping dimension or an aggregate works." ✅
  (Gap #3, closed at the resolution layer; see its honest limitation.)
- "Superset **AVG metrics work**: `AVG(col)` plans natively (sum/count
  post-aggregation), sorts, and reaches the wire in SELECT-list column order
  with correct DOUBLE typing — **and is NULL-faithful** (non-null denominator,
  all-NULL group → null, byte-matching Druid 35)." ✅ (Gap #4 + the no-compromises
  pass; verified live at the wire level AND in the Superset UI —
  `06-avg-bar-chart.png`.)
- "**COUNT(col), COUNT(DISTINCT col), APPROX_COUNT_DISTINCT, and
  ROUND/ABS/FLOOR/CEIL over aggregates** all plan and match Druid's default
  behavior — verified live against Druid 35/36." ✅
- Not yet claimable: a full saved **dashboard** with multiple chart types (only
  a single time-series chart + an AVG bar chart were driven); exact
  (non-approximate) COUNT DISTINCT; `PT5S`/`PT30S` and week-variant time
  grains (fail closed, Section 8 of the diff harness).

### Gap #3 (CLOSED 2026-07-11 at the resolution layer) — `ORDER BY <SELECT alias>`

Was: `ORDER BY c` where `c` is a SELECT alias (`SELECT city AS c ... ORDER BY c`)
was rejected with "ORDER BY key does not reference a SELECT output column";
`ORDER BY SUM(value)` (the raw expression) worked. Now the GroupBy ORDER-BY
resolver (`resolve_group_order_key` in `crates/ferrodruid-sql/src/planner.rs`)
also matches a bare ORDER BY name against the non-aggregate projection aliases
collected in `plan_select`, mapping the alias to its underlying grouped output
column. Verified by three planner tests (dimension-alias resolves, mixed
aggregate+dimension aliases, and an alias-of-unplanned-expression fails closed).

`ORDER BY <alias-of-a-grouping-dimension>`, `ORDER BY <aggregate-alias>`, and
(since Gap #4 below) `ORDER BY <avg-alias>` / `ORDER BY AVG(col)` all work.
A projection the planner cannot lower (e.g. `ROUND(AVG(x),1)`) now **fails
closed at plan time** with a message naming the projection, instead of being
silently dropped from an HTTP 200 result (see Gap #4).

Verified end-to-end 2026-07-11 against a live single-binary (the same
`/druid/v2/sql` endpoint Superset uses), with per-site totals chosen so the
lexical `site_id` order differs from the total order, so the sort column is
unambiguous:

```
SELECT site_id AS s, SUM(value) AS total FROM telemetry
  WHERE metric_name='power_kw' GROUP BY site_id ORDER BY s ASC
  -> site_a(30), site_b(10), site_c(20)   # lexical by site_id (not by total)
... ORDER BY total DESC
  -> site_a(30), site_c(20), site_b(10)   # by the aggregate
... ORDER BY avg_v  (avg_v = ROUND(AVG(value),1))
  -> fails closed at plan time (Gap #4): "SELECT projection `avg_rev` is
     neither an aggregate expression the planner supports nor a GROUP BY key;
     refusing to plan a query that would silently drop it from the results…"
```

### Gap #4 (CLOSED 2026-07-11) — AVG, arithmetic over aggregates, and silent column drops

Was (both verified live before the fix):

1. `AVG(value)` — Superset's AVG metric — hard-errored ("AVG is not supported…
   express it as SUM(x)/COUNT(x)"), breaking any chart with an AVG metric.
2. Worse, the suggested workaround itself was a **wrong-result** bug:
   `SELECT site_id, SUM(value)/COUNT(*) AS avg_v … GROUP BY site_id` returned
   **HTTP 200 with the `avg_v` column silently missing** — any
   non-plain-aggregate projection was misclassified as a grouping dimension and
   vanished without an error. No working path for an average existed.

Now (commits `31a6d91` planner + `d24e764` REST):

- **`AVG(x)` works in all three native paths** (Timeseries / GroupBy / TopN):
  lowered to a hidden `doubleSum` + `count` pair finalised by an
  `Arithmetic {"fn":"/"}` post-aggregation (the executor already supported
  post-aggregations; the gap was purely SQL lowering). `ORDER BY avg_alias` and
  `ORDER BY AVG(col)` both resolve; the TopN path ranks directly by the
  post-aggregation.
- **`+ - * /` arithmetic over aggregates** (`SUM(a)/COUNT(*)`, `SUM(a)*100`,
  `(SUM(a)+SUM(b))/COUNT(*)`) lowers to post-aggregations the same way.
- **Fail closed, never silent-drop**: a projection that is neither a supported
  aggregate/lowerable expression nor a GROUP BY key is rejected at plan time
  with a message naming it (`ensure_projections_grouped`). The silent-drop
  wrong-result class is gone.
- **Wire rows project to the SELECT list** (REST layer): hidden `$avg_sum_N`
  helpers never reach the wire, and JSON key order equals the SELECT-list
  order — pydruid/Superset map columns *positionally*, so the native map order
  (post-aggregations last) would have swapped metric columns client-side. AVG
  outputs stay DOUBLE on the wire (`10.0`, not `10`), matching Druid.

Verified live 2026-07-11 (release build, `/druid/v2/sql`, per-site values
chosen so every ordering is distinguishable):

```
SELECT site_id, AVG(value) AS avg_v, COUNT(*) AS c FROM telemetry GROUP BY site_id
  -> [{"site_id":"site_a","avg_v":10.0,"c":3}, …]     # SELECT-order keys, no $ leak
SELECT site_id, AVG(value) AS avg_v … ORDER BY avg_v DESC LIMIT 5
  -> site_a(10.0), site_c(6.67), site_b(3.33)          # TopN ranked by the AVG
SELECT site_id AS site_id, AVG(value) AS "AVG(value)" … LIMIT 1000   # Superset's exact shape
  -> correct values under the quoted alias
SELECT site_id, SUM(value)/COUNT(*) AS ratio …         # the old silent-drop repro
  -> ratio present and correct
```

**Update (same day, "no compromises" pass — all of the above limitations
consumed, each verified live against apache/druid 35.0.1/36.0.0 docker):**

- **AVG is now NULL-faithful end-to-end**: the denominator is a filtered
  not-null count and the finalization is an `expression` post-aggregation
  (IEEE 0/0 → SQL null), so `AVG` over a column with NULLs divides by the
  non-null count and an all-NULL group returns `null` — byte-matching Druid
  (`15.0 / 30.0 / null` on the probe dataset). This required null-faithful
  **ingestion** (typed dimensions, NaN null markers, string null-row bitmaps,
  count metrics — previously nulls were destroyed at ingest) and a
  null-faithful **read side** (scan renders string NULL as `null` not `""`,
  vectorized sums skip NULLs, NULL group keys distinct from `""`).
- **`COUNT(<col>)` works** (filtered not-null count: `2 / 1 / 0`);
  **`COUNT(DISTINCT col)` / `APPROX_COUNT_DISTINCT` work** (HLL sketch +
  rounded estimate, BIGINT `3` on the wire — matches Druid's default
  approximate mode; exact mode via `useApproximateCountDistinct=false` is
  implemented (E16) and its resource bounds fail closed with a Druid-shaped
  HTTP 400 rather than silently under-/over-counting — see compat-mode.md
  R-5); **`ROUND/ABS/FLOOR/CEIL` over aggregates
  work** (expression post-aggregations, NULL-propagating).
- **The 200→400 contract change is Druid-verified**: real Druid rejects the
  same ungrouped-bare-column query with HTTP 400 ("Expression 'site_id' is
  not being grouped").
- **The live-Druid diff harness was re-run against all seven versions**
  (30.0.1–36.0.0, local docker), with two new sections: Section 7 (NULL
  semantics, dual-engine ingest) and Section 8 (all 17 Superset
  `DruidEngineSpec` time-grain query shapes extracted from the running
  Superset 4.1.4 container).

Remaining documented divergences (surfaced, not hidden): `SUM` of an
all-NULL group returns `0.0` (Druid: `null`); exact (non-approximate)
COUNT DISTINCT is implemented (E16) with fail-closed resource bounds
(compat-mode.md R-5); `PT5S`/`PT30S` time grains and the nested
week-variant grains fail closed (clear errors, visible in Section 8);
segmentMetadata cardinality counts a shared `""`+NULL dictionary slot once.

## Reproduce

```bash
# 1. Start FerroDruid + ingest the 10-row sample
target/release/ferrodruid serve --no-auth --allow-insecure-public-bind \
  --mode single-binary --port 38899 --data-dir /tmp/fd-superset --bind 127.0.0.1 &
curl -fsS -XPOST localhost:38899/druid/indexer/v1/task \
  -H 'Content-Type: application/json' -d @tests/druid-compat/sample_ingestion_spec.json

# 2. Run the matrix (Python 3.11 recommended; matches Superset runtimes)
python3.11 -m venv /tmp/v && /tmp/v/bin/pip install "pydruid[sqlalchemy]"
/tmp/v/bin/python tests/superset-compat/pydruid_smoke.py --port 38899 --json matrix.json
```
