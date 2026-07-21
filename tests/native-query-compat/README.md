# Native Query Golden Fixtures

Offline golden fixtures for Druid native query JSON shapes. Each
`fixtures/<query_type>/` directory contains:

* `query.json` — a canonical Druid native query body
  (`queryType` / `dataSource` / `intervals` / etc.).
* `expected_response.json` — the canonical Druid response shape for that
  query type. Field names are taken from the documented Druid native
  query response contracts:
  * `timeseries` → `[{ "timestamp", "result": { ... } }]`
  * `topN`       → `[{ "timestamp", "result": [ { dim, metric }, ... ] }]`
  * `groupBy`    → `[{ "version": "v1", "timestamp", "event": { ... } }]`
  * `scan`       → `{ "segmentId", "columns", "events": [ ... ] }`
  * `timeBoundary` → `[{ "timestamp", "result": { "minTime"?, "maxTime"? } }]`

These are **offline golden contracts** — no live Druid is required to
run the round-trip checks. The Rust runner lives at
`crates/ferrodruid-rest/tests/native_query_golden_test.rs` and:

1. Parses each `query.json` into the FerroDruid `DruidQuery` enum and
   asserts the variant matches the directory name.
2. Re-serializes the parsed query back to JSON and parses it again to
   verify a clean round-trip.
3. Parses each `expected_response.json` as `serde_json::Value` to verify
   the fixture itself is well-formed JSON.

The on-the-wire FerroDruid response types currently use a mix of
`#[serde(untagged)]` (top-level `QueryResult`) and per-variant
structures, so the runner deliberately stops at JSON well-formedness
rather than asserting `expected_response.json` deserialises into the
strongly-typed `QueryResult` enum — that would couple the contract to
internal representation details (e.g. `GroupByResult.event` is a
`serde_json::Map`) rather than to the wire shape. See the runner
source for the exact assertion.

Sister harness `tests/druid-compat/` runs a real Druid 30.0.1 in Docker
and diffs SQL responses; this directory is the offline counterpart for
**native** (non-SQL) query JSON shapes.
