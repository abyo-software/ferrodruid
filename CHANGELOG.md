# Changelog

All notable changes to FerroDruid are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] — 2026-07-16 (durable streaming ingestion)

Kafka streaming ingestion is now **durable end-to-end in `kafka-io`
builds**, closing the FG-6 (non-durable Kafka ingestion) and FG-7 (no
deep-storage persistence) limitations documented since v1.2.0 — see
`docs/known-limitations.md` for the updated entries and the residuals
that remain. (`kafka-io` is an opt-in Cargo feature: the Docker gnu
images, the AWS Marketplace AMI/container, and a
`--features kafka-io` source build carry it; the static-musl
GitHub-release download does not link librdkafka and has no Kafka
ingestion. Deep-storage persistence and the spill residency mode are
in every build.)

### Changed

- **Durable-local persistence is now the default behaviour** (behaviour
  change): `serve` always attaches local-filesystem deep storage, so
  running with `--data-dir` alone persists published segments durably
  across restarts. There is no CLI toggle back to the previous
  non-durable in-memory mode.
- **Measured durability cost** (2026-07-16, AMD Ryzen 9 9950X, 2M rows,
  real Kafka broker, local deep storage on NVMe, 5-trial median):
  ingest throughput is **2.78% below** the previous non-durable
  baseline at realistic segment sizes, and **12.08% below** in an
  aggressive small-segment configuration (100k max rows per segment ⇒
  20 persists). The cost scales with the number of segment persists and
  is the price of the zero-loss restart guarantee; the non-durable code
  path itself shows no regression.

### Added

- **Durable Kafka offset commit + resume** (`kafka-io` build): consumer
  offsets are committed durably and a restart resumes from the
  committed position instead of replaying from `auto.offset.reset`.
  Zero data loss across hard restarts verified end-to-end against a
  real Apache Kafka 3.7.2 broker (restart-matrix scenarios 2a-2f,
  2026-07-16).
- **Kafka TLS + SASL transport** (`kafka-io` build): librdkafka is now
  built with TLS enabled (rdkafka `ssl-vendored`: OpenSSL is vendored,
  self-contained, no runtime libssl dependency), so supervisors can
  connect over SASL_SSL/TLS with SASL PLAIN, SCRAM-SHA-256/512, or
  OAUTHBEARER in addition to PLAINTEXT. GSSAPI/Kerberos is not compiled
  in (documented follow-on; a GSSAPI spec fails loud at consumer
  start). This is a librdkafka build-config change only — no Rust or
  durability logic changed. Honest scope: mechanism support is verified
  compiled-in by a runtime probe; the zero-loss restart E2E above ran
  over PLAINTEXT (no live TLS/SASL-broker E2E yet), and the
  recreation-detection probes remain PLAINTEXT-only by design — on a
  TLS/SASL broker the consumer ingests durably, but recreation
  detection runs in its documented degraded mode (FG-6 residuals).
- **Deep-storage persistence for streaming segments**: published
  segments are uploaded to local deep storage as v9 blobs with fsync
  and a SHA-256 content hash; REPLACE-semantics uploads prune the
  superseded blobs.
- **Bootstrap reload**: on restart, persisted segments are restored
  from deep storage before serving. Fail-loud: a blob that fails its
  SHA-256 check or v9 decode refuses startup instead of silently
  serving partial data.
- **Opt-in segment spill residency (FG-7)**: `--segment-spill` /
  `FERRODRUID_SEGMENT_SPILL` (default OFF) bounds Historical memory
  with a memory-budgeted LRU over spill-to-disk + on-demand reload
  (`--segment-cache-bytes`, default 1 GiB). Prototype measurement
  (2026-07-13, W-6 host): warm RSS at 20M rows 5.82 GB → 137.9 MB.
  The spill cache is a memory-offload tier, NOT the durability tier
  (deep storage is); heap mode is byte-for-byte unchanged.

### Known residual limitations (documented)

- Tampering with the durable store itself (a rewritten well-formed
  metadata-database payload, a swapped blob) is outside the durability
  threat model — that is database-integrity / operator domain. Honest
  bit-rot or corruption IS caught fail-loud by the SHA-256 check and
  decode validation at startup.
- If the Kafka cluster identity cannot be resolved (a pre-2.8 broker
  without a cluster id, or a transient metadata-fetch failure), durable
  rows deliberately omit Kafka offsets; a hard kill that also loses the
  asynchronous offset commit can then re-consume a bounded window on
  resume (bounded double-count, never loss). Does not occur with
  resolvable 2.8+ brokers.
- In a multi-broker cluster, a topic-id disagreement among bootstrap
  brokers in the propagation window right after a topic recreate is
  treated as recreation-suspected: the offset floor re-consumes a
  bounded window rather than skipping data.
- Topic delete→recreate under the same name remains a documented
  residual, now mitigated: persisted segments no longer depend on a
  Kafka replay to survive a supervisor cleanup.

## [1.2.0] — 2026-07-12 (initial public source release)

First public source release of FerroDruid, under the
**Business Source License 1.1**.

### Changed

- **License: BUSL-1.1** (see `LICENSE`). Additional Use Grant permits
  production use except offering FerroDruid to third parties as a
  competing hosted/managed database, analytics, or OLAP service; each
  version converts to **Apache License 2.0** four years after its
  public release. Commercial licensing: aws-support@abyo.net or AWS
  Marketplace.
- **The internal extended segment format is now named FDX.** It
  previously carried a version-number label that could be misread as
  an Apache Druid on-disk format version; Apache Druid's current
  on-disk format is **v9** and Apache Druid defines no version-10
  disk format. Module, identifiers, and docs renamed
  (`ferrodruid-segment::fdx`); on-disk bytes (`version.bin` = 10) are
  unchanged, so existing segments stay readable.
- Public-repo governance added: consolidated
  `docs/known-limitations.md`, CONTRIBUTING with CLA requirement,
  individual/corporate CLA texts, issue/PR templates, public CI
  workflow. Internal evidence-pack paths removed from docs and code
  comments.

### Added

- **Real Apache Druid v9 segment read from real AWS S3** (2026-07-12):
  a segment written by Apache Druid 31 was uploaded to S3 deep
  storage, downloaded, and natively read by FerroDruid with full
  row/column match (gated integration test in
  `crates/ferrodruid-deep-storage`).
- **TPC-H same-box head-to-head updated** (2026-07-11, AMD Ryzen 9
  9950X, SF10, byte-identical results): Q1 **12.0×** / Q3 **16.8×** /
  Q6 **5.5×** faster than Apache Druid 35.0.1 — see
  `docs/compatibility-matrix.md` for the conditions and caveats that
  must travel with these numbers.

## [1.1.1] — 2026-07-12 (AWS Marketplace patch)

Correctness-only patch over 1.1.0. AMI (`prod-25i4xszouqyxm`, arm64) and
Container (`prod-foc7n3tzd5sgk`).

### Fixed

- **Incorrect results for multi-segment post-aggregations.** In 1.1.0, when
  a query's output time-bucket or group drew rows from **2 or more
  segments** (the normal state of any datasource ingested by more than one
  task), post-aggregation outputs — SQL `AVG`, ratio/arithmetic over
  aggregates (e.g. `SUM(a)*100/SUM(b)`), `APPROX_COUNT_DISTINCT`, and native
  `postAggregations` — returned the **first segment's** value instead of the
  value recomputed from the correctly-merged aggregators. Affected the
  timeseries, topN, and groupBy paths. The broker now recomputes
  post-aggregations from the merged aggregator values after a multi-segment
  merge (single-segment results are unchanged). Verified against real Apache
  Druid 36.0.0: all 13 audited post-aggregation cases now match Druid's true
  merged values (multi-segment post-aggregation audit, 2026-07-12;
  evidence retained in the vendor evidence pack).

## [0.2.0 → 1.0.0 development log]

> Historical: post-0.2.0-GA work later released as v1.0.0. At the time
> this section was written the crate versions were still `0.2.0` and
> the Helm chart package tag had advanced to `0.2.2` (ECR tag
> immutability) with `appVersion` `0.2.0`; the measured test count at
> that HEAD was 1928 pass / 0 failed / 32 ignored across 119 test
> binaries.

### Added — post-GA (AWS Marketplace + auth)

- **AWS Marketplace release (2026-06-17).** Single-binary AMI product
  `prod-25i4xszouqyxm`,
  AMI `ami-085ab296edaf9cc61` (arm64 / AL2023, unencrypted boot
  snapshot). Visibility Limited → AWS listing review → Public. Listing
  copy, pricing rationale, and submission checklist under
  `docs/marketplace/`.
- **Force admin password change on first login** (AWS Marketplace review
  requirement). The random per-instance bootstrap password is persisted
  to `<data-dir>/auth/admin.json` (0600) and flagged must-change; every
  route returns 403 until it is rotated via
  `POST /druid-ext/basic-security/authentication/db/basic/users/admin/credential`.
  The cleared flag survives restart. Helm chart NOTES.txt / values.yaml
  updated to describe this model.

### Fixed — Wave 47-B

- `crates/ferrodruid-rest/tests/druid_diff_test.rs` — pass `--no-auth`
  when spawning the FerroDruid subprocess.  Wave 36-A (auth on by
  default) silently broke the Wave-30 diff test path, which would now
  see 401 Unauthorized on every SQL query.  The harness binds to
  loopback so the `--no-auth` guard accepts it without
  `--allow-insecure-public-bind`.

### Added — Wave 47-B (live wire-compat: Druid 32.0.1 + 35.0.1)

- `tests/druid-compat/Dockerfile.druid.template` — parametrized Druid
  image build (DRUID_VERSION arg) re-basing apache/druid on debian-
  bookworm-slim with perl + netcat for `start-micro-quickstart`.
- `tests/druid-compat/docker-compose.druid32.yml` — Druid 32.0.1
  micro-quickstart, host ports 18888..18091.
- `tests/druid-compat/docker-compose.druid35.yml` — Druid 35.0.1
  micro-quickstart, host ports 28888..28091.
- `tests/druid-compat/run_compat_v32.sh`, `run_compat_v35.sh` —
  end-to-end orchestration scripts mirroring `run_compat.sh`.
- `crates/ferrodruid-rest/tests/druid_diff_test.rs` — extracted the
  Wave-30 inner loop into a parametrized `run_diff_harness` helper.
  Three `#[ignore]` test fns now drive v30 / v32 / v35 against
  per-version FerroDruid ports (38888 / 38889 / 38890); each SKIPs
  gracefully if its Druid container is unreachable.
- `tests/druid-compat/RESULTS_wave47b.md` — scaffold notes + run
  instructions.
- `docs/compatibility-matrix.md` — new "Live wire-compat" section
  with per-version status row (30: 5/5 deep, 32/35: scaffold).

## [0.2.0] - 2026-05-06 — Multi-process v1.0 path GA

This release closes Waves W1 → W7 of the v1.0 multi-process plan: the
`classic` 6-role topology now boots on six dedicated binaries, all four
cross-role HTTP wires are real, the broker scatters real timeseries /
scan / groupBy / topN queries across historicals, and a Druid SQL →
native query bridge accepts `POST /druid/v2/sql` end-to-end.

Single-binary `ferrodruid` deployments from 0.1.x continue to work
unchanged.

### Highlights

- **6/6 role binaries wired** (Wave 38.FF) — `ferrodruid-broker`,
  `ferrodruid-historical`, `ferrodruid-coordinator`,
  `ferrodruid-router`, `ferrodruid-overlord`,
  `ferrodruid-middlemanager` all dispatch through a shared
  `ferrodruid-role` runtime.
- **4/4 cross-role HTTP wires** (Waves 39.HH + 40.LL) —
  `router → broker`, `overlord → middlemanager`, `broker → historical`,
  `coordinator → historical`. `MockX` clients ship alongside every
  `HttpX` impl so consumers can unit-test without spinning real
  binaries.
- **Real query execution** (Wave 41.OO) — historicals load JSON-Lines
  segment artifacts from local deep storage and execute real
  `timeseries` / `scan` queries; broker scatters per-segment fragments
  and merges (`merge_timeseries` sum-by-bucket / `merge_scan`
  concatenate-with-cap).
- **groupBy + topN + S3 deep storage** (Wave 42.RR) — `groupBy` (N
  dims, N aggs, optional filter / having / sort / limit, broker
  re-fold across segments) and `topN` (rank-DESC, broker re-rank).
  `--deep-storage-root` accepts `s3://bucket[/prefix]` URIs through
  the existing `S3DeepStorage` impl; AWS_ENDPOINT_URL honoured for
  LocalStack / MinIO.
- **Druid SQL → native query bridge** (Wave 43.TT) — `POST /druid/v2/sql`
  on the broker now parses Druid SQL, plans it, translates to one of
  the four wire-supported native queries, and scatters via the W5/W6
  merge machinery. Four SQL patterns covered (Scan / Timeseries /
  GroupBy / TopN); only scalar-equality filters round-trip (see
  Honest scope below).

### Added (W1 → W7 in chronological order)

#### W1 — 3-role split scaffold (Wave 34.T)

- `ferrodruid-role` runtime crate dispatches a role banner.
- `ferrodruid-broker`, `ferrodruid-historical`,
  `ferrodruid-coordinator` per-role launchers boot, log, and exit.
- `--mode classic --role <broker|historical|coordinator>` enters the
  scaffold from the single binary.

#### W2 — 6/6 roles wired (Wave 38.FF)

- Adds `ferrodruid-router`, `ferrodruid-overlord`,
  `ferrodruid-middlemanager` binaries, all dispatching through
  `ferrodruid-role`.
- `--role` accepts the full `broker|historical|coordinator|router|overlord|middlemanager` set.

#### W3 — first two cross-role HTTP wires (Wave 39.HH)

- `router → broker`: router's `/druid/v2/sql` proxy forwards SQL to
  the configured broker URL and replays the broker's response.
- `overlord → middlemanager`: overlord's task-submit handler dispatches
  to the middlemanager's `/druid/indexer/v1/task` endpoint.
- `MockBrokerClient` and `MockMiddleManagerClient` ship alongside the
  real `Http*` impls.

#### W4 — last two cross-role HTTP wires (Wave 40.LL)

- `broker → historical` scatter: broker accepts a comma-separated
  `--historical-url` list and, on `POST /druid/v2/sql/scatter`, fans a
  per-segment fragment out to every configured historical.
- `coordinator → historical` segment load / drop / status: coordinator
  exposes `POST /druid/coordinator/v1/loadqueue/{historical}`,
  `dropqueue/{historical}`, and aggregated
  `GET /druid/coordinator/v1/loadstatus`.
- New wire types: `SegmentQuery`, `SegmentQueryResponse`,
  `SegmentLoadCommand`, `SegmentDropCommand`, `SegmentLoadState`,
  `LoadStatusReport`.
- `HistoricalClient` trait with `HttpHistoricalClient` +
  `MockHistoricalClient` (records every call, FIFO-replays canned
  responses, supports `set_load_state` for status mutation in tests).

#### W5 — real query execution + segment store (Wave 41.OO)

- `ferrodruid-deep-storage::segment_artifact`: JSON-Lines segment
  artifact format (header line + one row per line), Druid-aligned
  identity, column schema (`long` / `double` / `string` / `json`).
  Sync + async readers, writer, in-memory parser.
- `ferrodruid-rpc::native_query`: Druid-aligned native-query subset
  for Wave 41.OO — `timeseries` (count + longSum + doubleSum, optional
  equality filter, granularity in milliseconds, ascending bucket order)
  and `scan` (optional projection, optional limit, optional equality
  filter). Operates directly against `Segment`; no dependency on the
  heavy `ferrodruid-segment` columnar reader.
- `HistoricalClient::native_scatter` posts a Druid-style native query
  body plus a target `segmentId` to the historical's
  `/druid/v2/native` route.
- `ferrodruid-historical --real-loader`: opt-in flag that swaps the W4
  timer-stub loader for real JSON-Lines I/O off
  `<deep-storage-root>/<dataSource>/<segmentId>/segment.jsonl`.
- Broker `POST /druid/v2/native`: real scatter+merge over an explicit
  `segmentIds` list across configured historicals.

#### W6 — groupBy + topN + S3 deep storage (Wave 42.RR)

- `NativeQuery::GroupBy(GroupBySpec)`: N dims, N aggs (count / longSum
  / doubleSum), optional pre-aggregation equality filter, optional
  `having` predicate (==, >, >=, <, <=), optional sort
  (ascending / descending) on dim or agg, optional limit. Per-segment
  executor folds rows by dim tuple; broker `merge_group_by` re-folds
  across segments + re-sorts + re-applies having + caps so the
  cluster-wide answer is correct rather than per-segment.
- `NativeQuery::TopN(TopNSpec)`: single dim, multiple aggs, ranked
  descending by a single metric, capped to `threshold`. Druid's "high"
  sort. Broker `merge_top_n` re-ranks across segments so a per-segment
  local winner that loses cluster-wide is correctly demoted.
- S3 deep storage: `--deep-storage-root` on `ferrodruid-historical`
  accepts `s3://bucket[/prefix]` URIs in addition to bare paths and
  `file://` URIs. Routes through the existing `S3DeepStorage` impl
  (AWS SDK credential chain). `AWS_ENDPOINT_URL` is honoured for
  LocalStack / MinIO. New `--cache-dir` controls where remote artifacts
  are materialised before parsing (default
  `<TMPDIR>/ferrodruid-historical-cache`).
- Wire shape: groupBy / topN per-segment fragments use the same
  row-vector shape as scan, so the broker can reuse the scan decoder.
- `HistoricalServerState::with_remote(...)` constructor accepts an
  `Arc<dyn DeepStorage>` plus a local cache root.

#### W7 — Druid SQL → native query bridge (Wave 43.TT)

- New `ferrodruid-rpc::sql_bridge` composes `ferrodruid-sql`'s
  parser + planner with a `DruidQuery → NativeQuery` translator. The
  broker's `POST /druid/v2/sql` parses SQL, plans it into one of the
  four wire-supported native queries, scatters it via the W5/W6
  `merge_*` machinery, and returns a Druid-aligned `SqlResponse`.
- Four SQL patterns:
  - `SELECT * FROM ds [WHERE eq] [LIMIT N]` → `Scan`
  - `SELECT TIME_FLOOR(__time, 'PT1H'), SUM(m) FROM ds GROUP BY 1`
    → `Timeseries`
  - `SELECT dim, …, COUNT(*) … FROM ds GROUP BY … HAVING … ORDER BY …
    LIMIT N` → `GroupBy`
  - `SELECT dim, COUNT(*) FROM ds GROUP BY dim ORDER BY <metric> DESC
    LIMIT N` → `TopN`
- Segment selection: a SQL request can carry `context.segmentIds`
  (array of strings) to target specific segments; without it the
  broker synthesises one segment id per configured historical
  (mirroring the W4 scatter behaviour).
- `default_schema_for_sql(&str) -> Option<DataSourceSchema>` extracts
  the FROM table name and builds a permissive schema (empty
  dimension / metric lists, `__time` time column) so the planner can
  drive without an external catalog — matching Druid's "lenient SQL
  with no metadata" behaviour.
- The broker's W3 SQL echo is gone: `POST /druid/v2/sql` either goes
  through the real bridge, returns 400 (unsupported pattern), or 503
  (no historicals).

### Changed

- All workspace member `Cargo.toml` files bumped from `0.1.0` to
  `0.2.0`.
- `ferrodruid-rpc` now depends on `ferrodruid-aggregator`,
  `ferrodruid-common`, `ferrodruid-query`, and `ferrodruid-sql` (via
  path) so it can compose the SQL parser + planner. There is no
  cycle: none of those crates depend on `ferrodruid-rpc`.
- `bins/ferrodruid-broker::broker_app::build_router` mounts
  `/druid/v2/sql` itself (calling `sql_bridge_handler`) instead of
  merging in `broker_server::router`. The W3 `broker_server::router`
  keeps its echo behaviour for unit tests that exercise it directly;
  only the broker bin's mounted endpoint switches over.

### Honest scope (what 0.2.0 still does **not** do)

- **Single-segment-equality filters only end-to-end.** `IN`,
  `BETWEEN`, `LIKE`, `BOUND`, range, and AND/OR combinators surface as
  `TranslateError::UnsupportedFilter` so callers can rewrite or fall
  back to single-binary.
- **Date/time function coverage** in the SQL bridge is `TIME_FLOOR`
  over the period strings already known to `period_to_granularity`.
  `TIME_SHIFT`, `DATE_TRUNC`, and custom origins are deferred.
- **Coordinator-driven `data_source → segment list` resolution** is
  not wired — callers of the broker's native and SQL routes either
  pass `segmentIds` explicitly or rely on the synthesised
  one-per-historical fallback.
- **Tier-aware broker / historical selection** still round-robins.
- **Authentication, mTLS, retry / timeout / circuit-breaker** for the
  cross-role wires are deferred. The four wires are unauthenticated
  HTTP today; production multi-process deployments must either keep
  using `single-binary` per-node + LB or front the cluster with a
  reverse proxy that adds these.
- **`UNION ALL`** is parseable + plannable but currently maps to
  `TranslateError::UnsupportedQueryType("unionAll")` because the wire
  surface does not carry a multi-query envelope. Lifting this is a
  follow-up wave.
- **GCS / Azure deep storage** behind the historical's loader. S3 is
  wired; GCS / Azure are deferred.
- **Window functions, joins, subqueries, and CTEs** are rejected
  upstream by the planner before reaching the bridge.
- The `simplified` 3-role mode binary still rejects with
  `not yet implemented`; only `single-binary` and `classic` (via the
  six per-role binaries) are implemented end-to-end.

### Compatibility

- Single-binary `ferrodruid serve --mode single-binary` is unchanged
  from 0.1.x.
- Live end-to-end wire validation is still against Apache Druid 30.0.1
  only; 31-36 compat remains spec-driven design-target. See
  [docs/known-limitations.md](docs/known-limitations.md) (CL-J2).
- Segment v9/FDX binary format unchanged.

---

## [Pre-0.2.0 development log] — Wave 43.TT (2026-05-04)

> Historical development snapshot. The work below was folded into the
> released **[0.2.0]** section above; this entry is retained for traceability.


### Added

- **v1.0 #7 — Druid SQL → native query bridge at `POST /druid/v2/sql`**
  (W7 of the v1.0 multi-process plan).
  - New `ferrodruid-rpc::sql_bridge` module composes
    `ferrodruid-sql`'s parser + planner with a `DruidQuery →
    NativeQuery` translator so the broker can accept a Druid SQL
    string at `POST /druid/v2/sql`, plan it into one of the four
    wire-supported native query types, scatter it via the existing
    W5/W6 `merge_*` machinery, and return a Druid-aligned
    `SqlResponse`.
  - Four SQL patterns are covered:
    - `SELECT * FROM ds [WHERE eq] [LIMIT N]` → `Scan`
    - `SELECT TIME_FLOOR(__time, 'PT1H'), SUM(m) FROM ds GROUP BY 1`
      → `Timeseries`
    - `SELECT dim, …, COUNT(*) … FROM ds GROUP BY … HAVING …
      ORDER BY … LIMIT N` → `GroupBy`
    - `SELECT dim, COUNT(*) FROM ds GROUP BY dim ORDER BY <metric>
      DESC LIMIT N` → `TopN`
  - The broker's W3 echo is gone — `POST /druid/v2/sql` now goes
    through the real bridge or returns 400 (unsupported pattern) /
    503 (no historicals). Callers wanting in-process execution
    should hit the single-binary `ferrodruid` directly.
  - Segment selection: a SQL request can carry `context.segmentIds`
    (array of strings) to target specific segments; without it the
    broker synthesises one segment id per configured historical
    (mirroring the W4 scatter behaviour).
  - New `default_schema_for_sql(&str) -> Option<DataSourceSchema>`
    helper extracts the FROM table name and builds a permissive
    schema (empty dimension / metric lists, `__time` time column)
    that the planner can drive without an external catalog —
    matching Druid's "lenient SQL with no metadata" behaviour.
- 22 new unit tests in `ferrodruid-rpc::sql_bridge` covering each
  SQL pattern, default-schema extraction, malformed SQL rejection,
  unsupported-filter / unsupported-query-type errors, and
  round-trips through `default_schema_for_sql + translate_sql`.
- 10 new unit tests in `bins/ferrodruid-broker::broker_app`
  exercising `sql_bridge_handler` end-to-end against
  `MockHistoricalClient` for COUNT(*), SELECT *, GROUP BY, ORDER BY
  DESC LIMIT (TopN), TIME_FLOOR (Timeseries with hour granularity),
  explicit `segmentIds` round-robin across two historicals,
  synthesised-segment fallback, and the 503 / 400 error paths.

### Changed

- `ferrodruid-rpc` now depends on `ferrodruid-aggregator`,
  `ferrodruid-common`, `ferrodruid-query`, and `ferrodruid-sql`
  (via path) so it can compose the SQL parser + planner. There is
  no cycle: none of those crates depend on `ferrodruid-rpc`.
- `bins/ferrodruid-broker::broker_app::build_router` now mounts
  `/druid/v2/sql` itself (calling `sql_bridge_handler`) instead of
  merging in `broker_server::router`. The `/druid/v2/info` route is
  reproduced inline via a tiny handler that reads the same
  `BrokerServerState` fields. The W3 `broker_server::router` keeps
  its echo behaviour for unit tests that exercise it directly; only
  the broker bin's mounted endpoint switches over.
- `crates/ferrodruid-rpc/tests/multi_process_smoke.rs` updated to
  reflect the new contract: a broker bin spawned without
  historicals returns 503 from `/druid/v2/sql` instead of an echo.
  The cross-process wire is still asserted: the broker accepts the
  request, parses the JSON, and returns the documented "no
  historicals" body.

### Honest scope (Wave 43.TT)

- Only scalar-equality filters round-trip end-to-end. `IN`,
  `BETWEEN`, `LIKE`, `BOUND`, range, and AND/OR combinators surface
  as `TranslateError::UnsupportedFilter` so the caller can rewrite
  the query or fall back to the single-binary path.
- Date/time function coverage is `TIME_FLOOR` over the period
  strings already known to `period_to_granularity` (`PT1S` / `PT1M`
  / `PT5M` / `PT10M` / `PT15M` / `PT30M` / `PT1H` / `PT6H` / `P1D`
  / `P1W` / `P1M` / `P3M` / `P1Y`). `TIME_SHIFT`, `DATE_TRUNC`, and
  custom origins are deferred.
- Type coercion depth is shallow: the wire translator folds
  `Long{Min,Max,First,Last}` and `Double{Min,Max,First,Last}` and
  `Float{Sum,Min,Max}` to `doubleSum` because the wire surface
  only carries the result-key name. The rich semantics still
  execute on the segment side; the wire only needs the column name
  the broker reads back.
- Window functions, joins, subqueries, and CTEs are rejected
  upstream by the planner before reaching the bridge.
- `UNION ALL` is parseable + plannable but currently maps to
  `TranslateError::UnsupportedQueryType("unionAll")` because the
  wire surface does not carry a multi-query envelope. Lifting this
  is a follow-up wave.

## [Pre-0.2.0 development log] — Wave 42.RR (2026-05-04)

### Added

- **v1.0 #6 — `groupBy` + `topN` query types + S3 deep-storage wire**
  (W6 #1 of the v1.0 multi-process plan).
  - **`groupBy`**: new `NativeQuery::GroupBy(GroupBySpec)` variant
    in `ferrodruid-rpc::native_query`. Accepts N dimensions, N
    aggregations (count / longSum / doubleSum), optional
    pre-aggregation equality filter, optional `having` predicate
    (==, >, >=, <, <=), optional sort (ascending / descending) on
    a dimension or aggregation, optional limit. Per-segment
    executor folds rows by dimension tuple; broker `merge_group_by`
    re-folds across segments + re-sorts + re-applies having + cap so
    the cluster-wide answer is correct rather than per-segment.
  - **`topN`**: new `NativeQuery::TopN(TopNSpec)` variant. Single
    dimension, multiple aggregations, ranked descending by a single
    metric, capped to `threshold`. Druid's "high" sort. Broker
    `merge_top_n` re-ranks across segments so a per-segment local
    winner that loses cluster-wide is correctly demoted.
  - **S3 deep storage**: `ferrodruid-historical`'s
    `--deep-storage-root` flag now accepts `s3://bucket[/prefix]`
    URIs in addition to bare paths and `file://` URIs. The new path
    routes through the existing `S3DeepStorage` impl in
    `ferrodruid-deep-storage`, which uses the standard AWS SDK
    credential chain. `AWS_ENDPOINT_URL` is honoured for
    LocalStack / MinIO. A new `--cache-dir` arg controls where
    remote artifacts are materialised before parsing (defaults to
    `<TMPDIR>/ferrodruid-historical-cache`). The local-FS path is
    unchanged so existing W5 deployments keep working.
  - **Wire shape**: `groupBy` and `topN` per-segment fragments use
    the same row-vector shape as `scan` — one element per row
    wrapping the result-row JSON object — so the broker can reuse
    the scan decoder for the over-the-wire decode step.
  - **Round-robin scatter**: the broker still assigns segments to
    historicals round-robin; tier-aware selection is deferred.
- New `HistoricalServerState::with_remote(...)` constructor accepts
  an `Arc<dyn DeepStorage>` plus a local cache root. The loader
  downloads `<dataSource>/<segmentId>/segment.jsonl` into the cache
  root before parsing, so the same JSON-Lines parser is reused
  across local + remote backends.
- New unit + integration tests covering: groupBy single-dimension
  fold, having post-aggregation, sort + limit, pre-aggregation
  filter, topN ranking + threshold cap, broker merge re-fold across
  segments, broker merge re-rank for topN, in-memory remote loader
  (download → parse → query), remote loader Failed-state handling,
  S3 URI parsing (bare path / `file://` / `s3://bucket/prefix` /
  `s3://bucket` / `s3://` rejection), end-to-end multi-process
  groupBy + topN against the spawned `ferrodruid-historical` binary.
  Total Wave 42.RR new tests: 19 (12 unit + 4 integration + 1
  multi-process + 2 in-bin URI parse).

### Notes

- The W5 honest-scope statements that called out groupBy / topN /
  S3 as deferred are now resolved; `docs/v1.0-roadmap.md` carries
  the new W6 #1 row + the strikethrough on the previously-deferred
  items.
- Search / segmentMetadata / dataSourceMetadata / timeBoundary /
  full SQL / tier-aware broker selection / GCS / Azure all remain
  deferred and are now the W6 #2+ honest scope.

## [Released-context] — Wave 41.OO (2026-05-04)

### Added

- **v1.0 #5 — real query execution + segment store** (W5 #1+#2 of
  the v1.0 multi-process plan). Two of the four W4 stub responses
  are now backed by real implementations:
  - **historical**: real segment loading from local deep storage +
    real timeseries / scan query execution against loaded segments.
    `--real-loader` reads JSON-Lines artifacts from
    `<deep-storage-root>/<dataSource>/<segmentId>/segment.jsonl`
    and reports `Loaded` / `Failed` based on actual I/O outcome.
    `POST /druid/v2/native` dispatches on `queryType` and executes
    real timeseries / scan queries against the loaded segment.
  - **broker**: real scatter+merge on a new `POST /druid/v2/native`
    route. Accepts a Druid-style native-query body plus an explicit
    `segmentIds` list, scatters per-segment fragments round-robin
    across configured historicals, and merges results with
    `merge_timeseries` (sum-by-bucket) / `merge_scan`
    (concatenate-with-cap).
- New module `ferrodruid-deep-storage::segment_artifact` ships a
  Wave 41.OO segment artifact format: JSON Lines (header line +
  one row per line), hand-writable and hand-diffable. Carries
  Druid-aligned identity (`segmentId`, `dataSource`) plus a column
  schema (`long` / `double` / `string` / `json`). Ships
  sync + async readers, a writer, and an in-memory parser for
  test fixtures.
- New module `ferrodruid-rpc::native_query` ships a Druid-aligned
  native-query subset focused on what Wave 41.OO needs:
  `timeseries` (count + longSum + doubleSum, with optional equality
  filter, granularity in milliseconds, ascending bucket order) and
  `scan` (optional projection, optional limit, optional equality
  filter). Both operate directly against the new `Segment`
  artifact — no dependency on the heavy `ferrodruid-segment`
  columnar reader.
- New extension on the `HistoricalClient` trait: `native_scatter`
  posts a Druid-style native query body plus a target `segmentId`
  to the historical's `/druid/v2/native` route. Implemented on
  both `HttpHistoricalClient` (real reqwest impl) and
  `MockHistoricalClient` (records every call, replays canned
  responses).
- `ferrodruid-historical` binary gains `--real-loader` and
  `--deep-storage-root` flags. Without `--real-loader` the W4 stub
  loader semantics apply unchanged so all pre-existing wire tests
  keep passing; with it, the binary opts into Wave 41.OO real
  artifact I/O.
- 31+ new tests across `ferrodruid-deep-storage`,
  `ferrodruid-rpc`, and `ferrodruid-broker-bin`: 9 segment-artifact
  unit tests, 10 native-query unit tests (including round-trip
  serde, every aggregation type, every filter type, every merge
  shape), 6 historical_server unit tests covering real loader +
  real native query dispatch + load-failure path, 4 broker unit
  tests for the new scatter+merge handler, 3 cross-role-wire
  integration tests covering the Wave 41.OO flow end to end (load
  artifact → query result, missing-artifact → Failed,
  scan-projection-with-filter), and 1 multi-process E2E test that
  spawns the real `ferrodruid-historical` binary with
  `--real-loader`, ingests a fixture segment, runs a real
  timeseries query through the binary, and verifies the result.

### Wave 41.OO honest scope

- Query types deferred to W6: `groupBy`, `topN`, `search`,
  `segmentMetadata`, `dataSourceMetadata`, `timeBoundary`, full
  SQL.
- Filter shapes deferred to W6: range, IN, NOT, AND, OR. Wave
  41.OO supports only equality on scalar dimensions.
- Real S3 / GCS / Azure deep-storage backends behind the
  historical's loader. The `ferrodruid-deep-storage` crate already
  ships an `S3DeepStorage` impl, but wiring it through
  `--real-loader` is the next wave's work; today the loader only
  reads from local FS.
- Coordinator-driven `data_source → segment list` resolution —
  callers of the broker's native route currently supply the
  segment list explicitly via the `segmentIds` field. W6 will
  resolve segment lists from the coordinator's metadata.
- The `coordinator → historical` flow is still the W4 timer-stub
  loader by default. `--real-loader` opts into the Wave 41.OO
  semantics.
- The `overlord → middleManager` flow is still the W3 simulated
  tokio-timer task executor; not in this wave's scope.
- Tier-aware broker selection in the router still round-robins.
- Authentication, mTLS, retry / timeout / circuit-breaker, and
  per-role readiness probes remain deferred.

## [Pre-0.2.0 development log] — Wave 40.LL (2026-05-04)

### Added

- **v1.0 last two cross-role HTTP wires** (W4 of the v1.0
  multi-process plan; W3 landed in Wave 39.HH). All four cross-role
  flows in the v1.0 architecture are now wired end-to-end:
  - **broker → historical** scatter via `POST /druid/v2/native`. The
    broker accepts a comma-separated `--historical-url` list (or
    `FERRODRUID_BROKER_HISTORICALS` env), and on
    `POST /druid/v2/sql/scatter` fans a per-segment fragment out to
    every configured historical and returns the aggregated
    responses. With no historicals configured the existing W3
    `POST /druid/v2/sql` echo path is unchanged.
  - **coordinator → historical** segment load / drop / status via
    `POST /druid/v1/historical/{load,drop}` and
    `GET /druid/v1/historical/loadstatus`. The coordinator accepts
    `--historical-url` and exposes
    `POST /druid/coordinator/v1/loadqueue/{historical}`,
    `dropqueue/{historical}`, and aggregated
    `GET /druid/coordinator/v1/loadstatus`.
- New module `ferrodruid-rpc::historical_client` ships the
  `HistoricalClient` trait + `HttpHistoricalClient` (real `reqwest`
  impl) + `MockHistoricalClient` (records every call, FIFO-replays
  canned responses, supports `set_load_state` for status-table
  mutation in tests).
- New module `ferrodruid-rpc::historical_server` ships the axum
  `Router` factory the historical binary mounts. The handlers
  implement a simulated `Loading → Loaded` tokio-timer state
  machine; real deep-storage fetch lands in W5.
- New wire types in `ferrodruid-rpc::types`: `SegmentQuery`,
  `SegmentQueryResponse`, `SegmentLoadCommand`, `SegmentDropCommand`,
  `SegmentLoadState` (`loading` / `loaded` / `dropped` / `failed` /
  `unknown`), and `LoadStatusReport`.
- `ferrodruid-historical` binary upgraded from a banner-and-exit
  scaffold to a real axum HTTP server hosting the four W4 endpoints.
- 35+ new tests across `ferrodruid-rpc`, `ferrodruid-broker-bin`,
  and `ferrodruid-coordinator-bin`: 16 in-crate unit tests
  (`historical_client` mock semantics, `historical_server` axum
  routes, types serde shape), 7 broker/coordinator app unit tests
  (mock-based scatter + load/drop/status dispatch), 6 new
  cross-role-wire integration tests (covering all four Wave 40.LL
  paths plus a 4/4 milestone test that loads a segment and scatters
  a query against the same historical), 2 new multi-process smoke
  tests spawning the real `ferrodruid-historical` binary under
  `tokio::process::Command`.

### Wave 40.LL honest scope

- The historical's `POST /druid/v2/native` handler echoes the query
  string back as a single-row response. Real per-segment query
  execution against `ferrodruid-query` lands in W5.
- The historical's `POST /druid/v1/historical/load` handler does
  not perform a real deep-storage fetch; a tokio timer flips state
  from `Loading` to `Loaded`. Wiring `ferrodruid-deep-storage` is
  W5 work.
- The broker's scatter planner issues one fragment per configured
  historical with a synthetic per-historical segment id. Real
  `data_source → segment list` resolution from the coordinator
  metadata is W5 work.
- Authentication, mTLS, retry/timeout/circuit-breaker remain
  deferred to W5.

## [Pre-0.2.0 development log] — Wave 39.HH (2026-05-04)

### Added

- **v1.0 first real cross-role HTTP wire** (W3 of the v1.0
  multi-process plan; W1 scaffold landed in Wave 34.T, W2 expansion
  in Wave 38.FF). Two cross-role flows are now end-to-end testable
  across separate processes:
  - **router → broker** SQL forward via `POST /druid/v2/sql`
    (Druid-aligned). The router accepts a comma-separated
    `--broker-url` list (or `FERRODRUID_ROUTER_BROKERS` env), picks
    one round-robin, and forwards the query. Broker introspection
    available via `GET /druid/v2/info`.
  - **overlord → middleManager** task dispatch via
    `POST /druid/v1/middlemanager/task`. The overlord accepts
    `POST /druid/indexer/v1/task` from clients, picks a
    middleManager round-robin, dispatches, then relays
    `GET /druid/indexer/v1/task/{id}/status` polls back to the
    correct middleManager via an in-memory routing table.
- New crate `ferrodruid-rpc` ships the cross-role contracts:
  - Wire types (`SqlQuery`, `SqlResponse`, `BrokerInfo`,
    `TaskAssignment`, `TaskStatus`, `TaskKind`, `TaskState`,
    `TierHint`).
  - Caller traits (`BrokerClient`, `MiddleManagerClient`) plus real
    HTTP impls (`HttpBrokerClient`, `HttpMiddleManagerClient`)
    backed by `reqwest`, plus mock impls
    (`MockBrokerClient`, `MockMiddleManagerClient`) that record
    every call for unit-test assertions.
  - Axum router factories (`broker_server::router`,
    `mm_server::router`) the per-role binaries mount.
- Per-role binary launchers updated:
  - `ferrodruid-broker` boots an axum HTTP server on
    `--bind:--port` (default `127.0.0.1:8082`); `--broker-id` and
    `--tier` flags populate `/druid/v2/info`.
  - `ferrodruid-middlemanager` boots an axum HTTP server (default
    `127.0.0.1:8091`); `--pending-to-running-ms` and
    `--running-to-success-ms` configure the simulated executor.
  - `ferrodruid-router` and `ferrodruid-overlord` each boot an axum
    server, hold an `Arc<dyn ...Client>` per peer, and forward
    requests via the new RPC traits.
- 28 new tests in `ferrodruid-rpc` (in-crate unit + cross-role-wire
  integration via real TCP loopback + multi-process smoke spawning
  the actual binaries) plus 6 router/overlord app unit tests using
  the mock impls. Workspace test count: 1284 (up from 1250+).

### Honest limitations (Wave 39.HH)

- The broker's `POST /druid/v2/sql` handler returns a **canned echo**
  response (single-row, column `echo`, value = the SQL text). Real
  query execution against `ferrodruid-query` lands in W4 alongside
  the broker→historical scatter wire.
- The middleManager's task handler runs a **simulated executor**: a
  tokio timer flips the task `Pending → Running → Success`. Real
  ingestion lands in W4.
- Tier-aware broker selection is wire-defined (`TierHint` enum) but
  unused — the router's selection policy is round-robin. Tier
  pinning lands in W4.
- No auth, no mTLS, no retry / timeout / circuit-breaker between
  roles (deferred to W5). Operators must place these processes on a
  trusted network for now.
- Multi-process integration tests are best-effort: they auto-skip
  with a `eprintln!` notice when the binaries are not yet built,
  rather than failing.

## [Pre-0.2.0 development log] — Wave 38.FF (2026-05-04)

### Added

- **v1.0 6-role binary scaffold complete** (W2 scaffold of the v1.0
  multi-process plan; cross-role wire still pending). Adds the three
  remaining Apache Druid roles on top of Wave 34.T.
  - `Role` enum extended to the complete 6-role topology:
    `{ Broker, Historical, Coordinator, Router, Overlord,
    MiddleManager, Standalone }`. `Role::all()` now returns 7
    variants; round-trip through `as_str` / `FromStr` covers every
    new role plus the `middle-manager` legacy spelling.
  - New helper `Role::druid_default_port()` returns the upstream
    Druid default for each dedicated role
    (`coordinator=8081`, `broker=8082`, `historical=8083`,
    `overlord=8090`, `middlemanager=8091`, `router=8888`) and `None`
    for `Standalone`. Tier predicates `is_query_tier` /
    `is_data_tier` / `is_master_tier` partition the dedicated roles
    for downstream config / topology code.
  - Three new launcher binaries under `bins/`:
    `ferrodruid-router` (default port `8888`),
    `ferrodruid-overlord` (default `8090`),
    `ferrodruid-middlemanager` (default `8091`). Each mirrors the
    Wave 34.T launcher pattern: parse CLI, build `RoleConfig`,
    print the role banner, then exit success — the cross-role wire
    is W3 work.
  - `ferrodruid serve --mode classic --role <r>` now accepts the
    new role values (`router`, `overlord`, `middlemanager`)
    through the same dispatcher as the per-role binaries; the
    earlier "Overlord not yet covered" stderr stub is removed.
  - 5 new tests added on top of the Wave 34.T 12+4 baseline
    (15 unit + 6 integration in `ferrodruid-role`): full 7-variant
    round-trip, default-port table assertion, tier-predicate
    partition, banner content for the new roles, and a
    default-port-uniqueness assertion across the 6 dedicated roles.
- README `Deployment Modes` table + `Quick Start` updated to
  describe the 6-role scaffold; `docs/v1.0-roadmap.md` W2 row
  flipped from "planned" to "scaffold landed" with an explicit
  follow-up list for the real-wire portion.

### Honest limitations (Wave 38.FF)

- All six dedicated role binaries still print a banner and exit;
  cross-role wire (broker→historical scatter, coordinator→historical
  assignment, overlord→middleManager dispatch, router→broker
  selection) is **not** wired. Single-binary mode
  (`--mode single-binary`) remains the only end-to-end path in
  v0.1.x.
- The `--mode classic` flow still uses the same `--port` regardless
  of role; Druid-aligned per-role defaults are only set on the
  dedicated binaries (`ferrodruid-router` etc.).
- Each role binary still pulls in only `ferrodruid-role`; the
  feature-gated per-role state-machine subsets are deferred to a
  later wave.

## [Pre-Wave-38.FF] — Wave 34.T (2026-05-04)

### Added

- **v1.0 3-role binary scaffold** (W1 of the v1.0 multi-process plan).
  - New crate `ferrodruid-role` exposing a `Role` enum
    (`Broker | Historical | Coordinator | Standalone`),
    `RoleConfig` shared launch surface, and a pure
    `dispatch(role) -> DispatchOutcome` helper. 12 in-crate unit tests
    cover parse / display / round-trip / dispatcher routing / banner
    content / bind-address validation; 4 integration tests under
    `crates/ferrodruid-role/tests/role_split.rs` exercise the public
    surface a downstream binary sees.
  - Three new launcher binaries under `bins/`: `ferrodruid-broker`
    (default port `8082`), `ferrodruid-historical` (default `8083`),
    `ferrodruid-coordinator` (default `8081`). Each parses CLI args,
    builds a `RoleConfig`, prints the role banner and (in v0.1.x)
    exits success — the cross-role wire is v1.0 work.
  - `ferrodruid serve --mode classic --role <broker|historical|coordinator>`
    now flows through the same dispatcher instead of printing the
    legacy "not yet implemented" stub. The Druid-only `Overlord`
    role is rejected with a clear "tracked in 6-role expansion" stderr
    message; `--mode single-binary` is unchanged.
  - New roadmap doc `docs/v1.0-roadmap.md` describing the W1 (this
    commit) → W2 (6-role + real broker→historical wire) → W3
    (per-role hardening) plan.
- README `Deployment Modes` table + `Quick Start` updated to describe
  the 3-role scaffold; `Architecture` line now points at
  `docs/v1.0-roadmap.md` for the v1.0 plan.

### Honest limitations

- Per-role binaries do **not** yet carry real broker→historical
  scatter or coordinator→historical assignment traffic; they print a
  banner and exit. Single-binary mode (`--mode single-binary`) remains
  the only end-to-end path in v0.1.x.
- The `--mode classic` flow uses the same `--port` regardless of role;
  Druid-aligned per-role default ports are only set on the dedicated
  binaries (`ferrodruid-broker` etc.).
- `Role::Standalone` is exposed by the dispatcher but `ferrodruid`
  still uses `--mode single-binary` to reach it; a single-binary
  `--role standalone` flag is W2 work.
- The 6-role expansion (router / overlord / middleManager) is W2
  work; the 3-role scaffold today does not cover them.

## [Pre-0.2.0 development log] — Wave 30 → Wave 57 sweep (2026-04-28 → 2026-04-29)

**Status**: Code-correctness gates green across Codex external review R1–R7 +
manual closure verification + 5 integration test suites + 18
cargo-fuzz targets + 17 proptest properties × ~256 cases each.
Wave 56 R7 surfaced four [High] *evidence-governance* findings (commit
drift, test-count drift, deployment-mode marketing inflation,
Druid-version compat marketing inflation) and one [Medium] cluster
doc-vs-code mismatch (`record_pre_vote` mixed-cluster compat). Wave 57
closes all five plus one [Low] (cardinality tag-bypass-of-key-cap).

**Stats** (HEAD `648e294` post-W59 baseline + W60/W61 doc-only deltas,
60 commits since v0.1.0 baseline `7025e31`):
- 1203 default tests (+270 over v0.1.0; +1 W59 regression on top of
  the W56/W57 1200/1202 trail) / 20 ignored / 100 suites — reproducible via
  `cargo test --workspace 2>&1 | grep "^test result"`
- 1208 tests with `--features cluster-tls` (+5 TLS-only) / 25 ignored
  — reproducible via
  `cargo test --workspace --features cluster-tls 2>&1 | grep "^test result"`
- Clippy 0/0/0 across default + `cluster-tls` feature
- ~3,840 hostile-input proptest executions per `cargo test` run
- IP rebrand: abyo software 合同会社 (abyo software LLC) per commit `15b53e9`

### Review-evidence reconcile (W56 + W57)

- **W56** (`380898c`) Codex R7 external review (4 audit runs, single
  round each, sandbox=read-only). Code-correctness layer green:
  W54-A/B/C closure re-verified, no NEW Critical/High in the cluster
  / aggregator / parser cores. Round surfaced 4 [High] +
  1 [Medium] in the *evidence-governance* layer (commit drift,
  test-count drift, deployment-mode marketing inflation,
  Druid-version compat marketing inflation, Bearer doc-vs-code
  mismatch) plus 2 [Low] in cardinality.
- **W57** doc-vs-code reconcile + 1 cluster code fix:
  - Ship-ready summary / known-limitations / CHANGELOG / README all
    repinned to HEAD `380898c` + actual test count
    (1202 default / 1207 cluster-tls).
  - README + SHIP_READY: deployment-mode reality table (only
    `single-binary` is implemented; `simplified` / `classic` rejected
    by binary; v1.0 plan).
  - README + SHIP_READY: Druid-version honesty (30.0.1 live-validated,
    31-36 spec-driven design-target only).
  - SECURITY.md + known-limitations CL-G3: Bearer auth claim
    corrected — middleware explicitly rejects Bearer with `401`
    (`crates/ferrodruid-rest/src/middleware.rs:231`); v1.0+ plan.
  - `crates/ferrodruid-cluster/src/replication.rs::record_pre_vote`:
    implemented the documented round_id=0 legacy-peer conditional
    (was bare equality, silently dropped every legacy grant after
    first `start_pre_vote()`). Mixed-cluster rolling-upgrade liveness
    restored. New regression
    `record_pre_vote_accepts_round_id_zero_legacy_peer`.
  - `crates/ferrodruid-aggregator/src/cardinality.rs::aggregate_field_at`:
    re-clip the *full* tagged key (tag + value) so the 10-byte
    `f<8hex>:` field-index tag cannot push the inserted key past
    `MAX_CARDINALITY_KEY_BYTES`. New regression
    `field_tagged_key_clips_to_max_total_bytes`.
  - 1 [Low] (W54-B theoretical u64::MAX wraparound) intentionally
    deferred — `STILL-OPEN` carryover.

### Real-environment validation (W30-W34)

- **W30** (`b60d7c2`) Real Apache Druid 30.0.1 wire-compat diff — HTTP envelope 5/5
- **W30B** (`f8f6174`) Overlord wired through BatchIngester → Historical → MetadataStore — Druid SQL deep match **5/5**
- **W31** (`6a2e7ae`) Real Kafka 3.7 KRaft E2E — 1000 rows produce/consume/segment v9 verified (10.81s)
- **W32** (`59ce919`) 3-node TCP replication via real sockets — cases A/B/C/D pass
- **W33** (`d19cbb6`) Apache Superset 6.0.1 + pydruid 0.6.9 — 5 SQL Lab queries + 1 dashboard
- **W34** (`6020110`) EC2 c7g.xlarge spot benchmark — 16 criterion + TPC-H Q1/Q3/Q6 (~$0.04 spend, 4-layer auto-shutdown)

### Security + RBAC + observability (W36-A/B + W40-A/C + W42-B + W45-G + W54-B)

- **W36-A** (`cc22749`) Auth on by default + insecure-bind refusal + admin bootstrap (32-char random pw printed once) + CI all-features clippy
- **W36-B** (`79667d3`) Real `/status/health` (subsystem readiness) + `/status/live` + Prometheus counters wired + per-IP rate limit
- **W40-A** (`56ab903`) Cluster transport PSK (HMAC-SHA256 per-frame) + handshake binding (per-connection node_id rejection) + `cluster_psk_required = true` default
- **W40-C** (`f9063a8`) AppState.authorizer per-route RBAC (admin/viewer roles + ResourceType + Action + path-prefix policy + default-deny + public allowlist)
- **W42-B** (`7a4bb5e`) Username-enumeration timing oracle fix (sentinel Argon2id verify) + Authorization header redaction + RFC 7235 case-insensitive scheme
- **W45-G** (`7eaec77`) submit() majority-ack via `tokio::sync::Notify` (sub-ms wake vs 5ms busy-poll)
- **W54-B** (`8a7b632`) Pre-vote round-id (`PreVoteRound { proposed_term, round_id }`) preventing stale-response tally

### Cluster correctness sprint (W38-A/B/C/DE + W47-A)

- **W38-A** (`204556d`) Vote dedup (`HashMap<Term, HashSet<NodeId>>`) + log monotonicity (3-case AppendEntries safety check)
- **W38-B** (`64c0750`) TcpTransport promoted to `src/transport.rs` (production-grade) + VoteResponse over TCP + 3-process binary smoke
- **W38-C** (`b745ffd`) Heartbeat-driven failover (election timer + tick loop, 1500ms+150ms jitter), removed bootstrap hack
- **W38-DE** (`c245b3f`) Pre-vote (Raft §9.6) + snapshot transfer + AppendEntries incremental replay + quorum-ack on submit
- **W47-A** (`3c4ddd5`) Pre-vote driven from `tick()` + replay loop transport-side (every 10 ticks ≈ 500ms) + case_l auto-replay

### Optional mTLS (W44 + W44B)

- **W44** (`256746f`) `cluster-tls` Cargo feature + `tokio-rustls 0.26` + PEM loaders + cert-mismatch test
- **W44B** (`69b3a2e`) TcpStream/TlsStream generic, **live wire encryption verified** (TLS 1.3 handshake bytes captured, JSON tokens absent), 3-node mTLS cluster forms in 686ms

### Segment hardening (W36-D/E + W40-B + W45-A/D)

- **W36-D** (`2d6182a`) Writer fsync + temp-rename + 6 reader caps + Historical cross-DS isolation default-deny
- **W36-E** (`af4fcf2`) Smoosh reversed-offset guard + bounded headers + v9/FDX column error propagation
- **W40-B** (`c5faac1`) Cardinality typed merge envelope + GroupKey typed enum + segment dir atomic rename
- **W45-A** (`af1117b`) TopN previousStop / Scan resultFormat / Scan offset+limit early cutoff
- **W45-D** (`550c4c2`) cluster_lib segment-action queue DoS bounds + segment string-column decoder invariants

### Query hardening (W36-G1/G2/G3 + W36-H + W45-E/F + W54-A)

- **W36-G1** (`a1a1e58`) Query TopN/GroupBy unbounded memory cap + DimensionSpec wrappers honored
- **W36-G2** (`43d1ba0`) Cardinality merge typed `HashSet` union + broker `merge_agg_maps` per-aggregator dispatch + group_key typed values
- **W36-G3** (`5e03d21`) REPLACE/REGEXP_EXTRACT pattern slot rejects column refs + Window COUNT preserves arg/distinct + PARTITION BY rejects non-column
- **W36-H** (`37baf34`) Function arity table 35 entries + window-frame ordering rejection + window-COUNT AST contract
- **W45-E** (`5514f52`) Multi-field cardinality (`with_fields(by_row, fields)`)
- **W45-F** (`d2bb920`) Regex DimSpec compile-once + named/numbered groups + DFA size limit 1MiB + per-row input bound 1MiB
- **W54-A** (`e5c94b5`) Cardinality `by_row=false` field-tagged keys (Druid `SUM_i |distinct(field_i)|` semantics correction) + `finish_row` composite cap

### SQL parser hardening (W36-F)

- **W36-F** (`9430ba3`) Negative LIMIT + expression LIMIT/OFFSET + UNION ALL outer ORDER/LIMIT + non-integer SUBSTRING all rejected at parse time
- FDX truncated `index.drd` returns explicit error (was: silent 1970-01-01 fabrication)

### Marketplace fail-closed (W36-C)

- **W36-C** (`5a16eb6`) Helm chart 5-guard + Fargate EFS removed (POSIX locking unsafe) → EphemeralStorage 50GB + Terraform SHA256 pin + CHECKSUMS.md

### Mediums sweep (W42-A + W45-B/C)

- **W42-A** (`a34f3c1`) 6 W37B Mediums close (cluster_lib IPv6 / ElectLeader peer resolve / ingest-kafka spec_type+bounds / msq Result return / SQL log redaction)
- **W45-B** (`b6f67cf`) 4 Mediums (PostAggregator validate / MSQ DAG validate / ingest-kafka deny_unknown_fields / MSQ datasource case-fold)
- **W45-C** (`c96c0e9`) 3 Mediums (ingest-kafka serde DoS bounds / cluster_lib apply Result / msq TTL+eviction LRU 1024 cap + 24h TTL)

### Hardening infrastructure (W48 + W50 + W53 + W54-C)

- **W48** (`b2126bd`) proptest hardening (15 properties / 5 crates / ~3840 hostile inputs/run / 0 production bugs found)
- **W50** (`4b83c2d`) cargo-fuzz audit (18/18 targets working) + nightly CI workflow (cron 12:00 UTC, 144 CPU-min/night)
- **W53** (`1f679c5`) 5 integration test suites (e2e pipeline / cluster soak / middleware stack / segment durability crash / mTLS replication soak)
- **W54-C** (`1325c71`) proptest `catch_unwind` pattern replacing `prop_assume!` discard-then-assert anti-pattern

### Operational documentation (W49)

- **W49** (`91dea3f`) `SECURITY.md` (251 LOC) / ops runbook (342 LOC) / known-limitations doc (313 LOC, 28 limitations) / ship-readiness summary (204 LOC)

### External review process (W35/W37/W37B/W39/W41/W41B/W52/W55)

- 6 Codex review rounds (R1-R6) — 79+ findings: 1 Critical + 28 Highs + 27 Mediums + 4 Lows closed
- 1 manual closure verification round (W41) when Codex rate-limited

### Legal + IP

- **`15b53e9`** IP holder rebrand from Youichi Uda to abyo software 合同会社 (abyo software LLC) — Cargo.toml authors + Helm Chart maintainers + Copyright lines + NOTICE; personal-fork URLs preserved at youichi-uda

### Out of scope (clearly disclosed in `docs/known-limitations.md`)

- Formal Jepsen-grade linearizability proof (mitigated by 8 cluster integration cases A-L)
- Confidentiality on cluster wire (PSK = integrity only; enable `cluster-tls` for mTLS)
- FIPS 140-2 (aws-lc-rs path available, audit pending)
- PSK rotation requires cluster restart (no rolling key-id yet)
- Multi-node v0.x topologies in Helm/Fargate (templates fail-close, v1.0 unblocks)
- Full Apache Calcite SQL parity (~95%, complex correlated subqueries unsupported)

---

## [0.1.0] - 2026-04-27

### Added

#### Query Engine
- Druid Native Query support for all 8 query types (timeseries, topN, groupBy, scan, search, segmentMetadata, dataSourceMetadata, timeBoundary)
- 18 aggregation types including count, sum, min/max, first/last, filtered, cardinality, hyperUnique, HLL sketch, Theta sketch, T-digest
- 14 filter types including selector, in, bound, range, like, regex, search, logical combinators, interval, expression, true/false, null
- 5 post-aggregation types (arithmetic, fieldAccess, constant, expression, hyperUniqueCardinality)
- Druid SQL support with parser (sqlparser-rs), planner, and executor (DataFusion)
- SQL functions: TIME_FLOOR, TIME_CEIL, TIME_FORMAT, TIME_PARSE, TIME_EXTRACT, EXTRACT, APPROX_COUNT_DISTINCT, string functions, numeric functions
- MSQ (Multi-Stage Query) execution engine with stage DAG (Scan, Aggregate, Shuffle, Sort, Insert)
- MSQ plan compiler from SQL (INSERT INTO, REPLACE INTO, SELECT)
- MSQ single-node executor with topological stage ordering
- UNION ALL support (multi-branch query combination)
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG OVER
- Expression filter: recursive-descent evaluator for expression-based filters

#### Storage
- Segment v9 read/write (FerroDruid round-trip; reading REAL
  Druid-written v9 segments was verified later — see [1.2.0]. Writer
  output uses a FerroDruid-private `index.drd` header and is not yet
  loadable by Apache Druid)
- Smoosh container format, front-coded dictionary, Roaring bitmap index
- LZ4, LZF, and Zstd compression support
- Multi-value string columns and COMPLEX columns (sketches)
- Local filesystem deep storage
- S3 deep storage (object_store integration with full read/write/list/delete API)
- FDX (FerroDruid-internal extended segment format) read/write with front-coded dictionaries v2 and compressed bitmaps
- SQLite, PostgreSQL, and MySQL metadata storage
- mmap-based segment serving for zero-copy access
- Tiered storage (Hot/Warm/Cold/Frozen) with 9 load rule types
- Metadata import/export for Druid migration

#### Ingestion
- Batch ingestion (index_parallel) from JSON with timestampSpec, dimensionsSpec, metricsSpec
- Kafka real consumer with buffer/flush pipeline and offset tracking
- Kafka supervisor spec parsing and task lifecycle management
- Kinesis supervisor spec parsing (80% coverage)
- Iceberg and Delta Lake connector crates (framework)

#### Cluster and Coordination
- ZooKeeper-free architecture via openraft (Raft consensus)
- Single-binary deployment mode (all 6 Druid roles in one process)
- Simplified 3-node deployment mode (Master/Query/Data)
- Service discovery via Raft state machine
- Segment announcement and load/drop queue via Raft
- Leader election for Coordinator and Overlord
- TCP replication transport with 3-node cluster tests

#### REST API (25+ Endpoints)
- Query: POST /druid/v2/, POST /druid/v2/sql
- Status: /status, /status/health, /status/properties, /status/selfDiscovered
- Coordinator: datasource listing/details/segments, load rules CRUD
- Indexer: task submit/list/get
- Supervisor: submit/list/get/shutdown
- Lookup: tier listing, lookup CRUD, Historical-side lookup serving
- Metrics: Prometheus /metrics endpoint

#### Security
- Basic authentication with Argon2id password hashing
- Bearer token support
- RBAC authorization (DATASOURCE/CONFIG/STATE resource types, READ/WRITE actions)
- TLS via rustls (no OpenSSL dependency)
- `#![forbid(unsafe_code)]` in all crates

#### Sketch Data Structures
- HyperLogLog (HLL) for cardinality estimation
- Theta sketch for set-operation cardinality
- T-digest for quantile/percentile estimation

#### Observability
- Prometheus metrics exposition (/metrics)
- Structured logging via tracing
- Audit event system (auth, authz, admin actions, ingestion)

#### Deployment
- Docker images (Debian and musl scratch)
- Docker Compose for single-binary and simplified modes
- Helm chart for Kubernetes
- CloudFormation templates (EC2 and Fargate)
- Packer and Terraform configurations
- GitHub Actions CI pipeline (9 jobs: check, clippy, fmt, test, audit, deny, docker, sbom, helm-lint)

#### Web Console
- 5-page server-rendered UI (datasources, query editor, tasks, segments, cluster health)

#### Fuzz Testing
- 18 fuzz targets covering segment parsing, query JSON, SQL, compression, sketches

#### CLI Tools
- `ferrodruid-cli` for command-line administration
- `ferrodruid-migrate` for metadata migration from Apache Druid

#### Benchmarking
- TPC-H star-schema data generator (`ferrodruid-common::tpch`) with deterministic PRNG
- TPC-H Q1 (Pricing Summary), Q3 (Shipping Priority), Q6 (Revenue Forecast) adapted for Druid Native Query
- Integration tests executing TPC-H queries against 10K-row segments

#### Supply Chain
- SBOM generation (CycloneDX JSON) via `cargo-cyclonedx` in CI pipeline

#### Infrastructure
- Terraform module for AWS EC2 single-binary deployment (Graviton recommended)
- Parameterized variables (instance type, VPC, subnets, CIDR allowlist, EBS size)

#### Code Quality
- 933 tests passing, 0 failures
- Clippy clean (0 warnings with -D warnings)
- SPDX headers on 100% of source files
- deny(missing_docs) on all crates
- cargo-audit and cargo-deny in CI
- Apache-2.0 license, all dependencies permissive
