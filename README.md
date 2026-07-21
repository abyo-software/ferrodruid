# FerroDruid

> Apache-Druid-spec-compatible, Rust-native real-time OLAP database.
> JVM-free. ZooKeeper-free. One binary, or six per-role binaries.
>
> **Status: v1.3.0 — durable Kafka streaming ingestion (in `kafka-io`
> builds) + deep-storage persistence (BUSL-1.1).**
> Lineage: v0.2.0 single-binary AMI released to AWS Marketplace
> 2026-06-17; v1.0.0 GA 2026-07-01; v1.1.x currently live on AWS
> Marketplace as AMI and container products; v1.2.0 initial public
> source release 2026-07-12. See [CHANGELOG.md](CHANGELOG.md).
>
> **Honest scope.** The live diff harness (`tests/druid-compat/`)
> records per-query Druid-vs-FerroDruid comparisons. As of the latest
> runs (results retained in the vendor evidence pack),
> **Apache Druid 31.0.2 through 36.0.0 are live-validated deep-match
> clean** — 42/48 deep / 0 mismatched per version (live-diff runs,
> 2026-06-30); the 6 non-deep outcomes per version are classified
> residuals (2 Druid-side quickstart limitations, 2 intentional
> JOIN/CTE fail-closed, 2 tracked follow-ups). **Druid 30.0.1 is
> partial** (24/48 deep; the recorded mismatches are Druid 30's
> upstream empty-result behaviour for SQL window functions, not
> FerroDruid defects). See
> [docs/compatibility-matrix.md](docs/compatibility-matrix.md)
> for the per-version evidence. The four cross-role HTTP wires in the
> `classic` topology default to **mTLS required** (default since
> 2026-06-30); see [SECURITY.md](SECURITY.md) for the cert layout,
> migration runbook, and honest residuals, and
> [docs/known-limitations.md](docs/known-limitations.md) for the rest.

[![CI](https://github.com/abyo-software/ferrodruid/actions/workflows/public-ci.yml/badge.svg)](https://github.com/abyo-software/ferrodruid/actions)
[![License: BUSL-1.1](https://img.shields.io/badge/license-BUSL--1.1-blue.svg)](LICENSE)

FerroDruid is **not affiliated with or endorsed by The Apache Software
Foundation**. "Apache Druid" is a trademark of The Apache Software
Foundation. FerroDruid is an independent clean-room implementation of
the publicly documented Druid wire / segment formats.

## Why FerroDruid?

A classic Apache Druid deployment runs six JVM process types plus an
external coordination service and a metadata database. FerroDruid
implements the same query semantics, wire envelopes, and segment
formats in Rust, and can run the entire stack as a **single static
binary** with embedded coordination.

| | Apache Druid | FerroDruid |
|---|---|---|
| Process types | 6 + coordination service + metadata DB | **1** (single binary) or **6** (per-role binaries) |
| Coordination service | Required | **Not needed** (embedded Raft via `openraft`) |
| Java runtime | Required | **Not needed** (native Rust) |
| Container image | GBs | tens of MB (musl scratch image) |
| Cold start | Minutes | Sub-second |
| `unsafe` Rust | n/a | `#![forbid(unsafe_code)]` in every crate |

On TPC-H Q1/Q3/Q6 at SF10 (60 M rows) on the same box, FerroDruid
measured **12.0× / 16.8× / 5.5× faster** than Apache Druid 35.0.1 with
**byte-identical results** (2026-07-11, AMD Ryzen 9 9950X, Druid
`large` profile with caching disabled; full conditions and caveats in
[docs/compatibility-matrix.md](docs/compatibility-matrix.md) — the
segmentation methodology differs between the engines, so read them).
The RAM / startup / container numbers above are qualitative ("native
Rust single binary vs. a JVM multi-process stack") rather than a
benchmarked head-to-head.

## Will it run *your* Druid workload?

Run [`ferro-logcompat`](docs/logcompat.md) on a Druid broker request
log to find out — **without installing anything near your cluster**:

```bash
cargo build --release -p ferrodruid-logcompat
./target/release/ferro-logcompat /path/to/broker-request.log
```

It statically classifies every query in the log as
supported / partial / unsupported and emits a report. The default
report contains no literal values from your queries and the tool
performs no network I/O. See [docs/logcompat.md](docs/logcompat.md).

## Quick Start

### Single binary (the production-validated mode)

```bash
# Build from source
cargo build --release -p ferrodruid

# Start single-binary mode (SQLite metadata + local-filesystem deep storage)
./target/release/ferrodruid serve --mode single-binary --port 8888

# Check status
curl http://localhost:8888/status
```

Auth is **on by default**. The binary prints a one-time random admin
password on first boot and refuses to bind to a non-loopback address
without an explicit opt-in. See [SECURITY.md](SECURITY.md).

### Per-role binaries (`classic` 6-role topology)

```bash
# Build the six per-role binaries.
#
# NOTE: the commands below pass --cross-role-mtls=disabled so a single-host
# demo works out of the box. The production default is `required` — generate
# certs first (`ferrodruid-migrate gen-cross-role-certs`, see the note after
# this block) and drop the flag.
cargo build --release \
  --bin ferrodruid-broker \
  --bin ferrodruid-historical \
  --bin ferrodruid-coordinator \
  --bin ferrodruid-router \
  --bin ferrodruid-overlord \
  --bin ferrodruid-middlemanager

# Bring up the cluster. Historicals load segments off local FS or S3
# via --deep-storage-root <path|file://|s3://bucket[/prefix]> and
# serve real timeseries / scan / groupBy / topN queries.
./target/release/ferrodruid-coordinator    --port 8081 --cross-role-mtls=disabled \
    --historical-url http://localhost:8083
./target/release/ferrodruid-broker         --port 8082 --cross-role-mtls=disabled \
    --historical-url http://localhost:8083
./target/release/ferrodruid-historical     --port 8083 --cross-role-mtls=disabled \
    --real-loader --deep-storage-root /var/lib/ferrodruid/segments
./target/release/ferrodruid-overlord       --port 8090 --cross-role-mtls=disabled \
    --middlemanager-url http://localhost:8091
./target/release/ferrodruid-middlemanager  --port 8091 --cross-role-mtls=disabled
./target/release/ferrodruid-router         --port 8888 --cross-role-mtls=disabled \
    --broker-url http://localhost:8082

# Druid SQL goes through the broker's SQL -> native bridge:
curl -sX POST http://localhost:8082/druid/v2/sql \
  -H 'content-type: application/json' \
  -d '{"query":"SELECT page, COUNT(*) FROM wikipedia GROUP BY page ORDER BY COUNT(*) DESC LIMIT 10"}'
```

> The four cross-role wires (`router → broker`, `broker → historical`,
> `coordinator → historical`, `overlord → middlemanager`) default to
> **mTLS required** (`--cross-role-mtls=required`; default since
> 2026-06-30): each role's TLS listener demands a client cert chaining
> to the configured CA bundle, and outbound clients present the role's
> leaf cert. Stage certs under `<data_dir>/cross-role/` (dev / staging:
> `ferrodruid-migrate gen-cross-role-certs`); `permissive` and
> `disabled` are operator-explicit downgrades for migration. See
> [SECURITY.md](SECURITY.md) for the runbook. There is still no
> cross-role retry / circuit-breaker — front with a proxy if needed.

### AMI / container / Helm / Terraform

Packaged deployments (arm64 AMI, metered container image with Helm
chart and Terraform module, buyer-facing deployment guides) ship
through [AWS Marketplace](#aws-marketplace).

## Compatibility

All counts below are verified directly against the source tree; see
[docs/compatibility-matrix.md](docs/compatibility-matrix.md) for the
per-version live-validation status.

### Native query (8/8 query types)

`timeseries`, `topN`, `groupBy`, `scan`, `search`, `segmentMetadata`,
`dataSourceMetadata`, `timeBoundary`. The single-binary `ferrodruid`
executes all eight in-process (`crates/ferrodruid-query`,
`DruidQuery` enum). The `classic` topology's cross-role wire executes
four end-to-end (`timeseries` / `scan` / `groupBy` / `topN`).

### SQL

- `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
- Druid SQL functions: `TIME_FLOOR`, `TIME_CEIL`, `TIME_FORMAT`,
  `TIME_PARSE`, `TIME_EXTRACT`, `TIME_SHIFT`, `APPROX_COUNT_DISTINCT`,
  string / numeric functions, and more
- `EXPLAIN PLAN FOR`
- Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`,
  `FIRST_VALUE`, `LAST_VALUE`, aggregate `OVER`) in single-binary mode
- Apache-Calcite parity is approximately 95% in single-binary mode;
  complex correlated subqueries are unsupported. The broker's
  SQL → native bridge in `classic` mode covers four SQL shapes
  (`Scan` / `Timeseries` / `GroupBy` / `TopN`) with scalar-equality
  filters only — see [Honest limitations](#honest-limitations).

### BI tools — Apache Superset

Superset connects to FerroDruid through the stock `druid://` (pydruid)
engine: connect, register datasets, build time-series charts, and run
SQL Lab — **verified end-to-end in the real Superset 4.1.4 UI**
(2026-07-12). See
[docs/superset-compatibility.md](docs/superset-compatibility.md) for
the verified matrix and the known setup gotchas.

### REST API (40+ endpoints)

The single-binary router (`crates/ferrodruid-rest::create_router`)
mounts **49 routes**, including:

- `POST /druid/v2/` — native query
- `POST /druid/v2/sql` — Druid SQL
- `POST /druid/v2/sql/task` — MSQ task submit
- Coordinator API (datasources, segments, load rules, lookups, servers,
  metadata)
- Indexer / Overlord API (tasks, supervisors)
- `/status`, `/status/health`, `/status/live`, `/status/properties`,
  `/status/selfDiscovered`
- `/metrics` (Prometheus exposition)
- Server-rendered web console (`/console/*`)

### Storage

- Segment v9 **read** — Apache Druid's on-disk format. FerroDruid reads
  genuine Druid-written v9 segments: deep-match verified against
  segments produced by Druid 31 and Druid 35, from local disk and from
  real S3 deep storage (2026-07-12). Non-default encodings fail loud
  rather than guessing.
- Segment v9-based **write** — FerroDruid writes its own v9-based
  segments (private `index.drd` header). They round-trip within
  FerroDruid, but are **not yet loadable by Apache Druid**; see
  [docs/known-limitations.md](docs/known-limitations.md).
- FDX — a FerroDruid-internal extended format for FerroDruid-native
  segments (`version.bin` = 10; not an Apache Druid format)
- Deep storage: local filesystem, S3 (via `object_store`, AWS SDK
  credential chain, `AWS_ENDPOINT_URL` honoured for LocalStack / MinIO)
- Metadata: SQLite (embedded, via `sqlx`; PostgreSQL/MySQL not yet wired)
- In-heap segment serving by default; opt-in spill-to-disk residency
  with a memory-budgeted LRU (`--segment-spill` — FG-7)
- Tiered storage (Hot / Warm / Cold / Frozen) with 9 load-rule types

### Ingestion

- Native batch (JSON, CSV) via `index_parallel`-style spec
- Kafka supervisor + real consumer, durable since v1.3.0 (requires a
  `kafka-io` build — the Docker gnu images, the AWS Marketplace
  AMI/container, or `cargo build --release -p ferrodruid --features
  kafka-io`; the static-musl download binary omits librdkafka and has
  no Kafka ingestion), over PLAINTEXT and SASL_SSL/TLS (SASL PLAIN,
  SCRAM-SHA-256/512, OAUTHBEARER, self-contained via vendored
  OpenSSL; GSSAPI/Kerberos is a follow-on): offsets are committed
  durably and resume after restart; published segments persist to
  deep storage and reload at startup (zero-loss restart verified
  against a real Kafka 3.7.2 broker over PLAINTEXT; residuals: FG-6)
- Kinesis supervisor (spec parse/validate only; no consumption)
- Iceberg / Delta Lake connector crates (framework)
- Rollup support

### Security

- Basic auth (Argon2id), on by default
- Role-based access control (datasource / config / state resources;
  read / write actions; default-deny)
- TLS via `rustls` (no OpenSSL)
- Cross-role HTTP wires (`classic` 6-role topology): **mTLS required
  by default** (default since 2026-06-30)
- Cluster wire (Raft TCP): **mTLS by default** — clustered mode refuses
  to start without certificates rather than silently downgrading.
  PSK-over-cleartext (per-frame HMAC: authentication + integrity, no
  confidentiality) is an explicit opt-in fallback
  (`--cluster-security psk`)
- Bearer tokens are parsed but explicitly **rejected** with `401`
  today (see [SECURITY.md](SECURITY.md))

## Deployment modes

| Mode | Use case | Status |
|---|---|---|
| `single-binary` | Dev, small production, Fargate, Lambda | **Implemented, end-to-end, Marketplace-validated** |
| `classic` | Migration from Druid (6 roles) | **Implemented** via six per-role binaries; 4/4 cross-role HTTP wires real; four query types execute end-to-end; SQL bridge covers four patterns. Cross-role wires: mTLS default-on. |
| `simplified` | Production (3 roles: Master/Query/Data) | **Not implemented** — the binary refuses with `not yet implemented`. |
| `embeddable` | Library mode (Rust crate) | Crate APIs exposed; not a binary mode. |

## Architecture

53 workspace packages: **44 library crates + 9 binary-crate packages**,
producing **11 executables** — `ferrodruid` (single binary),
`ferrodruid-cli`, `ferrodruid-migrate`, the six per-role launchers
(`ferrodruid-broker`, `-historical`, `-coordinator`, `-router`,
`-overlord`, `-middlemanager`), and the two compatibility tools
(`ferro-logcompat`, `ferro-compat-check`). Coordination is embedded Raft
(`openraft`) — there is no ZooKeeper and no external coordination
service anywhere in the stack. Design notes live under
[docs/design/](docs/design/).

## Build, test, run

```bash
cargo build --workspace                                   # build everything
cargo test  --workspace                                   # run the test suite
cargo clippy --workspace --all-targets -- -D warnings     # lint (must be 0)
cargo fmt --check                                          # format check
cargo build --release -p ferrodruid                       # release single binary
./target/release/ferrodruid serve --mode single-binary --port 8888
```

## Quality

Measured 2026-07-12 on the v1.2.0 tree via `cargo test --workspace`
(the v1.3.0 compat-3 durability and spill-residency work adds further
tests on top of this baseline; re-measure with the same command):

- **2,629 tests pass, 0 failed, 47 ignored** across **159 test
  binaries** (the ignored tests are the Docker / EC2 / soak-gated
  suites). Reproduce with
  `cargo test --workspace 2>&1 | grep '^test result'`.
- Clippy clean: `cargo clippy --workspace --all-targets -- -D warnings`
  reports 0 warnings.
- `#![forbid(unsafe_code)]` in every crate.
- SPDX license header on every source file.
- No `unwrap()` in non-test code; no TODO/FIXME/HACK markers.

The compat suites under `tests/druid-compat/` and
`tests/superset-compat/` ship in this repo but are `#[ignore]`d by
default — they need Docker, a real Apache Druid cluster, Kafka, and
Superset. Fuzzing (21 cargo-fuzz targets), the criterion / TPC-H bench
harness, EC2 benchmarks, and the performance gates run in the vendor's
private pipeline; their results are summarised in
[docs/compatibility-matrix.md](docs/compatibility-matrix.md).

## AWS Marketplace

FerroDruid is commercially available on
[AWS Marketplace](https://aws.amazon.com/marketplace/search/results?searchTerms=FerroDruid),
published by abyo software 合同会社 (abyo software LLC):

- **AMI product** (arm64 / AL2023) — single-binary mode, boots serving
  in seconds
- **Container product** — metered image for ECS / EKS

Marketplace subscriptions include commercial support. GitHub issues on
this repository are handled best-effort.

## Honest limitations

The full, sectioned list is in
[docs/known-limitations.md](docs/known-limitations.md).
The load-bearing ones:

- **Live wire validation is deep-match clean for Druid 31.0.2–36.0.0;
  Druid 30.0.1 is partial.** Each of 31–36 records 42/48 deep / 0
  mismatched (2026-06-30 runs) with 6 classified residuals per
  version; the v30 mismatches (24/48 deep) are Druid 30's upstream
  empty-result behaviour for SQL window functions. See
  [docs/compatibility-matrix.md](docs/compatibility-matrix.md).
- **`classic` cross-role wires: mTLS is default-on, but residuals
  remain.** Multi-host mTLS is live-validated for the
  broker → historical wire only (3-host EC2 run); the other three
  wires are loopback-validated. No CRL/OCSP revocation checking, and
  no cross-role retry / circuit-breaker yet. See SECURITY.md
  "Honest residuals".
- **`simplified` 3-role mode is not implemented** (binary rejects it).
- **SQL → native bridge (classic mode)** round-trips only scalar-equality
  filters; `IN` / `BETWEEN` / `LIKE` / `BOUND` / range / AND-OR and
  `UNION ALL` surface as explicit unsupported errors. Date/time bridge
  coverage is `TIME_FLOOR` over known period strings.
- **No coordinator-driven `dataSource → segment list` resolution** on
  the cross-role wire — callers pass `segmentIds` or rely on the
  one-segment-per-historical fallback. Broker / historical selection
  round-robins (no tier-aware routing yet).
- **S3 deep storage is wired; GCS / Azure are not.**
- **No formal Jepsen-grade linearizability proof** (a 3-node TCP /
  mTLS consensus test suite ships in-tree).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). A signed CLA is required
([.github/CLA/](.github/CLA/)); CLA Assistant prompts on your first
pull request. FerroDruid is a clean-room implementation — contributors
must not consult Apache Druid source code, only its published
documentation and wire behaviour.

## Security

See [SECURITY.md](SECURITY.md) for the threat model and controls.
Report vulnerabilities privately to **aws-support@abyo.net** — please
do not open public issues for suspected vulnerabilities.

## License

[Business Source License 1.1](LICENSE) (BUSL-1.1).

- **You may** copy, modify, redistribute, and make non-production use
  of the source.
- **Additional Use Grant**: You may make production use of FerroDruid,
  provided that you do not offer it to third parties as a hosted,
  managed, or embedded database, analytics, or OLAP service whose
  value derives substantially from FerroDruid's functionality and that
  competes with a commercial offering of the Licensor. Internal
  production use, and use in your own applications and services that
  are not themselves a database, analytics, or OLAP service offered to
  third parties, are permitted.
- **Change Date**: four years after each version's public release,
  that version automatically converts to the
  **Apache License, Version 2.0**.

For commercial licensing (including managed-service arrangements),
contact **aws-support@abyo.net** or subscribe via AWS Marketplace.
Dependency licenses are restricted to an explicit allowlist in
`deny.toml` (permissive licenses plus MPL-2.0; GPL / AGPL / LGPL /
SSPL are denied).
