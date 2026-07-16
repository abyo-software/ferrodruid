<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# FerroDruid Known Limitations

> Applies to: FerroDruid v1.3.0 · Last reconciled: 2026-07-16
>
> The public, consolidated list of what FerroDruid does **not** do, does
> **not** guarantee, and has **not** been validated for. Limitation IDs
> (`CL-*`, `TG-*`, `SL-*`, `FG-*`, `EF-*`) are stable and are
> cross-referenced from [SECURITY.md](../SECURITY.md),
> [CHANGELOG.md](../CHANGELOG.md), the
> [compatibility matrix](compatibility-matrix.md), and source
> doc-comments. IDs are never reused; closed items keep their ID, marked
> `[closed]`.

## Fail-closed philosophy

FerroDruid prefers loud, explicit errors over plausible-but-wrong
answers. Unsupported inputs are **rejected with a descriptive error**,
not silently approximated: an unrecognized segment encoding fails the
segment open, an unimplemented SQL execution path returns HTTP 501, and
an exact-cardinality query that exceeds its resource cap fails the query
instead of returning an under-count. Where FerroDruid cannot yet do
something correctly, it refuses to do it at all.

This document follows the same principle and is intentionally blunt. A
database that claims "100% compatible, production-ready, no caveats" is
a red flag, not a green one.

### Severity legend

- `[design]` — intentional design choice; not a bug.
- `[v1.0 plan]` — item from the original v1.0 roadmap; where still open
  at v1.3.0 it remains a committed roadmap item.
- `[deferred]` — low priority; revisited on customer signal.
- `[closed]` — resolved; kept so external references to the ID resolve.

---

## 1. Deployment modes and the supported product

### CL-J1 — `single-binary` and `classic` are implemented; `simplified` is not
- Severity: `[design]` for the implemented modes; `[v1.0 plan]` for
  `simplified`
- **`single-binary` — implemented, end-to-end validated.** All six Druid
  roles in one process; the supported, paid AWS Marketplace deliverable.
  It hardcodes **SQLite metadata + local-filesystem deep storage**, and
  every product claim is made against this mode. Roles talk in-process,
  so cross-role mTLS does not apply here (CL-G2).
- **`classic` (6-role) — implemented** via six per-role binaries;
  `timeseries` / `scan` / `groupBy` / `topN` execute across real HTTP
  wires with cross-role mTLS **required by default** (`permissive` /
  `disabled` exist for migration). **CL-J1-R1** (cross-role mTLS,
  validated in-process and on a 3-host EC2 fleet with fail-closed
  probes) is `[closed]` 2026-06-30.
- **CL-J1-R2 (open)**: the broker→coordinator wire (rules / lookups /
  datasource metadata) is not implemented as an outbound client;
  coordinator-driven segment-list resolution is not wired (callers pass
  `segmentIds` or rely on the one-per-historical fallback); broker→
  historical selection round-robins — **no tier-aware routing**.
- **`simplified` (3-role) — NOT implemented.** `--mode simplified` exits
  with an explicit `not yet implemented` error; no cluster is formed.
- **`embeddable`** is a Cargo library API surface, not a binary mode.
- S3 / external-metadata / multi-node capabilities in this document
  belong to `classic` unless stated otherwise — **not** to the
  single-binary product scope.

### CL-F1 — Helm chart fail-closes on multi-node deploys `[v1.0 plan]`
- The chart requires `acknowledgeV0xLimitations=true` to render a
  multi-node deployment: multi-node transport features ship opt-in, and
  the chart refuses to pretend otherwise.

### CL-F2 — Container image digest is stamped at release time `[v1.0 plan]`
- Pre-GA charts shipped an all-zero SHA256 image-digest placeholder; the
  real digest is set by the release tag pipeline. Verify the digest of
  the chart version you deploy.

---

## 2. Apache Druid version compatibility

### CL-J2 — Live-validated versions vs. spec targets
- Severity: `[design]` (spec target) / `[deferred]` (residuals)
- Spec target: Apache Druid 30–36 (REST envelope, native query JSON,
  segment v9 layout plus FerroDruid's FDX extension — FG-4). Live
  picture from the committed diff harness (Druid docker
  micro-quickstart, up to 48 queries/version; runs of 2026-06-30, v30
  re-run 2026-07-01):
  - **Druid 31.0.2 / 32.0.1 / 33.0.0 / 34.0.0 / 35.0.1 / 36.0.0:
    clean** — each 42/48 deep match, 0 mismatched, 2 Druid-side
    failures, 4 classified FerroDruid residuals (identical on all six):
    2 intentional fail-closed (JOIN inline `VALUES` + single-level CTE,
    HTTP 501 — CL-4-R8) and 2 bare-scan expression-projection
    rejections (§7).
  - **Druid 30.0.1: partial — 24/48 deep, 18 mismatched.** Every
    mismatch is Druid 30 itself returning `[]` for SQL window functions
    even with `enableWindowing: true` injected
    (**TG-1-finding-W2A-v30-A**); FerroDruid returns the correct
    windowed rows. If your clients must byte-match Druid 30 window
    behaviour, FerroDruid will *not* reproduce Druid 30's empty results.
  - `NTH_VALUE` fails on the Druid side of the harness (both 35.0.1 and
    36.0.0 micro-quickstart reject it), so FerroDruid's `NTH_VALUE` has
    no cross-engine diff evidence.
- Per-version tables: [compatibility matrix](compatibility-matrix.md).
  Per-surface semantics: [compatibility modes](design/compatibility-modes.md).

---

## 3. Cluster correctness and consensus

### CL-A1 — No formal Jepsen-grade linearizability proof
- Severity: `[v1.0 plan]` — substantially closed by measured chaos runs;
  a true linearizability-checker invocation remains open
- Validated, all with safety invariants holding (`dup_leader_terms=0`,
  `term_regressions=0`, `recovery_ok=1`):
  - 3-node TCP suite (election, replication, leader kill + failover,
    partition, byzantine duplicate votes, reordered AppendEntries,
    replay; TLS/mTLS variants) — `crates/ferrodruid-cluster/tests/`.
  - Single-host nemesis matrix (2026-06-30): symmetric / asymmetric
    partition, leader-kill, clock-skew (±30 s, ±5 min, libfaketime).
  - Two-physical-host real-LAN run (2026-06-16), real iptables
    partition.
  - 3-host EC2 nemesis matrix (3× c7g.large, ap-northeast-1a,
    2026-06-30): partitions + leader-kill + disk-full + OOM +
    corrupted-metadata; multi-host clock-skew (±30 s, ±300 s) clean
    2026-07-01.
- **Not done — why the ID stays open:**
  - **No Knossos / linearizability-checker invocation.** The checker is
    at-most-one-leader-per-term + monotonic-term — a consensus-safety
    bar, not a linearizability proof — and FerroDruid has no public
    linearizable-KV REST surface to drive such a workload.
  - **CL-A1-R-mTLS / CL-A1-R-mTLS-residual-churn** — under the default
    mTLS posture on a real LAN, TLS handshake latency (2.5–3.5 s per
    peer pair) exceeded the old 1500 ms election timeout, causing an
    election storm. Mitigated 2026-07-01 (5000 ms mTLS election-timeout
    floor, ±25% jitter, startup grace until majority handshake auth):
    churn fell from 0.93–1.20 to 0.19 promotions/sec on the same 3×
    c7g.large fleet (PSK baseline ~0.16/sec). Liveness only — safety
    held throughout. Residual: 0.19/sec is not fully quiescent.
  - **In-memory Raft log in single-binary mode** — see CL-3.
  - **`voted_for` durability edge**: `current_term` / `voted_for` fsync
    piggybacks on log appends; a vote grant followed by a crash without
    a subsequent append loses the `voted_for` write (see
    `crates/ferrodruid-cluster/src/persist.rs` module docs).
  - **OOM-kill nemesis never actually killed FerroDruid**: even with
    `oom_score_adj` forced to 1000 the kernel chose the higher-RSS
    stress workload; the "leader process OOM-killed" variant is
    unexercised.
  - **Chaos soak length**: nemeses ran in 15-minute windows (2026-07-01,
    all clean). The original multi-hour bar was formally withdrawn and
    re-scoped, not silently under-run; 2 h+ per-nemesis soak is open.

### CL-A5 — Split-vote prevention is probabilistic `[design]`
- Randomized election timeouts make split votes vanishingly unlikely,
  not impossible.

### Closed consensus items (kept for ID traceability)
- **CL-A2** `[closed]` — pre-vote tick driver (no real term bump without
  majority). **CL-A3** `[closed]` — automatic AppendEntries replay to
  lagging followers with Raft §5.3 fast back-off. **CL-A4** `[closed]` —
  chunked + resumable snapshot transfer (256 KiB chunks, per-follower
  cursor, resume after mid-stream drop); residual: chunk payload is JSON
  array-of-u8, ~4× wire inflation vs. base64.

---

## 4. Cluster wire authentication

| ID | Limitation | Severity |
|---|---|---|
| CL-B1 | Rotating the cluster PSK requires a full cluster restart (all nodes down with the old PSK, up with the new). No rolling-rekey grace window. | `[v1.0 plan]` |
| CL-B2 | `--cluster-psk-not-required` is a documented no-op: there is no unauthenticated cluster wire path, so PSK cannot be disabled. | `[design]` |
| CL-B3 | TOML `[cluster] psk = "..."` is parsed but not consumed; CLI / env-var is the operator-facing surface. | `[deferred]` |
| CL-B4 | No PSK→TLS rolling upgrade: the PSK-only vs. PSK+mTLS choice is per binary launch; migration requires a restart with a downtime window. Dual-listener design scoped, not implemented. | `[v1.0 plan]` |
| CL-B5 | Cluster mTLS performs no client-cert revocation (CRL/OCSP). Revoking a compromised peer means re-issuing the CA / certs. | `[v1.0 plan]` |
| CL-B6 | No strict SNI enforcement beyond building `ServerName` from the peer node id. Sufficient for the private-network trust model. | `[deferred]` |
| CL-B7 | A connected, authenticated peer can resend its own old frames. Replay is neutralized at the application layer (write-once `voted_for` latch, monotonic `match_index`, vote dedup); wire-layer nonce replay protection is intentionally deferred. | `[design]` |

---

## 5. Aggregation and broker merge semantics

### CL-C1 — Exact-cardinality cap; saturation fails closed `[design]`
- Exact `COUNT(DISTINCT ...)` caps at 1,000,000 keys per aggregator
  (deliberate DoS protection). Since 2026-07-11, hitting the cap **fails
  the query** (HTTP 400,
  `io.druid.query.ResourceLimitExceededException`) instead of silently
  under-counting — at executor finalization and at the broker merge
  alike. The error names the limit and points at `APPROX_COUNT_DISTINCT`
  (uncapped, HLL-based) as the remedy.

### CL-C2 `[closed]` — Multi-segment exact distinct union
- Closed 2026-07-12: partials carry the full exact set and the broker
  computes the true cross-segment union (verified end-to-end incl.
  >1,000-key shards). Beyond the cap the query still fails closed —
  never an over- or under-counted scalar. Trade-off: in `classic` mode
  an exact partial can ship up to 1,000,000 set keys per shard per
  group; exact mode is opt-in (`useApproximateCountDistinct: false`).

### CL-C3 `[closed]` — Multi-field cardinality merge. Listed so the ID resolves.

---

## 6. Query engine

### CL-D1 — Global query timeout only `[v1.0 plan]`
- No per-query CPU / row / byte budget yet.

### CL-D4 — Segment reader caps conservative, not tuned `[deferred]`
- Reader caps were chosen without measuring realistic Druid OSS segment
  statistics (`MAX_STRING_DICT_BYTES = 256 MiB` is generous). Over-cap
  inputs are rejected, not truncated.

### Closed query-engine items (kept for ID traceability)
- **CL-D2** `[closed]` — loader previously hardcoded the v9 reader
  regardless of the on-disk version byte; now dispatches v9 / FDX.
- **CL-D3** `[closed]` — GroupBy/TopN `LimitSpec` sort previously
  stringified keys (colliding `1` vs `"1"`, `null` vs `""`, `NaN`); now
  typed.
- **CL-D5** `[closed]` — continuous fuzzing: 21 libFuzzer targets
  (segment parsing, query/filter/aggregator JSON, SQL, compression,
  sketches, dictionaries, ingest specs) on a continuous farm, plus a
  per-PR build-survival check (`.github/workflows/fuzz-smoke.yml`)
  against the registry (`fuzz/farm-targets.txt`). Residual **TG-6-R1**:
  full-cycle freshness snapshot across all 21 targets.

---

## 7. SQL parity vs. Apache Druid (Calcite surface)

### CL-4 — 12 function families lowered natively; residuals below
- Severity: `[v1.0 plan]` — closed 2026-06-30 modulo the named residuals
- All 12 families parse, plan, and lower to real native primitives that
  execute against segments: JOIN (broadcast / lookup / inline `VALUES`;
  INNER + LEFT, equi-join only), non-recursive CTE, `GROUPING SETS` /
  `CUBE` / `ROLLUP`, window functions (`NTH_VALUE`, `NTILE`,
  `CUME_DIST`, `PERCENT_RANK`), `ARRAY_AGG`, `LISTAGG` / `STRING_AGG`,
  `BLOOM_FILTER` / `BLOOM_FILTER_TEST`, `MV_FILTER_ONLY` /
  `MV_FILTER_NONE`, `EARLIEST` / `LATEST` by non-`__time`, `GROUPING()`.
  Diff-validated against Druid 35.0.1 / 36.0.0 (2026-06-30; §2).
- **Open residuals — read before assuming full SQL parity:**
  - **CL-4-R8** — JOIN inline `VALUES` and single-level CTE **parse and
    plan but do not execute**: the REST layer returns HTTP 501 with a
    Druid-shaped error envelope. This fail-closed guard replaced an
    earlier silent-drop bug (the inner query used to be discarded
    without error). Full execution lowering is a tracked follow-up.
  - **R4-bytes** — `BLOOM_FILTER` uses a FerroDruid-private envelope
    (SipHash-2-4, `FDBF` magic), **not** byte-compatible with Apache
    Hive's `BloomKFilter` (Murmur3). A Druid-emitted bloom literal fed
    to FerroDruid (or vice versa) will not match; only the FerroDruid
    round-trip is guaranteed.
  - **Bare-scan expression projection** — `SELECT MV_FILTER_ONLY(col,
    ARRAY[...]) FROM t LIMIT 1` (and `MV_FILTER_NONE`) is rejected:
    expression projections in a bare scan (no GROUP BY) are
    unsupported. Filter-context use of the same functions works.
  - **`NTH_VALUE`** — supported end-to-end in FerroDruid but with no
    cross-engine diff evidence (§2); validate against a fuller Druid
    distribution if you depend on it.
  - `WITH RECURSIVE` is rejected explicitly.
  - **UNION ALL — positional column mapping (matches Druid).** A
    `UNION ALL` names its output columns from the **first** branch and
    maps every later branch's columns into them by **position**, so
    differently-named branches work: `SELECT city … UNION ALL SELECT
    revenue …` returns one column `city` holding both branches' values,
    and `SELECT city AS x … UNION ALL SELECT revenue AS x …` returns
    `x`. Branch **arity** must match — a differing column count is
    rejected fail-closed at plan time. A branch that projects the SAME
    source column more than once (`SELECT a AS x, a AS y`) is supported
    only when **every** branch repeats source columns in the **same
    positions** (a branch's native scan deduplicates repeated columns,
    which positional alignment can reconstruct only when the repetition
    pattern matches); a differing repetition pattern is rejected
    fail-closed rather than silently mis-mapped. Cross-branch type
    coercion is not performed: each branch's values are carried through
    as-is under the first branch's column names (mixing incompatible
    types across branches yields mixed-type cells, as at the native
    layer). Branches must be unordered and unbounded scans (no per-branch
    `ORDER BY` / `LIMIT` / `OFFSET`; the outer query's `ORDER BY`/`LIMIT`
    on top of a `UNION ALL` is also unsupported).

### CL-E2 `[closed]` — `SUBSTR` / `SUBSTRING` consolidated with invalid-bounds rewrite.

---

## 8. Authentication, RBAC, and middleware

### CL-G1 — Runtime-created users are not persisted
- Severity: bootstrap-admin lockout `[closed]`; multi-user persistence
  open
- The first-launch admin credential (Argon2id **hash**, never plaintext)
  persists to `<data_dir>/auth/admin.json` (mode `0600`) and reloads on
  boot. **Still open:** users created at runtime live in memory only and
  vanish on restart; no `users` table, no external IdP integration. For
  multi-user auth, use an external IdP / authenticating reverse proxy.

### CL-G2 — Intra-process role calls are unauthenticated `[design]`
- Coordinator → Historical calls inside one single-binary process do not
  require auth. The cross-process cluster wire is PSK + optional mTLS;
  `classic` cross-role HTTP wires default to mTLS (CL-J1).

### CL-G3 — Bearer auth is rejected at the middleware `[v1.0 plan]`
- The auth-header parser understands Bearer (RFC 7235, case-insensitive)
  but the active REST middleware
  (`crates/ferrodruid-rest/src/middleware.rs`) explicitly maps Bearer
  credentials to `401`. **Basic (Argon2id) is the only supported
  scheme.** Bearer support is roadmap, implemented if customer demand
  warrants. See [SECURITY.md](../SECURITY.md).

---

## 9. Operations and observability

| ID | Limitation | Severity |
|---|---|---|
| CL-H1 | Docker-gated compat suites (live Druid diff, 3-node TCP, Kafka end-to-end) run on demand only, not per-PR; a nightly trigger is the natural next step. | `[deferred]` |
| CL-H2 | No PSK-vs-mTLS cluster-wire latency/throughput benchmark has been published. | `[deferred]` |

---

## 10. Ingestion and deep storage validation

### CL-1 — Kafka: exactly-once proven in integration, **not the default runtime path**
- Severity: `[v1.0 plan]` — single-broker scope closed 2026-06-30
- Validated against a real `apache/kafka:3.7.2` KRaft broker:
  exactly-once (kill mid-flush + restart — zero duplicates, zero gaps),
  1→2→1 consumer-group rebalance, broker stop/start recovery,
  schema-drift skip (malformed records logged and skipped).
- **Open residuals:**
  - **CL-1-R2 — revised again 2026-07-16: durability LANDED (v1.3.0).**
    The supervisor runtime is WIRED end-to-end (`kafka-io` build: POST
    a supervisor → real consumer → queryable rows, E2E-verified
    2026-07-13) and its semantics are now **durable**: consumer offsets
    are committed durably and resumed after a restart, and published
    segments persist to local deep storage (fsync + SHA-256 content
    hash) and reload at startup — zero data loss across hard restarts
    verified end-to-end against a real `apache/kafka:3.7.2` broker
    (restart-matrix scenarios 2a-2f, 2026-07-16). Durable-local
    persistence is the DEFAULT (`--data-dir` alone; no non-durable
    toggle). The remaining durability residuals are documented at FG-6
    in §15: durable-store tampering is outside the threat model, an
    unresolved Kafka cluster identity degrades to bounded
    re-consumption after a hard kill, and a multi-broker topic-id
    disagreement window floors offsets (bounded re-consumption, never
    a skip).
  - **CL-1-R1** — multi-broker clusters (replication factor > 1),
    sustained ≥1 h soak at ~10K msg/s, and managed-cloud Kafka (e.g.
    Amazon MSK) are untested.

### CL-2 — S3 deep storage: MinIO + AWS S3 validated; R2 untested
- Severity: `[v1.0 plan]` — real-infra legs closed 2026-06-30
- Validated end-to-end against real MinIO (5/5 tests incl.
  >1,000-segment pagination, multi-file segments, missing-segment error
  surfacing, delete idempotency) and real AWS S3 `us-east-1` (3/3 incl.
  1,100-segment pagination). Retry semantics (transient 5xx,
  `Retry-After` on 503, DELETE retry) validated via HTTP fault
  injection.
- **Open residuals:** the Cloudflare R2 test leg is code-complete but
  has **never been executed** against a real R2 account; no literal TCP
  connection-reset injection (the retry layer treats that class as
  5xx); the S3 builder's env path ignores `AWS_PROFILE` and falls
  through to IMDS on non-EC2 hosts — use static env credentials or an
  instance role.

---

## 11. Distributed execution and extensibility

### CL-3 — Raft replication: closed, with an in-memory-log caveat
- Severity: `[v1.0 plan]` — closed 2026-06-30 (single-host nemesis
  matrix + two-host real-LAN + 3-host EC2 nemesis matrix; evidence and
  residuals under CL-A1)
- **Single-binary mode's Raft log is in-memory only.** The production
  single-binary does not wire the opt-in persistent Raft state
  (`crates/ferrodruid-cluster/src/persist.rs`); the only on-disk state
  is the SQLite metadata DB. A crashed or SIGKILL'd node loses its local
  log and rejoins by peer catch-up. Intentional `[design]` choice — but
  plan node-restart procedures around it.

### CL-5 — MSQ engine: distributed execution real, with named ceilings
- Severity: `[v1.0 plan]`
- Works: 3-worker stage dispatch, shuffle over real TCP, stage retry on
  worker kill (idempotent reassignment), real segment I/O through the
  deep-storage trait, end-to-end `INSERT INTO ... SELECT ... GROUP BY` —
  validated loopback and on a 3-host EC2 fleet (2026-06-30, golden
  outputs byte-equal).
- **Open residuals:** **CL-5-R2** — shuffle is coordinator-mediated,
  not peer-to-peer (performance ceiling, not correctness). **CL-5-R3** —
  the MSQ wire has no authentication or encryption; deploy only on an
  isolated network segment. **CL-5-R4** — Sort (limit pushdown),
  Broadcast joins, and Window processors exist single-node but are not
  driven distributed. **CL-5-R-multihost-segment** — real segment-I/O
  round-trip not exercised multi-host against shared deep storage.
  **CL-5-R-multihost-workload** — no application-layer invariant
  (Σ committed writes == Σ reads after heal) run under chaos; the chaos
  bar to date is consensus-safety only (TG-5).

### CL-6 — No Java extension SPI; WASM plugins are the extension surface
- **CL-6-R1 `[design]`, permanent:** Druid extensions are Java JARs via
  ServiceLoader. FerroDruid embeds no JVM and will never load Java
  extensions; existing Druid extension deployments do not carry over.
- The Wasmtime-hosted WASM plugin runtime is the alternative:
  deny-by-default capabilities (log / clock / random / net), per-call
  fuel budget (default 50 M units), per-instance memory cap (default
  16 MiB), SHA-256 manifest verification, three reference plugins
  (aggregator, input source, authenticator) in tree.
- **Open residuals:** **CL-6-R2** — manifest integrity is SHA-256 only:
  defends against accidental corruption and trivial swap-the-wasm
  attacks, **not** an attacker who rewrites both manifest and wasm
  payload; cosign / Sigstore signing is planned. **CL-6-R3** — no
  first-class "use plugin X" configuration shortcut; wiring the runtime
  requires code today.

---

## 12. Interoperability validation

### TG-1 — Segment binary compat: Druid→FerroDruid measured; FerroDruid→Druid untested
- Severity: `[v1.0 plan]`
- **Druid writes → FerroDruid reads: measured green (2026-07-12).**
  FerroDruid reads real Apache Druid on-disk **v9** segments written by
  Druid 31.0.2 and 35.0.1, with a per-column SHA256 deep match (11/11
  columns, 3/3 segments, 10/10 rows) against Druid SQL output. Druid 35
  still writes on-disk format v9 — Apache Druid defines no newer disk
  format; FerroDruid's own extended format is **FDX** (`version.bin` =
  10) and is FerroDruid-private (FG-4).
- Honest scope of that green: covered — default `indexSpec` paths (LZ4
  longs / doubles / floats, LZ4 + uncompressed + `none` variants, 1- and
  2-byte dictionary ordinals in compressed and uncompressed layouts).
  **Not covered — rejected with descriptive errors, not guessed at:**
  `longEncoding: auto`, multi-value dimensions, Concise bitmaps, LZF /
  ZSTD compression, front-coded dictionaries, nested / sketch columns,
  NULL-bearing columns (mapping implemented, no NULL fixture measured).
- **FerroDruid writes → Druid reads: NOT tested.** No test loads a
  FerroDruid-written segment into a real Druid cluster; the writer emits
  FerroDruid's private layout. **FerroDruid-written segments are not
  consumable by Apache Druid.** Relevant to reverse migration only.
  Writer symmetry is an estimated ~1 week of work, not scheduled.

### TG-4 — Ecosystem clients: 4 of 6 tools validated
- Severity: `[v1.0 plan]` — closed for the 4 tools, 2026-06-30
- Passing hello-query end-to-end: pydruid (6/6), druid-go (3/3), Grafana
  (Druid datasource proxy + dashboard create), Superset (SQL Lab 5/5 +
  chart render; see [superset-compatibility.md](superset-compatibility.md)).
- Wire findings surfaced and fixed with regression tests: single-string
  `intervals` shorthand rejected (**TG-4-finding-001**); double-quoted
  table / JOIN-RHS names silently returning empty (**TG-4-finding-003**,
  a silent-drop-class bug). **TG-4-finding-002** is an upstream
  grafadruid plugin nil-panic, not a FerroDruid defect.
- **Open residuals:** **TG-4-R1** — **no JDBC**: the Avatica server-side
  protocol is not implemented, so JDBC-based tools cannot connect.
  **TG-4-R2** — Imply Pivot untested (requires a commercial license).
- Log-shipping compatibility: [logcompat.md](logcompat.md).

---

## 13. Performance and scale

### TG-3 — TPC-H coverage is three queries on one box; the win's caveats travel with it
- Severity: `[v1.0 plan]`
- Current measurement (2026-07-11; AMD Ryzen 9 9950X x86_64; SF10 =
  60 M rows; Druid 35.0.1 single-server `large` profile, 31 processing
  threads, caching disabled; identical generated data; results
  **byte-identical**; median of ≥7 FerroDruid / 12 Druid iterations):
  FerroDruid Q1 = 28.3 ms / Q3 = 1.01 ms / Q6 = 2.56 ms vs. Druid 35
  Q1 = 338 ms / Q3 = 17 ms / Q6 = 14 ms — **12.0× / 16.8× / 5.5×
  faster**. Do not quote these ratios without their conditions:
  single-node Druid; FerroDruid ran one in-memory segment in-process
  while Druid processed 21 segments over HTTP (measured HTTP+serialize
  floor ~3.4 ms; subtracting it the wins remain ~12× / ~13× / ~4×);
  this is an engine-vs-engine same-data figure, not a
  same-segmentation one.
- What remains limited despite the win:
  - **Coverage**: Q1/Q3/Q6 only — not the 22-query TPC-H suite, no
    concurrent load, no cluster topology.
  - **Pending re-runs**: a multi-segment FerroDruid leg and a
    cloud-hardware (Graviton) corroboration with the optimized binary.
    The earlier pre-optimization run (2026-07-01, both engines
    sequential) measured FerroDruid 32×–2994× *slower* at SF10, and
    ~2.2× slower on Graviton3 than on the x86_64 host; arm64 has not
    been re-measured since the optimization work.
  - **Determinism**: parallel f64 summation is order-non-deterministic
    (an explicit product decision matching Druid's parallel
    aggregation); counts, integer aggregates, and row-set membership
    are exact.
- Correctness is verified independently of speed: Q1/Q3/Q6 results
  byte-identical on 60 M rows, plus golden tests in
  `crates/ferrodruid-query/tests/tpch_query_tests.rs`.
- See the [compatibility matrix](compatibility-matrix.md) for the full
  table and the measurement history.

### SL-1 — Performance unvalidated at production scale `[v1.0 plan]`
- Untested at 1 TB+ data, 10K+ segments, 100+ QPS. Expected bottlenecks:
  high-cardinality GroupBy memory growth, mmap segment-load behaviour
  under page-cache pressure, and the **single-threaded broker merge in
  single-binary mode**.
- **Historical segment-cache admission is O(1) amortized** (exact,
  atomic, fail-closed): load/drop/replace update the cache total by an
  incremental per-entry delta rather than re-folding the whole map, so a
  cold bootstrap of N segments is O(N). The limit weights **loaded
  segment payload** (each segment's estimated heap + its id/datasource
  string bytes) — the dominant, operator-controllable term. It does NOT
  count the Historical's own two routing `HashMap` bucket tables, a
  small O(loaded-segments) overhead of pointers/control bytes that is
  negligible next to segment payload for realistic segment sizes; the
  cache limit is therefore a payload-weight bound, not a hard
  resident-memory total for the process.

### SL-2 — No horizontal-scaling validation under load `[v1.0 plan]`
- Consensus safety is chaos-validated (CL-A1), but query fan-out across
  historicals, segment balancing, and Raft performance **under sustained
  load** are unvalidated.

---

## 14. Long-running validation

### TG-2 — No 24 h+ soak test `[v1.0 plan]` — **open**
- Memory leaks, fd leaks, metadata-DB growth, and segment-cache
  thrashing over long runtimes are unvalidated.

### TG-5 — Chaos testing: consensus-safety layer only
- Severity: `[v1.0 plan]` — closed at the consensus-safety layer,
  2026-06-30
- Six nemeses (symmetric / asymmetric partition, leader-kill, disk-full,
  OOM, corrupted metadata) pass the consensus-safety checker on a 3-host
  EC2 fleet. **Not proven:** no application-layer data-loss check under
  chaos (CL-5-R-multihost-workload); per-nemesis windows were
  minutes-scale, not hours (CL-A1).

### TG-6 — Fuzzing: continuous, with a freshness residual
- Closed 2026-06-30; residual **TG-6-R1** (full-cycle freshness evidence
  across all 21 targets). See CL-D5.

---

## 15. Feature gaps vs. Apache Druid

| ID | Gap | Severity | Detail |
|---|---|---|---|
| FG-1 | Web console is a 5-page server-rendered UI | `[deferred]` | Datasources, query editor (SQL + native), tasks, segments, cluster health. Not the React SPA console; no segment-timeline visualization; no live task-log streaming. |
| FG-2 | **No automatic compaction** | `[v1.0 plan]` — open | Ingestion produces many small segments over time, degrading query performance without manual re-ingestion. Plan ingestion granularity accordingly. |
| FG-3 | No nested columns | `[deferred]` — open | Druid 28+ JSON nested column type is unsupported. Primitives (LONG / FLOAT / DOUBLE / STRING) and sketch complex types only. |
| FG-4 | FDX is FerroDruid-private | `[design]` | The FDX extended segment format (`version.bin` = 10; front-coded dictionaries, compressed bitmap columns) has a reader and writer, but Apache Druid cannot read FDX and defines no equivalent. The only Druid-interchange format is v9, and the FerroDruid→Druid leg is untested (TG-1). |
| FG-5 | Multi-value dimension filter edge cases | `[deferred]` — open | Multi-value string columns store and read, but filter predicates on them use simplified logic that may not match Druid semantics in all edge cases. |
| FG-6 | **Kafka streaming ingestion durability** | `[closed]` 2026-07-16 — documented residuals | Closed in v1.3.0. The `kafka-io` build runs a real consumer end-to-end (POST a supervisor → records consumed → rows queryable; E2E-verified 2026-07-13), and ingestion is now durable: consumer offsets are committed durably and resume after a restart, and published segments persist to local deep storage (fsync + SHA-256 content hash; REPLACE-semantics uploads prune superseded blobs) and reload at startup — fail-loud on a hash or decode failure (startup refuses rather than silently serving partial data). Zero data loss across hard restarts verified end-to-end against a real `apache/kafka:3.7.2` broker (2026-07-16). Durable-local persistence is the DEFAULT behaviour (`--data-dir` alone; there is no non-durable toggle). Measured cost (2026-07-16, AMD Ryzen 9 9950X, 2M rows, real Kafka, NVMe, 5-trial median): ingest throughput 2.78% below the prior non-durable baseline at realistic segment sizes, 12.08% below with aggressively small segments (scales with persist count). **Transport (v1.3.0):** the `kafka-io` build's librdkafka speaks SASL_SSL/TLS in addition to PLAINTEXT — SASL PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER, self-contained via vendored OpenSSL (no runtime libssl dependency); GSSAPI/Kerberos is NOT compiled in and is a documented follow-on (a GSSAPI spec fails loud at consumer start: "No provider for SASL mechanism GSSAPI"). Mechanism support is verified compiled-in by a runtime probe; the zero-loss restart E2E ran over PLAINTEXT (no live TLS/SASL-broker E2E yet). **Residuals:** tampering with the durable store itself (a rewritten well-formed metadata payload, or a swapped blob whose recorded SHA-256 is rewritten in lockstep) is outside the durability threat model — an ordinary swapped or corrupted blob IS caught fail-loud by the SHA-256 + decode checks; an unresolved Kafka cluster identity (pre-2.8 broker, or a transient metadata-fetch failure) omits durable offsets, so a hard kill that also loses the asynchronous commit re-consumes a bounded window on resume (bounded double-count, never loss; does not occur on resolvable 2.8+ brokers); the recreation-detection probes are PLAINTEXT-only by design, so on a TLS/SASL-only listener the consumer ingests durably but topic-recreation detection is unavailable (durable rows advance the resume on the cluster identity alone until the topic id becomes resolvable); a multi-broker topic-id disagreement window right after a topic recreate re-consumes a bounded window rather than skipping; topic delete→recreate under the same name remains documented, mitigated because persisted segments no longer depend on a Kafka replay. |
| FG-7 | **Segment residency: in-heap by default; opt-in spill-to-disk; deep-storage persistence landed** | `[closed]` 2026-07-16 — spill is opt-in | Both halves landed in v1.3.0: (a) **durable deep-storage persistence + bootstrap reload** (see FG-6) — a restart no longer reloads nothing; and (b) an **opt-in spill-to-disk residency mode** (`--segment-spill` / `FERRODRUID_SEGMENT_SPILL`, default OFF; memory-budgeted LRU, `--segment-cache-bytes` default 1 GiB) that bounds resident memory. Default heap mode is unchanged: segments are held fully in-heap (~310 bytes/row measured on high-cardinality data, W-6 2026-07-13) and RSS crosses 4 GB at roughly 13 M such rows on one node — enable spill mode for larger working sets (prototype measured 5.82 GB → 138 MB at 20 M rows, 2026-07-13 host). The spill cache is a memory-offload tier wiped at startup; durability comes from deep storage, not spill files. |
| FG-8 | Numeric timestamps above `i64::MAX` are rejected | `[design]` | Apache Druid converts such a JSON number via Java `Number.longValue`, silently wrapping to a negative instant. FerroDruid refuses to store the wrapped value and dead-letters the record instead (fail-closed philosophy). |
| FG-9 | The default (no-`kafka-io`) build cannot validate Kafka client property **values** | `[design]` | Value validation requires librdkafka, which the default build does not link. A bad `consumerProperties` value is accepted at POST (with a loud WARN) and only surfaces when a `kafka-io` build starts the consumer, which then warn-skips the supervisor at startup until the spec is fixed. |

---

## 16. Enterprise feature gaps

| ID | Gap | Severity | Detail |
|---|---|---|---|
| EF-1 | No FIPS-validated cryptography | `[v1.0 plan]` — open | Migration path identified (`aws-lc-rs`), not implemented. |
| EF-2 | No signature on container images or plugin manifests | `[v1.0 plan]` — open | SBOM (CycloneDX) is in CI; cosign signing of images and WASM plugin manifests (CL-6-R2) is not implemented. |
| EF-3 | No LDAP / Active Directory | `[v1.0 plan]` — open | Basic (Argon2id) is the only implemented scheme (CL-G3); use an authenticating reverse proxy for directory-backed auth. |
| EF-4 | No data masking / column-level security | `[deferred]` — open | RBAC operates at the datasource level only. |

---

## Summary table

| ID | Limitation | Severity | Status (v1.3.0) |
|---|---|---|---|
| CL-1 | Kafka ingestion production validation | `[v1.0 plan]` | Single-broker EOS / rebalance / broker-kill / schema-drift closed (2026-06-30); supervisor runtime WIRED + E2E-verified (2026-07-13) and DURABLE since v1.3.0 — offset commit + resume, deep-storage persist + bootstrap reload, zero-loss restart E2E-verified (2026-07-16; FG-6, CL-1-R2 revised); multi-broker + soak + managed cloud open (CL-1-R1). |
| CL-2 | S3 deep storage real-infra validation | `[v1.0 plan]` | MinIO + AWS S3 closed (2026-06-30); Cloudflare R2 never executed; no literal TCP-reset nemesis. |
| CL-3 | Raft replication on real TCP | `[v1.0 plan]` | Closed (2026-06-30) across single-host, two-host LAN, 3-host EC2. Single-binary Raft log is in-memory `[design]`. |
| CL-4 | Druid SQL parity (12 families) | `[v1.0 plan]` | Closed (2026-06-30). Open: CL-4-R8 (JOIN/CTE execution → HTTP 501 fail-closed), R4-bytes (Hive bloom byte-compat), bare-scan expression projection. |
| CL-5 | MSQ distributed execution | `[v1.0 plan]` | Loopback + 3-host EC2 closed (2026-06-30). Open: peer-to-peer shuffle, MSQ wire auth, distributed Sort/Broadcast/Window, multi-host segment round-trip, workload-level chaos invariant. |
| CL-6 | Druid extension SPI | permanent / `[v1.0 plan]` | Java SPI permanently out of scope (no JVM). WASM plugin runtime landed (2026-06-30); manifest signing (CL-6-R2) and config integration (CL-6-R3) open. |
| CL-A1 | Jepsen-grade linearizability proof | `[v1.0 plan]` | Chaos matrices pass on safety invariants; no linearizability-checker invocation; mTLS liveness residual churn; `voted_for` fsync edge. |
| CL-A5 | Probabilistic split-vote prevention | `[design]` | Permanent. |
| CL-B1..B7 | Cluster wire auth gaps | mixed | PSK rotation restart-only; no PSK→TLS rolling upgrade; no CRL/OCSP; app-layer replay defense only (§4). |
| CL-C1 | Exact-distinct cap fails closed | `[design]` | 1,000,000-key cap; saturation = HTTP 400, never a wrong number (since 2026-07-11). |
| CL-D1 / CL-D4 | Global-only query timeout / untuned reader caps | `[v1.0 plan]` / `[deferred]` | Open; over-cap inputs rejected. |
| CL-F1/F2 | Helm multi-node gate / image digest | `[v1.0 plan]` | Multi-node render requires explicit acknowledgement; verify chart image digest at deploy time. |
| CL-G1..G3 | Auth gaps | mixed | Runtime users not persisted; intra-process calls unauthenticated `[design]`; Bearer rejected — Basic only (see [SECURITY.md](../SECURITY.md)). |
| CL-H1/H2 | On-demand compat CI / no mTLS bench | `[deferred]` | Open. |
| CL-J1 | Deployment modes | `[design]` / `[v1.0 plan]` | `single-binary` + `classic` implemented; `simplified` NOT implemented; broker→coordinator wire + tier-aware routing open (CL-J1-R2). |
| CL-J2 | Druid version compat | `[design]` / `[deferred]` | 31–36 live-validated clean (2026-06-30); Druid 30 partial (upstream window-function behaviour). |
| TG-1 | Segment binary interchange | `[v1.0 plan]` | Druid→FerroDruid v9 read measured green (2026-07-12, default encodings; non-default fail loud). FerroDruid→Druid untested; FerroDruid segments not consumable by Druid. |
| TG-2 | 24 h+ soak | `[v1.0 plan]` | **Open.** |
| TG-3 | TPC-H benchmarks | `[v1.0 plan]` | Q1/Q3/Q6 SF10 same-box: 12.0×/16.8×/5.5× faster than Druid 35, byte-identical (2026-07-11, conditions in §13); full suite / cluster / arm64 re-run pending. |
| TG-4 | Ecosystem clients | `[v1.0 plan]` | 4/6 validated; **no JDBC/Avatica** (TG-4-R1); Pivot untested (TG-4-R2). |
| TG-5 | Chaos testing | `[v1.0 plan]` | Consensus-safety layer only; no workload-level invariant; minutes-scale windows. |
| TG-6 | Continuous fuzzing | closed | 21 targets on a continuous farm + per-PR build check; freshness residual (TG-6-R1). |
| SL-1 / SL-2 | Scale performance / horizontal scaling | `[v1.0 plan]` | **Unvalidated** at 1 TB+ / 10K+ segments / 100+ QPS; fan-out and balancing under sustained load unvalidated. |
| FG-1..FG-9 | Feature gaps | mixed | See §15 — notably **no automatic compaction** (FG-2) and no nested columns (FG-3). Kafka ingestion durability (FG-6) and deep-storage persistence + opt-in spill residency (FG-7) **closed in v1.3.0** with documented residuals. |
| EF-1..EF-4 | Enterprise gaps | mixed | No FIPS, no image/manifest signing, no LDAP/AD, no column-level security. |

---

## How to read this document

1. **Start here, not at [README.md](../README.md).** A vendor that
   hides its limitations is a higher risk than one that documents them.
2. `[design]` items will not change. `[v1.0 plan]` items are committed
   roadmap. `[deferred]` items are awaiting customer signal.
3. Measured claims here carry their measurement date and conditions; a
   number without its conditions is not a claim.
4. Security impact: [SECURITY.md](../SECURITY.md). Per-feature picture:
   [compatibility matrix](compatibility-matrix.md). Contributing a fix:
   [CONTRIBUTING.md](../CONTRIBUTING.md). Closure history:
   [CHANGELOG.md](../CHANGELOG.md).

If you find a limitation missing from this document, please file an
issue (for security-relevant gaps, follow the process in
[SECURITY.md](../SECURITY.md)). Honest disclosure is a feature, not a
bug.
