<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# FerroDruid Known Limitations

> Applies to: FerroDruid v1.5.0 · Last reconciled: 2026-07-21
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
  at v1.5.0 it remains a committed roadmap item.
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

### TG-1 — Segment binary compat: Druid→FerroDruid measured; FerroDruid→Druid measured (LONG + single-value STRING)
- Severity: `[closed]` 2026-07-18 — both legs measured; writer milestone-scoped
- **Druid writes → FerroDruid reads: measured green (2026-07-12).**
  FerroDruid reads real Apache Druid on-disk **v9** segments written by
  Druid 31.0.2 and 35.0.1, with a per-column SHA256 deep match (11/11
  columns, 3/3 segments, 10/10 rows) against Druid SQL output. Druid 35
  still writes on-disk format v9 — Apache Druid defines no newer disk
  format; FerroDruid's own extended format is **FDX** (`version.bin` =
  10) and is FerroDruid-private (FG-4).
- Honest scope of that green: covered — default `indexSpec` paths (LZ4
  longs / doubles / floats, LZ4 + uncompressed + `none` variants, 1- and
  2-byte dictionary ordinals in compressed and uncompressed layouts),
  **`longEncoding: auto` (TABLE + DELTA)**, and **SQL-compatible NULL
  numeric columns** (Druid 28+ default null handling). The last two were
  added 2026-07-18 (compat-8) and each measured against a real Druid
  31.0.2 segment: a nullable LONG (NULL rows 2,4) + nullable DOUBLE (NULL
  row 3) read with values and NULL positions matching Druid SQL, and a
  300-row `longEncoding: auto` segment (TABLE column of 8 distinct values,
  DELTA columns) read row-for-row identical to Druid SQL — so a modern
  **default** Druid segment's numeric columns are now fully readable.
  **Not covered — rejected with descriptive errors, not guessed at:**
  multi-value dimensions (compat-11), Concise bitmaps, LZF / ZSTD
  compression, front-coded dictionaries, nested columns. Sketch complex
  columns: `thetaSketch` (compat-8) and, since v1.5.0, **`hyperUnique`**
  (W-A 2026-07-21, estimates bit-exact vs real Druid 31.0.2 — FG-17) DO
  decode; `HLLSketch` / `quantilesDoublesSketch` still fail loud.
- **FerroDruid writes → Druid reads: measured green (2026-07-18, milestone
  scope = LONG + single-value STRING).** A native v9 writer
  (`write_segment_v9_native`) emits real Apache Druid on-disk v9 — NOT the
  private FDX layout — and its output is verified two independent ways
  against **real Apache Druid 31.0.2**: (a) the writer's bytes are
  byte-identical to captured real-Druid-31 fixtures for the longV2 numeric
  part, the string column, and `index.drd`; (b) **Druid's own
  `dump-segment` reads a FerroDruid-written segment** and returns all 10
  rows across 7 columns with a per-column SHA256 deep match (the segment
  is written by FerroDruid, `docker cp`-ed into a Druid 31 container, and
  read back by Druid's `IndexIO`). String dictionaries are emitted in
  Druid-canonical **UTF-16 code-unit order** (Java `GenericIndexed.
  STRING_STRATEGY`), with ordinals+bitmaps remapped, so Druid's binary
  search over the dictionary is valid (verified for supplementary/non-BMP
  characters, where UTF-16 and Rust codepoint order diverge). The writer
  fail-closes on any input its own reader (and therefore Druid) would
  reject — every reader cap is enforced or provably unreachable
  (`MAX_COLUMN_VALUES` 2²⁶ rows, `MAX_GI_ELEMENTS` 2²⁴ dictionary entries,
  `SMOOSH_MAX_CHUNK_SIZE` 2³¹-1 checked pre-allocation, `MAX_SMOOSH_ENTRIES`
  16 384, `MAX_DIMENSIONS`/`MAX_METRICS` 16 384) — so it can never emit a
  reader-unopenable segment. Codex-gated to convergence (R5+R6 clean).
- **Honest scope of the writer green:** milestone 1 covers **LONG
  metrics/`__time` + single-value STRING dimensions** (uncompressed longV2
  `0xff`, v0 string columns, derived value bitmaps). NOT yet written by the
  native writer (a follow-on ~milestone 2, fail-loud until then): DOUBLE/
  FLOAT metrics, nullable numerics, multi-value dimensions, LZ4/compressed
  numeric encodings, front-coded dictionaries, complex/sketch columns.
  **Documented follow-on (separate scope, non-BMP-only):** the query-layer
  `ferrodruid-dict` `FrontCodedDictionary::find` value→ordinal binary search
  still uses Rust codepoint order, so an equality/selector filter on a
  *supplementary-character* dictionary value can mis-resolve at query time
  (positional groupBy/scan reads are correct; all-BMP data — every existing
  segment and fixture — is unaffected since UTF-16 and Rust order coincide
  there). Noted inline at the reader site for a separate gate.

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
| FG-4 | FDX is FerroDruid-private; v9 is the interchange format | `[design]` | The FDX extended segment format (front-coded dictionaries, compressed bitmap columns) has a reader and writer, but Apache Druid cannot read FDX and defines no equivalent. The Druid-interchange format is on-disk **v9**: FerroDruid reads real Druid v9 (measured, Druid 31/35) and, as of 2026-07-18, **writes real Druid v9** for LONG + single-value STRING columns via a native writer whose output Druid 31's own `dump-segment` reads back with a per-column SHA deep match (TG-1). DOUBLE/FLOAT/nullable/multi-value/compressed-encoding writing is a documented follow-on (fail-loud until then). |
| FG-5 | **Multi-value (MV) string dimensions — supported for FerroDruid-ingested data (Druid-verified); upstream-MV read + several MV query contexts fail loud** | `[closed]` 2026-07-18 — documented follow-ons | Landed 2026-07-18 (compat-11). A dimension holding a JSON array (tags) now ingests as a genuine multi-value column (`ColumnData::StringMulti` — per-row element lists over a flat ordinal array), CSV/TSV `listDelimiter` splits, and it queries Druid-faithfully — **verified against real Apache Druid 31.0.2** (docker A→B `druid_oracle_multivalue_queries`): **groupBy/topN EXPLOSION** (a row `["a","b"]` contributes to BOTH group a and b; an empty `[]`/null row → the null group; the filtered-groupBy explosion surprise — a `tags="b"` selector keeps the row's OTHER elements too), **any-element** selector/IN/Bound/Range/Like/Regex/Search + Bloom filters, scan array rendering (a 1-element row → scalar), metadata `hasMultipleValues:true`, and the persist → deep-storage → bootstrap-reload round-trip all match Druid. The Codex source-review gate dried over 7 rounds (14 Critical/High driven out — codec allocation bounds, and every silent-array-stringify path). **Fail-loud follow-ons (a clear error, never silently wrong — element-wise support pending):** reading an UPSTREAM Druid MV segment (via `attach`/`import`) stays a loud skip; and these contexts reject MV up front — aggregation over an MV field (incl. first/last `timeColumn`), virtual-column expressions, `expression`/`columnComparison`/`interval` filters over MV, MV join keys (schema-checked), MV window `PARTITION BY`/`ORDER BY`/inputs, `outputType`/`extractionFn` coercion over an MV grouping dim, and **rollup ingest over an MV dimension** (ingest with rollup disabled). `multiValueHandling` defaults to ingest order (= Druid `ARRAY` mode); Druid's default `SORTED_ARRAY` per-row sort/dedup is a follow-on. |
| FG-6 | **Kafka streaming ingestion durability** | `[closed]` 2026-07-16 — documented residuals | Closed in v1.3.0. The `kafka-io` build runs a real consumer end-to-end (POST a supervisor → records consumed → rows queryable; E2E-verified 2026-07-13), and ingestion is now durable: consumer offsets are committed durably and resume after a restart, and published segments persist to local deep storage (fsync + SHA-256 content hash; REPLACE-semantics uploads prune superseded blobs) and reload at startup — fail-loud on a hash or decode failure (startup refuses rather than silently serving partial data). Zero data loss across hard restarts verified end-to-end against a real `apache/kafka:3.7.2` broker (2026-07-16). Durable-local persistence is the DEFAULT behaviour (`--data-dir` alone; there is no non-durable toggle). Measured cost (2026-07-16, AMD Ryzen 9 9950X, 2M rows, real Kafka, NVMe, 5-trial median): ingest throughput 2.78% below the prior non-durable baseline at realistic segment sizes, 12.08% below with aggressively small segments (scales with persist count). **Transport (v1.3.0):** the `kafka-io` build's librdkafka speaks SASL_SSL/TLS in addition to PLAINTEXT — SASL PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER, self-contained via vendored OpenSSL (no runtime libssl dependency); GSSAPI/Kerberos is NOT compiled in and is a documented follow-on (a GSSAPI spec fails loud at consumer start: "No provider for SASL mechanism GSSAPI"). Mechanism support is verified compiled-in by a runtime probe; the zero-loss restart E2E ran over PLAINTEXT (no live TLS/SASL-broker E2E yet). **Residuals:** tampering with the durable store itself (a rewritten well-formed metadata payload, or a swapped blob whose recorded SHA-256 is rewritten in lockstep) is outside the durability threat model — an ordinary swapped or corrupted blob IS caught fail-loud by the SHA-256 + decode checks; an unresolved Kafka cluster identity (pre-2.8 broker, or a transient metadata-fetch failure) omits durable offsets, so a hard kill that also loses the asynchronous commit re-consumes a bounded window on resume (bounded double-count, never loss; does not occur on resolvable 2.8+ brokers); the recreation-detection probes are PLAINTEXT-only by design, so on a TLS/SASL-only listener the consumer ingests durably but topic-recreation detection is unavailable (durable rows advance the resume on the cluster identity alone until the topic id becomes resolvable); a multi-broker topic-id disagreement window right after a topic recreate re-consumes a bounded window rather than skipping; topic delete→recreate under the same name remains documented, mitigated because persisted segments no longer depend on a Kafka replay. |
| FG-7 | **Segment residency: in-heap by default; opt-in spill-to-disk; deep-storage persistence landed** | `[closed]` 2026-07-16 — spill is opt-in | Both halves landed in v1.3.0: (a) **durable deep-storage persistence + bootstrap reload** (see FG-6) — a restart no longer reloads nothing; and (b) an **opt-in spill-to-disk residency mode** (`--segment-spill` / `FERRODRUID_SEGMENT_SPILL`, default OFF; memory-budgeted LRU, `--segment-cache-bytes` default 1 GiB) that bounds resident memory. Default heap mode is unchanged: segments are held fully in-heap (~310 bytes/row measured on high-cardinality data, W-6 2026-07-13) and RSS crosses 4 GB at roughly 13 M such rows on one node — enable spill mode for larger working sets (prototype measured 5.82 GB → 138 MB at 20 M rows, 2026-07-13 host). The spill cache is a memory-offload tier wiped at startup; durability comes from deep storage, not spill files. |
| FG-8 | Numeric timestamps above `i64::MAX` are rejected | `[design]` | Apache Druid converts such a JSON number via Java `Number.longValue`, silently wrapping to a negative instant. FerroDruid refuses to store the wrapped value and dead-letters the record instead (fail-closed philosophy). |
| FG-9 | The default (no-`kafka-io`) build cannot validate Kafka client property **values** | `[design]` | Value validation requires librdkafka, which the default build does not link. A bad `consumerProperties` value is accepted at POST (with a loud WARN) and only surfaces when a `kafka-io` build starts the consumer, which then warn-skips the supervisor at startup until the spec is fixed. |
| FG-10 | **Attach existing Druid v9 segments: offline importer, single-binary; source = local dir or S3** | `[closed]` 2026-07-17 — documented limits (S3 source added 2026-07-20, W-C) | Landed 2026-07-17. `ferrodruid-migrate attach` imports an on-disk Apache Druid **v9** deep-storage tree so the segments become query-visible: it identifies each segment (partition-level `descriptor.json`, else the `<ds>/<interval>/<version>/<partition>` path), gates it on the real v9 reader, transcodes the blob into FerroDruid's flat deep-storage layout with a re-computed SHA-256, and writes a `used=true` metadata row — after which the next `serve` startup's bootstrap reload makes it queryable (verified end-to-end: attach → real `bootstrap_reload_segments` → the imported rows answer a timeseries with the expected aggregates). Ordering is per-segment persist-then-metadata (blob first, row second) so a crash never leaves a row referencing an absent/half-written blob (which would fail-loud-brick the next startup). **Limits:** (a) **single-binary only; the SOURCE may be a local directory or `s3://bucket/prefix`** (W-C, 2026-07-20: the s3 prefix is listed + staged into a private local tempdir through the product's own `S3DeepStorage`/object_store client — `AWS_*` env, `AWS_ENDPOINT`+`AWS_ALLOW_HTTP` for S3-compatible stores; `--datasource`/`--max-segments` applied at listing time; a `--dry-run` still downloads; per-object 64 GiB download cap; MinIO-docker E2E attach→bootstrap-reload→query verified; **real-AWS-S3 proving run 2026-07-20**: a genuine Druid 31.0.2 with `druid-s3-extensions` wrote 3 segments to a real `us-east-1` bucket — the interval-directory COLONS arrive INTACT in Druid's S3 keys and NO `descriptor.json` is written to S3, so identity came from the path fallback, which parsed the real layout unmodified; `assess` 3/3 readable, `attach` 3/3, and serve→SQL answered identically to Druid itself — COUNT/SUM exact, the full row scan set-identical, every per-day group exact) — the **TARGET FerroDruid deep storage stays LOCAL**, the distributed historical still speaks a JSONL toy format, not v9, and HDFS/GCS/Azure sources remain pull-to-local-first; (b) provenance here comes from `descriptor.json`/path; importing provenance from a Postgres/MySQL/SQLite Druid `druid_segments` metadata DB is the separate `import-druid-metadata` importer (FG-14); (c) **non-default v9 encodings** — a segment the v9 reader cannot decode (e.g. a Concise bitmap, or an `HLLSketch`/`quantilesDoublesSketch` complex column; a Druid **`thetaSketch`** column DECODES and is query-visible, verified vs real Druid 31 — compat-8 — and a Druid **`hyperUnique`** column now DECODES as well, estimates bit-exact vs real Druid 31.0.2 — W-A 2026-07-21, FG-17) is by default a loud whole-segment skip; the opt-in **`--allow-unreadable-columns`** flag (2026-07-18, verified against a real Druid 31.0.2 rollup segment with `hyperUnique`+`thetaSketch` columns) instead DROPS just the un-decodable column(s) and attaches the rest — loudly manifested in the report + `payload.droppedUnreadableColumns`, the segment re-written without the column and strict-re-opened before commit (so bootstrap reload can't brick), a query naming a dropped column behaving exactly as for a never-existed column. This unblocks migrating a rollup datasource's dimensions/`__time`/count/sums even when its sketch metric is one of the still-undecodable types (`HLLSketch`/`quantilesDoublesSketch`; `thetaSketch` and `hyperUnique` no longer need the flag); a segment losing `__time` or every dimension still fails loud; (d) the identity fields are **parsed, not trusted** — a present-but-malformed `descriptor.json`, a non-UTF-8/over-long/colliding identity, an interval that is not a pair of ISO-8601 instants, or a descriptor that vanishes mid-read is a loud per-segment skip rather than a wrong/duplicate metadata row; (e) **offline importer** — attach writes rows for the next startup to reload; there is no eager/no-restart attach; (f) attach E2E coverage: the committed E2E fixtures are FerroDruid-written v9 laid out in a Druid tree; a GENUINE Apache-Druid-written tree has now been exercised live by the W-C real-AWS-S3 proving run above (Druid 31.0.2 → real S3 → attach → bootstrap-reload → query, 2026-07-20) — one Druid version proven live, and the path-identity parse assumes Druid's conventional `<ds>/<interval>/<version>/<partition>` key layout (an unconventional layout is a loud per-segment skip, never a guessed identity; the metadata-DB importer FG-14 takes identity from the DB rows and is layout-immune). |
| FG-13 | **Metadata store: PostgreSQL + MySQL backends, single-writer-process only** | `[closed]` 2026-07-18 — documented limits | Landed 2026-07-18 (compat-6). `MetadataStore::connect(uri)` dispatches on URI scheme — `sqlite://`, `postgres://`, `mysql://` — via sqlx; select a backend with `ferrodruid serve --metadata-uri <URI>` (env `FERRODRUID_METADATA_URI`; the default is unchanged: `<data_dir>/metadata/ferrodruid.db`). Docker E2E-verified: the shared metadata-store test suite plus a `serve --metadata-uri` persist/reload round-trip on both **PostgreSQL 16** and **MySQL 8.0** (47 metadata tests total; `tests/metadata-compat`). **Limits:** (a) **the schema is FerroDruid's own** Druid-*shaped* schema (tables/columns modeled after Druid's), **not a bit-for-bit copy of Druid's metadata schema** — pointing FerroDruid at an existing Apache Druid Postgres/MySQL database does not make its rows visible; importing a genuine Druid metadata DB is a separate capability, now landed as the `import-druid-metadata` offline importer (FG-14); (b) **`datasource_publish_lock` is PROCESS-LOCAL** — it is an in-process mutex, not a database-level lock (e.g. no `SELECT ... FOR UPDATE`), so pointing more than one FerroDruid process (multiple Helm replicas, multiple classic-role instances, etc.) at the *same* shared Postgres/MySQL database does **not** give safe concurrent publish serialization; **single-writer-process remains the only supported topology**, shared-DB-as-HA is not a claim this closes; (c) **additive schema only** — `CREATE TABLE IF NOT EXISTS`, no migration/versioning machinery; (d) **no TLS configuration flags** — Postgres honors `?sslmode=` passed through the URI natively (rustls), but this is unclaimed/unverified surface, not a supported feature; (e) **MariaDB is untested** — only `mysql:8.0` was run; a `mariadb://` scheme URI is rejected; (f) **pool size is fixed at 4**, not operator-tunable; (g) **role binaries / classic per-role topology remain SQLite-only** — `--metadata-uri` is wired for `--mode single-binary` only; (h) the bundled AWS Marketplace Helm chart and CloudFormation artifacts have **not** been updated to expose this — they still target SQLite + local deep storage only, pending a separate chart/template update. |
| FG-14 | **Import an existing Druid metadata DB: offline importer, `local` + `s3_zip` loadSpecs, single-binary only** | `[closed]` 2026-07-18 — documented limits (s3_zip added 2026-07-20, W-C) | Landed 2026-07-18 (compat-7). `ferrodruid-migrate import-druid-metadata --source-uri <postgres\|mysql\|sqlite> --ferro-deep-storage <base> --metadata-uri <target> [--deep-storage <remap>] [--datasource D] [--dry-run] [--force] [--max-segments N]` reads an EXISTING Apache Druid metadata database's `druid_segments` (SELECT-only; the source DB is never written) and imports each `used=true` segment into FerroDruid by reusing the compat-2 attach persist→metadata tail (materialize → real v9 read gate → SHA-256 → durable blob → `used=true` row), so the next `serve` bootstrap-reload makes them queryable. Verified end-to-end: a crown SQLite-source `import → bootstrap_reload → timeseries` test (count/sum match; source DB byte-identical after), and the per-backend source reader against real **PostgreSQL 16 + MySQL 8.0** under docker; the Codex source-review gate dried over **12 rounds** (10 rounds of Critical/High fixes driven out — path containment, same-store-guard libpq semantics, payload-size DoS, dry-run mutation, terminal-escape injection, a root-symlink-swap TOCTOU, identity-from-columns — then R11+R12 clean). **Limits:** (a) **`local` + `s3_zip` loadSpecs** — an `s3_zip` row's `index.zip` is FETCHED directly from S3 (W-C, 2026-07-20: bucket+key from the row, validated as untrusted input; client from the `AWS_*` env through the product's own `S3DeepStorage`; streamed to a per-row staging tempdir, 64 GiB per-object cap, then the identical zip-cap/v9-gate/hash/P→M tail; a `--dry-run` still downloads; MinIO-docker E2E import→bootstrap-reload→query verified; **real-AWS-S3 proving run 2026-07-20**: 3 REAL Druid-31.0.2-written `s3_zip` metadata rows — verbatim coordinator payloads, incl. the extra `S3Schema` field, tolerated by the serde decode — imported 3/3 from a real `us-east-1` bucket, and serve→query answered set-identically to the Druid oracle; the Derby micro-quickstart was bridged by exporting its payload rows verbatim to SQLite — Derby itself remains unsupported, limit (c)). CAVEAT: the row names its OWN bucket, so the operator's credentials read whatever bucket the row names — scope credentials when the source DB is not fully trusted. `hdfs`/`google`/`azure` rows remain LOUD per-segment skips (pull the blob to a local dir and re-run with `--deep-storage`); (b) **`used = true` rows only**; rules / supervisors / config are counted in the report but NOT imported; (c) **PostgreSQL / MySQL / SQLite sources** — Derby has no Rust driver (externalize the Druid metadata to PostgreSQL/MySQL first); (d) **identity is derived from the row COLUMNS** (`dataSource`/`start`/`end`/`version`, the authoritative Druid identity) with the payload cross-checked (dataSource + version exact, interval compared as epoch-millis) — a disagreeing, malformed, over-long, or oversized (>16 MiB descriptor) row is a LOUD per-row skip, never a wrong/duplicate metadata row; (e) **`--deep-storage` is a MANDATORY containment root** for local loadSpec paths — an absolute path outside it, a `..`/symlink escape, or a non-UTF-8 path is refused; the entire-root symlink-swap TOCTOU is closed by canonical-path pinning, but a WITHIN-tree symlink race between the containment check and the file open is a documented residual (mitigated by `O_NOFOLLOW` + per-open re-verify; a full close needs `openat2`/fd-relative APIs unavailable under `#![forbid(unsafe_code)]`, shared with FG-10); (f) **`--dry-run` writes NOTHING** to the target (no schema creation on a fresh SQLite/PG/MySQL target — read-only `is_initialized()` gate); (g) the **source==target refusal is a best-effort footgun guard** — it resolves the connection target from libpq `hostaddr`/`host`/authority + loopback-folding + default ports + `dbname`/`user` precedence (matching sqlx for the common URI forms) but deliberately does NOT emulate exotic libpq features (multi-host lists, `service=` files, `passfile`, unix-socket `host=/path`, DNS-name-vs-IP), so for those the operator must ensure `--source-uri` ≠ `--metadata-uri`; (h) **`numbered_overwrite` + `range` shardSpecs** are imported; `tombstone` shardSpec/loadSpec (deletion markers) are out of scope (per-row skip); (i) same target scope as `attach` (FG-10) — **single-binary + local FerroDruid deep storage on the target side, offline** (restart to become query-visible). |
| FG-15 | **`timestampSpec.format` now honored on native batch; posix/nano/custom formats + some Joda edges are fail-closed residuals** | `[closed]` 2026-07-18 — documented residuals | Landed 2026-07-18 (compat-9 P0). **Fixed silent corruption:** native-batch (`index`/`index_parallel`) previously IGNORED `timestampSpec.format` and always parsed `auto`, so a declared format silently stored a WRONG instant (`format:"iso"` + `"2023"` → 2023 **ms** = 1970, not year 2023); it now threads the declared format through the ingester (correct for `auto`/`iso`/`millis`, matching Kafka/Kinesis) — **verified Druid-exact against a real Apache Druid 31.0.2 oracle** (`format:"iso"` reads `"2023"` → 2023-01-01, matching FerroDruid). A `transformSpec` carrying a real filter/transforms is now a LOUD task failure on native-batch (was silently dropped → rows ingested unfiltered), mirroring the streaming paths; the semantically-empty shapes (`null`/`{}`/empty arrays) stay accepted. **Documented residuals (fail-closed — loud reject, never silently wrong):** (a) `posix`/`nano`/`ruby`/arbitrary Joda `DateTimeFormat` patterns are consistently REJECTED across native-batch + Kafka + Kinesis (a spec declaring them fails at parse) — implementing them is future work; (b) `timestampSpec.missingValue` is not honored (a missing/unparseable timestamp row dead-letters rather than defaulting to the declared instant); (c) some Joda timestamp-grammar edges where Druid ACCEPTS but FerroDruid dead-letters: a colon-less UTC offset (`…T22:13-0800`), IANA `Area/Location` zone ids (`America/Los_Angeles` — needs a bundled tz db), and signed / whitespace-padded / ISO-year-only numeric strings under `auto` (`"-1"`, `" 2023 "`); (d) CSV `skipHeaderRows` IS now honored (2026-07-19) and CSV parsing does RFC 4180 quote handling (opencsv-parity: enclosing quotes stripped, quoted delimiters/`""` escapes), so quoted CSV parses like Druid; `findColumnsFromHeader` and `listDelimiter` (multi-value split, ties to FG-5/compat-11) are still not honored, and CSV cells are eagerly numeric-coerced (a zero-padded string dim `"007"` becomes `"7"`). Rollup emits metric sums under the metric OUTPUT name (not the source `fieldName`), matching Druid. **By-design fail-closed (NOT bugs):** numeric timestamps beyond `i64` range (FG-8) and Joda years beyond ±262 143 (chrono cannot represent) are rejected rather than silently wrapped/garbage; `Infinity`/`NaN` numeric-dim values map to NULL (NaN is the in-band null marker). Leap seconds and daylight abbreviations are rejected, matching Druid/Joda. |
| FG-16 | **Legacy null mode (`useDefaultValueForNull`): opt-in Druid ≤27 null semantics; oracle-unpinned surfaces keep ANSI under the flag** | `[closed]` 2026-07-21 — documented residuals | Landed 2026-07-21 (W-B, v1.5.0). An opt-in, startup-latched, process-global flag — TOML `useDefaultValueForNull` (Druid's own property name), env `FERRODRUID_USE_DEFAULT_VALUE_FOR_NULL`, or `serve --use-default-value-for-null`; **default OFF = ANSI, and with the flag off every path is byte-for-byte unchanged** (the full existing suite is the regression gate; the planner emits identical ANSI plan JSON) — switches to Apache Druid ≤27 legacy null semantics: string null ≡ `''` (one merged value), numeric null → `0`. Verified by a MEASURED diff battery against real **Druid 27.0.0** (legacy is its default) and **Druid 31.0.2** (ANSI): 52 captured oracle answer files per engine (`tests/segment-compat/fixtures/legacy_null_druid27/`), asserted by `crates/ferrodruid-rest/tests/legacy_null_sql_e2e.rs` (real ingest → historical → broker → `/druid/v2/sql` + `/druid/v2`) plus aggregator/ingest/latch suites — covering COUNT/SUM/AVG/MIN/MAX over all-null and partly-null LONG/DOUBLE/FLOAT columns, `''`≡null selector/IN/IS NULL filters, merged groupBy/topN keys and their ordering, approx AND exact COUNT DISTINCT (mirror-image legacy/ANSI cells, both matched), empty-set fold sentinels (SUM→0, longMin/Max→±i64 extremes, doubleMin/Max→the JSON STRINGS `"Infinity"`/`"-Infinity"`, AVG 0/0→0, preserved as broker merge identities), and the surface split (legacy renders the merged ''/null as `""` on the SQL wire but JSON null on native scan/groupBy/topN keys — mirrored). BOTH storage generations answer every cell identically, as Druid does: segments ingested UNDER the flag (legacy-coerced `''`/`0` literals, no null markers — matching the captured Druid-27 segment byte-shape) and ANSI-null-marker segments later served under the flag. Ingest coercion applies to native batch and, via the shared `BatchIngester`, to Kafka/Kinesis streaming; applies on the single-binary and role-split (broker/historical) execution paths. Real-binary smoke: `serve --use-default-value-for-null` answers the Druid-27 oracle cell-for-cell; the same binary without the flag answers the Druid-31 ANSI oracle. **Residuals:** (a) **oracle-unpinned surfaces deliberately keep ANSI behaviour under the flag** (no silent guesses): string-domain bound/like/regex/search filters over the merged ''/null value, expression-filter/virtual-column ''-edge cases, first/last, variance, stringAgg/arrayAgg, window functions, joins, lookups, datasketches columns fed from null cells, and MV `''` corners (MV rows keep ANSI reads); nonzero/0 division keeps ANSI null (only the measured 0/0→0 cell is gated); (b) legacy NON-STRICT BOOLEANS, the full compatibility-mode bundle, and the scan `legacy:true` envelope are out of scope; (c) **version floor**: the property is honored through Druid 31 and REMOVED fail-loud in Druid 32 (measured: 32.0.1 crash-loops on it), so "legacy" = "Druid ≤31 with the property" = "Druid ≤27 by default" — the canonical legacy oracle is Druid 27.0.0; (d) the new oracle surfaced **2 pre-existing ANSI divergences** (ANSI approx `COUNT(DISTINCT strcol)` over ''-bearing data: Druid 3 vs FerroDruid 4; ANSI `NOT (y = 10)` three-valued logic: Druid 2 vs FerroDruid 5) — recorded as a separate ANSI-parity follow-on, NOT fixed and NOT claimed fixed here; (e) the vectorized filter/groupBy/topN/timeseries fast paths BAIL to the row-map path under the flag — a migration-compat mode, not a perf mode (legacy perf twins are a follow-on); (f) the mode is process-global and startup-latched, like Druid's: flipping requires a restart, and legacy-written data later served by an ANSI process reads `''`/`0` as REAL values (exactly as in Druid after an upgrade without re-ingest; the separate `null_generation` detection warns); (g) the battery runs the single-binary in-process stack (per-segment execute + broker merge) — a multi-node legacy E2E has not been run; (h) rollup-under-legacy is derived from the pinned per-column coercion and unit-tested, without a rollup-specific Druid-legacy oracle fixture; (i) expression-in-aggregate SQL (`SUM(y+1)`, `SUM(COALESCE(y,-1))`) does not plan in ANY mode (pre-existing SQL-surface limitation — fails loud at planning; the oracle answers are already captured for when that lowering lands). Evidence: `tests/segment-compat/RESULTS_v150_wb_legacy_null.md` + `RESULTS_v150_oracle_env.md`. |
| FG-17 | **`hyperUnique` complex-column read: estimates bit-exact vs real Druid 31.0.2; the high-cardinality raw-HLL branch is literature-derived; `HLLSketch`/`quantilesDoublesSketch` still undecodable** | `[closed]` 2026-07-21 — documented residuals | Landed 2026-07-21 (W-A, v1.5.0). FerroDruid reads real Apache-Druid-written `COMPLEX<hyperUnique>` metric columns (Druid's classic pre-DataSketches HLL) — sparse AND dense register bodies, including buffer-relative sparse positions, zero-padded fold entries, and the estimator-inert max-overflow pair; the byte format and estimator were reverse-engineered strictly black-box from captured Druid-31.0.2 fixtures + public HLL literature (clean room — no Druid source read) — and answers native `hyperUnique` aggregations, the `hyperUniqueCardinality` post-agg, and SQL `APPROX_COUNT_DISTINCT` over them. **Measured bar:** the native double estimates are **bit-exact** (`to_bits()`-equal) against the captured Druid 31.0.2 oracle on every fixture — single-user, rollup-folded, dense ~700-distinct-per-blob, Druid's own sparse⊕dense fold, and the 6-segment multi-shard broker fold (the v1.1.1 bug-class shape) — and the rounded SQL integers are exact (Java `Math.round` = `floor(x+0.5)` semantics). Segmentation-invariance holds (rollup=false / rollup=day / 6-shard all answer `12.03529418544122`); arithmetic post-aggs over the estimate match (`events_per_uu` exact). A decoded Druid hyperUnique is a **separate, merge-only type** (it has no `add`; a raw-hash add is impossible at compile time) and is NEVER mixed with FerroDruid's own FNV HLL or with DataSketches `HLLSketch`: a genuine mix (raw column values, or a non-empty native sketch, vs a decoded feed — in the aggregator, the typed merge, or the broker JSON merge) produces a loud `mixError` envelope that absorbs further merges and finalizes to NO scalar (SQL renders NULL) — never a silently wrong estimate. Strict attach no longer skips a `hyperUnique` segment; lenient attach no longer drops the column. **Residuals:** (a) **the high-cardinality raw-HLL estimator branch is reproduced from the public HLL literature (Flajolet et al., 2007), not oracle-verified**: every captured fixture falls in the LINEAR-COUNTING regime (which is where bit-exactness is proven), so the `E_raw = α_m·m²/S` branch — its exact `α_m`, the `2.5·m` switch threshold, any high-range correction Druid may apply, and the f64 summation order — is un-pinned, and estimates for sketches with ≳2048 occupied registers (≈ tens of thousands of distinct per FOLDED sketch) may diverge from Druid in the last digits or bias constant; a mid/high-cardinality oracle capture would pin it; (b) **register-offset ≠ 0 fails loud** (observed `0x00` in every captured blob; semantics unverified — reached only at very high per-sketch cardinality); (c) **bit-exactness is libm-dependent**: glibc `ln` matches Druid's JVM on every oracle input here; a different libm could show a last-ULP residual in the UNROUNDED native double (the rounded SQL integer is safe) — the `to_bits` tests would surface it loudly; (d) **DataSketches `HLLSketch` and `quantilesDoublesSketch` complex columns remain undecodable** — loud rejection, or a lenient-attach column drop (FG-10(c)); (e) hyperUnique **write-back** (reverse migration) is out of scope and fail-loud in the native v9 writer (the FerroDruid-side v9/FDX round-trip uses its own validated codec); (f) the no-mix guard fires at aggregation/merge time (visible error envelope), not at plan time; (g) `bins/ferrodruid-migrate` doc strings still describe hyperUnique as unreadable — a messaging follow-up. Evidence: `tests/segment-compat/RESULTS_v150_wa_hyperunique.md` + `RESULTS_v150_oracle_env.md` (fixtures under `tests/segment-compat/fixtures/hyperunique_druid31/` + `hyperunique_sparse_druid31/`). |

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

| ID | Limitation | Severity | Status (v1.5.0) |
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
| TG-1 | Segment binary interchange | `[closed]` 2026-07-18 | Druid→FerroDruid v9 read measured green (2026-07-12, default encodings; non-default fail loud). FerroDruid→Druid **measured green** (2026-07-18): native v9 writer's output read back by Druid 31's own `dump-segment` (per-column SHA deep match) + byte-identical to real-Druid-31 fixtures, for LONG + single-value STRING (UTF-16-canonical dictionaries; all reader caps enforced). DOUBLE/FLOAT/nullable/MV/compressed writing = follow-on. |
| TG-2 | 24 h+ soak | `[v1.0 plan]` | **Open.** |
| TG-3 | TPC-H benchmarks | `[v1.0 plan]` | Q1/Q3/Q6 SF10 same-box: 12.0×/16.8×/5.5× faster than Druid 35, byte-identical (2026-07-11, conditions in §13); full suite / cluster / arm64 re-run pending. |
| TG-4 | Ecosystem clients | `[v1.0 plan]` | 4/6 validated; **no JDBC/Avatica** (TG-4-R1); Pivot untested (TG-4-R2). |
| TG-5 | Chaos testing | `[v1.0 plan]` | Consensus-safety layer only; no workload-level invariant; minutes-scale windows. |
| TG-6 | Continuous fuzzing | closed | 21 targets on a continuous farm + per-PR build check; freshness residual (TG-6-R1). |
| SL-1 / SL-2 | Scale performance / horizontal scaling | `[v1.0 plan]` | **Unvalidated** at 1 TB+ / 10K+ segments / 100+ QPS; fan-out and balancing under sustained load unvalidated. |
| FG-11 | **Native batch ingestion: `local`/`inline` inputSource, `json`/`csv`/`tsv`, synchronous, single-binary** | `[closed]` 2026-07-17 — documented limits | Landed 2026-07-17. A native-batch task (`POST /druid/indexer/v1/task`, `type` `index` or `index_parallel`) now actually EXECUTES instead of being accepted then parked PENDING forever: the overlord reads `ioConfig.inputSource`/`inputFormat`, enumerates the files, decodes rows, builds a segment, and publishes it through the durable deep-storage → `used=true` metadata row → Historical tail — verified end-to-end against the real binary (POST → `GET /task/{id}/status` SUCCESS → broker query returns the rows → restart → bootstrap-reload → rows still queryable). `GET /task/{id}/status` was added (every Druid client polls it) and survives restart via the persisted task row. **Limits:** (a) **inputSource: `local` + `inline` only** — `s3`/`http`/`hdfs`/`druid`/`azure`/`google` are a terminal `FAILED` with a clear "unsupported inputSource" message (NOT a silent hang), and `local` is `baseDir` + a `*`/`?` glob `filter`, **non-recursive**, no `files` array; (b) **inputFormat: `json` (JSONL) / `csv` / `tsv`** are first-class; `parquet`/`avro_ocf` decode through the same path but drop `flattenSpec`/nested extraction (top-level columns only); `avro_stream` is a terminal failure (needs an external reader schema); (c) **asynchronous submission (Druid-parity, 2026-07-19)** — `POST /task` now persists the RUNNING row + acquires locks + spawns a tracked background execute→publish tail and returns `{"task":"<id>"}` IMMEDIATELY; the client polls `GET /task/{id}/status` for RUNNING→SUCCESS/FAILED (matching Apache Druid; the earlier synchronous-submit form that ran the whole ingest+publish inside the HTTP request is gone). A lock-conflicted task queues on the interval lock (Druid `taskLockTimeout`-style, default 300 s) rather than failing. The publish verdict (SUCCESS/FAILED) is fence-derived and resolved from durable state on an abort/deadline/crash, so every terminal-transition path (`shutdown_task`/`transition_task`/`lose_worker`) and a bootstrap reconcile report the TRUTHFUL status and a committed `appendToExisting` is never mis-reported FAILED-then-resubmit-duplicated; (d) **one wide segment per task** — `BatchIngester` emits a single segment spanning `[min_ts, max_ts]`; there is no `segmentGranularity` bucketing (queries are correct; the on-disk segment layout differs from Druid); (e) **at-least-once, hardened toward exactly-once (2026-07-19)** — a crash strictly between segment publication and the terminal task-record commit leaves the row `RUNNING` with data already published, but the **next startup's bootstrap reconcile correlates such a RUNNING row against its committed segment (batch-provenance-gated) and finalizes it SUCCESS** (or FAILED if nothing committed), so a client polling status sees the truth and an `appendToExisting` commit is not re-submitted-as-duplicate; a **default (replace-mode) retry is idempotent** for the interval regardless. Publish I/O is deadline-bounded and every persist is timeout-bounded so a hung metadata store can't strand a task. The remaining duplication exposure is a client that LOSES the returned task id and resubmits (FerroDruid, like Druid, does not accept a client-supplied task id) and the fundamental unresolvable-Kafka-cluster-identity window (bounded re-consume, documented in FG-6); (f) a nullable `LONG` dimension is now stored as a first-class nullable-i64 column (`LongNullable` = i64 values + a null-row bitmap), so **NULL and values beyond 2^53 coexist EXACTLY** — the earlier degrade-to-`DOUBLE`-with-NaN (and its precision loss above 2^53) is gone as of compat-10 (2026-07-18, verified against a real Druid 31.0.2 nullable-LONG segment holding 9007199254740993; the v9 reader also now returns such Druid columns exactly instead of a lossy Double). Query correctness (SUM/AVG/COUNT/COUNT DISTINCT/group-by) for nullable longs routes through the null-faithful, i64-exact slow path; the vectorized `LongSum` fast-path is not yet extended to `LongNullable` (a performance-only follow-on — results are correct via fallback), and the Kafka streaming path still fail-closes a NULL-vs-`>2^53` conflict pending a rewire onto `LongNullable`; (g) local input files must be **stable during ingestion** — a symlink/rename/FIFO/truncate/rewrite of an enumerated file during the read is rejected fail-loud (O_NOFOLLOW + (device,inode,size,mtime,ctime) identity + containment under `baseDir`), never followed outside `baseDir` or silently published partial; (h) **only `index`/`index_parallel` have an in-process executor** — other task types (`kill`/`compact`/…) are not implemented and, if submitted with `ioConfig.intervals`, park PENDING holding an interval-reservation lock (the un-wired distributed task path). |
| FG-12 | **Kinesis streaming ingestion: real consume, `kinesis-io`-gated, single-task, no resharding** | `[closed]` 2026-07-18 — documented limits | Landed 2026-07-18 (was an explicit stub, de-claimed 2026-07-14). A `kinesis` supervisor now actually consumes: `ferrodruid-ingest-kinesis` (a `KinesisSource` trait + an `aws-sdk-kinesis` adapter behind the `kinesis-io` feature) polls `ListShards`/`GetShardIterator`/`GetRecords`, decodes records, and publishes through the SAME durable streaming tail as Kafka (deep-storage → `used=true` metadata row → Historical). Durability = per-shard **sequence-number** checkpoints in `payload.kinesisSequences` with the compat-3 P→M→swap ordering; the resume frontier is **anchor-rooted** — it only skips past coverage chained from a genuine start anchor (`prev_last=None`), and re-consumes from `TRIM_HORIZON` on any un-anchored / malformed / non-object / empty / recreated-stream evidence (at-least-once, never loss). E2E-verified against LocalStack (free): consume → restart-durable → resume-with-new-records (zero loss, no duplication). **Limits:** (a) **`kinesis-io`-gated** — the default and static-musl builds do NOT consume Kinesis (a POST is validated + persisted + loud-warned, never a silent no-op), so no shipped artifact consumes until a release build enables the feature (same posture as `kafka-io`/FG-6); (b) **single consumer task** polls all shards — `taskCount` is ignored (no task scheduler); (c) **no live resharding** — the shard set is snapshotted at start; a closed parent shard warns loudly that a **supervisor restart** is required to pick up child shards, and if not restarted before the retention window expires the child records are trimmed and lost; (d) **`awsAssumedRoleArn` parsed but not implemented** — the default AWS credential chain (env/profile/instance role) is used; both (b) and (d) warn at start; (e) **durability requires `deep_storage`** — a memory-only supervisor loud-warns it is non-durable and defaults to `TRIM_HORIZON` (re-consume on restart: duplication over loss); (f) classic `GetRecords` polling only (no enhanced fan-out / `SubscribeToShard`); each RPC is bounded by a timeout (a hung endpoint cannot freeze the loop); (g) **LocalStack-verified** — the real SigV4/credential-chain against the live AWS endpoint is billable and intentionally not smoke-tested. |
| FG-1..FG-17 | Feature gaps | mixed | See §15 — notably **no automatic compaction** (FG-2) and no nested columns (FG-3). Kafka ingestion durability (FG-6) and deep-storage persistence + opt-in spill residency (FG-7) **closed in v1.3.0** with documented residuals; the offline Druid-v9 segment attach importer (FG-10), native batch `local`/`inline` ingestion (FG-11), real Kinesis streaming consume (FG-12), the PostgreSQL/MySQL metadata store backend (FG-13, single-writer-process only), and the offline Druid metadata-DB importer (FG-14, `import-druid-metadata`, `local` + `s3_zip` loadSpecs, single-binary only) landed 2026-07-17/18 (single-binary / `kinesis-io`-gated / `--metadata-uri`, documented limits); `s3://` sources for `attach`/`assess` and `s3_zip` fetch for the importer landed 2026-07-20 (W-C, MinIO-docker E2E + real-AWS-S3 proving run against a genuine Druid-31-written tree, recorded at FG-10/FG-14). **v1.5.0 (2026-07-21):** opt-in legacy null mode `useDefaultValueForNull` (FG-16, verified vs real Druid 27.0.0 + 31.0.2) and `hyperUnique` complex-column read (FG-17, estimates bit-exact vs real Druid 31.0.2) landed, each with documented residuals. |
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
