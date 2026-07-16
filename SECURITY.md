<!-- SPDX-License-Identifier: BUSL-1.1 -->
<!-- Copyright 2026 abyo software 合同会社 (abyo software LLC) -->

# Security Policy

This document describes FerroDruid's threat model, the security
controls shipped as of v1.2.0 (most were introduced in the v0.2.0 GA
hardening waves), and how to report vulnerabilities.

> **Cross-role HTTP wire posture (W1-I, 2026-06-30).** The `classic`
> 6-role topology carries four cross-role HTTP wires (`router →
> broker`, `broker → historical`, `coordinator → historical`,
> `overlord → middlemanager`). **W1-I makes mTLS the default for those
> wires**: every per-role binary defaults to
> `--cross-role-mtls=required`, the receiving role's TLS listener
> demands a client cert signed by the configured CA bundle, and the
> outbound `reqwest::Client` presents the role's leaf cert when
> dialling peers. See the "Cross-role HTTP wire (mTLS default)" section
> below for the migration runbook (`permissive` mode + the
> `ferrodruid-migrate gen-cross-role-certs` helper). The single-binary
> HTTP API is unchanged (Basic / Argon2id, on by default; intra-process
> Arc-handle calls between roles bypass HTTP entirely — see CL-G2).

## Supported Versions

| Version | Supported |
|---|---|
| 1.2.x | Yes (current source release) |
| 1.1.x | Yes (AWS Marketplace AMI / container) |
| < 1.1 | No — upgrade |

## Threat Model (summary)

### Trust assumptions (v0.x)

- The cluster TCP port (default `:18888`) is reachable only from other
  cluster members. Operators are expected to run cluster members on a
  private network or with a host firewall.
- The metadata DB (SQLite / Postgres / MySQL) is trusted: connection
  string credentials are protected by the operator.
- Deep storage (local filesystem or S3) is trusted at the IAM /
  filesystem-permissions layer. Bucket policies are the operator's
  responsibility.
- Operators run a single trusted version of `ferrodruid` per cluster.
  Mixed-version clusters are not in the v0.x trust model.

### In-scope attack surfaces

| Surface | Protocol | Default authentication |
|---|---|---|
| HTTP API | HTTP/1.1 (TLS via reverse proxy) | Basic (Argon2id); auth on by default. Bearer parsed but rejected (see below) |
| Cluster wire | TCP framed JSON + HMAC, wrapped in TLS by default | **mTLS required by default** (clustered mode refuses to start without certs); PSK-over-cleartext only by explicit opt-in (`--cluster-security psk`) |
| Metrics | HTTP `/metrics` | Exempt from auth (Prometheus scrape) |
| Health probes | HTTP `/status/health`, `/status/live` | Exempt from auth |
| Kafka ingestion | Kafka wire | Operator-configured SASL/TLS via `rdkafka` |
| Deep storage | Local FS / S3 | IAM / file ACLs |

### Out of scope (v0.x — clearly disclosed)

- **Confidentiality of the cluster wire under the PSK opt-in**: the
  default posture is mTLS — clustered mode refuses to start without
  certificates rather than silently downgrading. If an operator
  explicitly opts into PSK-over-cleartext (`--cluster-security psk`),
  frames are authenticated and integrity-protected but **not**
  confidential. See "Cluster wire" below.
- **Formal Jepsen-grade linearizability proof**. We have a 3-node TCP
  test suite (W38-A, W40-A, W44B) and pre-vote / heartbeat-driven
  failover (Wave 38-D), but no Jepsen run.
- **FIPS 140-2/3 validated cryptography**. The path exists
  (`aws-lc-rs`); not enabled in v0.x.
- **Client-cert revocation (CRL/OCSP) checking** in mTLS mode.
- **PSK rotation without restart** (see Operations notes below).

## Authentication

- **Basic auth** (RFC 7617): credentials are stored as Argon2id hashes
  via `crates/ferrodruid-auth`. Argon2id default parameters are the
  `argon2` crate defaults.
  - **Bootstrap admin** (Wave 36-A): on first launch, if no users exist
    and `<data_dir>/.bootstrap_done` is absent, the binary generates a
    random 32-character alphanumeric admin password, prints it once to
    stderr, and writes the marker file. Operators must capture it.
  - **Username-enumeration timing oracle** is closed by a sentinel
    Argon2 verify on the missing-user path (Wave 42-B).
  - **Scheme matching is case-insensitive** (RFC 7235 §2.1) — `basic `,
    `Basic `, `BEARER `, etc. all parse correctly (Wave 42-B).
  - **Auth header redaction**: `parse_auth_header` never echoes the raw
    `Authorization` value into errors or logs (Wave 42-B).
- **Bearer token**: parsed by `parse_auth_header` (RFC 7235 case-
  insensitive) but **explicitly rejected** by the active middleware
  (`crates/ferrodruid-rest/src/middleware.rs:231`,
  `AuthMethod::Bearer { .. } => 401`). Basic is the only currently-
  supported scheme. Bearer is parsed (rather than ignored) so that
  malformed headers still surface a clear `401` instead of silently
  passing through. Bearer support — either local-store validated or
  delegated to a FerroAuth-style IdP — is on the v1.0+ roadmap if
  customer demand warrants it; see `docs/known-limitations.md` CL-G3.

Auth is **on by default**. The binary refuses to bind a non-loopback
address with `--no-auth` unless `--allow-insecure-public-bind` is
passed (fail-closed; Wave 36-A).

## Authorization (RBAC)

- `crates/ferrodruid-authz` implements per-resource RBAC across three
  resource types: `DATASOURCE`, `CONFIG`, `STATE`, with `Read` /
  `Write` actions.
- `AppState.authorizer` is wired into an Axum middleware
  (`authz_middleware`, Wave 40-C) that runs after authentication. The
  default policy table (`build_default_policy`) covers ~30 routes
  spanning query, ingestion, supervisor, MSQ, coordinator config, and
  status. The policy is **default-deny**: a route with no matching
  rule returns `403`.
- **Public path allowlist**: `/status/health`, `/status/live`, and
  `/metrics` short-circuit both `auth_middleware` and `authz_middleware`
  so that orchestrator probes and Prometheus scrape work without policy.

## Web Console (browser access + CSRF)

The server-rendered console (`/`, `/unified-console.html`, `/console/*`)
is gated by `STATE:Read` like any other route.

- **Browser login**: unauthenticated responses carry
  `WWW-Authenticate: Basic realm="FerroDruid"` so a browser prompts for
  credentials and then auto-attaches them to same-origin requests. On a
  fresh install the bootstrap admin is flagged *must-change-password*, so
  the console becomes usable only **after** the initial password is
  rotated (see Authentication above).
- **Output escaping**: all dynamic values the console renders (datasource /
  segment / task / supervisor fields, SQL column names and cell values, and
  the reflected `?ds=` parameter) are HTML-escaped before insertion, so
  ingested or reflected data cannot inject script (XSS).
- **CSRF protection**: state-changing requests (`POST`/`PUT`/`DELETE`/
  `PATCH`) whose `Origin` header does not match the server's own authority
  are rejected with `403`. Non-browser clients (curl, pydruid, server-to-
  server) send no `Origin` and are unaffected, preserving the Druid wire
  contract. `X-Forwarded-Host` is deliberately **not** trusted (page JS can
  set it).
  - **Reverse-proxy requirement**: a fronting proxy MUST preserve the
    original `Host` header, otherwise the browser's `Origin` will not match
    the rewritten `Host` and every authenticated console write is `403`'d.
    **AWS ALB preserves `Host` by default; nginx requires
    `proxy_set_header Host $host;`.** GET/console page loads are unaffected —
    only mutating actions fail — so verify a write (e.g. run a SQL query in
    the console) after fronting the service.
  - **Residual**: this is the standard OWASP Origin-verification pattern and
    relies on browsers sending `Origin` on cross-site writes (all current
    mainstream browsers do; the opaque `null` origin is blocked). For
    defense-in-depth, keep the console behind a trusted reverse proxy /
    network policy rather than exposing it directly.

## Cluster Wire (mTLS by default + PSK frame auth)

### PSK frame authentication (always on; cleartext transport is an explicit opt-in)

Every cluster TCP frame is wrapped as
`[u32 BE len][32-byte HMAC-SHA256][JSON payload]`. The HMAC is keyed
with the cluster PSK and verified in constant time
(`subtle`-backed `hmac::Mac::verify_slice`).

The first frame on every connection is a `HandshakeFrame` carrying an
`announced_node_id`. Every subsequent frame's `declared_sender_id()`
must equal the announced id, otherwise the frame is dropped with a
`warn!`. This **connection binding** prevents an authenticated peer
from impersonating another node id within the same TCP session.

PSK key distribution:

```bash
# Generate a fresh PSK (32 bytes / 64 hex)
head -c 32 /dev/urandom | xxd -p -c 64
```

The PSK is supplied via `--cluster-psk <hex>` or
`FERRODRUID_CLUSTER_PSK=<hex>` and is never logged (`ClusterPsk::Debug`
is redacted). Starting `ferrodruid` with `--cluster-peers` set but no
PSK is a fatal error.

Defends against:

- Network adversaries with reach to the cluster TCP port but no PSK
  (frames rejected at handshake or HMAC verify).
- An authenticated peer attempting to forge `ReplicateAck` /
  `VoteRequest` / `Heartbeat` as a different node id over the same
  session (rejected by connection binding).

Does not defend against:

- Eavesdropping, when the operator has explicitly opted into
  cleartext transport with `--cluster-security psk` (PSK provides
  integrity, not confidentiality — the mTLS default provides both).
- Host compromise that exposes the PSK on disk / in env / in process
  memory.
- Replay of frames *inside* the same authenticated connection. Closed
  by application-layer dedup: `voted_for` write-once latch and
  `match_index` monotonic advance (Wave 38-A).

### mTLS (the default posture; Waves 44 + 44B / Phase 2.4)

The `cluster-tls` Cargo feature is in the default feature set and the
runtime default is mTLS: starting clustered mode without
`--cluster-tls-{cert,key,ca}` is a fatal error unless the operator
explicitly passes `--cluster-security psk`. Pass three PEM paths to
wrap the cluster TCP wire in TLS 1.2/1.3 with mandatory client-cert
verification (`tokio-rustls` 0.26 + `rustls` 0.23):

```bash
cargo build --release --features cluster-tls

ferrodruid serve \
    --mode single-binary \
    --node-id node-1 \
    --cluster-bind 10.0.0.1:18888 \
    --cluster-peers 'node-2@10.0.0.2:18888,node-3@10.0.0.3:18888' \
    --cluster-psk "$(head -c 32 /dev/urandom | xxd -p -c 64)" \
    --cluster-tls-cert /etc/ferrodruid/node-1.pem \
    --cluster-tls-key  /etc/ferrodruid/node-1.key \
    --cluster-tls-ca   /etc/ferrodruid/ca.pem
```

- All three flags must be set together (partial set → fatal at
  startup).
- PSK is still required in mTLS mode — TLS is defence in depth.
- Wire is verified end-to-end by
  `case_k_wire_bytes_are_tls_encrypted` (peek-proxy asserts
  TLS handshake bytes and absence of cleartext JSON tokens).

## Cross-role HTTP wire (mTLS default; W1-I, 2026-06-30)

The `classic` 6-role topology runs `ferrodruid-broker`,
`ferrodruid-historical`, `ferrodruid-coordinator`, `ferrodruid-router`,
`ferrodruid-overlord`, and `ferrodruid-middlemanager` as separate
processes. Pre-W1-I they talked to each other over plain HTTP. W1-I
makes mTLS the **default** posture:

- The receiving role's HTTP listener is bound via
  `axum_server::bind_rustls`, with a
  `WebPkiClientVerifier::builder(...).build()` that **requires** the
  client cert to chain to the configured CA bundle. Connections
  without a valid client cert are dropped at the TLS handshake.
- The outbound `reqwest::Client` is built with
  `.use_rustls_tls().identity(...).add_root_certificate(...)` so the
  role presents its leaf cert when dialling peers and validates the
  peer's server cert against the same CA bundle.
- The mode is selected by `--cross-role-mtls=<mode>` (or
  `FERRODRUID_CROSS_ROLE_MTLS=<mode>` env var). `required` is the
  default; `permissive` and `disabled` are operator-explicit
  downgrades for migration / back-compat.

The single-binary `ferrodruid serve` path is unaffected: it issues
intra-process `Arc`-handle calls between roles (CL-G2 `[design]`) and
does not bind a cross-role wire at all. mTLS adds zero overhead to
single-binary deployments.

### Modes

| mode | binds plain HTTP | binds TLS | client cert required |
|------|------------------|-----------|----------------------|
| `required` (default) | no | yes | yes |
| `permissive` | yes | yes | no (TLS listener accepts with-or-without cert) |
| `disabled` | yes | no | n/a |

The TLS port defaults to `port + 1000` (matching Apache Druid's
`tlsPort` convention) so the canonical role ports become:

| role | plain port | TLS port |
|------|------------|----------|
| coordinator | 8081 | 9081 |
| broker | 8082 | 9082 |
| historical | 8083 | 9083 |
| overlord | 8090 | 9090 |
| middlemanager | 8091 | 9091 |
| router | 8888 | 9888 |

Pass `--tls-port=<port>` to override the default.

### Cert layout

Each role binary expects its credentials under
`<data_dir>/cross-role/`:

```text
<data_dir>/cross-role/
  ca.pem          # CA bundle used to verify peer certs (server + client side)
  leaf.pem        # this role's leaf cert chain (presented to peers)
  leaf.key        # this role's private key matching leaf.pem (mode 0600)
```

To use a different location, pass `--cross-role-tls-cert`,
`--cross-role-tls-key`, and `--cross-role-tls-ca` explicitly (all three
must be set together).

### Bootstrap helper

For dev / staging, `ferrodruid-migrate gen-cross-role-certs` writes a
self-signed CA + per-role leaf bundle:

```bash
ferrodruid-migrate gen-cross-role-certs \
    --out-dir /tmp/ferro-certs \
    --roles broker,historical,coordinator,router,overlord,middlemanager \
    --extra-sans localhost,127.0.0.1
```

The helper writes `/tmp/ferro-certs/ca.pem`, `/tmp/ferro-certs/ca.key`,
and per-role `<role>/leaf.pem` + `<role>/leaf.key` (private keys mode
`0600`). To install, copy `ca.pem` and the per-role leaf files into
each role's `<data_dir>/cross-role/` (rename `<role>/leaf.pem` and
`<role>/leaf.key` to just `leaf.pem` / `leaf.key`).

Production deployments should NOT use this helper — issue certs from
an operator-managed CA (cert-manager, HashiCorp Vault, your existing
PKI).

### Migration runbook (upgrading a running v0.2.0 cluster)

Goal: take a running 6-role v0.2.0 cluster from unauthenticated
cross-role HTTP to mTLS-required without coordinated downtime.

1. **Pre-flight: provision certs.** Run
   `ferrodruid-migrate gen-cross-role-certs` (or your CA) and stage
   the per-role bundles under each role's `<data_dir>/cross-role/`.
   Verify `cargo run --bin ferrodruid-migrate -- gen-cross-role-certs
   --out-dir /tmp/staging-certs --roles broker` produces a bundle that
   validates with `openssl verify -CAfile <ca.pem> <leaf.pem>`.
2. **Phase 1: flip every role to `--cross-role-mtls=permissive`.** A
   rolling restart of each role with the new flag binds BOTH the
   existing plain HTTP port AND a new TLS port. Peers still talking
   plain HTTP keep working through the plain port; peers that have
   been upgraded to dial the TLS port start using TLS. There is no
   coordinated cut-over window.
3. **Phase 2: flip outbound peer URLs to `https://` + the TLS port.**
   For each role with outbound peers (`--broker-url`,
   `--historical-url`, `--middlemanager-url`), edit the comma-
   separated URLs in the role's config / env / supervisor unit to use
   the TLS scheme + port. A rolling restart applies the change.
4. **Phase 3: flip every role to `--cross-role-mtls=required`.**
   Another rolling restart drops the plain HTTP listener; only the
   TLS listener remains. The plain port is no longer bound.
5. **Verify.** From each role's host, attempt `curl
   http://<role>:<plain_port>/...` and confirm "connection refused".
   Run a single SQL query through the router → broker → historical
   chain and confirm it returns rows; if mTLS is mis-wired the
   request fails at the TLS handshake, not silently.

If a Phase 2 or Phase 3 step fails (e.g. an outbound peer cannot
present a client cert), revert that role to Phase 1 (permissive) — the
plain HTTP listener accepts the legacy peer while you diagnose. Do
not run a steady-state cluster in permissive mode.

### Honest residuals

- **Real-LAN multi-VM validation**: the **broker → historical wire** is
  closed for 3-host EC2 (W1-L 2026-06-30,
  `tests/cross-host/RESULTS_w1l_3host_2026-06-30.md` Phase 2: happy
  path HTTP 200, fail-closed `tlsv13 alert certificate required`). The
  remaining three cross-role wires (**router → broker**,
  **coordinator → historical**, **overlord → middlemanager**) are
  loopback-only validated (`crates/ferrodruid-rpc/tests/cross_role_mtls_e2e.rs`)
  and use the same `axum_server::bind_rustls` + `reqwest::Identity` +
  `WebPkiClientVerifier` code paths. Their multi-host validation
  reuses W1-L's fleet template and is tracked as the same CL-J1-R1
  follow-up.
- **Cert revocation (CRL / OCSP)** is not consulted on the cross-role
  wire, mirroring the cluster-wire CL-B5 limitation. Operators must
  re-issue the CA / leaf bundle to revoke a compromised peer.
- The **broker → coordinator** wire (rules / lookups / datasource
  metadata) is not yet implemented as an outbound HTTP client (broker
  reads only what request context supplies). When that wire lands, it
  will reuse the same `cross_role_tls` infrastructure without further
  work. Tracked as CL-J1-R2.

## Rate Limiting

`rate_limit_middleware` (Wave 36-B) is layered as the outermost
middleware: per-IP token-bucket admission with default
`max_concurrent = 100`. Excess requests return `429 Too Many Requests`
before any auth verify CPU is spent. `/status/health`, `/status/live`,
and `/metrics` are exempt (Wave 40-C).

Configurable via `RateLimitConfig.max_concurrent` in the TOML config or
`AppState.rate_limit_max_concurrent`. Setting `max_concurrent = 0`
disables the limiter (kill-switch for tests).

## Segment Integrity

Hostile or corrupted on-disk segments cannot crash, OOM, or silently
deceive a Historical:

- **Crash-safe writer** (Wave 36-D / W40-B): `SmooshWriter::write_to_dir`
  writes each chunk to `<final>.tmp.<pid>.<ctr>.<nanos>`, fsyncs the
  file, atomically renames into place (chunks first, then
  `meta.smoosh`), and finally fsyncs the parent directory.
- **Bounded reader allocations** (W36-D / W36-E / W36-G4 / W45-A):
  every untrusted count from `index.drd` and string columns is
  validated against an explicit cap before any `Vec::with_capacity`
  call (`num_dimensions`, `num_metrics`, `num_rows`, `dict_len`,
  `num_bitmaps`, per-bitmap `bm_len`).
- **v9 / FDX column error propagation** (Wave 36-E): all column
  decoders return typed `DruidError::Segment` errors instead of
  panicking on truncated input.
- **Default-deny on unmapped segment query** (Wave 36-D): a segment
  that has been loaded but whose datasource mapping has not yet
  committed is excluded from any datasource-targeted query, closing
  the publish-time race window.
- **Atomic ingestion publish path** (W40-B): the Overlord flow
  publishes deep-storage upload + metadata commit before the segment
  is announced to query serving.

## Reporting Security Issues

**Email**: aws-support@abyo.net

Please include:

1. Description of the vulnerability and a clear impact statement
2. Minimal reproduction steps
3. Affected component (crate path, route, or wire frame)
4. Suggested fix, if any

**Do not** open a public GitHub issue, post details on social media,
or exploit the issue beyond the minimum needed to demonstrate it.

### Response timeline

| Milestone | Target |
|---|---|
| Acknowledgment | Within 48 hours |
| Initial assessment | Within 5 business days |
| Fix development (Critical/High) | Within 14 days |
| Fix release (Critical/High) | Within 30 days |
| Public disclosure | After fix release, coordinated with reporter |

### Severity classification

| Severity | Description |
|---|---|
| Critical | Remote code execution, auth bypass, data exfiltration |
| High | Privilege escalation, denial of service, data corruption |
| Medium | Information disclosure, configuration weakness |
| Low | Minor issues, defense-in-depth improvements |

Reporters who follow responsible disclosure are credited in the
release notes (unless they prefer to remain anonymous).

## Defence-in-depth posture

- `#![forbid(unsafe_code)]` in every crate. SIMD opt-ins (where they
  exist) are <50 LOC with `// SAFETY:` comments.
- Zero `unwrap()` / `expect()` in non-test code (CI-enforced).
- Zero `TODO/FIXME/HACK` markers (CI-enforced).
- TLS via `rustls` only (no OpenSSL).
- Parameterised SQL via `sqlx`; no string interpolation into queries.
- Supply chain: `cargo deny check advisories` runs in the public CI
  (every accepted exception is documented with rationale and a review
  date in `deny.toml`); `cargo audit` additionally runs in the
  vendor's private pipeline. CycloneDX SBOM emitted per release.
- Dependency licence policy: closed allowlist in `deny.toml`
  (permissive licenses plus MPL-2.0; BUSL-1.1 only for the
  first-party workspace crates); GPL / AGPL / LGPL / SSPL denied.

Detailed audit evidence is available to customers on request
(aws-support@abyo.net).
