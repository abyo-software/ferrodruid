// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Rate-limiting, authentication, and authorization middleware for the
//! FerroDruid REST API.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ferrodruid_auth::{AuthMethod, AuthStore, AuthenticatedUser, parse_auth_header};
use ferrodruid_authz::{Action, Authorizer, ResourceAction, ResourceType};
use ferrodruid_telemetry::Metrics;
use parking_lot::RwLock;
use std::sync::{Arc, LazyLock};

/// Caps concurrent Argon2id password verifications.
///
/// Argon2id is deliberately memory-hard (~tens of MiB and tens of ms per
/// verify). Because [`AuthStore::verify`] pays a full hash even for unknown
/// usernames (the timing sentinel), an unauthenticated flood of
/// `Authorization: Basic <anything>` requests would otherwise run one
/// memory-hard hash per in-flight request — pinning every core and spiking
/// RAM on a small instance, a pre-auth DoS. Bounding concurrency to the
/// available parallelism caps the cost of such a flood while keeping
/// legitimate logins responsive; the hash itself runs on
/// `spawn_blocking` so it never blocks the async worker threads.
static VERIFY_SLOTS: LazyLock<tokio::sync::Semaphore> = LazyLock::new(|| {
    let permits = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4);
    tokio::sync::Semaphore::new(permits)
});

/// Simple concurrency-based rate limiter (token-bucket style).
///
/// Limits the maximum number of concurrent query executions. When the limit is
/// reached, new requests receive a 429 Too Many Requests response.
///
/// A `max_concurrent_queries` of `0` disables the limiter entirely:
/// every request is admitted.  This is the explicit kill-switch the
/// `RateLimitConfig` exposes for sealed-network test rigs.
pub struct RateLimiter {
    max_concurrent_queries: u64,
    current_queries: AtomicU64,
}

impl RateLimiter {
    /// Create a new rate limiter with the given concurrency cap.
    ///
    /// `max_concurrent_queries == 0` is interpreted as "rate limiting
    /// disabled"; [`Self::try_acquire`] always returns `true`.
    #[must_use]
    pub fn new(max_concurrent_queries: usize) -> Self {
        Self {
            max_concurrent_queries: max_concurrent_queries as u64,
            current_queries: AtomicU64::new(0),
        }
    }

    /// Returns `true` if rate limiting is disabled (cap is `0`).
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.max_concurrent_queries == 0
    }

    /// Try to acquire a query slot. Returns `true` if the slot was acquired.
    pub fn try_acquire(&self) -> bool {
        if self.is_disabled() {
            return true;
        }
        let prev = self.current_queries.fetch_add(1, Ordering::AcqRel);
        if prev >= self.max_concurrent_queries {
            self.current_queries.fetch_sub(1, Ordering::Release);
            return false;
        }
        true
    }

    /// Release a query slot.
    pub fn release(&self) {
        if self.is_disabled() {
            return;
        }
        self.current_queries.fetch_sub(1, Ordering::Release);
    }

    /// Returns the current number of in-flight queries.
    #[must_use]
    pub fn current(&self) -> u64 {
        self.current_queries.load(Ordering::Acquire)
    }
}

/// Axum middleware layer that enforces rate limits.
///
/// When the server is at capacity, returns a Druid-compatible 429 error response.
///
/// Wave 40-B (Wave 39 [High] [NEW-VARIANT]): liveness / readiness / metrics
/// probes are exempt from the rate limit so a saturated node does not 429 its
/// own k8s probes — that would re-create the false-negative-health behaviour
/// that the Wave 36-B real-readiness fix was meant to eliminate. The exempt
/// set is the same one [`is_public_path`] uses for the auth bypass.
pub async fn rate_limit_middleware(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    // Probes / metrics endpoints are observability-critical and must remain
    // reachable even when the data-plane rate limit is saturated.
    if is_public_path(request.uri().path()) {
        return next.run(request).await;
    }

    if !limiter.try_acquire() {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            axum::Json(serde_json::json!({
                "error": "Too many requests",
                "errorMessage": "Server is at capacity. Please retry later.",
                "errorClass": "io.druid.server.QueryCapacityExceededException"
            })),
        )
            .into_response();
    }

    let response = next.run(request).await;
    limiter.release();
    response
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

/// Shared state for the auth middleware.
///
/// Holds the in-memory `AuthStore` plus a kill-switch.  When `enabled` is
/// `false` the middleware passes every request through unchanged — used by
/// loopback test rigs and explicitly opted-in insecure deployments.
///
/// Wave 36-B: an optional [`Metrics`] handle is attached so that
/// authentication failures bump `ferrodruid_auth_failures_total`.  When
/// `metrics` is `None` (legacy callers / unit tests) the middleware
/// silently skips the increment.
#[derive(Debug)]
pub struct AuthLayer {
    /// Whether auth enforcement is on.  Defaults to `true` in production.
    pub enabled: bool,
    /// Backing user store.
    ///
    /// Wrapped in a `RwLock` so the store stays runtime-mutable while it is
    /// shared (via `Arc`) with the change-credential handler: the
    /// middleware takes a read lock to [`AuthStore::verify`] each request,
    /// while a password rotation takes a brief write lock.
    pub store: Arc<RwLock<AuthStore>>,
    /// Optional metrics registry for the `auth_failures_total` counter.
    pub metrics: Option<Arc<Metrics>>,
}

impl AuthLayer {
    /// Construct a new `AuthLayer` without metrics wiring.
    ///
    /// Prefer [`Self::with_metrics`] in production so 401 responses
    /// bump `ferrodruid_auth_failures_total`.
    #[must_use]
    pub fn new(enabled: bool, store: Arc<RwLock<AuthStore>>) -> Self {
        Self {
            enabled,
            store,
            metrics: None,
        }
    }

    /// Construct a new `AuthLayer` wired to the given metrics registry.
    ///
    /// Each 401 response bumps the `ferrodruid_auth_failures_total`
    /// counter on `metrics` (Wave 36-B).
    #[must_use]
    pub fn with_metrics(
        enabled: bool,
        store: Arc<RwLock<AuthStore>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            enabled,
            store,
            metrics: Some(metrics),
        }
    }

    /// Create an `AuthLayer` with auth fully disabled.  Intended for tests
    /// and the default `cargo test` codepath; production must not use this.
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self::new(false, Arc::new(RwLock::new(AuthStore::new()))))
    }
}

/// Returns `true` if `path` is one of the routes that may be served without
/// authentication.  These are operator/health/observability surfaces that
/// orchestrators (k8s, Docker, ALB, ECS) probe before the operator can
/// authenticate.
///
/// Wave 36-B added `/status/live` (liveness probe, distinct from the
/// `/status/health` readiness probe).
#[must_use]
pub fn is_public_path(path: &str) -> bool {
    matches!(path, "/status/health" | "/status/live" | "/metrics")
}

/// Fixed prefix of the Druid-compatible change-credential route.
const CHANGE_CRED_PREFIX: &str = "/druid-ext/basic-security/authentication/db/basic/users/";
/// Fixed suffix of the Druid-compatible change-credential route.
const CHANGE_CRED_SUFFIX: &str = "/credential";

/// If `path` is the Druid change-credential route
/// (`/druid-ext/basic-security/authentication/db/basic/users/{userName}/credential`)
/// return the raw `{userName}` segment it targets; otherwise `None`.
///
/// The segment is returned exactly as it appears in the request URI (not
/// percent-decoded); for the bootstrap `admin` account — the only persisted
/// principal — this is an identity comparison.  A segment that is empty or
/// itself contains a `/` is rejected so only the single-segment form matches.
#[must_use]
pub fn change_credential_target(path: &str) -> Option<&str> {
    let inner = path.strip_prefix(CHANGE_CRED_PREFIX)?;
    let user = inner.strip_suffix(CHANGE_CRED_SUFFIX)?;
    if user.is_empty() || user.contains('/') {
        return None;
    }
    Some(user)
}

/// Axum middleware that enforces authentication on every non-public route.
///
/// On a missing or invalid `Authorization` header the request is rejected
/// with `401 Unauthorized` and a Druid-shaped JSON error envelope.  When
/// `AuthLayer.enabled` is `false` the middleware short-circuits and lets
/// the request through — this is the test/loopback-only mode.
pub async fn auth_middleware(
    State(layer): State<Arc<AuthLayer>>,
    request: Request,
    next: Next,
) -> Response {
    if !layer.enabled || is_public_path(request.uri().path()) {
        return next.run(request).await;
    }

    // CSRF defense (Origin check).  A browser attaches cached Basic
    // credentials to *any* same-origin request, including ones initiated
    // cross-site — so once the console prompts for a password (see
    // `unauthenticated_response`), a malicious page could silently POST a
    // state-changing request (e.g. supervisor shutdown) with the operator's
    // credentials.  Reject state-changing requests whose `Origin` header does
    // not match the request `Host`.  Non-browser clients (curl, pydruid,
    // server-to-server) send no `Origin` and are never blocked, so this stays
    // Druid-wire-compatible.
    if is_cross_origin_write(&request) {
        return csrf_rejected_response();
    }

    let header_value = match request.headers().get(AUTHORIZATION) {
        Some(v) => v,
        None => {
            bump_auth_failures(&layer);
            return unauthenticated_response("missing Authorization header");
        }
    };
    let header_str = match header_value.to_str() {
        Ok(s) => s,
        Err(_) => {
            bump_auth_failures(&layer);
            return unauthenticated_response("invalid Authorization header encoding");
        }
    };

    let method = match parse_auth_header(header_str) {
        Ok(m) => m,
        Err(_) => {
            bump_auth_failures(&layer);
            return unauthenticated_response("invalid Authorization header");
        }
    };

    let user_opt: Option<AuthenticatedUser> = match method {
        AuthMethod::Basic { username, password } => {
            // Bound concurrent Argon2id verifications (VERIFY_SLOTS) and run the
            // memory-hard hash on the blocking pool so a `Basic <anything>`
            // flood cannot pin the async runtime or spike RAM (pre-auth DoS).
            // The brief read lock is shared (Arc<RwLock<_>>) with the
            // change-credential handler, which takes a write lock to rotate.
            let store = Arc::clone(&layer.store);
            let _permit = VERIFY_SLOTS.acquire().await;
            match tokio::task::spawn_blocking(move || store.read().verify(&username, &password))
                .await
            {
                Ok(Ok(Some(u))) => Some(u),
                _ => None,
            }
        }
        // Bearer + Anonymous are not yet supported in the in-memory store.
        // Surface a clear 401 rather than silently letting the request through.
        AuthMethod::Bearer { .. } | AuthMethod::Anonymous => None,
    };

    let Some(user) = user_opt else {
        bump_auth_failures(&layer);
        return unauthenticated_response("invalid credentials");
    };

    // Force-password-change gate.  An account flagged `must_change_password`
    // (the freshly bootstrapped admin with its auto-generated initial
    // password) may reach ONLY its own change-credential endpoint; every
    // other route is refused with 403 until the password is rotated.  Public
    // paths already short-circuited above, so this only applies to
    // authenticated, non-public routes.
    if user.must_change_password {
        let path = request.uri().path();
        let is_own_credential_change = request.method() == axum::http::Method::POST
            && change_credential_target(path) == Some(user.username.as_str());
        if !is_own_credential_change {
            return password_change_required_response(&user.username);
        }
    }

    // Attach the authenticated identity for downstream extractors / handlers.
    let mut request = request;
    request.extensions_mut().insert(user);
    next.run(request).await
}

/// Build a Druid-compatible 403 response telling the operator they must
/// rotate the auto-generated initial password before using the API.
fn password_change_required_response(username: &str) -> Response {
    let msg = format!(
        "Password change required: POST your new password to \
         {CHANGE_CRED_PREFIX}{username}{CHANGE_CRED_SUFFIX} before using other endpoints."
    );
    (
        axum::http::StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({
            "error": "Forbidden",
            "errorMessage": msg,
            "errorClass": "io.druid.server.security.ForbiddenException"
        })),
    )
        .into_response()
}

/// Returns `true` when `request` is a state-changing request carrying a
/// browser `Origin` header that does not match this server's own authority —
/// i.e. a cross-site write that CSRF protection must reject.
///
/// Only `POST`/`PUT`/`DELETE`/`PATCH` are considered (safe methods can't
/// mutate state).  A request with **no** `Origin` header is treated as a
/// non-browser client (curl, pydruid, server-to-server) and allowed, which
/// keeps the Druid wire contract intact — browsers always send `Origin` on
/// cross-origin (and, in modern browsers, same-origin) state-changing fetches.
/// A present-but-unparseable `Origin` fails closed (blocked).
///
/// The request's own authority is taken from the `Host` header (HTTP/1.1) or,
/// on HTTP/2+, the request-target authority (`request.uri()`, which is where
/// HTTP/2's `:authority` pseudo-header lands — there is no `Host` header on
/// h2).  The URI authority is trusted **only** on HTTP/2+: there it is the
/// browser-set `:authority` and cannot be forged cross-site, whereas on
/// HTTP/1.1 an absolute-form request target (`POST http://evil/x`) also
/// populates `uri().authority()` and is caller-controlled — real HTTP/1.1
/// browsers always send origin-form (authority `None`) and rely on `Host`.
///
/// Deliberately **not** consulted: `X-Forwarded-Host` — page JS *can* set that
/// header (it is not on fetch's forbidden list), so trusting it would be a CSRF
/// bypass the moment a permissive CORS policy is ever added.  A fronting
/// reverse proxy must therefore preserve the original `Host` (AWS ALB does by
/// default; nginx needs `proxy_set_header Host $host`); this requirement is
/// documented for operators.
fn is_cross_origin_write(request: &Request) -> bool {
    use axum::http::Method;
    if !matches!(
        *request.method(),
        Method::POST | Method::PUT | Method::DELETE | Method::PATCH
    ) {
        return false;
    }
    let headers = request.headers();
    let origin = match headers.get(axum::http::header::ORIGIN) {
        // No Origin header → not a browser-initiated cross-site request.
        None => return false,
        // Present but unparseable (non-ASCII) on a write → fail closed.
        Some(v) => match v.to_str() {
            Ok(s) => s,
            Err(_) => return true,
        },
    };

    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok());
    // Only trust the request-target authority on HTTP/2+, where it is the
    // browser-set `:authority`. On HTTP/1.1 it can be an attacker-supplied
    // absolute-form target, so consulting it there would be a (non-browser)
    // trust hole.
    let uri_authority = (request.version() >= axum::http::Version::HTTP_2)
        .then(|| request.uri().authority().map(|a| a.as_str()))
        .flatten();

    let matches_self =
        origin_matches_host(origin, host) || origin_matches_host(origin, uri_authority);
    !matches_self
}

/// Returns `true` when the `Origin` header's authority equals `host`
/// (host + port), normalizing the scheme's default port (`80` for `http`,
/// `443` for `https`) on both sides so a browser that omits the default port
/// from `Origin` still matches a `Host` that includes it (and vice versa).
///
/// `origin` is `scheme://authority`; `host` is a bare `authority`
/// (`host[:port]`).  A missing/empty `host`, an unknown scheme, or an `Origin`
/// of `null` never matches — so an ambiguous request fails closed for
/// state-changing methods.
fn origin_matches_host(origin: &str, host: Option<&str>) -> bool {
    let Some((scheme, origin_authority)) = origin.split_once("://") else {
        return false;
    };
    let default_port = match scheme.to_ascii_lowercase().as_str() {
        "http" => "80",
        "https" => "443",
        _ => return false,
    };
    let Some(host) = host.filter(|h| !h.is_empty()) else {
        return false;
    };
    let (o_host, o_port) = split_host_port(origin_authority);
    let (h_host, h_port) = split_host_port(host);
    o_host.eq_ignore_ascii_case(h_host)
        && o_port.unwrap_or(default_port) == h_port.unwrap_or(default_port)
}

/// Split an authority (`host`, `host:port`, `[ipv6]`, or `[ipv6]:port`) into
/// its host and optional port. IPv6 literals keep their bracketed form on both
/// sides of a comparison, so they still match consistently.
fn split_host_port(authority: &str) -> (&str, Option<&str>) {
    if let Some(rest) = authority.strip_prefix('[') {
        // IPv6 literal: `[host]` or `[host]:port`.
        if let Some((host, after)) = rest.split_once(']') {
            let port = after.strip_prefix(':').filter(|p| !p.is_empty());
            return (host, port);
        }
        return (authority, None);
    }
    match authority.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => (h, Some(p)),
        _ => (authority, None),
    }
}

/// Build a Druid-compatible 403 response for a rejected cross-origin write.
fn csrf_rejected_response() -> Response {
    (
        axum::http::StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({
            "error": "Forbidden",
            "errorMessage": "cross-origin state-changing request rejected (CSRF protection): \
                             the Origin header does not match the request Host",
            "errorClass": "io.druid.server.security.ForbiddenException"
        })),
    )
        .into_response()
}

/// Bump `ferrodruid_auth_failures_total` if the `AuthLayer` was wired
/// to a metrics registry.  No-op otherwise (legacy callers / unit tests).
fn bump_auth_failures(layer: &AuthLayer) {
    if let Some(m) = layer.metrics.as_ref() {
        m.auth_failures_total.inc();
    }
}

/// Build a Druid-compatible 401 response.
///
/// Carries a `WWW-Authenticate: Basic` challenge so a browser navigating to
/// the Web Console (or any protected route) actually prompts for credentials
/// and then auto-attaches them to same-origin requests.  Without this header
/// the console is unusable under the default (auth-on) configuration: the
/// browser shows a bare 401 JSON body, never prompts, and every subsequent
/// `fetch` fails 401.
fn unauthenticated_response(msg: &str) -> Response {
    (
        axum::http::StatusCode::UNAUTHORIZED,
        [(
            axum::http::header::WWW_AUTHENTICATE,
            "Basic realm=\"FerroDruid\", charset=\"UTF-8\"",
        )],
        axum::Json(serde_json::json!({
            "error": "Unauthorized",
            "errorMessage": msg,
            "errorClass": "io.druid.server.security.UnauthorizedException"
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Authz middleware (Wave 40-C)
// ---------------------------------------------------------------------------

/// Required permission for a single route, attached as the per-route state of
/// [`authz_middleware`].
///
/// The middleware constructs a [`ResourceAction`] from these fields with
/// `resource_pattern` interpreted as the resource *name* the route operates
/// on.  Routes that do not depend on a specific resource name use `"*"` to
/// require a wildcard grant.
///
/// Wave 40-C closes the W35-C1 STILL-OPEN finding from the Wave 39 DD audit:
/// Wave 36-A wired authentication (Basic auth → `AuthenticatedUser`) but
/// `AppState.authorizer` was unused, so any authenticated user could hit
/// every endpoint regardless of role.
#[derive(Debug, Clone)]
pub struct RequiredPermission {
    /// Resource type the route mutates / reads.
    pub resource_type: ResourceType,
    /// Resource name the route operates on (or `"*"` for any).
    pub resource_name: String,
    /// Action being performed.
    pub action: Action,
}

impl RequiredPermission {
    /// Convenience constructor for a `Datasource` read on a wildcard name.
    #[must_use]
    pub fn datasource_read() -> Self {
        Self {
            resource_type: ResourceType::Datasource,
            resource_name: "*".to_string(),
            action: Action::Read,
        }
    }

    /// Convenience constructor for a `Datasource` write on a wildcard name.
    #[must_use]
    pub fn datasource_write() -> Self {
        Self {
            resource_type: ResourceType::Datasource,
            resource_name: "*".to_string(),
            action: Action::Write,
        }
    }

    /// Convenience constructor for a `Config` read on a wildcard name.
    #[must_use]
    pub fn config_read() -> Self {
        Self {
            resource_type: ResourceType::Config,
            resource_name: "*".to_string(),
            action: Action::Read,
        }
    }

    /// Convenience constructor for a `Config` write on a wildcard name.
    #[must_use]
    pub fn config_write() -> Self {
        Self {
            resource_type: ResourceType::Config,
            resource_name: "*".to_string(),
            action: Action::Write,
        }
    }

    /// Convenience constructor for a `State` read on a wildcard name.
    #[must_use]
    pub fn state_read() -> Self {
        Self {
            resource_type: ResourceType::State,
            resource_name: "*".to_string(),
            action: Action::Read,
        }
    }
}

/// Authorization policy: a static table mapping HTTP method + path prefix to
/// the [`RequiredPermission`] enforced on that route.
///
/// The table is consulted by [`authz_middleware`].  Wave 40-C builds the
/// default policy in [`build_default_policy`] (called from
/// `create_router`) and covers every non-public route.  The `enforce` flag
/// is the kill-switch (mirroring [`AuthLayer::enabled`]); when `false`
/// every request passes through unchanged.
#[derive(Debug)]
pub struct AuthzLayer {
    /// Whether authorization enforcement is on.  When `false`, every
    /// request passes through unchanged.
    pub enforce: bool,
    /// Shared RBAC authorizer.
    pub authorizer: Arc<Authorizer>,
    /// Ordered list of policy rules.  The first rule whose method and
    /// path-prefix match wins.
    pub policy: Vec<AuthzRule>,
}

/// A single authorization policy rule.
///
/// Matched in order from `AuthzLayer::policy`; the first rule whose method
/// and path-prefix match wins.  A rule with `method == None` matches any
/// HTTP verb.
#[derive(Debug, Clone)]
pub struct AuthzRule {
    /// HTTP method to match (`None` = any).
    pub method: Option<axum::http::Method>,
    /// Path prefix the rule covers (case-sensitive, no glob).
    pub path_prefix: String,
    /// Permission required to traverse this route.
    pub required: RequiredPermission,
}

impl AuthzLayer {
    /// Build a new [`AuthzLayer`] with the given policy table.
    #[must_use]
    pub fn new(enforce: bool, authorizer: Arc<Authorizer>, policy: Vec<AuthzRule>) -> Self {
        Self {
            enforce,
            authorizer,
            policy,
        }
    }

    /// Disabled layer for tests and loopback rigs (always admits).
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self::new(false, Arc::new(Authorizer::new()), Vec::new()))
    }

    /// Look up the first matching policy rule for the given method + path.
    ///
    /// Returns `None` when no rule covers the request — the middleware
    /// treats that as default-deny.
    #[must_use]
    pub fn lookup(&self, method: &axum::http::Method, path: &str) -> Option<&AuthzRule> {
        self.policy.iter().find(|rule| {
            rule.method.as_ref().is_none_or(|m| m == method) && path.starts_with(&rule.path_prefix)
        })
    }
}

/// Axum middleware that enforces RBAC on every non-public route.
///
/// Reads the [`AuthenticatedUser`] previously inserted by
/// [`auth_middleware`] from the request extensions and consults the
/// [`Authorizer`] using the static policy table on [`AuthzLayer`].  On any
/// failure (no extension present, no matching policy rule, or
/// `Authorizer::authorize` returning `false`) the request is rejected with
/// `403 Forbidden`.
///
/// Public endpoints (`/status/health`, `/status/live`, `/metrics`) are
/// short-circuited inside this middleware via [`is_public_path`] so
/// orchestrator probes and the Prometheus scrape continue to work without
/// a configured policy entry.
///
/// When `AuthzLayer.enforce` is `false` the middleware short-circuits
/// entirely — used by tests and explicit loopback rigs.
pub async fn authz_middleware(
    State(layer): State<Arc<AuthzLayer>>,
    request: Request,
    next: Next,
) -> Response {
    if !layer.enforce || is_public_path(request.uri().path()) {
        return next.run(request).await;
    }

    let user = match request.extensions().get::<AuthenticatedUser>() {
        Some(u) => u.clone(),
        None => {
            // The authz layer must always run *after* authn.  If there is
            // no authenticated user attached, default-deny rather than
            // letting the request through.
            return forbidden_response("authenticated identity required for this route");
        }
    };

    // Self-service password rotation is available to ANY authenticated
    // principal (it is how a forced-change user escapes the gate), so the
    // change-credential endpoint is exempt from the static RBAC policy
    // table — otherwise default-deny would block a non-admin from changing
    // their own password.  The handler itself enforces the own-vs-admin
    // rule (a caller may only rotate another user's credential with the
    // `admin` role).  Authentication is still required: `user` above is
    // present, so an unauthenticated request never reaches here.
    if request.method() == axum::http::Method::POST
        && change_credential_target(request.uri().path()).is_some()
    {
        return next.run(request).await;
    }

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    let rule = match layer.lookup(&method, &path) {
        Some(r) => r.clone(),
        None => {
            return forbidden_response(&format!(
                "no authorization policy for {method} {path}; default-deny"
            ));
        }
    };

    let check = ResourceAction {
        resource_type: rule.required.resource_type.clone(),
        resource_name: rule.required.resource_name.clone(),
        action: rule.required.action.clone(),
    };

    if !layer.authorizer.authorize(&user, &check) {
        return forbidden_response(&format!(
            "user {} is not authorized for {:?}:{}:{:?}",
            user.username,
            rule.required.resource_type,
            rule.required.resource_name,
            rule.required.action
        ));
    }

    next.run(request).await
}

/// Build a Druid-compatible 403 response.
fn forbidden_response(msg: &str) -> Response {
    (
        axum::http::StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({
            "error": "Forbidden",
            "errorMessage": msg,
            "errorClass": "io.druid.server.security.ForbiddenException"
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_and_release() {
        let limiter = RateLimiter::new(2);
        assert_eq!(limiter.current(), 0);

        assert!(limiter.try_acquire());
        assert_eq!(limiter.current(), 1);

        assert!(limiter.try_acquire());
        assert_eq!(limiter.current(), 2);

        // At capacity.
        assert!(!limiter.try_acquire());
        assert_eq!(limiter.current(), 2);

        limiter.release();
        assert_eq!(limiter.current(), 1);

        // Can acquire again.
        assert!(limiter.try_acquire());
        assert_eq!(limiter.current(), 2);
    }

    #[test]
    fn release_decrements() {
        let limiter = RateLimiter::new(10);
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert_eq!(limiter.current(), 2);
        limiter.release();
        limiter.release();
        assert_eq!(limiter.current(), 0);
    }

    #[test]
    fn single_slot_limiter() {
        let limiter = RateLimiter::new(1);
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        limiter.release();
        assert!(limiter.try_acquire());
    }

    #[test]
    fn public_paths_recognised() {
        assert!(is_public_path("/status/health"));
        assert!(is_public_path("/status/live"));
        assert!(is_public_path("/metrics"));
        assert!(!is_public_path("/status"));
        assert!(!is_public_path("/druid/v2/sql"));
        assert!(!is_public_path("/druid/coordinator/v1/datasources"));
    }

    #[test]
    fn auth_layer_disabled_helper() {
        let layer = AuthLayer::disabled();
        assert!(!layer.enabled);
    }

    // -----------------------------------------------------------------------
    // CSRF Origin-check unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn origin_matches_host_cases() {
        assert!(origin_matches_host(
            "http://victim:8888",
            Some("victim:8888")
        ));
        assert!(origin_matches_host(
            "https://Victim.example",
            Some("victim.example")
        ));
        assert!(!origin_matches_host("http://evil.com", Some("victim:8888")));
        // `null` origin and missing/empty host fail closed.
        assert!(!origin_matches_host("null", Some("victim:8888")));
        assert!(!origin_matches_host("http://victim:8888", None));
        assert!(!origin_matches_host("http://victim:8888", Some("")));
    }

    #[test]
    fn origin_matches_host_default_port_normalization() {
        // Browser omits the default port from Origin; a proxy annotates Host.
        assert!(origin_matches_host(
            "https://d.example",
            Some("d.example:443")
        ));
        assert!(origin_matches_host(
            "http://d.example:80",
            Some("d.example")
        ));
        assert!(origin_matches_host(
            "http://d.example",
            Some("d.example:80")
        ));
        // A non-default port must still match exactly, and mismatch when wrong.
        assert!(origin_matches_host(
            "https://d.example:8443",
            Some("d.example:8443")
        ));
        assert!(!origin_matches_host(
            "https://d.example",
            Some("d.example:80")
        ));
        assert!(!origin_matches_host(
            "https://d.example:9999",
            Some("d.example:443")
        ));
        // IPv6 literals compare consistently (brackets kept on both sides).
        assert!(origin_matches_host("http://[::1]:8888", Some("[::1]:8888")));
        assert!(origin_matches_host("http://[::1]", Some("[::1]:80")));
        assert!(!origin_matches_host("http://[::1]:1", Some("[::1]:2")));
        // Unknown scheme fails closed.
        assert!(!origin_matches_host(
            "ftp://d.example",
            Some("d.example:21")
        ));
    }

    #[test]
    fn split_host_port_cases() {
        assert_eq!(split_host_port("h"), ("h", None));
        assert_eq!(split_host_port("h:80"), ("h", Some("80")));
        assert_eq!(split_host_port("[::1]"), ("::1", None));
        assert_eq!(split_host_port("[::1]:8080"), ("::1", Some("8080")));
        // A trailing colon or non-numeric "port" is not treated as a port.
        assert_eq!(split_host_port("h:"), ("h:", None));
    }

    fn write_req(method: &str, origin: Option<&str>, host: Option<&str>) -> Request {
        use axum::body::Body;
        let mut b = axum::http::Request::builder().method(method).uri("/x");
        if let Some(o) = origin {
            b = b.header("origin", o);
        }
        if let Some(h) = host {
            b = b.header("host", h);
        }
        b.body(Body::empty()).expect("req")
    }

    #[test]
    fn cross_origin_write_detection() {
        // GET is never a CSRF concern.
        assert!(!is_cross_origin_write(&write_req(
            "GET",
            Some("http://evil.com"),
            Some("victim")
        )));
        // No Origin (curl / pydruid) → allowed.
        assert!(!is_cross_origin_write(&write_req(
            "POST",
            None,
            Some("victim")
        )));
        // Same-origin browser POST → allowed.
        assert!(!is_cross_origin_write(&write_req(
            "POST",
            Some("http://victim"),
            Some("victim")
        )));
        // Cross-origin browser POST/DELETE → blocked.
        assert!(is_cross_origin_write(&write_req(
            "POST",
            Some("http://evil.com"),
            Some("victim")
        )));
        assert!(is_cross_origin_write(&write_req(
            "DELETE",
            Some("http://evil.com"),
            Some("victim")
        )));
    }

    #[test]
    fn cross_origin_write_http2_proxy_and_unparseable() {
        use axum::body::Body;
        use axum::http::{HeaderValue, Request as R, Version};

        // HTTP/2: no Host header; the authority lives in the request URI
        // (`:authority`). Same-origin must still pass, cross-origin blocked.
        let h2_same = R::builder()
            .method("POST")
            .version(Version::HTTP_2)
            .uri("http://victim:8888/x")
            .header("origin", "http://victim:8888")
            .body(Body::empty())
            .unwrap();
        assert!(
            !is_cross_origin_write(&h2_same),
            "h2 same-origin (uri authority, no Host header) must pass"
        );
        let h2_cross = R::builder()
            .method("POST")
            .version(Version::HTTP_2)
            .uri("http://victim:8888/x")
            .header("origin", "http://evil.example")
            .body(Body::empty())
            .unwrap();
        assert!(
            is_cross_origin_write(&h2_cross),
            "h2 cross-origin must be blocked"
        );

        // HTTP/1.1 absolute-form target populates uri().authority() too, but is
        // caller-controlled (not browser-emitted). It must NOT be trusted as a
        // same-origin signal: a raw client sending `POST http://evil/x` with a
        // matching Origin but a different Host is still cross-origin → blocked.
        let h1_absolute_form = R::builder()
            .method("POST")
            .version(Version::HTTP_11)
            .uri("http://evil.example/x")
            .header("origin", "http://evil.example")
            .header("host", "victim:8888")
            .body(Body::empty())
            .unwrap();
        assert!(
            is_cross_origin_write(&h1_absolute_form),
            "HTTP/1.1 absolute-form authority must not be trusted (Origin != Host → blocked)"
        );

        // A reverse proxy that rewrites Host to an internal upstream and does
        // NOT preserve the original Host makes even a legit same-origin write
        // look cross-origin → blocked.  This is the documented requirement that
        // the fronting proxy must preserve Host; X-Forwarded-Host is
        // deliberately NOT trusted (page JS can set it), so it does not rescue
        // this misconfiguration.
        let host_rewritten = R::builder()
            .method("POST")
            .uri("/x")
            .header("origin", "https://public.example")
            .header("host", "10.0.1.5:8888")
            .header("x-forwarded-host", "public.example")
            .body(Body::empty())
            .unwrap();
        assert!(
            is_cross_origin_write(&host_rewritten),
            "Host-rewriting proxy that drops the original Host is blocked (X-Forwarded-Host is not trusted)"
        );

        // Present-but-unparseable Origin on a write → fail closed (blocked).
        let bad = R::builder()
            .method("POST")
            .uri("/x")
            .header("host", "victim")
            .header("origin", HeaderValue::from_bytes(b"http://\xff").unwrap())
            .body(Body::empty())
            .unwrap();
        assert!(
            is_cross_origin_write(&bad),
            "unparseable Origin must fail closed"
        );
    }

    // -----------------------------------------------------------------------
    // WWW-Authenticate + CSRF end-to-end through the middleware
    // -----------------------------------------------------------------------

    fn enabled_layer() -> Arc<AuthLayer> {
        Arc::new(AuthLayer::new(
            true,
            Arc::new(RwLock::new(AuthStore::new())),
        ))
    }

    #[tokio::test]
    async fn unauthenticated_401_carries_www_authenticate() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode, header};
        use axum::routing::get;
        use tower::ServiceExt;

        let app = Router::new()
            .route("/druid/v2/sql", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                enabled_layer(),
                auth_middleware,
            ));

        let req = HttpRequest::builder()
            .uri("/druid/v2/sql")
            .body(Body::empty())
            .expect("req");
        let resp = app.oneshot(req).await.expect("serve");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let challenge = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .expect("401 must carry a WWW-Authenticate challenge so browsers prompt")
            .to_str()
            .expect("ascii");
        assert!(challenge.starts_with("Basic"), "got: {challenge}");
    }

    #[tokio::test]
    async fn cross_origin_post_is_rejected_before_auth() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode};
        use axum::routing::post;
        use tower::ServiceExt;

        let app = Router::new()
            .route(
                "/druid/indexer/v1/supervisor/{id}/shutdown",
                post(|| async { "shut down" }),
            )
            .layer(axum::middleware::from_fn_with_state(
                enabled_layer(),
                auth_middleware,
            ));

        // Cross-origin browser POST → 403 CSRF (never reaches the handler,
        // never 401s in a way that would leak that the route exists).
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/druid/indexer/v1/supervisor/x/shutdown")
            .header("origin", "http://evil.example")
            .header("host", "victim:8888")
            .body(Body::empty())
            .expect("req");
        let resp = app.clone().oneshot(req).await.expect("serve");
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "cross-origin write must be CSRF-rejected"
        );

        // No Origin (server-to-server / curl) → not CSRF-blocked; falls through
        // to auth, which 401s for the missing credential.
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/druid/indexer/v1/supervisor/x/shutdown")
            .header("host", "victim:8888")
            .body(Body::empty())
            .expect("req");
        let resp = app.oneshot(req).await.expect("serve");
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "no-Origin client must pass the CSRF gate and hit auth"
        );
    }

    // -----------------------------------------------------------------------
    // Wave 40-B: rate limit must skip /status/* and /metrics
    // (Wave 39 [High] [NEW-VARIANT] — middleware.rs:67 / lib.rs:225)
    // -----------------------------------------------------------------------

    /// Drive the middleware once with a saturated limiter and confirm the
    /// request still goes through for `/status/health`.
    #[tokio::test]
    async fn rate_limit_does_not_apply_to_status_health() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode};
        use axum::routing::get;
        use tower::ServiceExt;

        // Saturated limiter (cap=1, already held).
        let limiter = Arc::new(RateLimiter::new(1));
        assert!(limiter.try_acquire(), "first acquire");
        assert!(!limiter.try_acquire(), "second must fail (saturated)");

        let app = Router::new()
            .route("/status/health", get(|| async { "ok" }))
            .route("/druid/v2", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&limiter),
                rate_limit_middleware,
            ));

        // /status/health bypasses the saturated limiter.
        let req = HttpRequest::builder()
            .uri("/status/health")
            .body(Body::empty())
            .expect("req");
        let resp = app.clone().oneshot(req).await.expect("serve");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "/status/health must bypass rate limit even when saturated"
        );

        // /druid/v2 is non-public, so it 429s.
        let req = HttpRequest::builder()
            .uri("/druid/v2")
            .body(Body::empty())
            .expect("req");
        let resp = app.oneshot(req).await.expect("serve");
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "non-public route must 429 when limiter is saturated"
        );
    }

    /// Same shape as the `/status/health` test but for `/metrics` and
    /// `/status/live`.
    #[tokio::test]
    async fn rate_limit_does_not_apply_to_metrics() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode};
        use axum::routing::get;
        use tower::ServiceExt;

        let limiter = Arc::new(RateLimiter::new(1));
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());

        let app = Router::new()
            .route("/metrics", get(|| async { "metrics-body" }))
            .route("/status/live", get(|| async { "live" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&limiter),
                rate_limit_middleware,
            ));

        for path in ["/metrics", "/status/live"] {
            let req = HttpRequest::builder()
                .uri(path)
                .body(Body::empty())
                .expect("req");
            let resp = app.clone().oneshot(req).await.expect("serve");
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "{path} must bypass rate limit (Wave 40-B exempt list)"
            );
        }
    }

    /// A valid Basic login still authenticates through the bounded
    /// (`VERIFY_SLOTS` + `spawn_blocking`) verify path, and a wrong password
    /// still 401s — i.e. the DoS-hardening did not change auth correctness.
    #[tokio::test]
    async fn valid_basic_login_passes_bounded_verify() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode};
        use axum::routing::get;
        use base64::Engine as _;
        use tower::ServiceExt;

        let mut store = AuthStore::new();
        store
            .add_user("alice", "s3cret-pw", vec!["admin".to_string()])
            .expect("add user");
        let layer = Arc::new(AuthLayer::new(true, Arc::new(RwLock::new(store))));

        let app = Router::new()
            .route("/status", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(layer, auth_middleware));

        let good = base64::engine::general_purpose::STANDARD.encode("alice:s3cret-pw");
        let req = HttpRequest::builder()
            .uri("/status")
            .header("authorization", format!("Basic {good}"))
            .body(Body::empty())
            .expect("req");
        let resp = app.clone().oneshot(req).await.expect("serve");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "valid Basic login must pass through the bounded verify"
        );

        let bad = base64::engine::general_purpose::STANDARD.encode("alice:wrong");
        let req = HttpRequest::builder()
            .uri("/status")
            .header("authorization", format!("Basic {bad}"))
            .body(Body::empty())
            .expect("req");
        let resp = app.oneshot(req).await.expect("serve");
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "wrong password must 401"
        );
    }
}
