// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! HTTP orchestration: health check, fixture ingestion, probe
//! execution, and optional fixture cleanup against one running
//! FerroDruid endpoint.
//!
//! Everything network-facing lives here; the assertion engine
//! ([`crate::probe`]) and the battery ([`crate::catalog`]) stay pure
//! so they can be unit-tested with canned JSON.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{Value, json};

use crate::catalog::{FixtureNames, ingestion_spec, probe_catalog};
use crate::probe::{Fixture, Probe, ProbeKind, QueryOutcome, Section, Verdict, evaluate};
use crate::report::ProbeResult;

/// Runner configuration (parsed from the CLI).
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URL of the running FerroDruid, e.g. `http://host:8888`.
    pub base_url: String,
    /// Optional HTTP Basic credentials (`user`, `password`).
    pub auth: Option<(String, String)>,
    /// Datasource name prefix for the self-ingested fixtures.
    pub prefix: String,
    /// Section filter; `None` runs the whole battery.
    pub sections: Option<Vec<Section>>,
    /// Attempt `DELETE /druid/coordinator/v1/datasources/{name}` on the
    /// fixtures after the run (soft-disable; best-effort).
    pub cleanup: bool,
    /// Per-request timeout in seconds.
    pub timeout_secs: u64,
    /// Emit per-probe progress on stderr.
    pub verbose: bool,
}

/// Thin authenticated HTTP client around the Druid-compatible API.
struct Client {
    http: reqwest::Client,
    base: String,
    auth: Option<(String, String)>,
}

impl Client {
    fn new(cfg: &Config) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build()
            .map_err(|e| format!("failed to build HTTP client: {e}"))?;
        Ok(Self {
            http,
            base: cfg.base_url.trim_end_matches('/').to_string(),
            auth: cfg.auth.clone(),
        })
    }

    fn with_auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Some((user, pass)) => rb.basic_auth(user, Some(pass)),
            None => rb,
        }
    }

    async fn health(&self) -> Result<(), String> {
        let url = format!("{}/status/health", self.base);
        let resp = self
            .with_auth(self.http.get(&url))
            .send()
            .await
            .map_err(|e| format!("cannot reach {url}: {e}"))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("{url} returned HTTP {}", resp.status()))
        }
    }

    /// `POST /druid/v2/sql` with `resultFormat: "object"` — the same
    /// wire shape the diff harness uses.
    async fn sql(&self, query: &str) -> QueryOutcome {
        let url = format!("{}/druid/v2/sql", self.base);
        let body = json!({"query": query, "resultFormat": "object"});
        let resp = match self
            .with_auth(self.http.post(&url))
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return QueryOutcome::Error(format!("SQL POST failed: {e}")),
        };
        let status = resp.status();
        let text = match resp.text().await {
            Ok(t) => t,
            Err(e) => return QueryOutcome::Error(format!("SQL body read failed: {e}")),
        };
        if !status.is_success() {
            return QueryOutcome::Error(format!("SQL returned {status}: {text}"));
        }
        match serde_json::from_str(&text) {
            Ok(v) => QueryOutcome::Rows(v),
            Err(e) => QueryOutcome::Error(format!("SQL response not JSON: {e}: {text}")),
        }
    }

    async fn submit_task(&self, spec: &Value) -> Result<String, String> {
        let url = format!("{}/druid/indexer/v1/task", self.base);
        let resp = self
            .with_auth(self.http.post(&url))
            .json(spec)
            .send()
            .await
            .map_err(|e| format!("task submit POST failed: {e}"))?;
        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .unwrap_or_else(|_| Value::String("<non-json body>".to_string()));
        if !status.is_success() {
            return Err(format!("task submit returned {status}: {body}"));
        }
        body.get("task")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| format!("missing 'task' field in submit response: {body}"))
    }

    /// Fetch the terminal status of an ingestion task. Tries Druid's
    /// canonical `GET .../task/{id}/status` subresource first, then
    /// falls back to `GET .../task/{id}` (FerroDruid serves the task
    /// object with an embedded `status.status` but not the
    /// subresource). Both shapes expose the `/status/status` pointer.
    async fn task_status(&self, task_id: &str) -> Option<String> {
        for url in [
            format!("{}/druid/indexer/v1/task/{task_id}/status", self.base),
            format!("{}/druid/indexer/v1/task/{task_id}", self.base),
        ] {
            let Ok(resp) = self.with_auth(self.http.get(&url)).send().await else {
                continue;
            };
            if !resp.status().is_success() {
                continue;
            }
            let Ok(body) = resp.json::<Value>().await else {
                continue;
            };
            if let Some(s) = body.pointer("/status/status").and_then(Value::as_str) {
                return Some(s.to_string());
            }
        }
        None
    }

    async fn count_rows(&self, datasource: &str) -> Result<i64, String> {
        match self
            .sql(&format!("SELECT COUNT(*) AS cnt FROM {datasource}"))
            .await
        {
            QueryOutcome::Rows(v) => v
                .get(0)
                .and_then(|r| r.get("cnt"))
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("unexpected COUNT(*) response shape: {v}")),
            QueryOutcome::Error(e) => Err(e),
        }
    }

    /// Soft-disable a datasource (marks its segments unused) — the
    /// Druid-compatible `DELETE /druid/coordinator/v1/datasources/{n}`.
    async fn disable_datasource(&self, datasource: &str) -> Result<(), String> {
        let url = format!(
            "{}/druid/coordinator/v1/datasources/{datasource}",
            self.base
        );
        let resp = self
            .with_auth(self.http.delete(&url))
            .send()
            .await
            .map_err(|e| format!("DELETE failed: {e}"))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("DELETE returned HTTP {}", resp.status()))
        }
    }
}

/// How long to wait for ingested rows to become queryable after the
/// task was accepted.
const INGEST_TIMEOUT: Duration = Duration::from_secs(120);
/// Poll interval for the ingestion wait.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Ensure one fixture is present and queryable. Returns a synthetic
/// probe result describing the ingestion outcome.
async fn ensure_fixture(client: &Client, fixture: Fixture, names: &FixtureNames) -> ProbeResult {
    // `ensure_fixture` is only called for real fixtures; a `None`
    // fixture yields a skip result rather than a panic.
    let (Some(ds), Some(spec)) = (names.name_of(fixture), ingestion_spec(fixture, names)) else {
        return ProbeResult {
            name: "fixture_none".to_string(),
            section: Section::Fixtures,
            kind: ProbeKind::Assertive,
            sql: String::new(),
            verdict: Verdict::Skip {
                reason: "no fixture required".to_string(),
            },
        };
    };
    let name = format!("fixture_ingest_{ds}");
    let sql = format!("(inline index_parallel ingestion of {ds})");
    let fail = |expected: String, actual: String| ProbeResult {
        name: name.clone(),
        section: Section::Fixtures,
        kind: ProbeKind::Assertive,
        sql: sql.clone(),
        verdict: Verdict::Fail {
            expected,
            actual,
            hint: None,
        },
    };

    // Idempotency: if the datasource already answers COUNT(*) > 0 the
    // fixture is assumed present (same convention as the diff harness).
    if let Ok(n) = client.count_rows(ds).await
        && n > 0
    {
        return ProbeResult {
            name,
            section: Section::Fixtures,
            kind: ProbeKind::Assertive,
            sql,
            verdict: Verdict::Pass,
        };
    }

    let task_id = match client.submit_task(&spec).await {
        Ok(id) => id,
        Err(e) => {
            return fail("task accepted (202/200 with a task id)".to_string(), e);
        }
    };

    // The success signal is "rows are queryable" — that is what the
    // probes need, and it is robust to engines that run the inline
    // task synchronously at submit time (FerroDruid) as well as to
    // ones that publish asynchronously (Druid). The task status is
    // polled alongside only to fail fast on an explicit FAILED.
    let deadline = std::time::Instant::now() + INGEST_TIMEOUT;
    let mut last_status = "UNKNOWN".to_string();
    while std::time::Instant::now() < deadline {
        if let Ok(n) = client.count_rows(ds).await
            && n > 0
        {
            return ProbeResult {
                name,
                section: Section::Fixtures,
                kind: ProbeKind::Assertive,
                sql,
                verdict: Verdict::Pass,
            };
        }
        if let Some(s) = client.task_status(&task_id).await {
            last_status = s;
            if last_status == "FAILED" {
                return fail(
                    "ingestion task reaches SUCCESS with queryable rows".to_string(),
                    format!("task {task_id} reported FAILED"),
                );
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    fail(
        "ingested rows become queryable".to_string(),
        format!(
            "COUNT(*) stayed 0 for {}s after submitting task {task_id} \
             (last task status: {last_status})",
            INGEST_TIMEOUT.as_secs()
        ),
    )
}

/// Run the battery. Returns all probe results (fixtures first).
///
/// # Errors
///
/// Returns `Err` only for setup-level problems (server unreachable) —
/// the caller maps that to exit code 2. Probe failures are reported
/// in the result set, not as `Err`.
pub async fn run(cfg: &Config) -> Result<Vec<ProbeResult>, String> {
    let client = Client::new(cfg)?;
    client
        .health()
        .await
        .map_err(|e| format!("FerroDruid health check failed — is the server running? {e}"))?;

    let names = FixtureNames::from_prefix(&cfg.prefix);
    let selected: Vec<Probe> = probe_catalog(&names)
        .into_iter()
        .filter(|p| {
            cfg.sections
                .as_ref()
                .is_none_or(|secs| secs.contains(&p.section))
        })
        .collect();

    let mut results: Vec<ProbeResult> = Vec::with_capacity(selected.len() + 4);
    let mut failed_fixtures: HashSet<Fixture> = HashSet::new();

    // Ingest each fixture the selected probes need, in stable order.
    for fixture in [Fixture::Wiki, Fixture::Null, Fixture::Rollup] {
        if !selected.iter().any(|p| p.fixture == fixture) {
            continue;
        }
        if cfg.verbose
            && let Some(ds) = names.name_of(fixture)
        {
            eprintln!("[setup] ensuring fixture {ds} ...");
        }
        let r = ensure_fixture(&client, fixture, &names).await;
        if matches!(r.verdict, Verdict::Fail { .. }) {
            failed_fixtures.insert(fixture);
        }
        if cfg.verbose {
            eprintln!(
                "[setup] {} -> {}",
                r.name,
                match &r.verdict {
                    Verdict::Pass => "ok".to_string(),
                    Verdict::Fail { actual, .. } => format!("FAILED ({actual})"),
                    other => format!("{other:?}"),
                }
            );
        }
        results.push(r);
    }

    let total = selected.len();
    for (i, probe) in selected.iter().enumerate() {
        let verdict = if failed_fixtures.contains(&probe.fixture) {
            Verdict::Skip {
                reason: format!(
                    "fixture {} failed to ingest",
                    names.name_of(probe.fixture).unwrap_or("?")
                ),
            }
        } else {
            let outcome = client.sql(&probe.sql).await;
            evaluate(probe, &outcome)
        };
        if cfg.verbose {
            eprintln!(
                "[{}/{}] {} ... {}",
                i + 1,
                total,
                probe.name,
                match &verdict {
                    Verdict::Pass => "PASS",
                    Verdict::Fail { .. } => "FAIL",
                    Verdict::Skip { .. } => "SKIP",
                    Verdict::Info { .. } => "INFO",
                }
            );
        }
        results.push(ProbeResult {
            name: probe.name.clone(),
            section: probe.section,
            kind: probe.kind,
            sql: probe.sql.clone(),
            verdict,
        });
    }

    // Optional cleanup: soft-disable the fixture datasources. Never a
    // failure — the outcome is recorded as informational.
    if cfg.cleanup {
        for fixture in [Fixture::Wiki, Fixture::Null, Fixture::Rollup] {
            let Some(ds) = names.name_of(fixture) else {
                continue;
            };
            if !selected.iter().any(|p| p.fixture == fixture) {
                continue;
            }
            let note = match client.disable_datasource(ds).await {
                Ok(()) => format!("datasource {ds} disabled (segments marked unused)"),
                Err(e) => format!("cleanup of {ds} did not succeed (non-fatal): {e}"),
            };
            results.push(ProbeResult {
                name: format!("fixture_cleanup_{ds}"),
                section: Section::Fixtures,
                kind: ProbeKind::Informational,
                sql: format!("DELETE /druid/coordinator/v1/datasources/{ds}"),
                verdict: Verdict::Info { note },
            });
        }
    }

    Ok(results)
}
