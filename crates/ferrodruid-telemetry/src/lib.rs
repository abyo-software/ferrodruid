// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Prometheus metrics and health endpoints for FerroDruid.
//!
//! The [`Metrics`] struct registers all FerroDruid counters, gauges, and
//! histograms in a Prometheus [`Registry`] and can export them in the
//! standard text format for a `/metrics` HTTP endpoint.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Telemetry errors.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// Prometheus encoding error.
    #[error("prometheus encoding error: {0}")]
    Encode(String),
    /// Prometheus registration error.
    #[error("prometheus registration error: {0}")]
    Registration(String),
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Global metrics registry for FerroDruid.
///
/// All metrics are registered at construction time. Clone is cheap (all
/// fields are `Arc`-backed).
///
/// # Wave 36-B observability wire-up
///
/// `queries_total` and `query_errors_total` are
/// [`IntCounterVec`](prometheus::IntCounterVec) so that REST handlers can
/// label each increment with the data source (`queries_total`) or error
/// class (`query_errors_total`).  `tasks_submitted_total`,
/// `tasks_completed_total`, and `auth_failures_total` are unlabeled
/// counters incremented by the indexer/auth middleware hot paths.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// The underlying Prometheus registry.
    pub registry: Registry,
    /// Total number of queries executed, labeled by `datasource`.
    pub queries_total: IntCounterVec,
    /// Total number of failed queries, labeled by error `class`
    /// (e.g. `parse`, `planning`, `execution`, `timeout`).
    pub query_errors_total: IntCounterVec,
    /// Query duration in seconds.
    pub query_duration_seconds: Histogram,
    /// Number of segments currently loaded on historicals.
    pub segments_loaded: IntGauge,
    /// Number of segments announced (published) by the coordinator.
    pub segments_announced: IntGauge,
    /// Total rows ingested across all tasks.
    pub ingestion_rows_total: IntCounter,
    /// Number of ingestion tasks currently running.
    pub ingestion_tasks_running: IntGauge,
    /// Total number of ingestion / index tasks submitted.
    pub tasks_submitted_total: IntCounter,
    /// Total number of ingestion / index tasks completed (any final state).
    pub tasks_completed_total: IntCounter,
    /// Total number of authentication failures (401s emitted by the auth
    /// middleware).  Wired by `ferrodruid-rest::middleware::auth_middleware`.
    pub auth_failures_total: IntCounter,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics registry with all FerroDruid metrics registered.
    ///
    /// # Panics
    ///
    /// This function is infallible under normal operation. Registration can
    /// only fail if metric names collide, which cannot happen with the
    /// hard-coded names used here.
    pub fn new() -> Self {
        let registry = Registry::new();

        let queries_total = IntCounterVec::new(
            Opts::new(
                "ferrodruid_queries_total",
                "Total number of queries executed, labeled by datasource",
            ),
            &["datasource"],
        )
        .expect("metric creation must succeed");

        let query_errors_total = IntCounterVec::new(
            Opts::new(
                "ferrodruid_query_errors_total",
                "Total number of failed queries, labeled by error class",
            ),
            &["class"],
        )
        .expect("metric creation must succeed");

        let query_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "ferrodruid_query_duration_seconds",
                "Query execution time in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("metric creation must succeed");

        let segments_loaded = IntGauge::with_opts(Opts::new(
            "ferrodruid_segments_loaded",
            "Number of segments currently loaded",
        ))
        .expect("metric creation must succeed");

        let segments_announced = IntGauge::with_opts(Opts::new(
            "ferrodruid_segments_announced",
            "Number of segments announced by the coordinator",
        ))
        .expect("metric creation must succeed");

        let ingestion_rows_total = IntCounter::with_opts(Opts::new(
            "ferrodruid_ingestion_rows_total",
            "Total number of rows ingested",
        ))
        .expect("metric creation must succeed");

        let ingestion_tasks_running = IntGauge::with_opts(Opts::new(
            "ferrodruid_ingestion_tasks_running",
            "Number of currently running ingestion tasks",
        ))
        .expect("metric creation must succeed");

        let tasks_submitted_total = IntCounter::with_opts(Opts::new(
            "ferrodruid_tasks_submitted_total",
            "Total number of ingestion/index tasks submitted",
        ))
        .expect("metric creation must succeed");

        let tasks_completed_total = IntCounter::with_opts(Opts::new(
            "ferrodruid_tasks_completed_total",
            "Total number of ingestion/index tasks completed",
        ))
        .expect("metric creation must succeed");

        let auth_failures_total = IntCounter::with_opts(Opts::new(
            "ferrodruid_auth_failures_total",
            "Total number of authentication failures (401s)",
        ))
        .expect("metric creation must succeed");

        registry
            .register(Box::new(queries_total.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(query_errors_total.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(query_duration_seconds.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(segments_loaded.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(segments_announced.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(ingestion_rows_total.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(ingestion_tasks_running.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(tasks_submitted_total.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(tasks_completed_total.clone()))
            .expect("registration must succeed");
        registry
            .register(Box::new(auth_failures_total.clone()))
            .expect("registration must succeed");

        Self {
            registry,
            queries_total,
            query_errors_total,
            query_duration_seconds,
            segments_loaded,
            segments_announced,
            ingestion_rows_total,
            ingestion_tasks_running,
            tasks_submitted_total,
            tasks_completed_total,
            auth_failures_total,
        }
    }

    /// Encode all registered metrics as Prometheus text exposition format.
    pub fn prometheus_text(&self) -> Result<String, TelemetryError> {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        let mut buf = Vec::new();
        encoder
            .encode(&families, &mut buf)
            .map_err(|e| TelemetryError::Encode(e.to_string()))?;
        String::from_utf8(buf).map_err(|e| TelemetryError::Encode(e.to_string()))
    }
}

/// Health check status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Node is healthy.
    Healthy,
    /// Node is degraded.
    Degraded,
    /// Node is unhealthy.
    Unhealthy,
}

/// Return a simple health status (stub).
pub fn health_check() -> HealthStatus {
    HealthStatus::Healthy
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_creation() {
        let m = Metrics::new();
        // Wave 36-B added `tasks_submitted_total`, `tasks_completed_total`,
        // `auth_failures_total`, and split `queries_failed` into the labeled
        // `query_errors_total`.  `IntCounterVec` only contributes a family
        // to `gather()` once a label combination has been observed, so a
        // freshly-built `Metrics` with no traffic yields the 8 unlabeled
        // families (Histogram/Counter/Gauge); after a single
        // `with_label_values` touch on each `*Vec`, all 10 families show up.
        assert_eq!(m.registry.gather().len(), 8);
        m.queries_total.with_label_values(&["__init__"]).inc_by(0);
        m.query_errors_total
            .with_label_values(&["__init__"])
            .inc_by(0);
        assert_eq!(m.registry.gather().len(), 10);
    }

    #[test]
    fn counter_increment_and_text() {
        let m = Metrics::new();
        m.queries_total.with_label_values(&["wiki"]).inc();
        m.queries_total.with_label_values(&["wiki"]).inc();
        m.query_errors_total.with_label_values(&["parse"]).inc();

        let text = m.prometheus_text().expect("encode");
        assert!(text.contains("ferrodruid_queries_total{datasource=\"wiki\"} 2"));
        assert!(text.contains("ferrodruid_query_errors_total{class=\"parse\"} 1"));
    }

    #[test]
    fn gauge_set_and_text() {
        let m = Metrics::new();
        m.segments_loaded.set(42);
        m.segments_announced.set(100);
        m.ingestion_tasks_running.set(3);

        let text = m.prometheus_text().expect("encode");
        assert!(text.contains("ferrodruid_segments_loaded 42"));
        assert!(text.contains("ferrodruid_segments_announced 100"));
        assert!(text.contains("ferrodruid_ingestion_tasks_running 3"));
    }

    #[test]
    fn histogram_observe_and_text() {
        let m = Metrics::new();
        m.query_duration_seconds.observe(0.05);
        m.query_duration_seconds.observe(0.15);
        m.query_duration_seconds.observe(1.5);

        let text = m.prometheus_text().expect("encode");
        assert!(text.contains("ferrodruid_query_duration_seconds_count 3"));
        assert!(text.contains("ferrodruid_query_duration_seconds_bucket"));
    }

    #[test]
    fn ingestion_rows_counter() {
        let m = Metrics::new();
        m.ingestion_rows_total.inc_by(1000);
        m.ingestion_rows_total.inc_by(500);

        let text = m.prometheus_text().expect("encode");
        assert!(text.contains("ferrodruid_ingestion_rows_total 1500"));
    }

    #[test]
    fn default_impl() {
        let m = Metrics::default();
        let text = m.prometheus_text().expect("encode");
        // Unlabeled counters start at 0 with no series; labeled vecs only
        // start emitting samples once a label combination has been touched.
        // Either way the registry must encode cleanly and the unlabeled
        // counters must be present and zero.
        assert!(text.contains("ferrodruid_tasks_submitted_total 0"));
        assert!(text.contains("ferrodruid_tasks_completed_total 0"));
        assert!(text.contains("ferrodruid_auth_failures_total 0"));
    }

    #[test]
    fn health_check_returns_healthy() {
        assert_eq!(health_check(), HealthStatus::Healthy);
    }

    #[test]
    fn metrics_clone() {
        let m1 = Metrics::new();
        m1.queries_total.with_label_values(&["wiki"]).inc();
        let m2 = m1.clone();
        // Cloned metrics share the same underlying data
        m2.queries_total.with_label_values(&["wiki"]).inc();
        let text = m1.prometheus_text().expect("encode");
        assert!(text.contains("ferrodruid_queries_total{datasource=\"wiki\"} 2"));
    }

    #[test]
    fn task_and_auth_counters_increment() {
        let m = Metrics::new();
        m.tasks_submitted_total.inc();
        m.tasks_submitted_total.inc();
        m.tasks_submitted_total.inc();
        m.tasks_completed_total.inc();
        m.auth_failures_total.inc_by(4);

        let text = m.prometheus_text().expect("encode");
        assert!(text.contains("ferrodruid_tasks_submitted_total 3"));
        assert!(text.contains("ferrodruid_tasks_completed_total 1"));
        assert!(text.contains("ferrodruid_auth_failures_total 4"));
    }
}
