// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Prometheus metrics endpoint.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use ferrodruid_telemetry::Metrics;

/// GET /metrics — Prometheus text exposition format.
pub(crate) async fn handle_metrics(
    State(metrics): State<Arc<Metrics>>,
) -> Result<String, StatusCode> {
    metrics
        .prometheus_text()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
