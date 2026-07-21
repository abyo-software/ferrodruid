// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! MSQ (Multi-Stage Query) REST endpoints.
//!
//! - `POST /druid/v2/sql/task` — submit an MSQ task
//! - `GET  /druid/v2/sql/queries/{id}` — get task status
//! - `GET  /druid/v2/sql/queries/{id}/reports` — get task report
//! - `DELETE /druid/v2/sql/queries/{id}` — cancel a task

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use ferrodruid_msq::MsqTaskSpec;
use ferrodruid_msq::executor::{execute_msq_task, plan_msq};

use crate::AppState;

/// POST /druid/v2/sql/task — submit an MSQ task.
///
/// Plans and executes the SQL statement as a multi-stage query task.
/// The task is executed synchronously in the current implementation;
/// real async execution is planned for a future release.
pub(crate) async fn handle_submit_msq_task(
    State(state): State<Arc<AppState>>,
    Json(spec): Json<MsqTaskSpec>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let sql = spec.query.clone();
    // Wave 42-A: `submit()` may now return `Err` on lock-poison (W37B
    // msq Medium #1 closure). Map it to a 503 so callers see a clean
    // failure instead of receiving a phantom task id.
    let task_id = match state.msq_manager.submit(spec) {
        Ok(id) => id,
        Err(e) => {
            return Err(crate::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Failed to register MSQ task",
                &e.to_string(),
                "io.druid.msq.MsqSubmissionFailedException",
            ));
        }
    };

    // Plan MSQ stages from the SQL statement.
    let plan = match plan_msq(&sql) {
        Ok(p) => p,
        Err(e) => {
            let error = ferrodruid_msq::MsqError {
                error: "SqlPlanningError".to_owned(),
                error_message: e.to_string(),
            };
            let _ = state.msq_manager.fail_task(&task_id, error);
            return Ok(Json(serde_json::json!({"taskId": task_id})));
        }
    };

    // Execute the plan (synchronous single-node execution).
    let _ = execute_msq_task(&plan, &state.msq_manager, &task_id).await;

    Ok(Json(serde_json::json!({"taskId": task_id})))
}

/// GET /druid/v2/sql/queries/:id — get MSQ task status.
pub(crate) async fn handle_get_msq_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let report = state.msq_manager.get_task(&id).ok_or_else(|| {
        crate::error_response(
            StatusCode::NOT_FOUND,
            "Query not found",
            &format!("No MSQ query with id [{id}]"),
            "io.druid.msq.MsqQueryNotFoundException",
        )
    })?;

    Ok(Json(serde_json::json!({
        "taskId": report.task_id,
        "status": report.status,
        "startTime": report.start_time,
        "durationMs": report.duration_ms,
    })))
}

/// GET /druid/v2/sql/queries/:id/reports — get full MSQ task report.
pub(crate) async fn handle_get_msq_report(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let report = state.msq_manager.get_report(&id).ok_or_else(|| {
        crate::error_response(
            StatusCode::NOT_FOUND,
            "Query not found",
            &format!("No MSQ query with id [{id}]"),
            "io.druid.msq.MsqQueryNotFoundException",
        )
    })?;

    serde_json::to_value(&report).map(Json).map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Serialization error",
            &e.to_string(),
            "io.druid.msq.MsqInternalError",
        )
    })
}

/// DELETE /druid/v2/sql/queries/:id — cancel an MSQ task.
pub(crate) async fn handle_cancel_msq_task(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state.msq_manager.cancel(&id).map_err(|e| {
        crate::error_response(
            StatusCode::BAD_REQUEST,
            "Cancel failed",
            &e.to_string(),
            "io.druid.msq.MsqCancelException",
        )
    })?;

    Ok(Json(serde_json::json!({"taskId": id})))
}
