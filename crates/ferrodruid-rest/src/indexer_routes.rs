// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Overlord/Indexer endpoints: task submission, supervisor management.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use crate::AppState;

/// POST /druid/indexer/v1/task — submit an ingestion task.
///
/// Wave 36-B: bumps `ferrodruid_tasks_submitted_total` on every accepted
/// submission.  The matching `tasks_completed_total` is bumped from
/// `handle_shutdown_task` and any future task-completion path.
pub(crate) async fn handle_submit_task(
    State(state): State<Arc<AppState>>,
    Json(spec): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let task_id = state.overlord.submit_task(spec).await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Task submission failed",
            &e.to_string(),
            "io.druid.indexing.common.TaskException",
        )
    })?;
    state.metrics.tasks_submitted_total.inc();
    Ok(Json(serde_json::json!({"task": task_id})))
}

/// GET /druid/indexer/v1/tasks — list running tasks.
pub(crate) async fn handle_get_tasks(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let tasks = state.overlord.get_running_tasks().await;
    let task_list: Vec<serde_json::Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "type": t.task_type,
                "dataSource": t.data_source,
                "status": t.status,
                "createdTime": t.created_time.to_rfc3339(),
            })
        })
        .collect();
    Json(serde_json::json!(task_list))
}

/// GET /druid/indexer/v1/task/:taskId — get a specific task.
pub(crate) async fn handle_get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let task = state.overlord.get_task(&task_id).await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Task lookup failed",
            &e.to_string(),
            "io.druid.indexing.common.TaskException",
        )
    })?;

    match task {
        Some(t) => Ok(Json(serde_json::json!({
            "id": t.id,
            "type": t.task_type,
            "dataSource": t.data_source,
            "status": {
                "id": t.id,
                "status": t.status,
                "duration": 0
            },
            "createdTime": t.created_time.to_rfc3339(),
        }))),
        None => Err(crate::error_response(
            StatusCode::NOT_FOUND,
            "Task not found",
            &format!("No task with id [{task_id}]"),
            "io.druid.indexing.common.TaskNotFoundException",
        )),
    }
}

/// GET /druid/indexer/v1/task/:taskId/status — the task's status envelope.
///
/// This is the endpoint every Druid client (the console, pydruid, and any
/// submit-then-poll ingestion loop) hits to learn when a task reaches
/// `SUCCESS`/`FAILED`. It returns Druid's `{"task": id, "status": {...}}`
/// shape; `status.statusCode` and `status.status` both carry the terminal
/// state so clients keying on either field work. Unlike `handle_get_task`
/// (which returns the whole task object), the status envelope is the
/// documented polling contract.
pub(crate) async fn handle_get_task_status(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let task = state.overlord.get_task(&task_id).await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Task lookup failed",
            &e.to_string(),
            "io.druid.indexing.common.TaskException",
        )
    })?;

    match task {
        Some(t) => {
            // Druid splits lifecycle across two fields: `statusCode` (and the
            // deprecated `status` alias) is only ever RUNNING/SUCCESS/FAILED,
            // while the queue position (WAITING/PENDING) lives in
            // `runnerStatusCode` (Codex compat-4 R1 H5). Map FerroDruid's
            // 5-state `TaskState` onto that model so a strict client never
            // sees an out-of-domain top-level `statusCode` like "PENDING".
            let (status_code, runner_status_code) = match t.state.as_str() {
                "SUCCESS" => ("SUCCESS", "NONE"),
                "FAILED" => ("FAILED", "NONE"),
                "RUNNING" => ("RUNNING", "RUNNING"),
                "PENDING" => ("RUNNING", "PENDING"),
                "WAITING" => ("RUNNING", "WAITING"),
                _ => ("RUNNING", "NONE"),
            };
            Ok(Json(serde_json::json!({
                "task": t.id,
                "status": {
                    "id": t.id,
                    "groupId": t.id,
                    "type": t.task_type,
                    "dataSource": t.data_source,
                    "createdTime": t.created_time.to_rfc3339(),
                    "queueInsertionTime": "1970-01-01T00:00:00.000Z",
                    "statusCode": status_code,
                    "status": status_code,
                    "runnerStatusCode": runner_status_code,
                    "duration": 0,
                    "location": {"host": null, "port": -1, "tlsPort": -1},
                    "errorMsg": serde_json::Value::Null,
                },
            })))
        }
        None => Err(crate::error_response(
            StatusCode::NOT_FOUND,
            "Task not found",
            &format!("No task with id [{task_id}]"),
            "io.druid.indexing.common.TaskNotFoundException",
        )),
    }
}

/// POST /druid/indexer/v1/task/:taskId/shutdown -- graceful shutdown.
///
/// Wave 36-B: bumps `ferrodruid_tasks_completed_total` on successful
/// shutdown (the task transitions out of `running`).
pub(crate) async fn handle_shutdown_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state.overlord.shutdown_task(&task_id).await.map_err(|e| {
        crate::error_response(
            StatusCode::NOT_FOUND,
            "Task not found",
            &e.to_string(),
            "io.druid.indexing.common.TaskNotFoundException",
        )
    })?;
    state.metrics.tasks_completed_total.inc();
    Ok(Json(serde_json::json!({"task": task_id})))
}

/// GET /druid/indexer/v1/completeTasks -- completed tasks.
pub(crate) async fn handle_complete_tasks(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let tasks = state.overlord.get_complete_tasks().await;
    let task_list: Vec<serde_json::Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "type": t.task_type,
                "dataSource": t.data_source,
                "status": t.status,
                "createdTime": t.created_time.to_rfc3339(),
            })
        })
        .collect();
    Json(serde_json::json!(task_list))
}

/// GET /druid/indexer/v1/runningTasks -- running tasks.
pub(crate) async fn handle_running_tasks(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let tasks = state.overlord.get_running_tasks().await;
    let task_list: Vec<serde_json::Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "type": t.task_type,
                "dataSource": t.data_source,
                "status": t.status,
                "createdTime": t.created_time.to_rfc3339(),
            })
        })
        .collect();
    Json(serde_json::json!(task_list))
}

/// GET /druid/indexer/v1/waitingTasks -- waiting tasks.
pub(crate) async fn handle_waiting_tasks(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let tasks = state.overlord.get_waiting_tasks().await;
    let task_list: Vec<serde_json::Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "type": t.task_type,
                "dataSource": t.data_source,
                "status": t.status,
                "createdTime": t.created_time.to_rfc3339(),
            })
        })
        .collect();
    Json(serde_json::json!(task_list))
}

/// POST /druid/indexer/v1/supervisor — create a supervisor.
pub(crate) async fn handle_create_supervisor(
    State(state): State<Arc<AppState>>,
    Json(spec): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let id = state.overlord.create_supervisor(spec).await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Supervisor creation failed",
            &e.to_string(),
            "io.druid.indexing.overlord.SupervisorException",
        )
    })?;
    Ok(Json(serde_json::json!({"id": id})))
}

/// GET /druid/indexer/v1/supervisor — list all supervisors.
pub(crate) async fn handle_get_supervisors(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let supervisors = state.overlord.get_all_supervisors().await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Supervisor list failed",
            &e.to_string(),
            "io.druid.indexing.overlord.SupervisorException",
        )
    })?;

    let list: Vec<serde_json::Value> = supervisors
        .iter()
        .map(|s| {
            serde_json::json!({
                "id": s.spec_id,
                "spec": s.payload,
                "createdTime": s.created_date,
            })
        })
        .collect();

    Ok(Json(serde_json::json!(list)))
}

/// GET /druid/indexer/v1/supervisor/:id — get a specific supervisor.
pub(crate) async fn handle_get_supervisor(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let supervisor = state.overlord.get_supervisor(&id).await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Supervisor lookup failed",
            &e.to_string(),
            "io.druid.indexing.overlord.SupervisorException",
        )
    })?;

    match supervisor {
        Some(spec) => Ok(Json(spec)),
        None => Err(crate::error_response(
            StatusCode::NOT_FOUND,
            "Supervisor not found",
            &format!("No supervisor with id [{id}]"),
            "io.druid.indexing.overlord.SupervisorNotFoundException",
        )),
    }
}

/// POST /druid/indexer/v1/supervisor/:id/shutdown — shutdown a supervisor.
pub(crate) async fn handle_shutdown_supervisor(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state.overlord.shutdown_supervisor(&id).await.map_err(|e| {
        crate::error_response(
            StatusCode::NOT_FOUND,
            "Supervisor not found",
            &e.to_string(),
            "io.druid.indexing.overlord.SupervisorNotFoundException",
        )
    })?;
    Ok(Json(serde_json::json!({"id": id})))
}
