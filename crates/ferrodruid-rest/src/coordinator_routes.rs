// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Coordinator endpoints: datasources, rules, load queue, config, servers, metadata.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use crate::AppState;

/// GET /druid/coordinator/v1/datasources -- list all datasource names.
pub(crate) async fn handle_datasources(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let datasources = state.coordinator.get_data_sources().await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Coordinator error",
            &e.to_string(),
            "io.druid.server.coordinator.CoordinatorException",
        )
    })?;
    Ok(Json(serde_json::json!(datasources)))
}

/// GET /druid/coordinator/v1/datasources/:datasource -- datasource info.
pub(crate) async fn handle_datasource(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let counts = state.coordinator.get_segment_counts().await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Coordinator error",
            &e.to_string(),
            "io.druid.server.coordinator.CoordinatorException",
        )
    })?;

    let segment_count = counts.get(&datasource).copied().unwrap_or(0);

    Ok(Json(serde_json::json!({
        "name": datasource,
        "segments": {
            "count": segment_count
        }
    })))
}

/// GET /druid/coordinator/v1/datasources/:datasource/segments -- segment list.
pub(crate) async fn handle_datasource_segments(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let segments = state
        .metadata
        .get_used_segments(&datasource)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;

    let segment_list: Vec<serde_json::Value> = segments
        .iter()
        .map(|s| {
            serde_json::json!({
                "id": s.id,
                "dataSource": s.data_source,
                "interval": format!("{}/{}", s.start, s.end),
                "version": s.version,
                "size": 0
            })
        })
        .collect();

    Ok(Json(serde_json::json!(segment_list)))
}

/// GET /druid/coordinator/v1/rules/:datasource -- get rules.
pub(crate) async fn handle_rules(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let rules = state.metadata.get_rules(&datasource).await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Metadata error",
            &e.to_string(),
            "io.druid.metadata.MetadataException",
        )
    })?;
    Ok(Json(serde_json::json!(rules)))
}

/// POST /druid/coordinator/v1/rules/:datasource -- set rules.
pub(crate) async fn handle_set_rules(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
    Json(rules): Json<Vec<serde_json::Value>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state
        .metadata
        .set_rules(&datasource, &rules)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

// ---------------------------------------------------------------------------
// Coordinator dynamic config
// ---------------------------------------------------------------------------

/// GET /druid/coordinator/v1/config -- current coordinator dynamic config.
pub(crate) async fn handle_get_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let config = state
        .metadata
        .get_config("coordinator.dynamic")
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;

    Ok(Json(config.unwrap_or(serde_json::json!({
        "millisToWaitBeforeDeleting": 900000,
        "mergeBytesLimit": 524288000,
        "mergeSegmentsLimit": 100,
        "maxSegmentsToMove": 5,
        "replicantLifetime": 15,
        "replicationThrottleLimit": 10,
        "balancerComputeThreads": 1,
        "killDataSourceWhitelist": [],
        "killAllDataSources": false,
        "maxSegmentsInNodeLoadingQueue": 0
    }))))
}

/// POST /druid/coordinator/v1/config -- update coordinator dynamic config.
pub(crate) async fn handle_set_config(
    State(state): State<Arc<AppState>>,
    Json(config): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state
        .metadata
        .set_config("coordinator.dynamic", &config)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;

    // Record audit entry
    let now = chrono::Utc::now().to_rfc3339();
    let audit = serde_json::json!({
        "key": "coordinator.dynamic",
        "type": "coordinator.config",
        "auditTime": now,
        "payload": config
    });
    let _ = state
        .metadata
        .set_config("coordinator.dynamic.audit.latest", &audit)
        .await;

    Ok(Json(serde_json::json!({"status": "ok"})))
}

/// GET /druid/coordinator/v1/config/history -- audit history of config changes.
pub(crate) async fn handle_config_history(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let audit = state
        .metadata
        .get_config("coordinator.dynamic.audit.latest")
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;

    match audit {
        Some(entry) => Ok(Json(serde_json::json!([entry]))),
        None => Ok(Json(serde_json::json!([]))),
    }
}

// ---------------------------------------------------------------------------
// Load queue
// ---------------------------------------------------------------------------

/// GET /druid/coordinator/v1/loadqueue -- segment load queue per server.
pub(crate) async fn handle_loadqueue(
    State(_state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    // In Phase 1 the load queue is managed in-memory during balance runs.
    // Return an empty map for now.
    Json(serde_json::json!({}))
}

/// GET /druid/coordinator/v1/loadqueue/:serverName -- specific server's load queue.
pub(crate) async fn handle_loadqueue_server(
    State(_state): State<Arc<AppState>>,
    Path(server_name): Path<String>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "segmentsToLoad": [],
        "segmentsToDrop": [],
        "segmentsToLoadSize": 0,
        "segmentsToDropSize": 0,
        "serverName": server_name
    }))
}

// ---------------------------------------------------------------------------
// Datasource management
// ---------------------------------------------------------------------------

/// DELETE /druid/coordinator/v1/datasources/:datasource -- disable (mark segments unused).
pub(crate) async fn handle_disable_datasource(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state
        .coordinator
        .disable_datasource(&datasource)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Coordinator error",
                &e.to_string(),
                "io.druid.server.coordinator.CoordinatorException",
            )
        })?;
    Ok(Json(serde_json::json!({"status": "disabled"})))
}

/// POST /druid/coordinator/v1/datasources/:datasource -- enable (mark segments used).
pub(crate) async fn handle_enable_datasource(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state
        .coordinator
        .enable_datasource(&datasource)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Coordinator error",
                &e.to_string(),
                "io.druid.server.coordinator.CoordinatorException",
            )
        })?;
    Ok(Json(serde_json::json!({"status": "enabled"})))
}

/// DELETE /druid/coordinator/v1/datasources/:datasource/segments/:segmentId -- disable specific segment.
pub(crate) async fn handle_disable_segment(
    State(state): State<Arc<AppState>>,
    Path((_, segment_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    state
        .coordinator
        .disable_segment(&segment_id)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Coordinator error",
                &e.to_string(),
                "io.druid.server.coordinator.CoordinatorException",
            )
        })?;
    Ok(Json(serde_json::json!({"status": "disabled"})))
}

// ---------------------------------------------------------------------------
// Server inventory
// ---------------------------------------------------------------------------

/// GET /druid/coordinator/v1/servers -- list of cluster servers.
pub(crate) async fn handle_servers(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let servers = state.coordinator.get_servers().await;
    let list: Vec<serde_json::Value> = servers
        .iter()
        .map(|s| {
            serde_json::json!({
                "host": format!("{}:{}", s.host, s.port),
                "tier": s.tier,
                "type": "historical",
                "priority": 0,
                "currSize": s.current_size,
                "maxSize": s.max_size
            })
        })
        .collect();
    Json(serde_json::json!(list))
}

/// GET /druid/coordinator/v1/servers/:serverName -- server details.
pub(crate) async fn handle_server(
    State(state): State<Arc<AppState>>,
    Path(server_name): Path<String>,
) -> Json<serde_json::Value> {
    let servers = state.coordinator.get_servers().await;
    let server = servers.iter().find(|s| s.name == server_name);
    match server {
        Some(s) => Json(serde_json::json!({
            "host": format!("{}:{}", s.host, s.port),
            "tier": s.tier,
            "type": "historical",
            "currSize": s.current_size,
            "maxSize": s.max_size,
            "segments": {}
        })),
        None => Json(serde_json::json!({
            "host": server_name,
            "tier": "_default_tier",
            "type": "historical",
            "currSize": 0,
            "maxSize": 0,
            "segments": {}
        })),
    }
}

/// GET /druid/coordinator/v1/servers/:serverName/segments -- segments on server.
pub(crate) async fn handle_server_segments(
    State(state): State<Arc<AppState>>,
    Path(server_name): Path<String>,
) -> Json<serde_json::Value> {
    let segments = state
        .coordinator
        .get_segments_for_server(&server_name)
        .await;
    let list: Vec<serde_json::Value> = segments
        .iter()
        .map(|s| {
            serde_json::json!({
                "segmentId": s.segment_id,
                "dataSource": s.data_source,
                "tier": s.tier
            })
        })
        .collect();
    Json(serde_json::json!(list))
}

// ---------------------------------------------------------------------------
// Metadata endpoints
// ---------------------------------------------------------------------------

/// GET /druid/coordinator/v1/metadata/datasources -- metadata about all datasources.
pub(crate) async fn handle_metadata_datasources(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let datasources = state.coordinator.get_data_sources().await.map_err(|e| {
        crate::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Metadata error",
            &e.to_string(),
            "io.druid.metadata.MetadataException",
        )
    })?;

    let mut result = Vec::new();
    for ds in &datasources {
        let segs = state.metadata.get_used_segments(ds).await.map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;
        result.push(serde_json::json!({
            "name": ds,
            "properties": {
                "segments": {
                    "count": segs.len(),
                    "size": 0
                }
            }
        }));
    }

    Ok(Json(serde_json::json!(result)))
}

/// GET /druid/coordinator/v1/metadata/datasources/:datasource/segments -- segment metadata.
pub(crate) async fn handle_metadata_datasource_segments(
    State(state): State<Arc<AppState>>,
    Path(datasource): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let segments = state
        .metadata
        .get_used_segments(&datasource)
        .await
        .map_err(|e| {
            crate::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata error",
                &e.to_string(),
                "io.druid.metadata.MetadataException",
            )
        })?;

    let list: Vec<serde_json::Value> = segments
        .iter()
        .map(|s| {
            serde_json::json!({
                "id": s.id,
                "dataSource": s.data_source,
                "interval": format!("{}/{}", s.start, s.end),
                "version": s.version,
                "size": 0,
                "binaryVersion": 9
            })
        })
        .collect();

    Ok(Json(serde_json::json!(list)))
}
