// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Lookup REST endpoints for Druid coordinator API.
//!
//! - `GET  /druid/coordinator/v1/lookups/config` — list all lookup tiers
//! - `GET  /druid/coordinator/v1/lookups/config/{tier}` — list lookups in tier
//! - `GET  /druid/coordinator/v1/lookups/config/{tier}/{id}` — get lookup
//! - `POST /druid/coordinator/v1/lookups/config/{tier}/{id}` — create/update
//! - `DELETE /druid/coordinator/v1/lookups/config/{tier}/{id}` — delete
//! - `GET  /druid/listen/v1/lookups` — internal lookup distribution

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use ferrodruid_lookup::{LookupExtractorFactory, LookupManager, LookupSpec, LookupTable};

/// GET /druid/coordinator/v1/lookups/config
///
/// Returns a map of tier names to lookup names. Currently only the
/// `__default` tier is supported.
pub(crate) async fn handle_list_tiers(
    State(mgr): State<Arc<LookupManager>>,
) -> Json<serde_json::Value> {
    let names = mgr.list();
    let mut tier_map = HashMap::new();
    let mut lookup_map = HashMap::new();
    for name in &names {
        if let Some(lookup) = mgr.get(name) {
            lookup_map.insert(
                name.clone(),
                serde_json::json!({
                    "version": lookup.version(),
                    "lookupExtractorFactory": {
                        "type": "map",
                        "map": lookup.to_map()
                    }
                }),
            );
        }
    }
    tier_map.insert("__default", lookup_map);
    Json(serde_json::json!(tier_map))
}

/// GET /druid/coordinator/v1/lookups/config/{tier}
///
/// Returns a map of lookup names to specs within the given tier.
pub(crate) async fn handle_list_lookups_in_tier(
    State(mgr): State<Arc<LookupManager>>,
    Path(_tier): Path<String>,
) -> Json<serde_json::Value> {
    // We treat all lookups as belonging to the requested tier for now.
    let names = mgr.list();
    let mut lookup_map = HashMap::new();
    for name in &names {
        if let Some(lookup) = mgr.get(name) {
            lookup_map.insert(
                name.clone(),
                serde_json::json!({
                    "version": lookup.version(),
                    "lookupExtractorFactory": {
                        "type": "map",
                        "map": lookup.to_map()
                    }
                }),
            );
        }
    }
    Json(serde_json::json!(lookup_map))
}

/// GET /druid/coordinator/v1/lookups/config/{tier}/{id}
///
/// Returns the lookup spec for the given id.
pub(crate) async fn handle_get_lookup(
    State(mgr): State<Arc<LookupManager>>,
    Path((_tier, id)): Path<(String, String)>,
) -> Result<Json<LookupSpec>, StatusCode> {
    let lookup = mgr.get(&id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(LookupSpec {
        version: lookup.version().to_string(),
        lookup_extractor_factory: LookupExtractorFactory::Map {
            map: lookup.to_map(),
        },
    }))
}

/// POST /druid/coordinator/v1/lookups/config/{tier}/{id}
///
/// Create or update a lookup.
pub(crate) async fn handle_create_lookup(
    State(mgr): State<Arc<LookupManager>>,
    Path((_tier, id)): Path<(String, String)>,
    Json(spec): Json<LookupSpec>,
) -> StatusCode {
    let map = match spec.lookup_extractor_factory {
        LookupExtractorFactory::Map { map } => map,
    };
    let table = LookupTable::from_map(id, spec.version, map);
    mgr.register(table);
    StatusCode::OK
}

/// DELETE /druid/coordinator/v1/lookups/config/{tier}/{id}
///
/// Delete a lookup.
pub(crate) async fn handle_delete_lookup(
    State(mgr): State<Arc<LookupManager>>,
    Path((_tier, id)): Path<(String, String)>,
) -> StatusCode {
    if mgr.remove(&id).is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// GET /druid/listen/v1/lookups
///
/// Internal endpoint for lookup distribution to data nodes.
pub(crate) async fn handle_listen_lookups(
    State(mgr): State<Arc<LookupManager>>,
) -> Json<serde_json::Value> {
    let all = mgr.get_all();
    let mut result = HashMap::new();
    for lookup in &all {
        result.insert(
            lookup.name().to_string(),
            serde_json::json!({
                "version": lookup.version(),
                "lookupExtractorFactory": {
                    "type": "map",
                    "map": lookup.to_map()
                }
            }),
        );
    }
    Json(serde_json::json!(result))
}
