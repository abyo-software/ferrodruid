// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid metadata import and segment migration for FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use ferrodruid_metadata::{MetadataStore, SegmentMetadataRow};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

/// Errors from import/export operations.
#[derive(Debug, Error)]
pub enum ImportExportError {
    /// Metadata import failed.
    #[error("metadata import failed: {0}")]
    MetadataImport(String),
    /// Segment migration failed.
    #[error("segment migration failed: {0}")]
    SegmentMigration(String),
    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Underlying store error.
    #[error("store error: {0}")]
    Store(#[from] ferrodruid_common::DruidError),
}

/// Result alias for import/export operations.
pub type Result<T> = std::result::Result<T, ImportExportError>;

/// Summary of an import operation.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ImportSummary {
    /// Number of segments imported.
    pub segments_imported: usize,
    /// Number of rules imported.
    pub rules_imported: usize,
    /// Number of supervisors imported.
    pub supervisors_imported: usize,
    /// Number of config entries imported.
    pub config_imported: usize,
}

/// Export metadata from a FerroDruid metadata store.
pub struct MetadataExporter {
    metadata: Arc<MetadataStore>,
}

impl MetadataExporter {
    /// Create a new exporter.
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self { metadata }
    }

    /// Export all segments metadata as JSON values.
    pub async fn export_segments(&self) -> Result<Vec<serde_json::Value>> {
        let segments = self.metadata.get_all_segments().await?;
        let values: Vec<serde_json::Value> = segments
            .iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(values)
    }

    /// Export all rules keyed by datasource.
    pub async fn export_rules(&self) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        let datasources = self.metadata.get_all_data_sources().await?;
        let mut out = HashMap::new();
        for ds in datasources {
            let rules = self.metadata.get_rules(&ds).await?;
            if !rules.is_empty() {
                out.insert(ds, rules);
            }
        }
        Ok(out)
    }

    /// Export all supervisors as JSON values.
    pub async fn export_supervisors(&self) -> Result<Vec<serde_json::Value>> {
        let supervisors = self.metadata.get_all_supervisors().await?;
        let values: Vec<serde_json::Value> = supervisors
            .iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(values)
    }

    /// Export all config entries as a name-to-value map.
    pub async fn export_config(&self) -> Result<HashMap<String, serde_json::Value>> {
        let entries = self.metadata.get_all_config().await?;
        Ok(entries.into_iter().collect())
    }

    /// Full export of all metadata as a single JSON value.
    pub async fn export_all(&self) -> Result<serde_json::Value> {
        let segments = self.export_segments().await?;
        let rules = self.export_rules().await?;
        let supervisors = self.export_supervisors().await?;
        let config = self.export_config().await?;

        Ok(serde_json::json!({
            "version": "1",
            "segments": segments,
            "rules": rules,
            "supervisors": supervisors,
            "config": config,
        }))
    }
}

/// Import metadata into a FerroDruid metadata store.
pub struct MetadataImporter {
    metadata: Arc<MetadataStore>,
}

impl MetadataImporter {
    /// Create a new importer.
    pub fn new(metadata: Arc<MetadataStore>) -> Self {
        Self { metadata }
    }

    /// Import segments from export JSON.
    pub async fn import_segments(&self, segments: &[serde_json::Value]) -> Result<usize> {
        let mut count = 0;
        for val in segments {
            let row: SegmentMetadataRow = serde_json::from_value(val.clone())
                .map_err(|e| ImportExportError::MetadataImport(format!("bad segment: {e}")))?;
            self.metadata.insert_segment(&row).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Import rules from export JSON (datasource -> rule array).
    pub async fn import_rules(
        &self,
        rules: &HashMap<String, Vec<serde_json::Value>>,
    ) -> Result<usize> {
        let mut count = 0;
        for (ds, rule_list) in rules {
            self.metadata.set_rules(ds, rule_list).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Import supervisors from export JSON.
    pub async fn import_supervisors(&self, supervisors: &[serde_json::Value]) -> Result<usize> {
        let mut count = 0;
        for val in supervisors {
            let spec_id = val.get("spec_id").and_then(|v| v.as_str()).ok_or_else(|| {
                ImportExportError::MetadataImport("supervisor missing spec_id".to_string())
            })?;
            let payload = val
                .get("payload")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            self.metadata.insert_supervisor(spec_id, &payload).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Import config entries from export JSON (name -> value).
    pub async fn import_config(
        &self,
        config: &HashMap<String, serde_json::Value>,
    ) -> Result<usize> {
        let mut count = 0;
        for (name, value) in config {
            self.metadata.set_config(name, value).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Full import from an export JSON blob (as produced by [`MetadataExporter::export_all`]).
    pub async fn import_all(&self, data: &serde_json::Value) -> Result<ImportSummary> {
        let mut summary = ImportSummary::default();

        if let Some(segments) = data.get("segments").and_then(|v| v.as_array()) {
            summary.segments_imported = self.import_segments(segments).await?;
        }

        if let Some(rules_val) = data.get("rules") {
            let rules: HashMap<String, Vec<serde_json::Value>> =
                serde_json::from_value(rules_val.clone())
                    .map_err(|e| ImportExportError::MetadataImport(format!("bad rules: {e}")))?;
            summary.rules_imported = self.import_rules(&rules).await?;
        }

        if let Some(supervisors) = data.get("supervisors").and_then(|v| v.as_array()) {
            summary.supervisors_imported = self.import_supervisors(supervisors).await?;
        }

        if let Some(config_val) = data.get("config") {
            let config: HashMap<String, serde_json::Value> =
                serde_json::from_value(config_val.clone())
                    .map_err(|e| ImportExportError::MetadataImport(format!("bad config: {e}")))?;
            summary.config_imported = self.import_config(&config).await?;
        }

        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn setup() -> Arc<MetadataStore> {
        let store = MetadataStore::new_in_memory()
            .await
            .expect("create in-memory store");
        store.initialize().await.expect("initialize schema");
        Arc::new(store)
    }

    fn make_segment(id: &str, ds: &str) -> SegmentMetadataRow {
        SegmentMetadataRow {
            id: id.to_string(),
            data_source: ds.to_string(),
            created_date: "2024-01-01T00:00:00Z".to_string(),
            start: "2024-01-01T00:00:00Z".to_string(),
            end: "2024-02-01T00:00:00Z".to_string(),
            version: "2024-01-01T00:00:00.000Z".to_string(),
            used: true,
            payload: json!({"dataSource": ds, "dimensions": ["page"]}),
        }
    }

    #[tokio::test]
    async fn export_empty_store() {
        let store = setup().await;
        let exporter = MetadataExporter::new(store);
        let all = exporter.export_all().await.expect("export all");

        assert_eq!(all["segments"].as_array().expect("array").len(), 0);
        assert_eq!(all["rules"].as_object().expect("object").len(), 0);
        assert_eq!(all["supervisors"].as_array().expect("array").len(), 0);
        assert_eq!(all["config"].as_object().expect("object").len(), 0);
    }

    #[tokio::test]
    async fn export_with_data() {
        let store = setup().await;
        store
            .insert_segment(&make_segment("seg-1", "wiki"))
            .await
            .expect("insert");
        store
            .set_rules("wiki", &[json!({"type": "loadForever"})])
            .await
            .expect("set rules");
        store
            .insert_supervisor("kafka-wiki", &json!({"type": "kafka"}))
            .await
            .expect("insert supervisor");
        store
            .set_config("lookups", &json!({"version": 1}))
            .await
            .expect("set config");

        let exporter = MetadataExporter::new(store);
        let all = exporter.export_all().await.expect("export all");

        assert_eq!(all["segments"].as_array().expect("array").len(), 1);
        assert_eq!(all["rules"].as_object().expect("object").len(), 1);
        assert_eq!(all["supervisors"].as_array().expect("array").len(), 1);
        assert_eq!(all["config"].as_object().expect("object").len(), 1);
    }

    #[tokio::test]
    async fn import_export_round_trip() {
        // 1. Populate source.
        let source = setup().await;
        source
            .insert_segment(&make_segment("seg-1", "wiki"))
            .await
            .expect("insert");
        source
            .insert_segment(&make_segment("seg-2", "clicks"))
            .await
            .expect("insert");
        source
            .set_rules("wiki", &[json!({"type": "loadForever"})])
            .await
            .expect("set rules");
        source
            .insert_supervisor("kafka-wiki", &json!({"type": "kafka", "topic": "wiki"}))
            .await
            .expect("insert supervisor");
        source
            .set_config("lookups", &json!({"version": 1}))
            .await
            .expect("set config");

        // 2. Export.
        let exporter = MetadataExporter::new(Arc::clone(&source));
        let exported = exporter.export_all().await.expect("export all");

        // 3. Import into a fresh store.
        let dest = setup().await;
        let importer = MetadataImporter::new(Arc::clone(&dest));
        let summary = importer.import_all(&exported).await.expect("import all");

        assert_eq!(summary.segments_imported, 2);
        assert_eq!(summary.rules_imported, 1);
        assert_eq!(summary.supervisors_imported, 1);
        assert_eq!(summary.config_imported, 1);

        // 4. Re-export from dest and compare.
        let re_exporter = MetadataExporter::new(dest);
        let re_exported = re_exporter.export_all().await.expect("re-export");

        assert_eq!(re_exported["segments"].as_array().expect("array").len(), 2);
        assert_eq!(re_exported["config"]["lookups"], json!({"version": 1}));
    }

    #[tokio::test]
    async fn import_invalid_segment_returns_error() {
        let store = setup().await;
        let importer = MetadataImporter::new(store);
        let bad = vec![json!({"not_a_segment": true})];
        let result = importer.import_segments(&bad).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn import_supervisor_missing_spec_id() {
        let store = setup().await;
        let importer = MetadataImporter::new(store);
        let bad = vec![json!({"payload": {}})]; // no spec_id
        let result = importer.import_supervisors(&bad).await;
        assert!(result.is_err());
    }
}
