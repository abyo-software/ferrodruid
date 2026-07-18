// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Lookup dimension mapping (key-value) for FerroDruid.
//!
//! Provides [`LookupTable`] for individual dimension value mapping and
//! [`LookupManager`] for managing multiple lookup tables. Lookups allow
//! dimension value transformation at query time (e.g., country_code ->
//! country_name).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from lookup operations.
#[derive(Debug, Error)]
pub enum LookupError {
    /// The requested lookup was not found.
    #[error("lookup not found: {0}")]
    NotFound(String),
    /// Configuration error.
    #[error("lookup config error: {0}")]
    Config(String),
}

// ---------------------------------------------------------------------------
// LookupTable
// ---------------------------------------------------------------------------

/// A lookup table for dimension value mapping.
///
/// Each table has a name, a version string, and an underlying concurrent map
/// of key-value string pairs. All operations are lock-free for readers.
pub struct LookupTable {
    name: String,
    map: DashMap<String, String>,
    version: String,
}

impl LookupTable {
    /// Create a new empty lookup table.
    pub fn new(name: String, version: String) -> Self {
        Self {
            name,
            map: DashMap::new(),
            version,
        }
    }

    /// Create a lookup table pre-populated from a `HashMap`.
    pub fn from_map(name: String, version: String, map: HashMap<String, String>) -> Self {
        let dm = DashMap::with_capacity(map.len());
        for (k, v) in map {
            dm.insert(k, v);
        }
        Self {
            name,
            map: dm,
            version,
        }
    }

    /// Look up a key and return the mapped value, if present.
    pub fn get(&self, key: &str) -> Option<String> {
        self.map.get(key).map(|v| v.value().clone())
    }

    /// Insert or update a key-value pair.
    pub fn put(&self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Remove a key and return its previous value, if any.
    pub fn remove(&self, key: &str) -> Option<String> {
        self.map.remove(key).map(|(_, v)| v)
    }

    /// Return the number of entries in the lookup.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Return `true` if the lookup has no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Return all keys in the lookup (unordered).
    pub fn keys(&self) -> Vec<String> {
        self.map.iter().map(|e| e.key().clone()).collect()
    }

    /// Return the lookup name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the lookup version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Snapshot the lookup as a regular `HashMap`.
    pub fn to_map(&self) -> HashMap<String, String> {
        self.map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// LookupManager
// ---------------------------------------------------------------------------

/// Manages all lookup tables.
///
/// Thread-safe: backed by a `DashMap` keyed by lookup name.
pub struct LookupManager {
    lookups: DashMap<String, Arc<LookupTable>>,
}

impl Default for LookupManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LookupManager {
    /// Create a new empty lookup manager.
    pub fn new() -> Self {
        Self {
            lookups: DashMap::new(),
        }
    }

    /// Register (or replace) a lookup table.
    ///
    /// Returns the previously registered table with the same name, if any.
    pub fn register(&self, lookup: LookupTable) -> Option<Arc<LookupTable>> {
        let name = lookup.name.clone();
        let arc = Arc::new(lookup);
        self.lookups.insert(name, arc)
    }

    /// Retrieve a lookup table by name.
    pub fn get(&self, name: &str) -> Option<Arc<LookupTable>> {
        self.lookups.get(name).map(|v| Arc::clone(v.value()))
    }

    /// Remove a lookup table by name.
    pub fn remove(&self, name: &str) -> Option<Arc<LookupTable>> {
        self.lookups.remove(name).map(|(_, v)| v)
    }

    /// List all registered lookup names (unordered).
    pub fn list(&self) -> Vec<String> {
        self.lookups.iter().map(|e| e.key().clone()).collect()
    }

    /// Return all registered lookup tables.
    pub fn get_all(&self) -> Vec<Arc<LookupTable>> {
        self.lookups.iter().map(|e| Arc::clone(e.value())).collect()
    }
}

// ---------------------------------------------------------------------------
// REST API types
// ---------------------------------------------------------------------------

/// Druid lookup spec (for REST API serialization).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupSpec {
    /// Version string for this lookup.
    pub version: String,
    /// The extractor factory configuration.
    #[serde(rename = "lookupExtractorFactory")]
    pub lookup_extractor_factory: LookupExtractorFactory,
}

/// Supported lookup extractor factory types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LookupExtractorFactory {
    /// Map-based extractor: an inline key-value map.
    #[serde(rename = "map")]
    Map {
        /// The key-value mapping.
        map: HashMap<String, String>,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_table_new_is_empty() {
        let t = LookupTable::new("test".into(), "v1".into());
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        assert_eq!(t.name(), "test");
        assert_eq!(t.version(), "v1");
    }

    #[test]
    fn lookup_table_put_get_remove() {
        let t = LookupTable::new("cc".into(), "v1".into());
        t.put("US".into(), "United States".into());
        t.put("JP".into(), "Japan".into());

        assert_eq!(t.len(), 2);
        assert_eq!(t.get("US"), Some("United States".into()));
        assert_eq!(t.get("JP"), Some("Japan".into()));
        assert_eq!(t.get("XX"), None);

        let removed = t.remove("US");
        assert_eq!(removed, Some("United States".into()));
        assert_eq!(t.len(), 1);
        assert_eq!(t.get("US"), None);
    }

    #[test]
    fn lookup_table_from_map() {
        let mut m = HashMap::new();
        m.insert("a".into(), "1".into());
        m.insert("b".into(), "2".into());

        let t = LookupTable::from_map("test".into(), "v2".into(), m.clone());
        assert_eq!(t.len(), 2);
        assert_eq!(t.get("a"), Some("1".into()));

        let snap = t.to_map();
        assert_eq!(snap, m);
    }

    #[test]
    fn lookup_table_keys() {
        let t = LookupTable::new("k".into(), "v1".into());
        t.put("x".into(), "1".into());
        t.put("y".into(), "2".into());

        let mut keys = t.keys();
        keys.sort();
        assert_eq!(keys, vec!["x", "y"]);
    }

    #[test]
    fn manager_register_get_remove() {
        let mgr = LookupManager::new();
        let t = LookupTable::new("cc".into(), "v1".into());
        t.put("US".into(), "United States".into());

        // First register returns None (no previous).
        assert!(mgr.register(t).is_none());
        assert_eq!(mgr.list().len(), 1);

        let lookup = mgr.get("cc").expect("should exist");
        assert_eq!(lookup.get("US"), Some("United States".into()));

        // Replace returns old.
        let t2 = LookupTable::new("cc".into(), "v2".into());
        let old = mgr.register(t2);
        assert!(old.is_some());
        assert_eq!(old.expect("old").version(), "v1");

        // Remove.
        let removed = mgr.remove("cc");
        assert!(removed.is_some());
        assert!(mgr.get("cc").is_none());
        assert!(mgr.list().is_empty());
    }

    #[test]
    fn manager_get_all() {
        let mgr = LookupManager::new();
        mgr.register(LookupTable::new("a".into(), "v1".into()));
        mgr.register(LookupTable::new("b".into(), "v1".into()));

        let all = mgr.get_all();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn manager_remove_nonexistent() {
        let mgr = LookupManager::new();
        assert!(mgr.remove("nope").is_none());
    }

    #[test]
    fn concurrent_access() {
        use std::thread;

        let t = Arc::new(LookupTable::new("concurrent".into(), "v1".into()));
        let mut handles = Vec::new();

        for i in 0..10 {
            let t = Arc::clone(&t);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("{i}-{j}");
                    t.put(key.clone(), format!("val-{key}"));
                }
            }));
        }
        for h in handles {
            h.join().expect("thread join");
        }

        assert_eq!(t.len(), 1000);
    }

    #[test]
    fn lookup_spec_json_map() {
        let spec = LookupSpec {
            version: "v1".into(),
            lookup_extractor_factory: LookupExtractorFactory::Map {
                map: {
                    let mut m = HashMap::new();
                    m.insert("US".into(), "United States".into());
                    m
                },
            },
        };

        let json = serde_json::to_string(&spec).expect("serialize");
        assert!(json.contains("\"type\":\"map\""));
        assert!(json.contains("\"version\":\"v1\""));
        assert!(json.contains("\"US\":\"United States\""));

        let parsed: LookupSpec = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.version, "v1");
        match &parsed.lookup_extractor_factory {
            LookupExtractorFactory::Map { map } => {
                assert_eq!(map.get("US"), Some(&"United States".to_string()));
            }
        }
    }

    #[test]
    fn lookup_spec_json_roundtrip() {
        let raw = r#"{
            "version": "v2",
            "lookupExtractorFactory": {
                "type": "map",
                "map": {"JP": "Japan", "DE": "Germany"}
            }
        }"#;

        let spec: LookupSpec = serde_json::from_str(raw).expect("parse");
        assert_eq!(spec.version, "v2");
        match &spec.lookup_extractor_factory {
            LookupExtractorFactory::Map { map } => {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get("JP"), Some(&"Japan".to_string()));
            }
        }
    }

    #[test]
    fn default_manager() {
        let mgr = LookupManager::default();
        assert!(mgr.list().is_empty());
    }
}
