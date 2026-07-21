// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Server registry and segment-size accounting for the Coordinator.
//!
//! The registry tracks the Historical nodes the Coordinator knows about, their
//! tier membership, and how much segment data each one currently holds. The
//! Coordinator updates `current_size` as it issues Load and Drop actions so
//! that capacity gating reflects the real on-disk footprint of segments rather
//! than a placeholder.

use std::collections::HashMap;

use ferrodruid_metadata::SegmentMetadataRow;

/// Fallback segment size, in bytes, used when a segment payload does not carry
/// a `"size"` field. Druid segment metadata normally includes the byte size of
/// the segment, but historical or hand-written rows may omit it; we use a
/// modest non-zero default so capacity accounting never treats such a segment
/// as free.
pub const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 1;

/// Extract the byte size of a segment from its metadata payload.
///
/// Druid stores the segment size under the top-level `"size"` key of the
/// segment payload JSON. This reader accepts either an integer or a numeric
/// string (some serializers emit large sizes as strings). When the field is
/// absent or unparsable, [`DEFAULT_SEGMENT_SIZE_BYTES`] is returned so that the
/// segment still consumes some capacity.
#[must_use]
pub fn segment_size_bytes(segment: &SegmentMetadataRow) -> u64 {
    match segment.payload.get("size") {
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .or_else(|| n.as_i64().filter(|v| *v >= 0).map(|v| v as u64))
            .unwrap_or(DEFAULT_SEGMENT_SIZE_BYTES),
        Some(serde_json::Value::String(s)) => s
            .trim()
            .parse::<u64>()
            .unwrap_or(DEFAULT_SEGMENT_SIZE_BYTES),
        _ => DEFAULT_SEGMENT_SIZE_BYTES,
    }
}

/// Information about a Historical server available for segment assignment.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerInfo {
    /// Unique server name.
    pub name: String,
    /// Server hostname.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Tier name (e.g. `"_default_tier"`, `"hot"`, `"cold"`).
    pub tier: String,
    /// Maximum segment data size (bytes) this server can hold.
    pub max_size: u64,
    /// Current segment data size (bytes) loaded on this server.
    pub current_size: u64,
}

impl ServerInfo {
    /// Returns the remaining capacity in bytes.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.max_size.saturating_sub(self.current_size)
    }

    /// Returns `true` if this server can hold `size` additional bytes.
    #[must_use]
    pub fn can_hold(&self, size: u64) -> bool {
        self.remaining() >= size
    }

    /// Returns the fill ratio in `[0.0, 1.0]` (`current / max`).
    ///
    /// A server with zero `max_size` is treated as fully loaded (ratio `1.0`).
    #[must_use]
    pub fn fill_ratio(&self) -> f64 {
        if self.max_size == 0 {
            return 1.0;
        }
        (self.current_size as f64 / self.max_size as f64).min(1.0)
    }
}

/// In-memory registry of known Historical servers.
///
/// The registry owns the authoritative `current_size` for each server. The
/// Coordinator increments / decrements it as segments are placed and removed.
#[derive(Debug, Default)]
pub struct ServerRegistry {
    servers: HashMap<String, ServerInfo>,
}

impl ServerRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            servers: HashMap::new(),
        }
    }

    /// Register a new server, or replace an existing one with the same name.
    ///
    /// Returns the previous entry if the server was already registered.
    pub fn register(&mut self, server: ServerInfo) -> Option<ServerInfo> {
        self.servers.insert(server.name.clone(), server)
    }

    /// Update an existing server's mutable fields (tier / `max_size`) in place,
    /// preserving the tracked `current_size`.
    ///
    /// Returns `true` if a server with `name` existed and was updated.
    pub fn update(&mut self, name: &str, tier: Option<String>, max_size: Option<u64>) -> bool {
        match self.servers.get_mut(name) {
            Some(srv) => {
                if let Some(t) = tier {
                    srv.tier = t;
                }
                if let Some(m) = max_size {
                    srv.max_size = m;
                }
                true
            }
            None => false,
        }
    }

    /// Remove a server from the registry, returning it if present.
    pub fn remove(&mut self, name: &str) -> Option<ServerInfo> {
        self.servers.remove(name)
    }

    /// Look up a server by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ServerInfo> {
        self.servers.get(name)
    }

    /// Return all registered servers, sorted by name for deterministic order.
    #[must_use]
    pub fn list(&self) -> Vec<ServerInfo> {
        let mut out: Vec<ServerInfo> = self.servers.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Number of registered servers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.servers.len()
    }

    /// Returns `true` if no servers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.servers.is_empty()
    }

    /// Add `size` bytes to a server's `current_size`. No-op if absent.
    pub fn add_size(&mut self, name: &str, size: u64) {
        if let Some(srv) = self.servers.get_mut(name) {
            srv.current_size = srv.current_size.saturating_add(size);
        }
    }

    /// Subtract `size` bytes from a server's `current_size`. No-op if absent.
    pub fn sub_size(&mut self, name: &str, size: u64) {
        if let Some(srv) = self.servers.get_mut(name) {
            srv.current_size = srv.current_size.saturating_sub(size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seg_with_payload(payload: serde_json::Value) -> SegmentMetadataRow {
        SegmentMetadataRow {
            id: "s".into(),
            data_source: "ds".into(),
            created_date: "2024-01-01T00:00:00Z".into(),
            start: "2024-01-01T00:00:00+00:00".into(),
            end: "2024-02-01T00:00:00+00:00".into(),
            version: "v1".into(),
            used: true,
            payload,
        }
    }

    #[test]
    fn size_from_integer_payload() {
        let s = seg_with_payload(json!({"size": 12_345}));
        assert_eq!(segment_size_bytes(&s), 12_345);
    }

    #[test]
    fn size_from_string_payload() {
        let s = seg_with_payload(json!({"size": "9876543210"}));
        assert_eq!(segment_size_bytes(&s), 9_876_543_210);
    }

    #[test]
    fn size_absent_uses_default() {
        let s = seg_with_payload(json!({"dataSource": "ds"}));
        assert_eq!(segment_size_bytes(&s), DEFAULT_SEGMENT_SIZE_BYTES);
    }

    #[test]
    fn size_negative_uses_default() {
        let s = seg_with_payload(json!({"size": -5}));
        assert_eq!(segment_size_bytes(&s), DEFAULT_SEGMENT_SIZE_BYTES);
    }

    #[test]
    fn registry_crud() {
        let mut reg = ServerRegistry::new();
        assert!(reg.is_empty());
        let srv = ServerInfo {
            name: "h1".into(),
            host: "127.0.0.1".into(),
            port: 8083,
            tier: "_default_tier".into(),
            max_size: 1000,
            current_size: 0,
        };
        assert!(reg.register(srv).is_none());
        assert_eq!(reg.len(), 1);
        assert!(reg.get("h1").is_some());

        // Update tier + max_size, preserve current_size.
        reg.add_size("h1", 200);
        assert!(reg.update("h1", Some("hot".into()), Some(5000)));
        let s = reg.get("h1").expect("present");
        assert_eq!(s.tier, "hot");
        assert_eq!(s.max_size, 5000);
        assert_eq!(s.current_size, 200);

        // Update of unknown server returns false.
        assert!(!reg.update("nope", None, None));

        // Remove.
        assert!(reg.remove("h1").is_some());
        assert!(reg.is_empty());
        assert!(reg.remove("h1").is_none());
    }

    #[test]
    fn size_accounting_saturates() {
        let mut reg = ServerRegistry::new();
        reg.register(ServerInfo {
            name: "h1".into(),
            host: "h".into(),
            port: 1,
            tier: "t".into(),
            max_size: 100,
            current_size: 0,
        });
        reg.sub_size("h1", 50); // below zero -> saturates at 0
        assert_eq!(reg.get("h1").expect("present").current_size, 0);
        reg.add_size("h1", 30);
        assert_eq!(reg.get("h1").expect("present").current_size, 30);
    }

    #[test]
    fn list_sorted() {
        let mut reg = ServerRegistry::new();
        for name in ["c", "a", "b"] {
            reg.register(ServerInfo {
                name: name.into(),
                host: "h".into(),
                port: 1,
                tier: "t".into(),
                max_size: 1,
                current_size: 0,
            });
        }
        let names: Vec<String> = reg.list().into_iter().map(|s| s.name).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn capacity_helpers() {
        let s = ServerInfo {
            name: "h".into(),
            host: "h".into(),
            port: 1,
            tier: "t".into(),
            max_size: 100,
            current_size: 60,
        };
        assert_eq!(s.remaining(), 40);
        assert!(s.can_hold(40));
        assert!(!s.can_hold(41));
        assert!((s.fill_ratio() - 0.6).abs() < 1e-9);

        let zero = ServerInfo {
            max_size: 0,
            ..s.clone()
        };
        assert_eq!(zero.fill_ratio(), 1.0);
    }
}
