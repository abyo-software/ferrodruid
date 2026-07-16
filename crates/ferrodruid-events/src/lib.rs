// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Audit log and event listener for FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

/// Event system errors.
#[derive(Debug, Error)]
pub enum EventError {
    /// Failed to emit event.
    #[error("event emit failed: {0}")]
    EmitFailed(String),
}

/// Audit event types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    /// A query has started.
    QueryStart,
    /// A query completed successfully.
    QueryComplete,
    /// A query failed.
    QueryFailed,
    /// A segment was loaded onto a historical node.
    SegmentLoaded,
    /// A segment was dropped from a historical node.
    SegmentDropped,
    /// A supervisor was created.
    SupervisorCreated,
    /// A supervisor was shut down.
    SupervisorShutdown,
    /// A task was submitted.
    TaskSubmitted,
    /// A task completed successfully.
    TaskCompleted,
    /// A task failed.
    TaskFailed,
    /// Configuration was changed.
    ConfigChanged,
    /// Load rules were changed.
    RulesChanged,
    /// A lookup was created.
    LookupCreated,
    /// A lookup was deleted.
    LookupDeleted,
    /// A user was authenticated.
    UserAuthenticated,
    /// An authorization request was denied.
    AuthorizationDenied,
}

/// A single audit event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    /// Unique event identifier.
    pub id: u64,
    /// Event type.
    pub event_type: EventType,
    /// Timestamp when the event was emitted.
    pub timestamp: DateTime<Utc>,
    /// Author / principal that triggered the event.
    pub author: Option<String>,
    /// Optional comment.
    pub comment: Option<String>,
    /// Event key (e.g. datasource name, task id).
    pub key: String,
    /// Event payload (JSON).
    pub payload: serde_json::Value,
}

/// In-memory ring-buffer event emitter for audit events.
pub struct EventEmitter {
    events: RwLock<Vec<AuditEvent>>,
    next_id: AtomicU64,
    max_events: usize,
}

impl EventEmitter {
    /// Create a new event emitter with the given ring buffer size.
    pub fn new(max_events: usize) -> Self {
        Self {
            events: RwLock::new(Vec::with_capacity(max_events)),
            next_id: AtomicU64::new(1),
            max_events,
        }
    }

    /// Emit an audit event.
    pub fn emit(
        &self,
        event_type: EventType,
        key: &str,
        author: Option<&str>,
        payload: serde_json::Value,
    ) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let event = AuditEvent {
            id,
            event_type,
            timestamp: Utc::now(),
            author: author.map(|s| s.to_string()),
            comment: None,
            key: key.to_string(),
            payload,
        };

        tracing::info!(
            event_type = ?event.event_type,
            key = %event.key,
            id = event.id,
            "audit event"
        );

        let mut events = self
            .events
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events.push(event);
        // Ring buffer: keep only the most recent max_events.
        if events.len() > self.max_events {
            let excess = events.len() - self.max_events;
            events.drain(..excess);
        }
    }

    /// Get events, optionally filtered by type, limited to `limit` most recent.
    pub fn get_events(&self, event_type: Option<&EventType>, limit: usize) -> Vec<AuditEvent> {
        let events = self
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let iter = events.iter().rev();
        match event_type {
            Some(et) => iter
                .filter(|e| &e.event_type == et)
                .take(limit)
                .cloned()
                .collect(),
            None => iter.take(limit).cloned().collect(),
        }
    }

    /// Get all events since the given timestamp.
    pub fn get_events_since(&self, since: DateTime<Utc>) -> Vec<AuditEvent> {
        let events = self
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events
            .iter()
            .filter(|e| e.timestamp >= since)
            .cloned()
            .collect()
    }

    /// Return the total number of events currently stored.
    pub fn event_count(&self) -> usize {
        let events = self
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn emit_and_retrieve() {
        let emitter = EventEmitter::new(100);
        emitter.emit(EventType::QueryStart, "wiki", Some("admin"), json!({}));
        emitter.emit(
            EventType::QueryComplete,
            "wiki",
            Some("admin"),
            json!({"rows": 42}),
        );
        assert_eq!(emitter.event_count(), 2);

        let all = emitter.get_events(None, 10);
        assert_eq!(all.len(), 2);
        // Most recent first.
        assert_eq!(all[0].event_type, EventType::QueryComplete);
        assert_eq!(all[1].event_type, EventType::QueryStart);
    }

    #[test]
    fn filter_by_type() {
        let emitter = EventEmitter::new(100);
        emitter.emit(EventType::QueryStart, "wiki", None, json!({}));
        emitter.emit(EventType::SegmentLoaded, "wiki", None, json!({}));
        emitter.emit(EventType::QueryStart, "clicks", None, json!({}));

        let queries = emitter.get_events(Some(&EventType::QueryStart), 10);
        assert_eq!(queries.len(), 2);
        for e in &queries {
            assert_eq!(e.event_type, EventType::QueryStart);
        }

        let segments = emitter.get_events(Some(&EventType::SegmentLoaded), 10);
        assert_eq!(segments.len(), 1);
    }

    #[test]
    fn ring_buffer_overflow() {
        let emitter = EventEmitter::new(3);
        for i in 0..5 {
            emitter.emit(
                EventType::TaskSubmitted,
                &format!("task-{i}"),
                None,
                json!({"i": i}),
            );
        }
        assert_eq!(emitter.event_count(), 3);

        let events = emitter.get_events(None, 10);
        assert_eq!(events.len(), 3);
        // Should have the 3 most recent (task-4, task-3, task-2 in reverse order).
        assert_eq!(events[0].key, "task-4");
        assert_eq!(events[1].key, "task-3");
        assert_eq!(events[2].key, "task-2");
    }

    #[test]
    fn get_events_since() {
        let emitter = EventEmitter::new(100);
        emitter.emit(EventType::ConfigChanged, "config-a", None, json!({}));

        let cutoff = Utc::now();
        // Small sleep not needed — events emitted after cutoff share the same
        // millisecond but we compare >=.
        emitter.emit(EventType::ConfigChanged, "config-b", None, json!({}));

        let since = emitter.get_events_since(cutoff);
        // At minimum the second event; both may appear if same timestamp.
        assert!(!since.is_empty());
        assert!(since.iter().any(|e| e.key == "config-b"));
    }

    #[test]
    fn limit_restricts_results() {
        let emitter = EventEmitter::new(100);
        for i in 0..10 {
            emitter.emit(EventType::QueryStart, &format!("q-{i}"), None, json!({}));
        }
        let limited = emitter.get_events(None, 3);
        assert_eq!(limited.len(), 3);
    }

    #[test]
    fn event_ids_are_unique_and_monotonic() {
        let emitter = EventEmitter::new(100);
        emitter.emit(EventType::QueryStart, "a", None, json!({}));
        emitter.emit(EventType::QueryStart, "b", None, json!({}));

        let events = emitter.get_events(None, 10);
        assert!(events[0].id > events[1].id);
    }

    #[test]
    fn serde_round_trip() {
        let event = AuditEvent {
            id: 1,
            event_type: EventType::LookupCreated,
            timestamp: Utc::now(),
            author: Some("admin".to_string()),
            comment: Some("initial".to_string()),
            key: "my-lookup".to_string(),
            payload: json!({"version": "v1"}),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: AuditEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.event_type, EventType::LookupCreated);
        assert_eq!(deserialized.key, "my-lookup");
    }

    #[test]
    fn all_event_types_serialize() {
        let types = [
            EventType::QueryStart,
            EventType::QueryComplete,
            EventType::QueryFailed,
            EventType::SegmentLoaded,
            EventType::SegmentDropped,
            EventType::SupervisorCreated,
            EventType::SupervisorShutdown,
            EventType::TaskSubmitted,
            EventType::TaskCompleted,
            EventType::TaskFailed,
            EventType::ConfigChanged,
            EventType::RulesChanged,
            EventType::LookupCreated,
            EventType::LookupDeleted,
            EventType::UserAuthenticated,
            EventType::AuthorizationDenied,
        ];
        for et in types {
            let json = serde_json::to_string(&et).expect("serialize");
            let back: EventType = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, et);
        }
    }
}
