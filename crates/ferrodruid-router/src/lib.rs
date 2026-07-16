// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Query routing to Brokers for FerroDruid.
//!
//! The [`QueryRouter`] distributes API requests to available Broker nodes
//! using a configurable [`RoutingStrategy`] (round-robin, random, or
//! least-connections).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// BrokerEndpoint
// ---------------------------------------------------------------------------

/// A Broker endpoint that the router can direct requests to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerEndpoint {
    /// Human-readable broker name.
    pub name: String,
    /// Hostname or IP address.
    pub host: String,
    /// Plain-text HTTP port.
    pub port: u16,
    /// Optional TLS port.
    pub tls_port: Option<u16>,
}

// ---------------------------------------------------------------------------
// RoutingStrategy
// ---------------------------------------------------------------------------

/// Strategy used by [`QueryRouter`] to select a broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Cycle through brokers in order.
    RoundRobin,
    /// Pick a random broker (uniform).
    Random,
    /// Pick the broker with the fewest in-flight connections (stub:
    /// currently falls back to round-robin).
    LeastConnections,
}

// ---------------------------------------------------------------------------
// QueryRouter
// ---------------------------------------------------------------------------

/// Routes query requests to available Broker nodes.
pub struct QueryRouter {
    brokers: RwLock<Vec<BrokerEndpoint>>,
    strategy: RoutingStrategy,
    next_index: AtomicUsize,
}

impl QueryRouter {
    /// Create a new router with the given strategy and no brokers.
    pub fn new(strategy: RoutingStrategy) -> Self {
        Self {
            brokers: RwLock::new(Vec::new()),
            strategy,
            next_index: AtomicUsize::new(0),
        }
    }

    /// Add a broker endpoint.
    pub fn add_broker(&self, endpoint: BrokerEndpoint) {
        let mut brokers = self.brokers.write().expect("lock poisoned");
        brokers.push(endpoint);
    }

    /// Remove a broker by name. If multiple brokers share a name, removes the
    /// first match.
    pub fn remove_broker(&self, name: &str) {
        let mut brokers = self.brokers.write().expect("lock poisoned");
        if let Some(pos) = brokers.iter().position(|b| b.name == name) {
            brokers.remove(pos);
        }
    }

    /// Select a broker according to the configured strategy.
    ///
    /// Returns `None` if no brokers are registered.
    pub fn select_broker(&self) -> Option<BrokerEndpoint> {
        let brokers = self.brokers.read().expect("lock poisoned");
        if brokers.is_empty() {
            return None;
        }

        let idx = match self.strategy {
            RoutingStrategy::RoundRobin | RoutingStrategy::LeastConnections => {
                let i = self.next_index.fetch_add(1, Ordering::Relaxed);
                i % brokers.len()
            }
            RoutingStrategy::Random => {
                // Simple pseudo-random: use the atomic counter XOR'd with a
                // time-derived value to avoid pulling in `rand` as a non-dev
                // dependency. Good enough for load spreading.
                let tick = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as usize)
                    .unwrap_or(0);
                let i = self.next_index.fetch_add(1, Ordering::Relaxed);
                (i ^ tick) % brokers.len()
            }
        };

        Some(brokers[idx].clone())
    }

    /// Return the number of registered brokers.
    pub fn broker_count(&self) -> usize {
        self.brokers.read().expect("lock poisoned").len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(name: &str) -> BrokerEndpoint {
        BrokerEndpoint {
            name: name.into(),
            host: "127.0.0.1".into(),
            port: 8082,
            tls_port: None,
        }
    }

    #[test]
    fn empty_router_returns_none() {
        let r = QueryRouter::new(RoutingStrategy::RoundRobin);
        assert!(r.select_broker().is_none());
        assert_eq!(r.broker_count(), 0);
    }

    #[test]
    fn add_and_count() {
        let r = QueryRouter::new(RoutingStrategy::RoundRobin);
        r.add_broker(broker("b1"));
        r.add_broker(broker("b2"));
        assert_eq!(r.broker_count(), 2);
    }

    #[test]
    fn remove_broker() {
        let r = QueryRouter::new(RoutingStrategy::RoundRobin);
        r.add_broker(broker("b1"));
        r.add_broker(broker("b2"));
        r.remove_broker("b1");
        assert_eq!(r.broker_count(), 1);

        let selected = r.select_broker().expect("one broker");
        assert_eq!(selected.name, "b2");
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let r = QueryRouter::new(RoutingStrategy::RoundRobin);
        r.add_broker(broker("b1"));
        r.remove_broker("nope");
        assert_eq!(r.broker_count(), 1);
    }

    #[test]
    fn round_robin_cycles() {
        let r = QueryRouter::new(RoutingStrategy::RoundRobin);
        r.add_broker(broker("b1"));
        r.add_broker(broker("b2"));
        r.add_broker(broker("b3"));

        let names: Vec<String> = (0..6)
            .map(|_| r.select_broker().expect("broker").name)
            .collect();

        assert_eq!(names, vec!["b1", "b2", "b3", "b1", "b2", "b3"]);
    }

    #[test]
    fn round_robin_single_broker() {
        let r = QueryRouter::new(RoutingStrategy::RoundRobin);
        r.add_broker(broker("only"));

        for _ in 0..5 {
            assert_eq!(r.select_broker().expect("broker").name, "only");
        }
    }

    #[test]
    fn random_returns_valid_broker() {
        let r = QueryRouter::new(RoutingStrategy::Random);
        r.add_broker(broker("b1"));
        r.add_broker(broker("b2"));

        for _ in 0..20 {
            let b = r.select_broker().expect("broker");
            assert!(b.name == "b1" || b.name == "b2");
        }
    }

    #[test]
    fn least_connections_fallback() {
        // LeastConnections currently falls back to round-robin.
        let r = QueryRouter::new(RoutingStrategy::LeastConnections);
        r.add_broker(broker("b1"));
        r.add_broker(broker("b2"));

        let names: Vec<String> = (0..4)
            .map(|_| r.select_broker().expect("broker").name)
            .collect();

        assert_eq!(names, vec!["b1", "b2", "b1", "b2"]);
    }

    #[test]
    fn broker_endpoint_tls_port() {
        let ep = BrokerEndpoint {
            name: "secure".into(),
            host: "10.0.0.1".into(),
            port: 8082,
            tls_port: Some(8283),
        };
        assert_eq!(ep.tls_port, Some(8283));
    }

    #[test]
    fn broker_endpoint_serde_roundtrip() {
        let ep = BrokerEndpoint {
            name: "b1".into(),
            host: "localhost".into(),
            port: 8082,
            tls_port: Some(8283),
        };
        let json = serde_json::to_string(&ep).expect("serialize");
        let parsed: BrokerEndpoint = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.name, "b1");
        assert_eq!(parsed.port, 8082);
        assert_eq!(parsed.tls_port, Some(8283));
    }
}
