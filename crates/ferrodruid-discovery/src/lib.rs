// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Service discovery via cluster state for FerroDruid.
//!
//! [`ServiceDiscovery`] provides a simple interface for locating services
//! (brokers, historicals, coordinators, overlords) by querying the
//! [`ClusterManager`](ferrodruid_cluster::ClusterManager).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;

use ferrodruid_cluster::{ClusterManager, NodeInfo, ServiceEntry};

/// Service discovery backed by cluster state.
///
/// This is a thin convenience layer that queries [`ClusterManager`] for
/// registered services of specific types.
pub struct ServiceDiscovery {
    cluster: Arc<ClusterManager>,
}

impl ServiceDiscovery {
    /// Create a new service discovery instance backed by the given cluster manager.
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self { cluster }
    }

    /// Find all registered broker services.
    pub fn find_brokers(&self) -> Vec<ServiceEntry> {
        self.cluster.services("broker")
    }

    /// Find all registered historical services.
    pub fn find_historicals(&self) -> Vec<ServiceEntry> {
        self.cluster.services("historical")
    }

    /// Find all registered coordinator services.
    pub fn find_coordinators(&self) -> Vec<ServiceEntry> {
        self.cluster.services("coordinator")
    }

    /// Find all registered overlord services.
    pub fn find_overlords(&self) -> Vec<ServiceEntry> {
        self.cluster.services("overlord")
    }

    /// Get the current cluster leader.
    pub fn get_leader(&self) -> Option<NodeInfo> {
        self.cluster.leader()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use ferrodruid_cluster::{NodeInfo, NodeRole};

    fn make_cluster() -> Arc<ClusterManager> {
        let node = NodeInfo {
            id: "node-1".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8888,
            role: NodeRole::AllInOne,
        };
        Arc::new(ClusterManager::new_single_node(node))
    }

    #[test]
    fn find_services_empty() {
        let cluster = make_cluster();
        let discovery = ServiceDiscovery::new(cluster);

        assert!(discovery.find_brokers().is_empty());
        assert!(discovery.find_historicals().is_empty());
        assert!(discovery.find_coordinators().is_empty());
        assert!(discovery.find_overlords().is_empty());
    }

    #[test]
    fn find_registered_services() {
        let cluster = make_cluster();

        cluster.register_service(ServiceEntry {
            service_type: "broker".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8082,
            node_id: "node-1".to_string(),
        });
        cluster.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8083,
            node_id: "node-1".to_string(),
        });
        cluster.register_service(ServiceEntry {
            service_type: "coordinator".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8081,
            node_id: "node-1".to_string(),
        });
        cluster.register_service(ServiceEntry {
            service_type: "overlord".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8090,
            node_id: "node-1".to_string(),
        });

        let discovery = ServiceDiscovery::new(cluster);

        assert_eq!(discovery.find_brokers().len(), 1);
        assert_eq!(discovery.find_brokers()[0].port, 8082);

        assert_eq!(discovery.find_historicals().len(), 1);
        assert_eq!(discovery.find_historicals()[0].port, 8083);

        assert_eq!(discovery.find_coordinators().len(), 1);
        assert_eq!(discovery.find_coordinators()[0].port, 8081);

        assert_eq!(discovery.find_overlords().len(), 1);
        assert_eq!(discovery.find_overlords()[0].port, 8090);
    }

    #[test]
    fn get_leader() {
        let cluster = make_cluster();
        let discovery = ServiceDiscovery::new(cluster);

        let leader = discovery.get_leader();
        assert!(leader.is_some());
        assert_eq!(leader.expect("leader").id, "node-1");
    }

    #[test]
    fn multiple_historicals() {
        let cluster = make_cluster();

        cluster.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "10.0.0.1".to_string(),
            port: 8083,
            node_id: "hist-1".to_string(),
        });
        cluster.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "10.0.0.2".to_string(),
            port: 8083,
            node_id: "hist-2".to_string(),
        });
        cluster.register_service(ServiceEntry {
            service_type: "historical".to_string(),
            host: "10.0.0.3".to_string(),
            port: 8083,
            node_id: "hist-3".to_string(),
        });

        let discovery = ServiceDiscovery::new(cluster);
        assert_eq!(discovery.find_historicals().len(), 3);
    }
}
