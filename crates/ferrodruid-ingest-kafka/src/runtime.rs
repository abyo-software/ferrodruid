// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Kafka supervisor runtime that manages multiple consumer tasks.

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::KafkaSupervisorSpec;
use crate::consumer::KafkaConsumerConfig;

/// Runtime that manages multiple [`KafkaConsumerTask`] instances for a supervisor.
pub struct KafkaSupervisorRuntime {
    /// Unique supervisor identifier.
    pub supervisor_id: String,
    /// The supervisor specification.
    pub spec: KafkaSupervisorSpec,
    shutdown_txs: Vec<mpsc::Sender<()>>,
    task_count: usize,
    running: bool,
}

impl KafkaSupervisorRuntime {
    /// Create a new supervisor runtime (not yet started).
    pub fn new(supervisor_id: String, spec: KafkaSupervisorSpec) -> Self {
        Self {
            supervisor_id,
            spec,
            shutdown_txs: Vec::new(),
            task_count: 0,
            running: false,
        }
    }

    /// Build a [`KafkaConsumerConfig`] from the supervisor spec.
    pub fn build_consumer_config(&self) -> KafkaConsumerConfig {
        let brokers = self
            .spec
            .io_config
            .consumer_properties
            .get("bootstrap.servers")
            .cloned()
            .unwrap_or_else(|| "localhost:9092".to_string());

        let dimensions: Vec<String> = self
            .spec
            .data_schema
            .dimensions_spec
            .dimensions
            .iter()
            .map(|d| match d {
                crate::DimensionEntry::String(s) => s.clone(),
                crate::DimensionEntry::Typed { name, .. } => name.clone(),
            })
            .collect();

        let max_rows = self
            .spec
            .tuning_config
            .as_ref()
            .and_then(|t| t.max_rows_per_segment)
            .unwrap_or(5_000_000);

        KafkaConsumerConfig {
            brokers,
            topic: self.spec.io_config.topic.clone(),
            group_id: format!("ferrodruid-{}", self.supervisor_id),
            data_source: self.spec.data_schema.data_source.clone(),
            timestamp_column: self.spec.data_schema.timestamp_spec.column.clone(),
            dimensions,
            max_rows_per_segment: max_rows,
            segment_flush_interval_ms: 10_000,
            use_earliest_offset: self.spec.io_config.use_earliest_offset.unwrap_or(false),
            additional_properties: HashMap::new(),
            output_dir: None,
        }
    }

    /// Start the supervisor by creating consumer tasks.
    ///
    /// Each task gets its own shutdown channel. In a real implementation with the
    /// `kafka-io` feature, each task would be spawned as a tokio task. Without it,
    /// this just records the task count and marks the runtime as running.
    pub fn start(&mut self, task_count: usize) {
        self.task_count = task_count;
        self.shutdown_txs.clear();

        for _ in 0..task_count {
            let (tx, _rx) = mpsc::channel(1);
            self.shutdown_txs.push(tx);
        }
        self.running = true;
    }

    /// Stop all consumer tasks gracefully.
    pub async fn stop(&mut self) {
        for tx in self.shutdown_txs.drain(..) {
            let _ = tx.send(()).await;
        }
        self.running = false;
        self.task_count = 0;
    }

    /// Get the number of running tasks.
    pub fn running_task_count(&self) -> usize {
        self.task_count
    }

    /// Whether the supervisor runtime is running.
    pub fn is_running(&self) -> bool {
        self.running
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DataSchema, DimensionEntry, DimensionsSpec, KafkaIoConfig, KafkaSupervisorSpec,
        KafkaTuningConfig, TimestampSpec,
    };

    fn sample_spec() -> KafkaSupervisorSpec {
        KafkaSupervisorSpec {
            spec_type: "kafka".to_string(),
            data_schema: DataSchema {
                data_source: "events".to_string(),
                timestamp_spec: TimestampSpec {
                    column: "__time".to_string(),
                    format: "auto".to_string(),
                },
                dimensions_spec: DimensionsSpec {
                    dimensions: vec![
                        DimensionEntry::String("page".to_string()),
                        DimensionEntry::Typed {
                            name: "user".to_string(),
                            dim_type: "string".to_string(),
                        },
                    ],
                    dimension_exclusions: vec![],
                },
                metrics_spec: vec![],
                granularity_spec: None,
            },
            io_config: KafkaIoConfig {
                topic: "wiki-events".to_string(),
                consumer_properties: {
                    let mut m = HashMap::new();
                    m.insert("bootstrap.servers".to_string(), "kafka:9092".to_string());
                    m
                },
                task_count: Some(3),
                replicas: Some(1),
                task_duration: None,
                use_earliest_offset: Some(true),
            },
            tuning_config: Some(KafkaTuningConfig {
                max_rows_in_memory: Some(75_000),
                max_rows_per_segment: Some(1_000_000),
                max_total_rows: None,
                intermediate_persist_period: None,
            }),
        }
    }

    #[test]
    fn runtime_build_config() {
        let runtime = KafkaSupervisorRuntime::new("test-sup".to_string(), sample_spec());
        let config = runtime.build_consumer_config();

        assert_eq!(config.brokers, "kafka:9092");
        assert_eq!(config.topic, "wiki-events");
        assert_eq!(config.group_id, "ferrodruid-test-sup");
        assert_eq!(config.data_source, "events");
        assert_eq!(config.timestamp_column, "__time");
        assert_eq!(config.dimensions, vec!["page", "user"]);
        assert_eq!(config.max_rows_per_segment, 1_000_000);
        assert!(config.use_earliest_offset);
    }

    #[test]
    fn runtime_start_stop() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio rt");
        rt.block_on(async {
            let mut runtime = KafkaSupervisorRuntime::new("test-sup".to_string(), sample_spec());

            assert!(!runtime.is_running());
            assert_eq!(runtime.running_task_count(), 0);

            runtime.start(3);
            assert!(runtime.is_running());
            assert_eq!(runtime.running_task_count(), 3);

            runtime.stop().await;
            assert!(!runtime.is_running());
            assert_eq!(runtime.running_task_count(), 0);
        });
    }

    #[test]
    fn runtime_default_max_rows() {
        let mut spec = sample_spec();
        spec.tuning_config = None;
        let runtime = KafkaSupervisorRuntime::new("test".to_string(), spec);
        let config = runtime.build_consumer_config();
        assert_eq!(config.max_rows_per_segment, 5_000_000);
    }
}
