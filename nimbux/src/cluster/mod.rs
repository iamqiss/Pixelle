// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Cluster management and elastic scalability

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::{NimbuxError, Result};

pub mod node;
pub mod load_balancer;
pub mod auto_scaler;
pub mod distributed_storage;
pub mod consensus;
pub mod sharding;

// Re-export commonly used types
pub use node::{Node, NodeStatus, NodeRole, NodeMetrics};
pub use load_balancer::{LoadBalancer, LoadBalancingStrategy, LoadBalancerConfig};
pub use auto_scaler::{AutoScaler, ScalingPolicy, ScalingMetrics, ScalingDecision};
pub use distributed_storage::{DistributedStorage, ReplicationStrategy, ConsistencyLevel};
pub use consensus::{ConsensusManager, ConsensusConfig, ConsensusState};
pub use sharding::{ShardManager, ShardKey, ShardInfo, ShardDistribution};

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub min_nodes: usize,
    pub max_nodes: usize,
    pub target_cpu_utilization: f64,
    pub target_memory_utilization: f64,
    pub scale_up_threshold: f64,
    pub scale_down_threshold: f64,
    pub scale_up_cooldown: u64, // seconds
    pub scale_down_cooldown: u64, // seconds
    pub replication_factor: usize,
    pub consistency_level: ConsistencyLevel,
    pub shard_count: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: Uuid::new_v4().to_string(),
            min_nodes: 3,
            max_nodes: 100,
            target_cpu_utilization: 70.0,
            target_memory_utilization: 80.0,
            scale_up_threshold: 80.0,
            scale_down_threshold: 30.0,
            scale_up_cooldown: 300, // 5 minutes
            scale_down_cooldown: 600, // 10 minutes
            replication_factor: 3,
            consistency_level: ConsistencyLevel::Strong,
            shard_count: 16,
        }
    }
}

/// Cluster manager that orchestrates all cluster operations
pub struct ClusterManager {
    config: ClusterConfig,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    load_balancer: Arc<LoadBalancer>,
    auto_scaler: Arc<AutoScaler>,
    distributed_storage: Arc<DistributedStorage>,
    consensus_manager: Arc<ConsensusManager>,
    shard_manager: Arc<ShardManager>,
}

impl ClusterManager {
    pub fn new(config: ClusterConfig) -> Result<Self> {
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        
        let load_balancer = Arc::new(LoadBalancer::new(
            LoadBalancerConfig {
                strategy: LoadBalancingStrategy::ConsistentHash,
                health_check_interval: 30,
                max_retries: 3,
                timeout: 5000,
            },
            Arc::clone(&nodes),
        )?);
        
        let auto_scaler = Arc::new(AutoScaler::new(
            ScalingPolicy {
                min_nodes: config.min_nodes,
                max_nodes: config.max_nodes,
                target_cpu: config.target_cpu_utilization,
                target_memory: config.target_memory_utilization,
                scale_up_threshold: config.scale_up_threshold,
                scale_down_threshold: config.scale_down_threshold,
                scale_up_cooldown: config.scale_up_cooldown,
                scale_down_cooldown: config.scale_down_cooldown,
            },
            Arc::clone(&nodes),
        )?);
        
        let distributed_storage = Arc::new(DistributedStorage::new(
            ReplicationStrategy {
                factor: config.replication_factor,
                consistency_level: config.consistency_level.clone(),
                async_replication: true,
            },
            Arc::clone(&nodes),
        )?);
        
        let consensus_manager = Arc::new(ConsensusManager::new(
            ConsensusConfig {
                election_timeout: 5000,
                heartbeat_interval: 1000,
                max_log_entries: 10000,
                snapshot_interval: 1000,
            },
            Arc::clone(&nodes),
        )?);
        
        let shard_manager = Arc::new(ShardManager::new(
            config.shard_count,
            Arc::clone(&nodes),
        )?);
        
        Ok(Self {
            config,
            nodes,
            load_balancer,
            auto_scaler,
            distributed_storage,
            consensus_manager,
            shard_manager,
        })
    }
    
    /// Add a new node to the cluster
    pub async fn add_node(&self, node: Node) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let node_id = node.id.clone();
        
        // Validate node capacity
        if nodes.len() >= self.config.max_nodes {
            return Err(NimbuxError::Cluster("Maximum node limit reached".to_string()));
        }
        
        // Add node to cluster
        nodes.insert(node_id.clone(), node);
        
        // Update load balancer
        self.load_balancer.add_node(&node_id).await?;
        
        // Update shard distribution
        self.shard_manager.redistribute_shards().await?;
        
        tracing::info!("Added node {} to cluster", node_id);
        Ok(())
    }
    
    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: &str) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        
        // Check minimum node requirement
        if nodes.len() <= self.config.min_nodes {
            return Err(NimbuxError::Cluster("Cannot remove node: minimum node count reached".to_string()));
        }
        
        // Remove node from cluster
        if let Some(node) = nodes.remove(node_id) {
            // Update load balancer
            self.load_balancer.remove_node(node_id).await?;
            
            // Redistribute shards
            self.shard_manager.redistribute_shards().await?;
            
            tracing::info!("Removed node {} from cluster", node_id);
            Ok(())
        } else {
            Err(NimbuxError::Cluster(format!("Node {} not found", node_id)))
        }
    }
    
    /// Get cluster health status
    pub async fn get_cluster_health(&self) -> Result<ClusterHealth> {
        let nodes = self.nodes.read().await;
        let mut healthy_nodes = 0;
        let mut total_cpu = 0.0;
        let mut total_memory = 0.0;
        let mut total_storage = 0u64;
        
        for node in nodes.values() {
            if node.status == NodeStatus::Healthy {
                healthy_nodes += 1;
                total_cpu += node.metrics.cpu_utilization;
                total_memory += node.metrics.memory_utilization;
                total_storage += node.metrics.storage_used;
            }
        }
        
        let node_count = nodes.len();
        let avg_cpu = if node_count > 0 { total_cpu / node_count as f64 } else { 0.0 };
        let avg_memory = if node_count > 0 { total_memory / node_count as f64 } else { 0.0 };
        
        Ok(ClusterHealth {
            total_nodes: node_count,
            healthy_nodes,
            avg_cpu_utilization: avg_cpu,
            avg_memory_utilization: avg_memory,
            total_storage_used: total_storage,
            cluster_status: if healthy_nodes >= self.config.min_nodes {
                ClusterStatus::Healthy
            } else {
                ClusterStatus::Degraded
            },
        })
    }
    
    /// Start auto-scaling
    pub async fn start_auto_scaling(&self) -> Result<()> {
        self.auto_scaler.start().await?;
        tracing::info!("Auto-scaling started");
        Ok(())
    }
    
    /// Stop auto-scaling
    pub async fn stop_auto_scaling(&self) -> Result<()> {
        self.auto_scaler.stop().await?;
        tracing::info!("Auto-scaling stopped");
        Ok(())
    }
    
    /// Get load balancer
    pub fn get_load_balancer(&self) -> Arc<LoadBalancer> {
        Arc::clone(&self.load_balancer)
    }
    
    /// Get distributed storage
    pub fn get_distributed_storage(&self) -> Arc<DistributedStorage> {
        Arc::clone(&self.distributed_storage)
    }
    
    /// Get shard manager
    pub fn get_shard_manager(&self) -> Arc<ShardManager> {
        Arc::clone(&self.shard_manager)
    }
}

/// Cluster health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub avg_cpu_utilization: f64,
    pub avg_memory_utilization: f64,
    pub total_storage_used: u64,
    pub cluster_status: ClusterStatus,
}

/// Cluster status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClusterStatus {
    Healthy,
    Degraded,
    Critical,
    Unavailable,
}

/// Cluster metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetrics {
    pub timestamp: u64,
    pub cluster_id: String,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time: f64,
    pub throughput: f64, // requests per second
    pub error_rate: f64,
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub storage_utilization: f64,
    pub network_io: u64,
    pub disk_io: u64,
}

impl ClusterMetrics {
    pub fn new(cluster_id: String) -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            cluster_id,
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            avg_response_time: 0.0,
            throughput: 0.0,
            error_rate: 0.0,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            storage_utilization: 0.0,
            network_io: 0,
            disk_io: 0,
        }
    }
    
    pub fn calculate_error_rate(&mut self) {
        if self.total_requests > 0 {
            self.error_rate = (self.failed_requests as f64 / self.total_requests as f64) * 100.0;
        }
    }
}