// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Sharding system for horizontal scaling

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use crate::errors::{NimbuxError, Result};
use super::node::{Node, NodeStatus};

/// Shard key for object routing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub key: String,
    pub hash: u64,
}

/// Shard information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: u32,
    pub primary_node: String,
    pub replica_nodes: Vec<String>,
    pub range_start: u64,
    pub range_end: u64,
    pub object_count: u64,
    pub size_bytes: u64,
    pub status: ShardStatus,
}

/// Shard status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardStatus {
    Active,
    Migrating,
    Rebalancing,
    Inactive,
    Error,
}

/// Shard distribution strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardDistributionStrategy {
    ConsistentHash,
    RangeBased,
    RoundRobin,
    Weighted,
}

/// Shard manager for distributed sharding
pub struct ShardManager {
    shard_count: u32,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    shards: Arc<RwLock<HashMap<u32, ShardInfo>>>,
    distribution_strategy: ShardDistributionStrategy,
    rebalancing_threshold: f64,
    migration_in_progress: Arc<RwLock<HashMap<u32, MigrationTask>>>,
}

/// Migration task for shard rebalancing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTask {
    pub shard_id: u32,
    pub source_node: String,
    pub target_node: String,
    pub objects_to_migrate: Vec<String>,
    pub progress: f64,
    pub status: MigrationStatus,
    pub created_at: u64,
    pub started_at: Option<u64>,
    pub completed_at: Option<u64>,
}

/// Migration status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MigrationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl ShardManager {
    pub fn new(shard_count: u32, nodes: Arc<RwLock<HashMap<String, Node>>>) -> Result<Self> {
        let mut shard_manager = Self {
            shard_count,
            nodes,
            shards: Arc::new(RwLock::new(HashMap::new())),
            distribution_strategy: ShardDistributionStrategy::ConsistentHash,
            rebalancing_threshold: 0.8, // 80% threshold for rebalancing
            migration_in_progress: Arc::new(RwLock::new(HashMap::new())),
        };
        
        // Initialize shards
        shard_manager.initialize_shards()?;
        
        Ok(shard_manager)
    }
    
    /// Initialize shards based on current nodes
    fn initialize_shards(&self) -> Result<()> {
        let mut shards = HashMap::new();
        let hash_range = u64::MAX / self.shard_count as u64;
        
        for i in 0..self.shard_count {
            let shard_id = i;
            let range_start = i as u64 * hash_range;
            let range_end = if i == self.shard_count - 1 {
                u64::MAX
            } else {
                (i + 1) as u64 * hash_range - 1
            };
            
            let shard_info = ShardInfo {
                shard_id,
                primary_node: "".to_string(), // Will be set during distribution
                replica_nodes: Vec::new(),
                range_start,
                range_end,
                object_count: 0,
                size_bytes: 0,
                status: ShardStatus::Inactive,
            };
            
            shards.insert(shard_id, shard_info);
        }
        
        // This would be async in a real implementation
        // For now, we'll handle it synchronously
        Ok(())
    }
    
    /// Redistribute shards across available nodes
    pub async fn redistribute_shards(&self) -> Result<()> {
        let nodes = self.nodes.read().await;
        let available_nodes: Vec<&Node> = nodes
            .values()
            .filter(|node| node.status == NodeStatus::Healthy)
            .collect();
        
        if available_nodes.is_empty() {
            return Err(NimbuxError::Sharding("No healthy nodes available for shard distribution".to_string()));
        }
        
        let mut shards = self.shards.write().await;
        
        // Distribute shards across available nodes
        for (shard_id, shard_info) in shards.iter_mut() {
            let node_index = *shard_id as usize % available_nodes.len();
            let primary_node = available_nodes[node_index].id.clone();
            
            // Select replica nodes
            let mut replica_nodes = Vec::new();
            for i in 1..3 { // 2 replicas
                let replica_index = (*shard_id as usize + i) % available_nodes.len();
                if replica_index != node_index {
                    replica_nodes.push(available_nodes[replica_index].id.clone());
                }
            }
            
            shard_info.primary_node = primary_node;
            shard_info.replica_nodes = replica_nodes;
            shard_info.status = ShardStatus::Active;
        }
        
        tracing::info!("Redistributed {} shards across {} nodes", shards.len(), available_nodes.len());
        Ok(())
    }
    
    /// Get shard for a given key
    pub async fn get_shard_for_key(&self, key: &str) -> Result<u32> {
        let hash = self.hash_key(key);
        let shards = self.shards.read().await;
        
        for (shard_id, shard_info) in shards.iter() {
            if hash >= shard_info.range_start && hash <= shard_info.range_end {
                return Ok(*shard_id);
            }
        }
        
        Err(NimbuxError::Sharding(format!("No shard found for key: {}", key)))
    }
    
    /// Get shard information
    pub async fn get_shard_info(&self, shard_id: u32) -> Result<ShardInfo> {
        let shards = self.shards.read().await;
        shards.get(&shard_id)
            .cloned()
            .ok_or_else(|| NimbuxError::Sharding(format!("Shard {} not found", shard_id)))
    }
    
    /// Get all shards
    pub async fn get_all_shards(&self) -> Result<Vec<ShardInfo>> {
        let shards = self.shards.read().await;
        Ok(shards.values().cloned().collect())
    }
    
    /// Get shards for a specific node
    pub async fn get_shards_for_node(&self, node_id: &str) -> Result<Vec<ShardInfo>> {
        let shards = self.shards.read().await;
        let mut node_shards = Vec::new();
        
        for shard_info in shards.values() {
            if shard_info.primary_node == node_id || shard_info.replica_nodes.contains(&node_id.to_string()) {
                node_shards.push(shard_info.clone());
            }
        }
        
        Ok(node_shards)
    }
    
    /// Check if rebalancing is needed
    pub async fn needs_rebalancing(&self) -> Result<bool> {
        let shards = self.shards.read().await;
        let nodes = self.nodes.read().await;
        
        // Calculate load per node
        let mut node_loads: HashMap<String, f64> = HashMap::new();
        
        for shard_info in shards.values() {
            if shard_info.status == ShardStatus::Active {
                let load = shard_info.object_count as f64 + (shard_info.size_bytes as f64 / 1024.0 / 1024.0); // MB
                
                // Add to primary node
                *node_loads.entry(shard_info.primary_node.clone()).or_insert(0.0) += load;
                
                // Add to replica nodes (with lower weight)
                for replica in &shard_info.replica_nodes {
                    *node_loads.entry(replica.clone()).or_insert(0.0) += load * 0.1;
                }
            }
        }
        
        if node_loads.is_empty() {
            return Ok(false);
        }
        
        // Calculate load variance
        let loads: Vec<f64> = node_loads.values().cloned().collect();
        let avg_load = loads.iter().sum::<f64>() / loads.len() as f64;
        let variance = loads.iter()
            .map(|load| (load - avg_load).powi(2))
            .sum::<f64>() / loads.len() as f64;
        let std_dev = variance.sqrt();
        let coefficient_of_variation = std_dev / avg_load;
        
        Ok(coefficient_of_variation > self.rebalancing_threshold)
    }
    
    /// Start rebalancing process
    pub async fn start_rebalancing(&self) -> Result<()> {
        if !self.needs_rebalancing().await? {
            tracing::info!("No rebalancing needed");
            return Ok(());
        }
        
        tracing::info!("Starting shard rebalancing");
        
        // Identify overloaded and underloaded nodes
        let (overloaded_nodes, underloaded_nodes) = self.identify_load_imbalance().await?;
        
        // Create migration tasks
        for (overloaded_node, underloaded_node) in overloaded_nodes.iter().zip(underloaded_nodes.iter()) {
            if let Some(shard_id) = self.find_shard_to_migrate(overloaded_node).await? {
                self.create_migration_task(shard_id, overloaded_node, underloaded_node).await?;
            }
        }
        
        // Start migration process
        self.process_migrations().await?;
        
        Ok(())
    }
    
    /// Identify load imbalance between nodes
    async fn identify_load_imbalance(&self) -> Result<(Vec<String>, Vec<String>)> {
        let shards = self.shards.read().await;
        let mut node_loads: HashMap<String, f64> = HashMap::new();
        
        // Calculate load per node
        for shard_info in shards.values() {
            if shard_info.status == ShardStatus::Active {
                let load = shard_info.object_count as f64 + (shard_info.size_bytes as f64 / 1024.0 / 1024.0);
                *node_loads.entry(shard_info.primary_node.clone()).or_insert(0.0) += load;
            }
        }
        
        // Sort nodes by load
        let mut sorted_nodes: Vec<(String, f64)> = node_loads.into_iter().collect();
        sorted_nodes.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        let mid_point = sorted_nodes.len() / 2;
        let overloaded: Vec<String> = sorted_nodes[mid_point..].iter().map(|(node, _)| node.clone()).collect();
        let underloaded: Vec<String> = sorted_nodes[..mid_point].iter().map(|(node, _)| node.clone()).collect();
        
        Ok((overloaded, underloaded))
    }
    
    /// Find a shard to migrate from an overloaded node
    async fn find_shard_to_migrate(&self, node_id: &str) -> Result<Option<u32>> {
        let shards = self.shards.read().await;
        
        for shard_info in shards.values() {
            if shard_info.primary_node == node_id && shard_info.status == ShardStatus::Active {
                return Ok(Some(shard_info.shard_id));
            }
        }
        
        Ok(None)
    }
    
    /// Create a migration task
    async fn create_migration_task(&self, shard_id: u32, source_node: &str, target_node: &str) -> Result<()> {
        let migration_task = MigrationTask {
            shard_id,
            source_node: source_node.to_string(),
            target_node: target_node.to_string(),
            objects_to_migrate: Vec::new(), // Would be populated with actual objects
            progress: 0.0,
            status: MigrationStatus::Pending,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            started_at: None,
            completed_at: None,
        };
        
        let mut migrations = self.migration_in_progress.write().await;
        migrations.insert(shard_id, migration_task);
        
        tracing::info!("Created migration task for shard {} from {} to {}", shard_id, source_node, target_node);
        Ok(())
    }
    
    /// Process pending migrations
    async fn process_migrations(&self) -> Result<()> {
        let mut migrations = self.migration_in_progress.write().await;
        let mut completed_migrations = Vec::new();
        
        for (shard_id, migration_task) in migrations.iter_mut() {
            if migration_task.status == MigrationStatus::Pending {
                migration_task.status = MigrationStatus::InProgress;
                migration_task.started_at = Some(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs());
            }
            
            if migration_task.status == MigrationStatus::InProgress {
                // Simulate migration progress
                migration_task.progress += 0.1;
                
                if migration_task.progress >= 1.0 {
                    migration_task.status = MigrationStatus::Completed;
                    migration_task.completed_at = Some(std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs());
                    
                    // Update shard primary node
                    self.update_shard_primary_node(*shard_id, &migration_task.target_node).await?;
                    
                    completed_migrations.push(*shard_id);
                }
            }
        }
        
        // Remove completed migrations
        for shard_id in completed_migrations {
            migrations.remove(&shard_id);
        }
        
        Ok(())
    }
    
    /// Update shard primary node
    async fn update_shard_primary_node(&self, shard_id: u32, new_primary: &str) -> Result<()> {
        let mut shards = self.shards.write().await;
        
        if let Some(shard_info) = shards.get_mut(&shard_id) {
            shard_info.primary_node = new_primary.to_string();
            tracing::info!("Updated shard {} primary node to {}", shard_id, new_primary);
        }
        
        Ok(())
    }
    
    /// Hash a key for shard assignment
    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Get sharding statistics
    pub async fn get_stats(&self) -> Result<ShardingStats> {
        let shards = self.shards.read().await;
        let migrations = self.migration_in_progress.read().await;
        
        let mut total_objects = 0;
        let mut total_size = 0;
        let mut active_shards = 0;
        let mut migrating_shards = 0;
        
        for shard_info in shards.values() {
            total_objects += shard_info.object_count;
            total_size += shard_info.size_bytes;
            
            match shard_info.status {
                ShardStatus::Active => active_shards += 1,
                ShardStatus::Migrating => migrating_shards += 1,
                _ => {}
            }
        }
        
        Ok(ShardingStats {
            total_shards: shards.len(),
            active_shards,
            migrating_shards,
            total_objects,
            total_size,
            pending_migrations: migrations.len(),
            distribution_strategy: self.distribution_strategy.clone(),
        })
    }
}

/// Sharding statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingStats {
    pub total_shards: usize,
    pub active_shards: usize,
    pub migrating_shards: usize,
    pub total_objects: u64,
    pub total_size: u64,
    pub pending_migrations: usize,
    pub distribution_strategy: ShardDistributionStrategy,
}