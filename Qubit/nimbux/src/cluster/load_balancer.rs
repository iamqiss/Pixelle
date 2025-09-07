// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Load balancer for distributed requests

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use crate::errors::{NimbuxError, Result};
use super::node::{Node, NodeStatus};

/// Load balancing strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    LeastConnections,
    LeastResponseTime,
    ConsistentHash,
    WeightedRoundRobin,
    Random,
}

/// Load balancer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    pub strategy: LoadBalancingStrategy,
    pub health_check_interval: u64, // seconds
    pub max_retries: u32,
    pub timeout: u64, // milliseconds
}

/// Load balancer for distributing requests across nodes
pub struct LoadBalancer {
    config: LoadBalancerConfig,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    round_robin_index: Arc<RwLock<usize>>,
    node_weights: Arc<RwLock<HashMap<String, u32>>>,
    consistent_hash_ring: Arc<RwLock<Vec<HashRingEntry>>>,
    health_checker: Arc<HealthChecker>,
}

/// Entry in the consistent hash ring
#[derive(Debug, Clone)]
struct HashRingEntry {
    hash: u64,
    node_id: String,
    weight: u32,
}

/// Health checker for nodes
struct HealthChecker {
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    interval: u64,
    timeout: u64,
}

impl LoadBalancer {
    pub fn new(config: LoadBalancerConfig, nodes: Arc<RwLock<HashMap<String, Node>>>) -> Result<Self> {
        let health_checker = Arc::new(HealthChecker {
            nodes: Arc::clone(&nodes),
            interval: config.health_check_interval,
            timeout: config.timeout,
        });
        
        let lb = Self {
            config,
            nodes,
            round_robin_index: Arc::new(RwLock::new(0)),
            node_weights: Arc::new(RwLock::new(HashMap::new())),
            consistent_hash_ring: Arc::new(RwLock::new(Vec::new())),
            health_checker,
        };
        
        // Start health checking
        lb.start_health_checking()?;
        
        Ok(lb)
    }
    
    /// Add a node to the load balancer
    pub async fn add_node(&self, node_id: &str) -> Result<()> {
        let nodes = self.nodes.read().await;
        if let Some(node) = nodes.get(node_id) {
            // Add to consistent hash ring
            self.add_to_hash_ring(node_id, 1).await?;
            
            // Set default weight
            let mut weights = self.node_weights.write().await;
            weights.insert(node_id.to_string(), 1);
            
            tracing::info!("Added node {} to load balancer", node_id);
            Ok(())
        } else {
            Err(NimbuxError::LoadBalancer(format!("Node {} not found", node_id)))
        }
    }
    
    /// Remove a node from the load balancer
    pub async fn remove_node(&self, node_id: &str) -> Result<()> {
        // Remove from consistent hash ring
        self.remove_from_hash_ring(node_id).await?;
        
        // Remove from weights
        let mut weights = self.node_weights.write().await;
        weights.remove(node_id);
        
        tracing::info!("Removed node {} from load balancer", node_id);
        Ok(())
    }
    
    /// Select a node for the given request
    pub async fn select_node(&self, request_key: Option<&str>) -> Result<String> {
        let nodes = self.nodes.read().await;
        let available_nodes: Vec<&Node> = nodes
            .values()
            .filter(|node| node.is_available())
            .collect();
        
        if available_nodes.is_empty() {
            return Err(NimbuxError::LoadBalancer("No available nodes".to_string()));
        }
        
        match self.config.strategy {
            LoadBalancingStrategy::RoundRobin => {
                self.select_round_robin(available_nodes).await
            }
            LoadBalancingStrategy::LeastConnections => {
                self.select_least_connections(available_nodes).await
            }
            LoadBalancingStrategy::LeastResponseTime => {
                self.select_least_response_time(available_nodes).await
            }
            LoadBalancingStrategy::ConsistentHash => {
                self.select_consistent_hash(available_nodes, request_key).await
            }
            LoadBalancingStrategy::WeightedRoundRobin => {
                self.select_weighted_round_robin(available_nodes).await
            }
            LoadBalancingStrategy::Random => {
                self.select_random(available_nodes).await
            }
        }
    }
    
    /// Round-robin selection
    async fn select_round_robin(&self, nodes: Vec<&Node>) -> Result<String> {
        let mut index = self.round_robin_index.write().await;
        let selected_node = nodes[*index % nodes.len()];
        *index += 1;
        Ok(selected_node.id.clone())
    }
    
    /// Least connections selection
    async fn select_least_connections(&self, nodes: Vec<&Node>) -> Result<String> {
        let selected_node = nodes
            .iter()
            .min_by_key(|node| node.metrics.active_connections)
            .ok_or_else(|| NimbuxError::LoadBalancer("No nodes available".to_string()))?;
        
        Ok(selected_node.id.clone())
    }
    
    /// Least response time selection
    async fn select_least_response_time(&self, nodes: Vec<&Node>) -> Result<String> {
        let selected_node = nodes
            .iter()
            .min_by(|a, b| a.metrics.response_time_avg.partial_cmp(&b.metrics.response_time_avg).unwrap())
            .ok_or_else(|| NimbuxError::LoadBalancer("No nodes available".to_string()))?;
        
        Ok(selected_node.id.clone())
    }
    
    /// Consistent hash selection
    async fn select_consistent_hash(&self, nodes: Vec<&Node>, request_key: Option<&str>) -> Result<String> {
        let key = request_key.unwrap_or("default");
        let hash = self.hash_key(key);
        
        let ring = self.consistent_hash_ring.read().await;
        let available_node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
        
        // Find the first node with hash >= key hash
        for entry in ring.iter() {
            if entry.hash >= hash && available_node_ids.contains(&entry.node_id) {
                return Ok(entry.node_id.clone());
            }
        }
        
        // If no node found, wrap around to the first node
        if let Some(entry) = ring.first() {
            if available_node_ids.contains(&entry.node_id) {
                return Ok(entry.node_id.clone());
            }
        }
        
        Err(NimbuxError::LoadBalancer("No nodes available in hash ring".to_string()))
    }
    
    /// Weighted round-robin selection
    async fn select_weighted_round_robin(&self, nodes: Vec<&Node>) -> Result<String> {
        let weights = self.node_weights.read().await;
        let total_weight: u32 = nodes.iter()
            .map(|node| weights.get(&node.id).unwrap_or(&1))
            .sum();
        
        if total_weight == 0 {
            return self.select_round_robin(nodes).await;
        }
        
        let mut index = self.round_robin_index.write().await;
        let mut current_weight = 0u32;
        
        for node in nodes.iter() {
            let weight = weights.get(&node.id).unwrap_or(&1);
            current_weight += weight;
            if *index < current_weight as usize {
                *index = (*index + 1) % total_weight as usize;
                return Ok(node.id.clone());
            }
        }
        
        // Fallback to first node
        Ok(nodes[0].id.clone())
    }
    
    /// Random selection
    async fn select_random(&self, nodes: Vec<&Node>) -> Result<String> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        std::time::SystemTime::now().hash(&mut hasher);
        let index = hasher.finish() as usize % nodes.len();
        
        Ok(nodes[index].id.clone())
    }
    
    /// Hash a key for consistent hashing
    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Add node to consistent hash ring
    async fn add_to_hash_ring(&self, node_id: &str, weight: u32) -> Result<()> {
        let mut ring = self.consistent_hash_ring.write().await;
        
        // Add multiple virtual nodes for better distribution
        for i in 0..weight * 100 {
            let virtual_key = format!("{}:{}", node_id, i);
            let hash = self.hash_key(&virtual_key);
            
            ring.push(HashRingEntry {
                hash,
                node_id: node_id.to_string(),
                weight,
            });
        }
        
        // Sort by hash for efficient lookup
        ring.sort_by_key(|entry| entry.hash);
        
        Ok(())
    }
    
    /// Remove node from consistent hash ring
    async fn remove_from_hash_ring(&self, node_id: &str) -> Result<()> {
        let mut ring = self.consistent_hash_ring.write().await;
        ring.retain(|entry| entry.node_id != node_id);
        Ok(())
    }
    
    /// Start health checking
    fn start_health_checking(&self) -> Result<()> {
        let health_checker = Arc::clone(&self.health_checker);
        let interval = self.config.health_check_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval));
            loop {
                interval.tick().await;
                if let Err(e) = health_checker.check_all_nodes().await {
                    tracing::error!("Health check failed: {}", e);
                }
            }
        });
        
        Ok(())
    }
    
    /// Get load balancer statistics
    pub async fn get_stats(&self) -> Result<LoadBalancerStats> {
        let nodes = self.nodes.read().await;
        let mut total_connections = 0;
        let mut healthy_nodes = 0;
        let mut total_requests = 0;
        let mut total_errors = 0;
        let mut total_response_time = 0.0;
        
        for node in nodes.values() {
            if node.is_available() {
                healthy_nodes += 1;
            }
            total_connections += node.metrics.active_connections;
            total_requests += node.metrics.request_count;
            total_errors += node.metrics.error_count;
            total_response_time += node.metrics.response_time_avg;
        }
        
        let avg_response_time = if healthy_nodes > 0 {
            total_response_time / healthy_nodes as f64
        } else {
            0.0
        };
        
        Ok(LoadBalancerStats {
            total_nodes: nodes.len(),
            healthy_nodes,
            total_connections,
            total_requests,
            total_errors,
            avg_response_time,
            error_rate: if total_requests > 0 {
                (total_errors as f64 / total_requests as f64) * 100.0
            } else {
                0.0
            },
        })
    }
}

impl HealthChecker {
    async fn check_all_nodes(&self) -> Result<()> {
        let nodes = self.nodes.read().await;
        let mut tasks = Vec::new();
        
        for (node_id, node) in nodes.iter() {
            let node_id = node_id.clone();
            let node = node.clone();
            let timeout = self.timeout;
            
            let task = tokio::spawn(async move {
                Self::check_node_health(&node_id, &node, timeout).await
            });
            
            tasks.push(task);
        }
        
        // Wait for all health checks to complete
        for task in tasks {
            if let Err(e) = task.await {
                tracing::error!("Health check task failed: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn check_node_health(node_id: &str, node: &Node, timeout: u64) -> Result<()> {
        // Simulate health check by checking if node is responsive
        // In a real implementation, this would make an HTTP request to the node
        
        let start = std::time::Instant::now();
        let duration = start.elapsed();
        
        if duration.as_millis() > timeout as u128 {
            tracing::warn!("Node {} health check timeout", node_id);
            return Err(NimbuxError::LoadBalancer(format!("Node {} health check timeout", node_id)));
        }
        
        // For now, consider all nodes healthy
        // In a real implementation, you would check actual health endpoints
        tracing::debug!("Node {} health check passed", node_id);
        Ok(())
    }
}

/// Load balancer statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub total_connections: u32,
    pub total_requests: u64,
    pub total_errors: u64,
    pub avg_response_time: f64,
    pub error_rate: f64,
}