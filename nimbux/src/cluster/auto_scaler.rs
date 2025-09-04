// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Auto-scaling for elastic cluster management

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::{NimbuxError, Result};
use super::node::{Node, NodeStatus, NodeRole, NodeCapacity};

/// Scaling policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    pub min_nodes: usize,
    pub max_nodes: usize,
    pub target_cpu: f64,
    pub target_memory: f64,
    pub scale_up_threshold: f64,
    pub scale_down_threshold: f64,
    pub scale_up_cooldown: u64, // seconds
    pub scale_down_cooldown: u64, // seconds
}

/// Auto-scaler for managing cluster size
pub struct AutoScaler {
    policy: ScalingPolicy,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    last_scale_up: Arc<RwLock<u64>>,
    last_scale_down: Arc<RwLock<u64>>,
    scaling_metrics: Arc<RwLock<ScalingMetrics>>,
    is_running: Arc<RwLock<bool>>,
}

/// Scaling metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingMetrics {
    pub timestamp: u64,
    pub current_nodes: usize,
    pub avg_cpu_utilization: f64,
    pub avg_memory_utilization: f64,
    pub total_requests: u64,
    pub avg_response_time: f64,
    pub scaling_decision: Option<ScalingDecision>,
    pub scale_up_events: u32,
    pub scale_down_events: u32,
}

/// Scaling decision made by the auto-scaler
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScalingDecision {
    ScaleUp(usize), // Number of nodes to add
    ScaleDown(usize), // Number of nodes to remove
    NoAction,
    Cooldown(String), // Reason for cooldown
}

impl AutoScaler {
    pub fn new(policy: ScalingPolicy, nodes: Arc<RwLock<HashMap<String, Node>>>) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Ok(Self {
            policy,
            nodes,
            last_scale_up: Arc::new(RwLock::new(now)),
            last_scale_down: Arc::new(RwLock::new(now)),
            scaling_metrics: Arc::new(RwLock::new(ScalingMetrics::new())),
            is_running: Arc::new(RwLock::new(false)),
        })
    }
    
    /// Start the auto-scaler
    pub async fn start(&self) -> Result<()> {
        let mut running = self.is_running.write().await;
        if *running {
            return Err(NimbuxError::AutoScaler("Auto-scaler is already running".to_string()));
        }
        
        *running = true;
        drop(running);
        
        // Start the scaling loop
        self.start_scaling_loop().await?;
        
        Ok(())
    }
    
    /// Stop the auto-scaler
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.is_running.write().await;
        *running = false;
        Ok(())
    }
    
    /// Start the scaling loop
    async fn start_scaling_loop(&self) -> Result<()> {
        let is_running = Arc::clone(&self.is_running);
        let nodes = Arc::clone(&self.nodes);
        let policy = self.policy.clone();
        let last_scale_up = Arc::clone(&self.last_scale_up);
        let last_scale_down = Arc::clone(&self.last_scale_down);
        let scaling_metrics = Arc::clone(&self.scaling_metrics);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // Check if auto-scaler is still running
                {
                    let running = is_running.read().await;
                    if !*running {
                        break;
                    }
                }
                
                // Collect metrics and make scaling decision
                if let Err(e) = Self::evaluate_scaling(
                    &nodes,
                    &policy,
                    &last_scale_up,
                    &last_scale_down,
                    &scaling_metrics,
                ).await {
                    tracing::error!("Scaling evaluation failed: {}", e);
                }
            }
        });
        
        Ok(())
    }
    
    /// Evaluate scaling needs and make decisions
    async fn evaluate_scaling(
        nodes: &Arc<RwLock<HashMap<String, Node>>>,
        policy: &ScalingPolicy,
        last_scale_up: &Arc<RwLock<u64>>,
        last_scale_down: &Arc<RwLock<u64>>,
        scaling_metrics: &Arc<RwLock<ScalingMetrics>>,
    ) -> Result<()> {
        let nodes = nodes.read().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Calculate current metrics
        let mut total_cpu = 0.0;
        let mut total_memory = 0.0;
        let mut total_requests = 0u64;
        let mut total_response_time = 0.0;
        let mut healthy_nodes = 0;
        
        for node in nodes.values() {
            if node.status == NodeStatus::Healthy {
                healthy_nodes += 1;
                total_cpu += node.metrics.cpu_utilization;
                total_memory += node.metrics.memory_utilization;
                total_requests += node.metrics.request_count;
                total_response_time += node.metrics.response_time_avg;
            }
        }
        
        let avg_cpu = if healthy_nodes > 0 { total_cpu / healthy_nodes as f64 } else { 0.0 };
        let avg_memory = if healthy_nodes > 0 { total_memory / healthy_nodes as f64 } else { 0.0 };
        let avg_response_time = if healthy_nodes > 0 { total_response_time / healthy_nodes as f64 } else { 0.0 };
        
        // Update scaling metrics
        {
            let mut metrics = scaling_metrics.write().await;
            metrics.timestamp = now;
            metrics.current_nodes = healthy_nodes;
            metrics.avg_cpu_utilization = avg_cpu;
            metrics.avg_memory_utilization = avg_memory;
            metrics.total_requests = total_requests;
            metrics.avg_response_time = avg_response_time;
        }
        
        // Check cooldown periods
        let last_up = *last_scale_up.read().await;
        let last_down = *last_scale_down.read().await;
        
        let scale_up_cooldown_remaining = if now > last_up + policy.scale_up_cooldown {
            0
        } else {
            last_up + policy.scale_up_cooldown - now
        };
        
        let scale_down_cooldown_remaining = if now > last_down + policy.scale_down_cooldown {
            0
        } else {
            last_down + policy.scale_down_cooldown - now
        };
        
        // Make scaling decision
        let decision = if scale_up_cooldown_remaining > 0 {
            ScalingDecision::Cooldown(format!("Scale up cooldown: {}s remaining", scale_up_cooldown_remaining))
        } else if scale_down_cooldown_remaining > 0 {
            ScalingDecision::Cooldown(format!("Scale down cooldown: {}s remaining", scale_down_cooldown_remaining))
        } else if avg_cpu > policy.scale_up_threshold || avg_memory > policy.scale_up_threshold {
            // Scale up
            let nodes_to_add = Self::calculate_scale_up_nodes(healthy_nodes, avg_cpu, avg_memory, policy);
            if nodes_to_add > 0 && healthy_nodes + nodes_to_add <= policy.max_nodes {
                ScalingDecision::ScaleUp(nodes_to_add)
            } else {
                ScalingDecision::NoAction
            }
        } else if avg_cpu < policy.scale_down_threshold && avg_memory < policy.scale_down_threshold {
            // Scale down
            let nodes_to_remove = Self::calculate_scale_down_nodes(healthy_nodes, avg_cpu, avg_memory, policy);
            if nodes_to_remove > 0 && healthy_nodes - nodes_to_remove >= policy.min_nodes {
                ScalingDecision::ScaleDown(nodes_to_remove)
            } else {
                ScalingDecision::NoAction
            }
        } else {
            ScalingDecision::NoAction
        };
        
        // Update metrics with decision
        {
            let mut metrics = scaling_metrics.write().await;
            metrics.scaling_decision = Some(decision.clone());
        }
        
        // Execute scaling decision
        match decision {
            ScalingDecision::ScaleUp(count) => {
                tracing::info!("Auto-scaling: Adding {} nodes", count);
                // In a real implementation, this would trigger node creation
                // For now, we just log the decision
                let mut last_up = last_scale_up.write().await;
                *last_up = now;
                
                let mut metrics = scaling_metrics.write().await;
                metrics.scale_up_events += 1;
            }
            ScalingDecision::ScaleDown(count) => {
                tracing::info!("Auto-scaling: Removing {} nodes", count);
                // In a real implementation, this would trigger node removal
                // For now, we just log the decision
                let mut last_down = last_scale_down.write().await;
                *last_down = now;
                
                let mut metrics = scaling_metrics.write().await;
                metrics.scale_down_events += 1;
            }
            ScalingDecision::NoAction => {
                tracing::debug!("Auto-scaling: No action needed");
            }
            ScalingDecision::Cooldown(reason) => {
                tracing::debug!("Auto-scaling: {}", reason);
            }
        }
        
        Ok(())
    }
    
    /// Calculate number of nodes to add for scale up
    fn calculate_scale_up_nodes(
        current_nodes: usize,
        avg_cpu: f64,
        avg_memory: f64,
        policy: &ScalingPolicy,
    ) -> usize {
        // Calculate the overload factor
        let cpu_overload = (avg_cpu - policy.target_cpu) / policy.target_cpu;
        let memory_overload = (avg_memory - policy.target_memory) / policy.target_memory;
        let overload_factor = cpu_overload.max(memory_overload);
        
        // Calculate nodes needed based on overload
        let nodes_needed = (current_nodes as f64 * overload_factor).ceil() as usize;
        
        // Ensure we don't exceed max nodes
        nodes_needed.min(policy.max_nodes - current_nodes)
    }
    
    /// Calculate number of nodes to remove for scale down
    fn calculate_scale_down_nodes(
        current_nodes: usize,
        avg_cpu: f64,
        avg_memory: f64,
        policy: &ScalingPolicy,
    ) -> usize {
        // Calculate the underload factor
        let cpu_underload = (policy.target_cpu - avg_cpu) / policy.target_cpu;
        let memory_underload = (policy.target_memory - avg_memory) / policy.target_memory;
        let underload_factor = cpu_underload.max(memory_underload);
        
        // Calculate nodes that can be removed
        let nodes_to_remove = (current_nodes as f64 * underload_factor).floor() as usize;
        
        // Ensure we don't go below min nodes
        let max_removable = current_nodes - policy.min_nodes;
        nodes_to_remove.min(max_removable)
    }
    
    /// Get current scaling metrics
    pub async fn get_metrics(&self) -> Result<ScalingMetrics> {
        let metrics = self.scaling_metrics.read().await;
        Ok(metrics.clone())
    }
    
    /// Get scaling policy
    pub fn get_policy(&self) -> &ScalingPolicy {
        &self.policy
    }
    
    /// Update scaling policy
    pub fn update_policy(&mut self, policy: ScalingPolicy) {
        self.policy = policy;
    }
    
    /// Check if auto-scaler is running
    pub async fn is_running(&self) -> bool {
        let running = self.is_running.read().await;
        *running
    }
}

impl ScalingMetrics {
    pub fn new() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            current_nodes: 0,
            avg_cpu_utilization: 0.0,
            avg_memory_utilization: 0.0,
            total_requests: 0,
            avg_response_time: 0.0,
            scaling_decision: None,
            scale_up_events: 0,
            scale_down_events: 0,
        }
    }
    
    /// Calculate scaling efficiency
    pub fn get_scaling_efficiency(&self) -> f64 {
        let total_events = self.scale_up_events + self.scale_down_events;
        if total_events == 0 {
            return 1.0;
        }
        
        // Simple efficiency metric based on decision frequency
        // In a real implementation, this would be more sophisticated
        let decision_frequency = total_events as f64 / 100.0; // Assuming 100 evaluation cycles
        1.0 - decision_frequency.min(1.0)
    }
    
    /// Get scaling trend
    pub fn get_scaling_trend(&self) -> ScalingTrend {
        if self.scale_up_events > self.scale_down_events {
            ScalingTrend::ScalingUp
        } else if self.scale_down_events > self.scale_up_events {
            ScalingTrend::ScalingDown
        } else {
            ScalingTrend::Stable
        }
    }
}

/// Scaling trend enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScalingTrend {
    ScalingUp,
    ScalingDown,
    Stable,
}