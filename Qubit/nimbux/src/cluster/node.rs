// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Node management for cluster operations

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::errors::{NimbuxError, Result};

/// Node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub role: NodeRole,
    pub status: NodeStatus,
    pub metrics: NodeMetrics,
    pub capacity: NodeCapacity,
    pub created_at: u64,
    pub last_heartbeat: u64,
    pub version: String,
    pub region: Option<String>,
    pub zone: Option<String>,
}

/// Node role in the cluster
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeRole {
    Master,
    Worker,
    Storage,
    Coordinator,
}

/// Node status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Healthy,
    Unhealthy,
    Starting,
    Stopping,
    Maintenance,
    Unknown,
}

/// Node capacity information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu_cores: u32,
    pub memory_gb: u64,
    pub storage_gb: u64,
    pub network_bandwidth_mbps: u64,
    pub max_connections: u32,
    pub max_objects: u64,
}

/// Node metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub storage_used: u64,
    pub storage_available: u64,
    pub network_in: u64,
    pub network_out: u64,
    pub disk_read: u64,
    pub disk_write: u64,
    pub active_connections: u32,
    pub request_count: u64,
    pub error_count: u64,
    pub response_time_avg: f64,
    pub response_time_p95: f64,
    pub response_time_p99: f64,
    pub last_updated: u64,
}

impl Node {
    /// Create a new node
    pub fn new(
        address: String,
        port: u16,
        role: NodeRole,
        capacity: NodeCapacity,
        version: String,
        region: Option<String>,
        zone: Option<String>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            id: Uuid::new_v4().to_string(),
            address,
            port,
            role,
            status: NodeStatus::Starting,
            metrics: NodeMetrics::new(),
            capacity,
            created_at: now,
            last_heartbeat: now,
            version,
            region,
            zone,
        }
    }
    
    /// Update node metrics
    pub fn update_metrics(&mut self, metrics: NodeMetrics) {
        self.metrics = metrics;
        self.last_heartbeat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Check if node is healthy
    pub fn is_healthy(&self) -> bool {
        self.status == NodeStatus::Healthy
    }
    
    /// Check if node is available for new requests
    pub fn is_available(&self) -> bool {
        self.is_healthy() && 
        self.metrics.cpu_utilization < 90.0 &&
        self.metrics.memory_utilization < 95.0 &&
        self.metrics.active_connections < self.capacity.max_connections
    }
    
    /// Get node utilization score (0.0 to 1.0)
    pub fn get_utilization_score(&self) -> f64 {
        let cpu_score = self.metrics.cpu_utilization / 100.0;
        let memory_score = self.metrics.memory_utilization / 100.0;
        let connection_score = self.metrics.active_connections as f64 / self.capacity.max_connections as f64;
        
        (cpu_score + memory_score + connection_score) / 3.0
    }
    
    /// Get node health score (0.0 to 1.0)
    pub fn get_health_score(&self) -> f64 {
        if !self.is_healthy() {
            return 0.0;
        }
        
        let utilization_score = self.get_utilization_score();
        let error_rate = if self.metrics.request_count > 0 {
            self.metrics.error_count as f64 / self.metrics.request_count as f64
        } else {
            0.0
        };
        
        // Health score decreases with high utilization and error rate
        let health_score = 1.0 - (utilization_score * 0.5 + error_rate * 0.5);
        health_score.max(0.0).min(1.0)
    }
    
    /// Check if node needs maintenance
    pub fn needs_maintenance(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Check if node hasn't sent heartbeat in 60 seconds
        if now - self.last_heartbeat > 60 {
            return true;
        }
        
        // Check if error rate is too high
        if self.metrics.request_count > 100 {
            let error_rate = self.metrics.error_count as f64 / self.metrics.request_count as f64;
            if error_rate > 0.1 { // 10% error rate
                return true;
            }
        }
        
        // Check if response time is too high
        if self.metrics.response_time_p95 > 1000.0 { // 1 second
            return true;
        }
        
        false
    }
    
    /// Get node endpoint URL
    pub fn get_endpoint(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }
    
    /// Get node identifier for routing
    pub fn get_identifier(&self) -> String {
        format!("{}:{}:{}", self.id, self.address, self.port)
    }
}

impl NodeMetrics {
    pub fn new() -> Self {
        Self {
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            storage_used: 0,
            storage_available: 0,
            network_in: 0,
            network_out: 0,
            disk_read: 0,
            disk_write: 0,
            active_connections: 0,
            request_count: 0,
            error_count: 0,
            response_time_avg: 0.0,
            response_time_p95: 0.0,
            response_time_p99: 0.0,
            last_updated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
    
    /// Update metrics with new values
    pub fn update(&mut self, new_metrics: NodeMetrics) {
        self.cpu_utilization = new_metrics.cpu_utilization;
        self.memory_utilization = new_metrics.memory_utilization;
        self.storage_used = new_metrics.storage_used;
        self.storage_available = new_metrics.storage_available;
        self.network_in = new_metrics.network_in;
        self.network_out = new_metrics.network_out;
        self.disk_read = new_metrics.disk_read;
        self.disk_write = new_metrics.disk_write;
        self.active_connections = new_metrics.active_connections;
        self.request_count = new_metrics.request_count;
        self.error_count = new_metrics.error_count;
        self.response_time_avg = new_metrics.response_time_avg;
        self.response_time_p95 = new_metrics.response_time_p95;
        self.response_time_p99 = new_metrics.response_time_p99;
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    /// Calculate error rate
    pub fn get_error_rate(&self) -> f64 {
        if self.request_count > 0 {
            (self.error_count as f64 / self.request_count as f64) * 100.0
        } else {
            0.0
        }
    }
    
    /// Calculate throughput (requests per second)
    pub fn get_throughput(&self) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if now > self.last_updated {
            self.request_count as f64 / (now - self.last_updated) as f64
        } else {
            0.0
        }
    }
}

impl NodeCapacity {
    pub fn new(cpu_cores: u32, memory_gb: u64, storage_gb: u64) -> Self {
        Self {
            cpu_cores,
            memory_gb,
            storage_gb,
            network_bandwidth_mbps: 1000, // Default 1 Gbps
            max_connections: 10000,
            max_objects: 1000000,
        }
    }
    
    /// Get storage utilization percentage
    pub fn get_storage_utilization(&self, used: u64) -> f64 {
        if self.storage_gb > 0 {
            (used as f64 / (self.storage_gb * 1024 * 1024 * 1024) as f64) * 100.0
        } else {
            0.0
        }
    }
    
    /// Check if node has capacity for new connections
    pub fn has_connection_capacity(&self, current_connections: u32) -> bool {
        current_connections < self.max_connections
    }
    
    /// Check if node has storage capacity
    pub fn has_storage_capacity(&self, used: u64, requested: u64) -> bool {
        let available = (self.storage_gb * 1024 * 1024 * 1024) - used;
        available >= requested
    }
}