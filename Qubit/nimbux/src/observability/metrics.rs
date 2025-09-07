// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Metrics collection

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::errors::{NimbuxError, Result};

/// Metric types supported by Nimbux
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// Individual metric data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    pub name: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub timestamp: u64,
    pub metric_type: MetricType,
}

/// Metric aggregation for histograms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramData {
    pub count: u64,
    pub sum: f64,
    pub buckets: Vec<(f64, u64)>, // (upper_bound, count)
}

/// Comprehensive metrics for Nimbux operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NimbuxMetrics {
    // Request metrics
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub request_duration_histogram: HistogramData,
    
    // Storage metrics
    pub total_objects: u64,
    pub total_bytes_stored: u64,
    pub storage_operations: HashMap<String, u64>,
    
    // Network metrics
    pub tcp_connections: u64,
    pub http_connections: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    
    // Authentication metrics
    pub auth_successes: u64,
    pub auth_failures: u64,
    pub active_users: u64,
    
    // Performance metrics
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub compression_ratio: f64,
    
    // System metrics
    pub memory_usage: u64,
    pub cpu_usage: f64,
    pub disk_usage: u64,
    
    // Custom metrics
    pub custom_metrics: HashMap<String, MetricPoint>,
}

/// Metrics collector for Nimbux
pub struct MetricsCollector {
    metrics: Arc<RwLock<NimbuxMetrics>>,
    start_time: u64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            metrics: Arc::new(RwLock::new(NimbuxMetrics {
                total_requests: 0,
                successful_requests: 0,
                failed_requests: 0,
                request_duration_histogram: HistogramData {
                    count: 0,
                    sum: 0.0,
                    buckets: vec![
                        (0.001, 0),   // 1ms
                        (0.01, 0),    // 10ms
                        (0.1, 0),     // 100ms
                        (1.0, 0),     // 1s
                        (10.0, 0),    // 10s
                        (f64::INFINITY, 0),
                    ],
                },
                total_objects: 0,
                total_bytes_stored: 0,
                storage_operations: HashMap::new(),
                tcp_connections: 0,
                http_connections: 0,
                bytes_sent: 0,
                bytes_received: 0,
                auth_successes: 0,
                auth_failures: 0,
                active_users: 0,
                cache_hits: 0,
                cache_misses: 0,
                compression_ratio: 0.0,
                memory_usage: 0,
                cpu_usage: 0.0,
                disk_usage: 0,
                custom_metrics: HashMap::new(),
            })),
            start_time: now,
        }
    }

    /// Record a request
    pub async fn record_request(&self, success: bool, duration: Duration) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        metrics.total_requests += 1;
        if success {
            metrics.successful_requests += 1;
        } else {
            metrics.failed_requests += 1;
        }

        // Update histogram
        let duration_secs = duration.as_secs_f64();
        metrics.request_duration_histogram.count += 1;
        metrics.request_duration_histogram.sum += duration_secs;

        // Update buckets
        for (upper_bound, count) in &mut metrics.request_duration_histogram.buckets {
            if duration_secs <= *upper_bound {
                *count += 1;
            }
        }

        debug!("Recorded request: success={}, duration={:?}", success, duration);
        Ok(())
    }

    /// Record storage operation
    pub async fn record_storage_operation(&self, operation: &str, bytes: u64) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        *metrics.storage_operations.entry(operation.to_string()).or_insert(0) += 1;
        metrics.total_bytes_stored += bytes;
        
        if operation == "put" {
            metrics.total_objects += 1;
        } else if operation == "delete" {
            metrics.total_objects = metrics.total_objects.saturating_sub(1);
        }

        debug!("Recorded storage operation: {} ({} bytes)", operation, bytes);
        Ok(())
    }

    /// Record network activity
    pub async fn record_network_activity(&self, protocol: &str, bytes_sent: u64, bytes_received: u64) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        match protocol {
            "tcp" => metrics.tcp_connections += 1,
            "http" => metrics.http_connections += 1,
            _ => {}
        }
        
        metrics.bytes_sent += bytes_sent;
        metrics.bytes_received += bytes_received;

        debug!("Recorded network activity: {} (sent: {}, received: {})", protocol, bytes_sent, bytes_received);
        Ok(())
    }

    /// Record authentication event
    pub async fn record_auth_event(&self, success: bool) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        if success {
            metrics.auth_successes += 1;
        } else {
            metrics.auth_failures += 1;
        }

        debug!("Recorded auth event: success={}", success);
        Ok(())
    }

    /// Record cache event
    pub async fn record_cache_event(&self, hit: bool) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        if hit {
            metrics.cache_hits += 1;
        } else {
            metrics.cache_misses += 1;
        }

        debug!("Recorded cache event: hit={}", hit);
        Ok(())
    }

    /// Update system metrics
    pub async fn update_system_metrics(&self, memory: u64, cpu: f64, disk: u64) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        metrics.memory_usage = memory;
        metrics.cpu_usage = cpu;
        metrics.disk_usage = disk;

        debug!("Updated system metrics: memory={}, cpu={}, disk={}", memory, cpu, disk);
        Ok(())
    }

    /// Add custom metric
    pub async fn add_custom_metric(&self, name: String, value: f64, labels: HashMap<String, String>, metric_type: MetricType) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metric_point = MetricPoint {
            name: name.clone(),
            value,
            labels,
            timestamp: now,
            metric_type,
        };

        metrics.custom_metrics.insert(name, metric_point);
        debug!("Added custom metric: {}", name);
        Ok(())
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> Result<NimbuxMetrics> {
        let metrics = self.metrics.read().await;
        Ok(metrics.clone())
    }

    /// Get metrics summary
    pub async fn get_metrics_summary(&self) -> Result<MetricsSummary> {
        let metrics = self.metrics.read().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let uptime = now - self.start_time;
        let success_rate = if metrics.total_requests > 0 {
            (metrics.successful_requests as f64 / metrics.total_requests as f64) * 100.0
        } else {
            0.0
        };

        let avg_request_duration = if metrics.request_duration_histogram.count > 0 {
            metrics.request_duration_histogram.sum / metrics.request_duration_histogram.count as f64
        } else {
            0.0
        };

        let cache_hit_rate = if metrics.cache_hits + metrics.cache_misses > 0 {
            (metrics.cache_hits as f64 / (metrics.cache_hits + metrics.cache_misses) as f64) * 100.0
        } else {
            0.0
        };

        Ok(MetricsSummary {
            uptime_seconds: uptime,
            total_requests: metrics.total_requests,
            success_rate_percent: success_rate,
            avg_request_duration_seconds: avg_request_duration,
            total_objects: metrics.total_objects,
            total_bytes_stored: metrics.total_bytes_stored,
            active_connections: metrics.tcp_connections + metrics.http_connections,
            cache_hit_rate_percent: cache_hit_rate,
            memory_usage_bytes: metrics.memory_usage,
            cpu_usage_percent: metrics.cpu_usage,
            disk_usage_bytes: metrics.disk_usage,
        })
    }

    /// Reset metrics (useful for testing)
    pub async fn reset_metrics(&self) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = NimbuxMetrics {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            request_duration_histogram: HistogramData {
                count: 0,
                sum: 0.0,
                buckets: vec![
                    (0.001, 0),
                    (0.01, 0),
                    (0.1, 0),
                    (1.0, 0),
                    (10.0, 0),
                    (f64::INFINITY, 0),
                ],
            },
            total_objects: 0,
            total_bytes_stored: 0,
            storage_operations: HashMap::new(),
            tcp_connections: 0,
            http_connections: 0,
            bytes_sent: 0,
            bytes_received: 0,
            auth_successes: 0,
            auth_failures: 0,
            active_users: 0,
            cache_hits: 0,
            cache_misses: 0,
            compression_ratio: 0.0,
            memory_usage: 0,
            cpu_usage: 0.0,
            disk_usage: 0,
            custom_metrics: HashMap::new(),
        };

        info!("Metrics reset");
        Ok(())
    }
}

/// Summary of key metrics for monitoring dashboards
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub uptime_seconds: u64,
    pub total_requests: u64,
    pub success_rate_percent: f64,
    pub avg_request_duration_seconds: f64,
    pub total_objects: u64,
    pub total_bytes_stored: u64,
    pub active_connections: u64,
    pub cache_hit_rate_percent: f64,
    pub memory_usage_bytes: u64,
    pub cpu_usage_percent: f64,
    pub disk_usage_bytes: u64,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}