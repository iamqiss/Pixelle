// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Async I/O operations for high performance

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::collections::HashMap;

use crate::errors::{NimbuxError, Result};

/// I/O configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOConfig {
    pub max_request_size: usize,
    pub max_response_size: usize,
    pub enable_compression: bool,
}

/// I/O operation types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IOOperation {
    Read,
    Write,
    ReadWrite,
    Delete,
    List,
    Head,
}

/// I/O metrics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IOMetrics {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub avg_read_latency: f64, // milliseconds
    pub avg_write_latency: f64, // milliseconds
    pub avg_operation_latency: f64, // milliseconds
    pub p95_latency: f64, // milliseconds
    pub p99_latency: f64, // milliseconds
    pub throughput: f64, // operations per second
    pub error_rate: f64, // percentage
}

/// Async I/O handler for high-performance operations
pub struct AsyncIO {
    config: IOConfig,
    metrics: Arc<RwLock<IOMetrics>>,
    latency_tracker: Arc<RwLock<Vec<f64>>>,
    operation_counts: Arc<RwLock<HashMap<IOOperation, u64>>>,
}

impl AsyncIO {
    pub fn new(config: IOConfig) -> Result<Self> {
        Ok(Self {
            config,
            metrics: Arc::new(RwLock::new(IOMetrics {
                total_operations: 0,
                successful_operations: 0,
                failed_operations: 0,
                bytes_read: 0,
                bytes_written: 0,
                avg_read_latency: 0.0,
                avg_write_latency: 0.0,
                avg_operation_latency: 0.0,
                p95_latency: 0.0,
                p99_latency: 0.0,
                throughput: 0.0,
                error_rate: 0.0,
            })),
            latency_tracker: Arc::new(RwLock::new(Vec::new())),
            operation_counts: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Perform async read operation
    pub async fn read_async<R: AsyncRead + Unpin>(&self, reader: &mut R, buffer: &mut [u8]) -> Result<usize> {
        let start_time = Instant::now();
        
        match reader.read(buffer).await {
            Ok(bytes_read) => {
                let latency = start_time.elapsed().as_millis() as f64;
                self.update_read_metrics(bytes_read as u64, latency).await;
                Ok(bytes_read)
            }
            Err(e) => {
                self.update_error_metrics().await;
                Err(NimbuxError::AsyncIO(format!("Read operation failed: {}", e)))
            }
        }
    }
    
    /// Perform async write operation
    pub async fn write_async<W: AsyncWrite + Unpin>(&self, writer: &mut W, data: &[u8]) -> Result<usize> {
        let start_time = Instant::now();
        
        match writer.write_all(data).await {
            Ok(_) => {
                let latency = start_time.elapsed().as_millis() as f64;
                self.update_write_metrics(data.len() as u64, latency).await;
                Ok(data.len())
            }
            Err(e) => {
                self.update_error_metrics().await;
                Err(NimbuxError::AsyncIO(format!("Write operation failed: {}", e)))
            }
        }
    }
    
    /// Perform async read-write operation
    pub async fn read_write_async<RW: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut RW,
        request: &[u8],
        response_buffer: &mut [u8],
    ) -> Result<usize> {
        let start_time = Instant::now();
        
        // Write request
        if let Err(e) = stream.write_all(request).await {
            self.update_error_metrics().await;
            return Err(NimbuxError::AsyncIO(format!("Write request failed: {}", e)));
        }
        
        // Flush to ensure data is sent
        if let Err(e) = stream.flush().await {
            self.update_error_metrics().await;
            return Err(NimbuxError::AsyncIO(format!("Flush failed: {}", e)));
        }
        
        // Read response
        let bytes_read = match stream.read(response_buffer).await {
            Ok(bytes) => bytes,
            Err(e) => {
                self.update_error_metrics().await;
                return Err(NimbuxError::AsyncIO(format!("Read response failed: {}", e)));
            }
        };
        
        let latency = start_time.elapsed().as_millis() as f64;
        self.update_read_write_metrics(request.len() as u64, bytes_read as u64, latency).await;
        
        Ok(bytes_read)
    }
    
    /// Perform batch read operations
    pub async fn batch_read_async<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        requests: Vec<ReadRequest>,
    ) -> Result<Vec<ReadResponse>> {
        let start_time = Instant::now();
        let mut responses = Vec::new();
        
        for request in requests {
            let mut buffer = vec![0u8; request.buffer_size];
            match self.read_async(reader, &mut buffer).await {
                Ok(bytes_read) => {
                    responses.push(ReadResponse {
                        request_id: request.request_id,
                        data: buffer[..bytes_read].to_vec(),
                        bytes_read,
                        success: true,
                    });
                }
                Err(e) => {
                    responses.push(ReadResponse {
                        request_id: request.request_id,
                        data: Vec::new(),
                        bytes_read: 0,
                        success: false,
                    });
                    tracing::warn!("Batch read failed for request {}: {}", request.request_id, e);
                }
            }
        }
        
        let latency = start_time.elapsed().as_millis() as f64;
        self.update_batch_metrics(requests.len(), latency).await;
        
        Ok(responses)
    }
    
    /// Perform batch write operations
    pub async fn batch_write_async<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        requests: Vec<WriteRequest>,
    ) -> Result<Vec<WriteResponse>> {
        let start_time = Instant::now();
        let mut responses = Vec::new();
        
        for request in requests {
            match self.write_async(writer, &request.data).await {
                Ok(bytes_written) => {
                    responses.push(WriteResponse {
                        request_id: request.request_id,
                        bytes_written,
                        success: true,
                    });
                }
                Err(e) => {
                    responses.push(WriteResponse {
                        request_id: request.request_id,
                        bytes_written: 0,
                        success: false,
                    });
                    tracing::warn!("Batch write failed for request {}: {}", request.request_id, e);
                }
            }
        }
        
        let latency = start_time.elapsed().as_millis() as f64;
        self.update_batch_metrics(requests.len(), latency).await;
        
        Ok(responses)
    }
    
    /// Update read metrics
    async fn update_read_metrics(&self, bytes_read: u64, latency: f64) {
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += 1;
        metrics.successful_operations += 1;
        metrics.bytes_read += bytes_read;
        
        // Update average read latency
        let total_reads = metrics.successful_operations as f64;
        metrics.avg_read_latency = (metrics.avg_read_latency * (total_reads - 1.0) + latency) / total_reads;
        
        // Update overall average latency
        metrics.avg_operation_latency = (metrics.avg_operation_latency * (total_reads - 1.0) + latency) / total_reads;
        
        // Track latency for percentiles
        self.track_latency(latency).await;
    }
    
    /// Update write metrics
    async fn update_write_metrics(&self, bytes_written: u64, latency: f64) {
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += 1;
        metrics.successful_operations += 1;
        metrics.bytes_written += bytes_written;
        
        // Update average write latency
        let total_writes = metrics.successful_operations as f64;
        metrics.avg_write_latency = (metrics.avg_write_latency * (total_writes - 1.0) + latency) / total_writes;
        
        // Update overall average latency
        metrics.avg_operation_latency = (metrics.avg_operation_latency * (total_writes - 1.0) + latency) / total_writes;
        
        // Track latency for percentiles
        self.track_latency(latency).await;
    }
    
    /// Update read-write metrics
    async fn update_read_write_metrics(&self, bytes_written: u64, bytes_read: u64, latency: f64) {
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += 1;
        metrics.successful_operations += 1;
        metrics.bytes_read += bytes_read;
        metrics.bytes_written += bytes_written;
        
        // Update average latency
        let total_operations = metrics.successful_operations as f64;
        metrics.avg_operation_latency = (metrics.avg_operation_latency * (total_operations - 1.0) + latency) / total_operations;
        
        // Track latency for percentiles
        self.track_latency(latency).await;
    }
    
    /// Update batch metrics
    async fn update_batch_metrics(&self, operation_count: usize, latency: f64) {
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += operation_count as u64;
        metrics.successful_operations += operation_count as u64;
        
        // Update average latency
        let total_operations = metrics.successful_operations as f64;
        metrics.avg_operation_latency = (metrics.avg_operation_latency * (total_operations - operation_count as f64) + latency) / total_operations;
        
        // Track latency for percentiles
        self.track_latency(latency).await;
    }
    
    /// Update error metrics
    async fn update_error_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        metrics.total_operations += 1;
        metrics.failed_operations += 1;
        
        // Update error rate
        if metrics.total_operations > 0 {
            metrics.error_rate = (metrics.failed_operations as f64 / metrics.total_operations as f64) * 100.0;
        }
    }
    
    /// Track latency for percentile calculations
    async fn track_latency(&self, latency: f64) {
        let mut tracker = self.latency_tracker.write().await;
        tracker.push(latency);
        
        // Keep only last 1000 measurements for memory efficiency
        if tracker.len() > 1000 {
            tracker.remove(0);
        }
        
        // Update percentiles
        if tracker.len() >= 10 {
            let mut sorted_latencies = tracker.clone();
            sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
            
            let p95_index = (sorted_latencies.len() as f64 * 0.95) as usize;
            let p99_index = (sorted_latencies.len() as f64 * 0.99) as usize;
            
            let mut metrics = self.metrics.write().await;
            metrics.p95_latency = sorted_latencies[p95_index.min(sorted_latencies.len() - 1)];
            metrics.p99_latency = sorted_latencies[p99_index.min(sorted_latencies.len() - 1)];
        }
    }
    
    /// Update throughput calculation
    async fn update_throughput(&self) {
        let metrics = self.metrics.read().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Simple throughput calculation (operations per second)
        // In a real implementation, this would be more sophisticated
        let mut metrics = self.metrics.write().await;
        metrics.throughput = metrics.successful_operations as f64 / 60.0; // Assuming 1 minute window
    }
    
    /// Get I/O metrics
    pub async fn get_stats(&self) -> Result<IOMetrics> {
        self.update_throughput().await;
        let metrics = self.metrics.read().await;
        Ok(metrics.clone())
    }
    
    /// Reset metrics
    pub async fn reset_metrics(&self) -> Result<()> {
        let mut metrics = self.metrics.write().await;
        *metrics = IOMetrics {
            total_operations: 0,
            successful_operations: 0,
            failed_operations: 0,
            bytes_read: 0,
            bytes_written: 0,
            avg_read_latency: 0.0,
            avg_write_latency: 0.0,
            avg_operation_latency: 0.0,
            p95_latency: 0.0,
            p99_latency: 0.0,
            throughput: 0.0,
            error_rate: 0.0,
        };
        
        let mut tracker = self.latency_tracker.write().await;
        tracker.clear();
        
        Ok(())
    }
}

/// Read request for batch operations
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub request_id: String,
    pub buffer_size: usize,
}

/// Read response for batch operations
#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub request_id: String,
    pub data: Vec<u8>,
    pub bytes_read: usize,
    pub success: bool,
}

/// Write request for batch operations
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub request_id: String,
    pub data: Vec<u8>,
}

/// Write response for batch operations
#[derive(Debug, Clone)]
pub struct WriteResponse {
    pub request_id: String,
    pub bytes_written: usize,
    pub success: bool,
}