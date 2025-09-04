// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Connection pooling and performance optimization

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::net::TcpStream;
use tracing::{info, debug, warn, error};

use crate::errors::{NimbuxError, Result};

/// Connection pool for managing TCP connections
pub struct ConnectionPool {
    /// Pool of available connections
    connections: Arc<RwLock<HashMap<String, Vec<PooledConnection>>>>,
    /// Maximum connections per endpoint
    max_connections_per_endpoint: usize,
    /// Connection timeout
    connection_timeout: Duration,
    /// Idle timeout
    idle_timeout: Duration,
    /// Semaphore for limiting total connections
    connection_semaphore: Arc<Semaphore>,
}

/// A pooled connection with metadata
#[derive(Debug)]
pub struct PooledConnection {
    pub stream: TcpStream,
    pub created_at: Instant,
    pub last_used: Instant,
    pub endpoint: String,
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub connections_per_endpoint: HashMap<String, usize>,
    pub total_requests: u64,
    pub connection_hits: u64,
    pub connection_misses: u64,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(
        max_connections_per_endpoint: usize,
        max_total_connections: usize,
        connection_timeout: Duration,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_endpoint,
            connection_timeout,
            idle_timeout,
            connection_semaphore: Arc::new(Semaphore::new(max_total_connections)),
        }
    }

    /// Get a connection from the pool or create a new one
    pub async fn get_connection(&self, endpoint: &str) -> Result<PooledConnection> {
        // Try to get an existing connection
        {
            let mut connections = self.connections.write().await;
            if let Some(endpoint_connections) = connections.get_mut(endpoint) {
                // Find a valid connection
                while let Some(pos) = endpoint_connections.iter().position(|conn| {
                    !self.is_connection_stale(conn)
                }) {
                    let mut connection = endpoint_connections.remove(pos);
                    connection.last_used = Instant::now();
                    
                    debug!("Reused connection to {}", endpoint);
                    return Ok(connection);
                }
            }
        }

        // No valid connection found, create a new one
        self.create_new_connection(endpoint).await
    }

    /// Return a connection to the pool
    pub async fn return_connection(&self, mut connection: PooledConnection) -> Result<()> {
        let endpoint = connection.endpoint.clone();
        
        // Check if connection is still valid
        if self.is_connection_stale(&connection) {
            debug!("Dropping stale connection to {}", endpoint);
            return Ok(());
        }

        // Check if we have room for this connection
        {
            let connections = self.connections.read().await;
            let endpoint_connections = connections.get(&endpoint).map(|v| v.len()).unwrap_or(0);
            
            if endpoint_connections >= self.max_connections_per_endpoint {
                debug!("Pool full for {}, dropping connection", endpoint);
                return Ok(());
            }
        }

        // Add connection back to pool
        {
            let mut connections = self.connections.write().await;
            connections.entry(endpoint.clone()).or_insert_with(Vec::new).push(connection);
        }

        debug!("Returned connection to pool for {}", endpoint);
        Ok(())
    }

    /// Create a new connection
    async fn create_new_connection(&self, endpoint: &str) -> Result<PooledConnection> {
        // Acquire semaphore permit
        let _permit = self.connection_semaphore.acquire().await
            .map_err(|e| NimbuxError::Network(format!("Failed to acquire connection permit: {}", e)))?;

        // Parse endpoint
        let (host, port) = self.parse_endpoint(endpoint)?;

        // Create connection with timeout
        let stream = tokio::time::timeout(
            self.connection_timeout,
            TcpStream::connect((host.as_str(), port))
        ).await
        .map_err(|_| NimbuxError::Network("Connection timeout".to_string()))?
        .map_err(|e| NimbuxError::Network(format!("Failed to connect to {}: {}", endpoint, e)))?;

        let now = Instant::now();
        let connection = PooledConnection {
            stream,
            created_at: now,
            last_used: now,
            endpoint: endpoint.to_string(),
        };

        debug!("Created new connection to {}", endpoint);
        Ok(connection)
    }

    /// Check if a connection is stale
    fn is_connection_stale(&self, connection: &PooledConnection) -> bool {
        let now = Instant::now();
        now.duration_since(connection.last_used) > self.idle_timeout
    }

    /// Parse endpoint into host and port
    fn parse_endpoint(&self, endpoint: &str) -> Result<(String, u16)> {
        if let Some(colon_pos) = endpoint.rfind(':') {
            let host = endpoint[..colon_pos].to_string();
            let port = endpoint[colon_pos + 1..].parse::<u16>()
                .map_err(|_| NimbuxError::Network(format!("Invalid port in endpoint: {}", endpoint)))?;
            Ok((host, port))
        } else {
            // Default port
            Ok((endpoint.to_string(), 80))
        }
    }

    /// Clean up stale connections
    pub async fn cleanup_stale_connections(&self) -> Result<usize> {
        let mut connections = self.connections.write().await;
        let mut total_removed = 0;

        for (endpoint, endpoint_connections) in connections.iter_mut() {
            let initial_count = endpoint_connections.len();
            endpoint_connections.retain(|conn| !self.is_connection_stale(conn));
            let removed = initial_count - endpoint_connections.len();
            total_removed += removed;
            
            if removed > 0 {
                debug!("Removed {} stale connections for {}", removed, endpoint);
            }
        }

        if total_removed > 0 {
            info!("Cleaned up {} stale connections", total_removed);
        }

        Ok(total_removed)
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> Result<PoolStats> {
        let connections = self.connections.read().await;
        let mut total_connections = 0;
        let mut connections_per_endpoint = HashMap::new();

        for (endpoint, endpoint_connections) in connections.iter() {
            let count = endpoint_connections.len();
            total_connections += count;
            connections_per_endpoint.insert(endpoint.clone(), count);
        }

        Ok(PoolStats {
            total_connections,
            active_connections: total_connections,
            idle_connections: total_connections,
            connections_per_endpoint,
            total_requests: 0, // Would need to track this separately
            connection_hits: 0, // Would need to track this separately
            connection_misses: 0, // Would need to track this separately
        })
    }

    /// Close all connections
    pub async fn close_all(&self) -> Result<()> {
        let mut connections = self.connections.write().await;
        let total_connections: usize = connections.values().map(|v| v.len()).sum();
        
        connections.clear();
        
        info!("Closed {} connections", total_connections);
        Ok(())
    }
}

/// HTTP connection pool for REST API calls
pub struct HttpConnectionPool {
    /// Underlying connection pool
    pool: ConnectionPool,
    /// User agent string
    user_agent: String,
    /// Default headers
    default_headers: HashMap<String, String>,
}

impl HttpConnectionPool {
    /// Create a new HTTP connection pool
    pub fn new(
        max_connections_per_endpoint: usize,
        max_total_connections: usize,
        connection_timeout: Duration,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            pool: ConnectionPool::new(
                max_connections_per_endpoint,
                max_total_connections,
                connection_timeout,
                idle_timeout,
            ),
            user_agent: "Nimbux-Client/1.0".to_string(),
            default_headers: HashMap::new(),
        }
    }

    /// Set user agent
    pub fn with_user_agent(mut self, user_agent: String) -> Self {
        self.user_agent = user_agent;
        self
    }

    /// Add default header
    pub fn add_default_header(&mut self, key: String, value: String) {
        self.default_headers.insert(key, value);
    }

    /// Get connection from pool
    pub async fn get_connection(&self, endpoint: &str) -> Result<PooledConnection> {
        self.pool.get_connection(endpoint).await
    }

    /// Return connection to pool
    pub async fn return_connection(&self, connection: PooledConnection) -> Result<()> {
        self.pool.return_connection(connection).await
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> Result<PoolStats> {
        self.pool.get_stats().await
    }

    /// Clean up stale connections
    pub async fn cleanup_stale_connections(&self) -> Result<usize> {
        self.pool.cleanup_stale_connections().await
    }
}

/// Async I/O buffer pool for efficient memory management
pub struct BufferPool {
    /// Pool of available buffers
    buffers: Arc<RwLock<Vec<Vec<u8>>>>,
    /// Buffer size
    buffer_size: usize,
    /// Maximum number of buffers in pool
    max_buffers: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(buffer_size: usize, max_buffers: usize) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(Vec::new())),
            buffer_size,
            max_buffers,
        }
    }

    /// Get a buffer from the pool
    pub async fn get_buffer(&self) -> Vec<u8> {
        let mut buffers = self.buffers.write().await;
        buffers.pop().unwrap_or_else(|| vec![0u8; self.buffer_size])
    }

    /// Return a buffer to the pool
    pub async fn return_buffer(&self, mut buffer: Vec<u8>) {
        // Clear the buffer
        buffer.clear();
        
        // Resize to standard size
        buffer.resize(self.buffer_size, 0);

        let mut buffers = self.buffers.write().await;
        if buffers.len() < self.max_buffers {
            buffers.push(buffer);
        }
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> BufferPoolStats {
        let buffers = self.buffers.read().await;
        BufferPoolStats {
            available_buffers: buffers.len(),
            max_buffers: self.max_buffers,
            buffer_size: self.buffer_size,
        }
    }
}

/// Buffer pool statistics
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub available_buffers: usize,
    pub max_buffers: usize,
    pub buffer_size: usize,
}

/// Performance monitoring for connection pools
pub struct PerformanceMonitor {
    /// Request latency histogram
    latency_histogram: Arc<RwLock<Vec<Duration>>>,
    /// Throughput counter
    throughput_counter: Arc<RwLock<u64>>,
    /// Error counter
    error_counter: Arc<RwLock<u64>>,
    /// Start time
    start_time: Instant,
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    pub fn new() -> Self {
        Self {
            latency_histogram: Arc::new(RwLock::new(Vec::new())),
            throughput_counter: Arc::new(RwLock::new(0)),
            error_counter: Arc::new(RwLock::new(0)),
            start_time: Instant::now(),
        }
    }

    /// Record request latency
    pub async fn record_latency(&self, latency: Duration) {
        let mut histogram = self.latency_histogram.write().await;
        histogram.push(latency);
        
        // Keep only last 1000 measurements
        if histogram.len() > 1000 {
            histogram.remove(0);
        }
    }

    /// Record successful request
    pub async fn record_success(&self) {
        let mut counter = self.throughput_counter.write().await;
        *counter += 1;
    }

    /// Record error
    pub async fn record_error(&self) {
        let mut counter = self.error_counter.write().await;
        *counter += 1;
    }

    /// Get performance statistics
    pub async fn get_stats(&self) -> PerformanceStats {
        let histogram = self.latency_histogram.read().await;
        let throughput = *self.throughput_counter.read().await;
        let errors = *self.error_counter.read().await;
        
        let avg_latency = if !histogram.is_empty() {
            let total: Duration = histogram.iter().sum();
            total / histogram.len() as u32
        } else {
            Duration::from_millis(0)
        };

        let p95_latency = if histogram.len() >= 20 {
            let mut sorted = histogram.clone();
            sorted.sort();
            sorted[sorted.len() * 95 / 100]
        } else {
            Duration::from_millis(0)
        };

        let uptime = self.start_time.elapsed();
        let requests_per_second = if uptime.as_secs() > 0 {
            throughput as f64 / uptime.as_secs() as f64
        } else {
            0.0
        };

        PerformanceStats {
            uptime,
            total_requests: throughput,
            total_errors: errors,
            requests_per_second,
            avg_latency,
            p95_latency,
            error_rate: if throughput > 0 {
                errors as f64 / throughput as f64
            } else {
                0.0
            },
        }
    }
}

/// Performance statistics
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub uptime: Duration,
    pub total_requests: u64,
    pub total_errors: u64,
    pub requests_per_second: f64,
    pub avg_latency: Duration,
    pub p95_latency: Duration,
    pub error_rate: f64,
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
}