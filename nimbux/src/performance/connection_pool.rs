// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// High-performance connection pool for low latency

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, Notify};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::net::{SocketAddr, TcpStream};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio::time::timeout;

use crate::errors::{NimbuxError, Result};

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub max_connections: u32,
    pub connection_timeout: u64, // milliseconds
    pub idle_timeout: u64, // milliseconds
}

/// Connection pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    pub total_connections: u32,
    pub active_connections: u32,
    pub idle_connections: u32,
    pub max_connections: u32,
    pub connection_attempts: u64,
    pub successful_connections: u64,
    pub failed_connections: u64,
    pub connection_errors: u64,
    pub avg_connection_time: f64, // milliseconds
    pub avg_idle_time: f64, // milliseconds
    pub pool_utilization: f64, // percentage
}

/// Connection wrapper with metadata
#[derive(Debug)]
pub struct PooledConnection {
    pub stream: AsyncTcpStream,
    pub created_at: Instant,
    pub last_used: Instant,
    pub use_count: u64,
    pub is_healthy: bool,
}

/// Connection pool for managing TCP connections
pub struct ConnectionPool {
    config: PoolConfig,
    connections: Arc<RwLock<VecDeque<PooledConnection>>>,
    semaphore: Arc<Semaphore>,
    stats: Arc<RwLock<PoolStats>>,
    notify: Arc<Notify>,
    cleanup_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Result<Self> {
        let semaphore = Arc::new(Semaphore::new(config.max_connections as usize));
        let stats = Arc::new(RwLock::new(PoolStats {
            total_connections: 0,
            active_connections: 0,
            idle_connections: 0,
            max_connections: config.max_connections,
            connection_attempts: 0,
            successful_connections: 0,
            failed_connections: 0,
            connection_errors: 0,
            avg_connection_time: 0.0,
            avg_idle_time: 0.0,
            pool_utilization: 0.0,
        }));
        
        let pool = Self {
            config,
            connections: Arc::new(RwLock::new(VecDeque::new())),
            semaphore,
            stats,
            notify: Arc::new(Notify::new()),
            cleanup_task: Arc::new(RwLock::new(None)),
        };
        
        // Start cleanup task
        pool.start_cleanup_task()?;
        
        Ok(pool)
    }
    
    /// Get a connection from the pool
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<PooledConnection> {
        // Try to get an existing connection
        if let Some(connection) = self.try_get_existing_connection().await? {
            return Ok(connection);
        }
        
        // Wait for semaphore permit
        let _permit = self.semaphore.acquire().await
            .map_err(|_| NimbuxError::ConnectionPool("Failed to acquire semaphore".to_string()))?;
        
        // Try to get connection again (in case one was returned while waiting)
        if let Some(connection) = self.try_get_existing_connection().await? {
            return Ok(connection);
        }
        
        // Create new connection
        self.create_new_connection(addr).await
    }
    
    /// Return a connection to the pool
    pub async fn return_connection(&self, mut connection: PooledConnection) -> Result<()> {
        // Update connection metadata
        connection.last_used = Instant::now();
        connection.use_count += 1;
        
        // Check if connection is still healthy
        if !self.is_connection_healthy(&connection).await {
            // Connection is unhealthy, drop it
            self.update_stats_on_connection_drop().await;
            return Ok(());
        }
        
        // Return to pool
        {
            let mut connections = self.connections.write().await;
            connections.push_back(connection);
        }
        
        // Notify waiting threads
        self.notify.notify_one();
        
        Ok(())
    }
    
    /// Try to get an existing connection from the pool
    async fn try_get_existing_connection(&self) -> Result<Option<PooledConnection>> {
        let mut connections = self.connections.write().await;
        
        while let Some(mut connection) = connections.pop_front() {
            // Check if connection is still healthy
            if self.is_connection_healthy(&connection).await {
                connection.last_used = Instant::now();
                self.update_stats_on_connection_acquire().await;
                return Ok(Some(connection));
            } else {
                // Connection is unhealthy, drop it
                self.update_stats_on_connection_drop().await;
            }
        }
        
        Ok(None)
    }
    
    /// Create a new connection
    async fn create_new_connection(&self, addr: SocketAddr) -> Result<PooledConnection> {
        let start_time = Instant::now();
        self.update_stats_on_connection_attempt().await;
        
        // Create connection with timeout
        let connection_result = timeout(
            Duration::from_millis(self.config.connection_timeout),
            AsyncTcpStream::connect(addr)
        ).await;
        
        let stream = match connection_result {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                self.update_stats_on_connection_failure().await;
                return Err(NimbuxError::ConnectionPool(format!("Failed to connect to {}: {}", addr, e)));
            }
            Err(_) => {
                self.update_stats_on_connection_failure().await;
                return Err(NimbuxError::ConnectionPool(format!("Connection timeout to {}", addr)));
            }
        };
        
        // Configure connection
        if let Err(e) = stream.set_nodelay(true) {
            tracing::warn!("Failed to set TCP_NODELAY: {}", e);
        }
        
        let connection_time = start_time.elapsed().as_millis() as f64;
        self.update_stats_on_connection_success(connection_time).await;
        
        Ok(PooledConnection {
            stream,
            created_at: Instant::now(),
            last_used: Instant::now(),
            use_count: 0,
            is_healthy: true,
        })
    }
    
    /// Check if a connection is healthy
    async fn is_connection_healthy(&self, connection: &PooledConnection) -> bool {
        // Check if connection is too old
        let age = connection.created_at.elapsed();
        if age > Duration::from_secs(3600) { // 1 hour max age
            return false;
        }
        
        // Check if connection has been idle too long
        let idle_time = connection.last_used.elapsed();
        if idle_time > Duration::from_millis(self.config.idle_timeout) {
            return false;
        }
        
        // Check if connection has been used too many times
        if connection.use_count > 10000 { // Max uses per connection
            return false;
        }
        
        // In a real implementation, you might also check:
        // - TCP keepalive status
        // - Network connectivity
        // - Server health
        
        true
    }
    
    /// Start cleanup task for idle connections
    fn start_cleanup_task(&self) -> Result<()> {
        let connections = Arc::clone(&self.connections);
        let stats = Arc::clone(&self.stats);
        let config = self.config.clone();
        let notify = Arc::clone(&self.notify);
        
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // Clean up idle connections
                let mut connections = connections.write().await;
                let mut cleaned_count = 0;
                
                while let Some(connection) = connections.pop_front() {
                    let idle_time = connection.last_used.elapsed();
                    if idle_time > Duration::from_millis(config.idle_timeout) {
                        cleaned_count += 1;
                    } else {
                        connections.push_back(connection);
                        break;
                    }
                }
                
                if cleaned_count > 0 {
                    tracing::debug!("Cleaned up {} idle connections", cleaned_count);
                }
                
                // Update stats
                let mut stats = stats.write().await;
                stats.idle_connections = connections.len() as u32;
                stats.pool_utilization = (stats.active_connections as f64 / stats.max_connections as f64) * 100.0;
            }
        });
        
        let mut cleanup_task = self.cleanup_task.write().await;
        *cleanup_task = Some(handle);
        
        Ok(())
    }
    
    /// Update stats on connection attempt
    async fn update_stats_on_connection_attempt(&self) {
        let mut stats = self.stats.write().await;
        stats.connection_attempts += 1;
    }
    
    /// Update stats on connection success
    async fn update_stats_on_connection_success(&self, connection_time: f64) {
        let mut stats = self.stats.write().await;
        stats.successful_connections += 1;
        stats.total_connections += 1;
        stats.active_connections += 1;
        
        // Update average connection time
        let total_connections = stats.successful_connections as f64;
        stats.avg_connection_time = (stats.avg_connection_time * (total_connections - 1.0) + connection_time) / total_connections;
    }
    
    /// Update stats on connection failure
    async fn update_stats_on_connection_failure(&self) {
        let mut stats = self.stats.write().await;
        stats.failed_connections += 1;
        stats.connection_errors += 1;
    }
    
    /// Update stats on connection acquire
    async fn update_stats_on_connection_acquire(&self) {
        let mut stats = self.stats.write().await;
        stats.active_connections += 1;
        stats.idle_connections = stats.idle_connections.saturating_sub(1);
    }
    
    /// Update stats on connection drop
    async fn update_stats_on_connection_drop(&self) {
        let mut stats = self.stats.write().await;
        stats.total_connections = stats.total_connections.saturating_sub(1);
        stats.active_connections = stats.active_connections.saturating_sub(1);
    }
    
    /// Get connection pool statistics
    pub async fn get_stats(&self) -> Result<PoolStats> {
        let stats = self.stats.read().await;
        Ok(stats.clone())
    }
    
    /// Get pool utilization percentage
    pub async fn get_utilization(&self) -> Result<f64> {
        let stats = self.stats.read().await;
        Ok(stats.pool_utilization)
    }
    
    /// Get number of available connections
    pub async fn get_available_connections(&self) -> Result<u32> {
        let connections = self.connections.read().await;
        Ok(connections.len() as u32)
    }
    
    /// Get number of active connections
    pub async fn get_active_connections(&self) -> Result<u32> {
        let stats = self.stats.read().await;
        Ok(stats.active_connections)
    }
    
    /// Clear all connections from the pool
    pub async fn clear(&self) -> Result<()> {
        let mut connections = self.connections.write().await;
        connections.clear();
        
        let mut stats = self.stats.write().await;
        stats.total_connections = 0;
        stats.active_connections = 0;
        stats.idle_connections = 0;
        
        Ok(())
    }
    
    /// Shutdown the connection pool
    pub async fn shutdown(&self) -> Result<()> {
        // Cancel cleanup task
        if let Some(handle) = self.cleanup_task.write().await.take() {
            handle.abort();
        }
        
        // Clear all connections
        self.clear().await?;
        
        Ok(())
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // Connection is being dropped, update stats if needed
        tracing::debug!("Connection dropped after {} uses", self.use_count);
    }
}

/// Connection pool builder for configuration
pub struct ConnectionPoolBuilder {
    config: PoolConfig,
}

impl ConnectionPoolBuilder {
    pub fn new() -> Self {
        Self {
            config: PoolConfig {
                max_connections: 100,
                connection_timeout: 5000,
                idle_timeout: 300000,
            },
        }
    }
    
    pub fn max_connections(mut self, max: u32) -> Self {
        self.config.max_connections = max;
        self
    }
    
    pub fn connection_timeout(mut self, timeout: u64) -> Self {
        self.config.connection_timeout = timeout;
        self
    }
    
    pub fn idle_timeout(mut self, timeout: u64) -> Self {
        self.config.idle_timeout = timeout;
        self
    }
    
    pub fn build(self) -> Result<ConnectionPool> {
        ConnectionPool::new(self.config)
    }
}

impl Default for ConnectionPoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}