// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Configuration management

use crate::{Result, LargetableError, StorageEngine};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info};

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server host
    pub host: String,
    /// Server port
    pub port: u16,
    /// Default storage engine
    pub default_storage_engine: StorageEngine,
    /// Data directory
    pub data_dir: String,
    /// Log level
    pub log_level: String,
    /// Maximum connections
    pub max_connections: usize,
    /// Worker threads
    pub worker_threads: usize,
    /// Memory limit in MB
    pub memory_limit_mb: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Enable replication
    pub enable_replication: bool,
    /// Replication factor
    pub replication_factor: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 27017, // MongoDB compatible port
            default_storage_engine: StorageEngine::Lsm,
            data_dir: "./data".to_string(),
            log_level: "info".to_string(),
            max_connections: 1000,
            worker_threads: num_cpus::get(),
            memory_limit_mb: 1024,
            enable_compression: true,
            enable_replication: false,
            replication_factor: 1,
        }
    }
}

impl ServerConfig {
    /// Create configuration from environment variables and config files
    pub async fn from_env_and_files() -> Result<Self> {
        let mut config = Self::default();
        
        // Load from config file if it exists
        if Path::new("largetable.toml").exists() {
            config = Self::from_file("largetable.toml").await?;
        }
        
        // Override with environment variables
        config.apply_env_overrides();
        
        info!("Loaded configuration: {:?}", config);
        Ok(config)
    }

    /// Load configuration from a TOML file
    pub async fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = tokio::fs::read_to_string(path).await
            .map_err(|e| LargetableError::Config(format!("Failed to read config file: {}", e)))?;
        
        let config: Self = toml::from_str(&content)
            .map_err(|e| LargetableError::Config(format!("Failed to parse config file: {}", e)))?;
        
        debug!("Loaded configuration from file");
        Ok(config)
    }

    /// Save configuration to a TOML file
    pub async fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| LargetableError::Config(format!("Failed to serialize config: {}", e)))?;
        
        tokio::fs::write(path, content).await
            .map_err(|e| LargetableError::Config(format!("Failed to write config file: {}", e)))?;
        
        debug!("Saved configuration to file");
        Ok(())
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) {
        if let Ok(host) = std::env::var("LARGETABLE_HOST") {
            self.host = host;
        }
        
        if let Ok(port) = std::env::var("LARGETABLE_PORT") {
            if let Ok(port_num) = port.parse() {
                self.port = port_num;
            }
        }
        
        if let Ok(engine) = std::env::var("LARGETABLE_STORAGE_ENGINE") {
            self.default_storage_engine = match engine.to_lowercase().as_str() {
                "lsm" => StorageEngine::Lsm,
                "btree" => StorageEngine::BTree,
                "columnar" => StorageEngine::Columnar,
                "graph" => StorageEngine::Graph,
                _ => StorageEngine::Lsm,
            };
        }
        
        if let Ok(data_dir) = std::env::var("LARGETABLE_DATA_DIR") {
            self.data_dir = data_dir;
        }
        
        if let Ok(log_level) = std::env::var("LARGETABLE_LOG_LEVEL") {
            self.log_level = log_level;
        }
        
        if let Ok(max_conn) = std::env::var("LARGETABLE_MAX_CONNECTIONS") {
            if let Ok(conn_num) = max_conn.parse() {
                self.max_connections = conn_num;
            }
        }
        
        if let Ok(workers) = std::env::var("LARGETABLE_WORKER_THREADS") {
            if let Ok(worker_num) = workers.parse() {
                self.worker_threads = worker_num;
            }
        }
        
        if let Ok(memory) = std::env::var("LARGETABLE_MEMORY_LIMIT_MB") {
            if let Ok(memory_num) = memory.parse() {
                self.memory_limit_mb = memory_num;
            }
        }
        
        if let Ok(compression) = std::env::var("LARGETABLE_ENABLE_COMPRESSION") {
            self.enable_compression = compression.to_lowercase() == "true";
        }
        
        if let Ok(replication) = std::env::var("LARGETABLE_ENABLE_REPLICATION") {
            self.enable_replication = replication.to_lowercase() == "true";
        }
        
        if let Ok(repl_factor) = std::env::var("LARGETABLE_REPLICATION_FACTOR") {
            if let Ok(factor_num) = repl_factor.parse() {
                self.replication_factor = factor_num;
            }
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 {
            return Err(LargetableError::Config("Port cannot be 0".to_string()));
        }
        
        if self.max_connections == 0 {
            return Err(LargetableError::Config("Max connections cannot be 0".to_string()));
        }
        
        if self.worker_threads == 0 {
            return Err(LargetableError::Config("Worker threads cannot be 0".to_string()));
        }
        
        if self.memory_limit_mb == 0 {
            return Err(LargetableError::Config("Memory limit cannot be 0".to_string()));
        }
        
        if self.enable_replication && self.replication_factor < 2 {
            return Err(LargetableError::Config("Replication factor must be at least 2 when replication is enabled".to_string()));
        }
        
        Ok(())
    }
}