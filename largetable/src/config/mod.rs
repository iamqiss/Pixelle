// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Configuration management

pub mod server;
pub mod cluster;
pub mod security;
pub mod performance;
pub mod storage_engines;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub log_level: String,
    pub storage_engine: crate::StorageEngine,
    pub max_connections: usize,
    pub thread_pool_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 27017,
            data_dir: "./data".to_string(),
            log_level: "info".to_string(),
            storage_engine: crate::StorageEngine::Lsm,
            max_connections: 10000,
            thread_pool_size: num_cpus::get(),
        }
    }
}

impl ServerConfig {
    pub async fn from_env_and_files() -> crate::Result<Self> {
        // Load configuration from environment variables and config files
        Ok(Self::default())
    }
}
