// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
//! Configuration management

pub mod server;
pub mod cluster;
pub mod security;
pub mod performance;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 27017,
            data_dir: "./data".to_string(),
            log_level: "info".to_string(),
        }
    }
}
