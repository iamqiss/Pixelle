// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Largetable Database Server - High-Performance NoSQL

mod engine;
mod network;
mod config;
mod observability;

use config::ServerConfig;
use network::async_server::LargetableServer;
use observability::tracing::init_tracing;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize distributed tracing
    init_tracing();
    
    tracing::info!("🚀 Largetable Database Server starting...");
    
    let config = ServerConfig::from_env_and_files().await?;
    let server = LargetableServer::new(config).await?;
    
    tracing::info!("🌐 Server ready - MongoDB compatibility mode enabled");
    server.run().await
}
