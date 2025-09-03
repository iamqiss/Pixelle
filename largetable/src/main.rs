// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
mod engine;
mod network;
mod config;
mod monitoring;

use config::ServerConfig;
use network::server::LargetableServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Largetable Database Server starting...");
    let config = ServerConfig::default();
    let server = LargetableServer::new(config).await?;
    server.run().await
}
