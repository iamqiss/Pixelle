// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
//! Largetable server implementation

use crate::{Result, config::ServerConfig};
use tokio::net::TcpListener;

pub struct LargetableServer {
    config: ServerConfig,
}

impl LargetableServer {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        Ok(Self { config })
    }
    
    pub async fn run(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        
        println!("🌐 Largetable server listening on {}", addr);
        
        loop {
            let (socket, addr) = listener.accept().await?;
            println!("📡 New connection from {}", addr);
            
            tokio::spawn(async move {
                // Handle connection
            });
        }
    }
}
