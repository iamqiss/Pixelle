// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================

use std::sync::Arc;
use tracing_subscriber;

use nimbux::errors::Result;
use nimbux::storage::{MemoryStorage, ContentAddressableStorage, StorageEngine};
use nimbux::network::SimpleHttpServer;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    tracing::info!("Starting Nimbux server...");
    
    // Create storage backends
    let _memory_storage = Arc::new(MemoryStorage::new());
    let _content_storage = Arc::new(ContentAddressableStorage::new());
    
    // Create storage engine with content-addressable storage as default
    let mut storage_engine = StorageEngine::new("content".to_string());
    storage_engine.add_backend("memory".to_string(), Box::new(MemoryStorage::new()));
    storage_engine.add_backend("content".to_string(), Box::new(ContentAddressableStorage::new()));
    
    // Create HTTP server
    let http_server = SimpleHttpServer::new(Arc::new(storage_engine), 8080);
    
    // Start the server
    tracing::info!("Nimbux server ready on http://localhost:8080");
    tracing::info!("API endpoints:");
    tracing::info!("  GET  /health - Health check");
    tracing::info!("  GET  /stats - Get storage statistics");
    
    http_server.start().await?;
    
    Ok(())
}
