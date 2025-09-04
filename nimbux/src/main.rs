// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================

use std::sync::Arc;
use tracing_subscriber;

use nimbux::errors::Result;
use nimbux::storage::{MemoryStorage, ContentAddressableStorage, StorageEngine};
use nimbux::network::{SimpleHttpServer, TcpServer, NimbuxApiServer};
use nimbux::auth::AuthManager;
use nimbux::observability::MetricsCollector;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    tracing::info!("Starting Nimbux server...");
    
    // Create storage backends
    let memory_storage = Arc::new(MemoryStorage::new());
    let content_storage = Arc::new(ContentAddressableStorage::new());
    
    // Create storage engine with content-addressable storage as default
    let mut storage_engine = StorageEngine::new("content".to_string());
    storage_engine.add_backend("memory".to_string(), Box::new(MemoryStorage::new()));
    storage_engine.add_backend("content".to_string(), Box::new(ContentAddressableStorage::new()));
    
    let storage = Arc::new(storage_engine);
    
    // Create authentication manager
    let auth_manager = Arc::new(AuthManager::new());
    
    // Create a default admin user for testing
    let admin_user = auth_manager.create_user(
        "admin".to_string(),
        "admin@nimbux.local".to_string(),
    ).await?;
    
    let admin_key = auth_manager.create_access_key(&admin_user.user_id).await?;
    
    // Add admin policy
    let admin_policy = nimbux::auth::PolicyDocument {
        version: "2012-10-17".to_string(),
        statement: vec![
            nimbux::auth::PolicyStatement {
                effect: "Allow".to_string(),
                action: vec!["*".to_string()],
                resource: vec!["*".to_string()],
                condition: None,
            }
        ],
    };
    
    auth_manager.add_policy(&admin_user.user_id, admin_policy).await?;
    
    tracing::info!("Created admin user with access key: {}", admin_key.access_key_id);
    tracing::info!("Admin secret key: {}", admin_key.secret_access_key);
    
    // Create metrics collector
    let metrics = Arc::new(MetricsCollector::new());
    
    // Create servers
    let http_server = SimpleHttpServer::new(Arc::clone(&storage), 8080);
    let tcp_server = TcpServer::new(Arc::clone(&storage), 8081)
        .with_max_connections(1000);
    let nimbux_api_server = NimbuxApiServer::new(
        Arc::clone(&storage),
        Arc::clone(&auth_manager),
        Arc::clone(&metrics),
        8082,
    );
    
    // Start all servers concurrently
    tracing::info!("Nimbux server ready!");
    tracing::info!("Servers:");
    tracing::info!("  HTTP API: http://localhost:8080");
    tracing::info!("  TCP Protocol: tcp://localhost:8081");
    tracing::info!("  Nimbux API: http://localhost:8082");
    tracing::info!("");
    tracing::info!("API endpoints:");
    tracing::info!("  GET  /health - Health check");
    tracing::info!("  GET  /stats - Get storage statistics");
    tracing::info!("  GET  /metrics - Get detailed metrics");
    tracing::info!("");
    tracing::info!("Nimbux API endpoints (NO S3 COMPATIBILITY):");
    tracing::info!("  GET  /api/v1/buckets - List buckets");
    tracing::info!("  POST /api/v1/buckets - Create bucket");
    tracing::info!("  GET  /api/v1/buckets/:bucket - Get bucket details");
    tracing::info!("  PUT  /api/v1/buckets/:bucket - Update bucket");
    tracing::info!("  DEL  /api/v1/buckets/:bucket - Delete bucket");
    tracing::info!("  GET  /api/v1/buckets/:bucket/objects - List objects");
    tracing::info!("  POST /api/v1/buckets/:bucket/objects - Upload object");
    tracing::info!("  GET  /api/v1/buckets/:bucket/objects/:key - Get object");
    tracing::info!("  PUT  /api/v1/buckets/:bucket/objects/:key - Update object");
    tracing::info!("  DEL  /api/v1/buckets/:bucket/objects/:key - Delete object");
    tracing::info!("  POST /api/v1/search - Search objects");
    tracing::info!("  POST /api/v1/batch - Batch operations");
    tracing::info!("  GET  /api/v1/analytics - Analytics dashboard");
    tracing::info!("");
    tracing::info!("Authentication:");
    tracing::info!("  Use JWT tokens or custom Nimbux authentication");
    
    // Start all servers concurrently
    tokio::try_join!(
        http_server.start(),
        tcp_server.start(),
        nimbux_api_server.start()
    )?;
    
    Ok(())
}
