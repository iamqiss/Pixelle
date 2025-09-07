// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Simple HTTP API server

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde_json::json;
use std::sync::Arc;

use crate::errors::Result;
use crate::storage::StorageBackend;

/// Simple HTTP server for Nimbux
pub struct SimpleHttpServer {
    storage: Arc<dyn StorageBackend>,
    port: u16,
}

impl SimpleHttpServer {
    /// Create a new simple HTTP server
    pub fn new(storage: Arc<dyn StorageBackend>, port: u16) -> Self {
        Self { storage, port }
    }
    
    /// Start the HTTP server
    pub async fn start(&self) -> Result<()> {
        let app = self.create_router();
        
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .map_err(|e| crate::errors::NimbuxError::Network(format!("Failed to bind to port {}: {}", self.port, e)))?;
        
        tracing::info!("Simple HTTP server starting on port {}", self.port);
        
        axum::serve(listener, app)
            .await
            .map_err(|e| crate::errors::NimbuxError::Network(format!("HTTP server error: {}", e)))?;
        
        Ok(())
    }
    
    /// Create the API router
    fn create_router(&self) -> Router {
        let storage = Arc::clone(&self.storage);
        
        Router::new()
            .route("/health", get(health_check))
            .route("/stats", get(get_stats))
            .with_state(storage)
    }
}

/// Health check endpoint
async fn health_check() -> Json<serde_json::Value> {
    Json(json!({
        "status": "healthy",
        "service": "nimbux",
        "version": "0.1.0"
    }))
}

/// Get storage statistics
async fn get_stats(State(storage): State<Arc<dyn StorageBackend>>) -> std::result::Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    match storage.stats().await {
        Ok(stats) => Ok(Json(json!({
            "total_objects": stats.total_objects,
            "total_size": stats.total_size,
            "available_space": stats.available_space,
            "used_space": stats.used_space
        }))),
        Err(e) => {
            let error_response = json!({
                "error": "Failed to get stats",
                "message": e.to_string()
            });
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
    }
}