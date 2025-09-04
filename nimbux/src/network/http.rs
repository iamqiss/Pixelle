// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// HTTP API module

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{get, post, put, delete},
    Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::errors::{NimbuxError, Result};
use crate::storage::{Object, ObjectMetadata, StorageBackend, StorageStats};

/// HTTP API server for Nimbux
pub struct HttpServer {
    storage: Arc<dyn StorageBackend>,
    port: u16,
}

impl HttpServer {
    /// Create a new HTTP server
    pub fn new(storage: Arc<dyn StorageBackend>, port: u16) -> Self {
        Self { storage, port }
    }
    
    /// Start the HTTP server
    pub async fn start(&self) -> Result<()> {
        let app = self.create_router();
        
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .map_err(|e| NimbuxError::Network(format!("Failed to bind to port {}: {}", self.port, e)))?;
        
        tracing::info!("HTTP server starting on port {}", self.port);
        
        axum::serve(listener, app)
            .await
            .map_err(|e| NimbuxError::Network(format!("HTTP server error: {}", e)))?;
        
        Ok(())
    }
    
    /// Create the API router
    fn create_router(&self) -> Router {
        let storage = Arc::clone(&self.storage);
        
        Router::new()
            .route("/health", get(health_check))
            .route("/objects", post(create_object))
            .route("/objects/:id", get(get_object))
            .route("/objects/:id", put(update_object))
            .route("/objects/:id", delete(delete_object))
            .route("/objects/:id/head", get(head_object))
            .route("/objects", get(list_objects))
            .route("/stats", get(get_stats))
            .with_state(storage)
    }
}

/// Health check endpoint
async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "nimbux",
        "version": "0.1.0"
    }))
}

/// Create object request
#[derive(Debug, Deserialize)]
struct CreateObjectRequest {
    name: String,
    content_type: Option<String>,
    tags: Option<HashMap<String, String>>,
}

/// Create object response
#[derive(Debug, Serialize)]
struct CreateObjectResponse {
    id: String,
    name: String,
    size: u64,
    checksum: String,
    created_at: u64,
}

/// Create a new object
async fn create_object(
    State(storage): State<Arc<dyn StorageBackend>>,
    _headers: HeaderMap,
    Json(payload): Json<CreateObjectRequest>,
) -> Result<(StatusCode, Json<CreateObjectResponse>), axum::response::Response> {
    // Extract data from request body (in a real implementation, you'd read from the body)
    // For now, we'll create a placeholder
    let data = b"Placeholder data".to_vec();
    
    let mut object = Object::new(payload.name.clone(), data, payload.content_type);
    
    // Add tags if provided
    if let Some(tags) = payload.tags {
        for (key, value) in tags {
            object.add_tag(key, value);
        }
    }
    
    let object_id = object.metadata.id.clone();
    let metadata = object.metadata.clone();
    
    match storage.put(object).await {
        Ok(_) => {
            let response = CreateObjectResponse {
                id: object_id,
                name: payload.name,
                size: metadata.size,
                checksum: metadata.checksum,
                created_at: metadata.created_at,
            };
            Ok((StatusCode::CREATED, Json(response)))
        }
        Err(e) => {
            let error_response = serde_json::json!({
                "error": "Failed to create object",
                "message": e.to_string()
            });
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into())
        }
    }
}

/// Get object response
#[derive(Debug, Serialize)]
struct GetObjectResponse {
    metadata: ObjectMetadata,
    data: String, // Base64 encoded
}

/// Get an object by ID
async fn get_object(
    State(storage): State<Arc<dyn StorageBackend>>,
    Path(id): Path<String>,
) -> Result<Json<GetObjectResponse>> {
    let object = storage.get(&id).await?;
    
    let response = GetObjectResponse {
        metadata: object.metadata,
        data: base64::engine::general_purpose::STANDARD.encode(&object.data),
    };
    
    Ok(Json(response))
}

/// Update object request
#[derive(Debug, Deserialize)]
struct UpdateObjectRequest {
    content_type: Option<String>,
    tags: Option<HashMap<String, String>>,
}

/// Update an object
async fn update_object(
    State(storage): State<Arc<dyn StorageBackend>>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateObjectRequest>,
) -> Result<StatusCode> {
    let mut object = storage.get(&id).await?;
    
    // In a real implementation, you'd read the new data from the request body
    // For now, we'll just update metadata
    if let Some(content_type) = payload.content_type {
        object.metadata.content_type = Some(content_type);
    }
    
    if let Some(tags) = payload.tags {
        for (key, value) in tags {
            object.add_tag(key, value);
        }
    }
    
    storage.put(object).await?;
    Ok(StatusCode::OK)
}

/// Delete an object
async fn delete_object(
    State(storage): State<Arc<dyn StorageBackend>>,
    Path(id): Path<String>,
) -> Result<StatusCode> {
    storage.delete(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Get object metadata (HEAD request)
async fn head_object(
    State(storage): State<Arc<dyn StorageBackend>>,
    Path(id): Path<String>,
) -> Result<Json<ObjectMetadata>> {
    let metadata = storage.head(&id).await?;
    Ok(Json(metadata))
}

/// List objects query parameters
#[derive(Debug, Deserialize)]
struct ListObjectsQuery {
    prefix: Option<String>,
    limit: Option<usize>,
}

/// List objects
async fn list_objects(
    State(storage): State<Arc<dyn StorageBackend>>,
    Query(params): Query<ListObjectsQuery>,
) -> Result<Json<Vec<ObjectMetadata>>> {
    let objects = storage.list(params.prefix.as_deref(), params.limit).await?;
    Ok(Json(objects))
}

/// Get storage statistics
async fn get_stats(
    State(storage): State<Arc<dyn StorageBackend>>,
) -> Result<Json<StorageStats>> {
    let stats = storage.stats().await?;
    Ok(Json(stats))
}

/// Error response
#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

/// Convert NimbuxError to HTTP response
impl From<NimbuxError> for (StatusCode, Json<ErrorResponse>) {
    fn from(err: NimbuxError) -> Self {
        let (status, message) = match err {
            NimbuxError::ObjectNotFound { object_id } => {
                (StatusCode::NOT_FOUND, format!("Object not found: {}", object_id))
            }
            NimbuxError::ObjectExists { object_id } => {
                (StatusCode::CONFLICT, format!("Object already exists: {}", object_id))
            }
            NimbuxError::InvalidObjectId { object_id } => {
                (StatusCode::BAD_REQUEST, format!("Invalid object ID: {}", object_id))
            }
            NimbuxError::Authentication(msg) => {
                (StatusCode::UNAUTHORIZED, format!("Authentication error: {}", msg))
            }
            NimbuxError::Authorization(msg) => {
                (StatusCode::FORBIDDEN, format!("Authorization error: {}", msg))
            }
            NimbuxError::Storage(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage error: {}", msg))
            }
            NimbuxError::Network(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Network error: {}", msg))
            }
            _ => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal error: {}", err))
            }
        };
        
        let response = ErrorResponse {
            error: status.to_string(),
            message,
        };
        
        (status, Json(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_check() {
        let storage = Arc::new(MemoryStorage::new());
        let server = HttpServer::new(storage, 8080);
        let app = server.create_router();
        
        let request = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
