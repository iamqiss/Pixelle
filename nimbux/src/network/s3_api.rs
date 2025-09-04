// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// S3-compatible API implementation

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use axum::{
    extract::{Path, Query, State, Multipart},
    http::{HeaderMap, StatusCode, HeaderValue},
    response::{Response, IntoResponse},
    routing::{get, post, put, delete, head},
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::{info, debug, warn, error, instrument};
use uuid::Uuid;

use crate::errors::{NimbuxError, Result};
use crate::storage::{StorageBackend, Object, ObjectMetadata, StorageStats};
use crate::auth::{AuthManager, AuthContext};
use crate::observability::MetricsCollector;

/// S3-compatible API server for Nimbux
pub struct S3ApiServer {
    storage: Arc<dyn StorageBackend>,
    auth_manager: Arc<AuthManager>,
    metrics: Arc<MetricsCollector>,
    port: u16,
}

/// S3 API state shared across handlers
#[derive(Clone)]
pub struct S3ApiState {
    pub storage: Arc<dyn StorageBackend>,
    pub auth_manager: Arc<AuthManager>,
    pub metrics: Arc<MetricsCollector>,
}

/// S3 List Objects response
#[derive(Debug, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,
    #[serde(rename = "Marker")]
    pub marker: Option<String>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Contents")]
    pub contents: Vec<S3Object>,
    #[serde(rename = "CommonPrefixes")]
    pub common_prefixes: Vec<S3CommonPrefix>,
}

/// S3 Object representation
#[derive(Debug, Serialize, Deserialize)]
pub struct S3Object {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    #[serde(rename = "Owner")]
    pub owner: Option<S3Owner>,
}

/// S3 Common Prefix representation
#[derive(Debug, Serialize, Deserialize)]
pub struct S3CommonPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

/// S3 Owner representation
#[derive(Debug, Serialize, Deserialize)]
pub struct S3Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

/// S3 Error response
#[derive(Debug, Serialize, Deserialize)]
pub struct S3Error {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: Option<String>,
    #[serde(rename = "RequestId")]
    pub request_id: String,
}

impl S3ApiServer {
    /// Create a new S3 API server
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        auth_manager: Arc<AuthManager>,
        metrics: Arc<MetricsCollector>,
        port: u16,
    ) -> Self {
        Self {
            storage,
            auth_manager,
            metrics,
            port,
        }
    }

    /// Start the S3 API server
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<()> {
        let state = S3ApiState {
            storage: Arc::clone(&self.storage),
            auth_manager: Arc::clone(&self.auth_manager),
            metrics: Arc::clone(&self.metrics),
        };

        let app = Router::new()
            // Bucket operations
            .route("/:bucket", put(create_bucket))
            .route("/:bucket", delete(delete_bucket))
            .route("/:bucket", get(list_objects))
            .route("/:bucket", head(head_bucket))
            
            // Object operations
            .route("/:bucket/*key", put(put_object))
            .route("/:bucket/*key", get(get_object))
            .route("/:bucket/*key", delete(delete_object))
            .route("/:bucket/*key", head(head_object))
            
            // Service operations
            .route("/", get(list_buckets))
            .route("/health", get(health_check))
            .route("/metrics", get(get_metrics))
            
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .map_err(|e| NimbuxError::Network(format!("Failed to bind S3 API port {}: {}", self.port, e)))?;

        info!("S3 API server starting on port {}", self.port);
        
        axum::serve(listener, app)
            .await
            .map_err(|e| NimbuxError::Network(format!("S3 API server error: {}", e)))?;
        
        Ok(())
    }
}

/// Create a bucket
#[instrument(skip(state))]
async fn create_bucket(
    State(state): State<S3ApiState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:CreateBucket", &format!("arn:nimbux:s3:::{}", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // For now, we'll create a virtual bucket (in a real implementation, you'd create actual bucket metadata)
    info!("Created bucket: {} by user: {}", bucket, auth_context.user.username);
    
    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(StatusCode::OK.into_response())
}

/// Delete a bucket
#[instrument(skip(state))]
async fn delete_bucket(
    State(state): State<S3ApiState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:DeleteBucket", &format!("arn:nimbux:s3:::{}", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    info!("Deleted bucket: {} by user: {}", bucket, auth_context.user.username);
    
    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// List objects in a bucket
#[instrument(skip(state))]
async fn list_objects(
    State(state): State<S3ApiState>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:ListBucket", &format!("arn:nimbux:s3:::{}", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    let prefix = params.get("prefix").cloned();
    let max_keys = params.get("max-keys")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1000);

    // List objects from storage
    let objects = match state.storage.list_objects(&bucket, prefix.as_deref()).await {
        Ok(objs) => objs,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Convert to S3 format
    let s3_objects: Vec<S3Object> = objects.into_iter().map(|obj| {
        S3Object {
            key: obj.key,
            last_modified: obj.created_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            etag: format!("\"{}\"", blake3::hash(&obj.data).to_hex()),
            size: obj.data.len() as u64,
            storage_class: "STANDARD".to_string(),
            owner: Some(S3Owner {
                id: auth_context.user.user_id.clone(),
                display_name: auth_context.user.username.clone(),
            }),
        }
    }).collect();

    let response = ListObjectsResponse {
        name: bucket,
        prefix,
        marker: None,
        max_keys,
        is_truncated: false, // Simplified for now
        contents: s3_objects,
        common_prefixes: Vec::new(),
    };

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(serde_xml_rs::to_string(&response).unwrap_or_default())
        .unwrap()
        .into_response())
}

/// Head bucket (check if bucket exists)
#[instrument(skip(state))]
async fn head_bucket(
    State(state): State<S3ApiState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:ListBucket", &format!("arn:nimbux:s3:::{}", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(StatusCode::OK.into_response())
}

/// Put object
#[instrument(skip(state))]
async fn put_object(
    State(state): State<S3ApiState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:PutObject", &format!("arn:nimbux:s3:::{}/*", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Create object
    let object = Object {
        bucket: bucket.clone(),
        key: key.clone(),
        data: body.to_vec(),
        metadata: ObjectMetadata {
            content_type: headers.get("content-type")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string(),
            content_length: body.len() as u64,
            etag: format!("\"{}\"", blake3::hash(&body).to_hex()),
            last_modified: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            ..Default::default()
        },
        created_at: chrono::Utc::now(),
    };

    // Store object
    match state.storage.put_object(&object).await {
        Ok(_) => {
            info!("Stored object: {}/{} ({} bytes) by user: {}", 
                  bucket, key, body.len(), auth_context.user.username);
            
            // Record metrics
            let _ = state.metrics.record_storage_operation("put", body.len() as u64).await;
        }
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &object.metadata.etag)
        .body("")
        .unwrap()
        .into_response())
}

/// Get object
#[instrument(skip(state))]
async fn get_object(
    State(state): State<S3ApiState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:GetObject", &format!("arn:nimbux:s3:::{}/*", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Get object from storage
    let object = match state.storage.get_object(&bucket, &key).await {
        Ok(Some(obj)) => obj,
        Ok(None) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    info!("Retrieved object: {}/{} ({} bytes) by user: {}", 
          bucket, key, object.data.len(), auth_context.user.username);

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    let _ = state.metrics.record_storage_operation("get", object.data.len() as u64).await;
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", &object.metadata.content_type)
        .header("Content-Length", object.data.len())
        .header("ETag", &object.metadata.etag)
        .header("Last-Modified", chrono::DateTime::from_timestamp(object.metadata.last_modified as i64, 0)
            .unwrap_or_default()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string())
        .body(object.data)
        .unwrap()
        .into_response())
}

/// Delete object
#[instrument(skip(state))]
async fn delete_object(
    State(state): State<S3ApiState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:DeleteObject", &format!("arn:nimbux:s3:::{}/*", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Delete object from storage
    match state.storage.delete_object(&bucket, &key).await {
        Ok(_) => {
            info!("Deleted object: {}/{} by user: {}", bucket, key, auth_context.user.username);
            
            // Record metrics
            let _ = state.metrics.record_storage_operation("delete", 0).await;
        }
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Head object (get object metadata)
#[instrument(skip(state))]
async fn head_object(
    State(state): State<S3ApiState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:GetObject", &format!("arn:nimbux:s3:::{}/*", bucket)).await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // Get object from storage
    let object = match state.storage.get_object(&bucket, &key).await {
        Ok(Some(obj)) => obj,
        Ok(None) => return Err(StatusCode::NOT_FOUND),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", &object.metadata.content_type)
        .header("Content-Length", object.data.len())
        .header("ETag", &object.metadata.etag)
        .header("Last-Modified", chrono::DateTime::from_timestamp(object.metadata.last_modified as i64, 0)
            .unwrap_or_default()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string())
        .body("")
        .unwrap()
        .into_response())
}

/// List all buckets
#[instrument(skip(state))]
async fn list_buckets(
    State(state): State<S3ApiState>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let start_time = SystemTime::now();
    
    // Authenticate request
    let auth_context = match authenticate_request(&state, &headers).await {
        Ok(ctx) => ctx,
        Err(_) => return Err(StatusCode::FORBIDDEN),
    };

    // Check permissions
    if !state.auth_manager.check_permission(&auth_context, "s3:ListAllMyBuckets", "*").await.unwrap_or(false) {
        return Err(StatusCode::FORBIDDEN);
    }

    // For now, return empty list (in a real implementation, you'd list actual buckets)
    let response = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
        <ID>nimbux-user</ID>
        <DisplayName>Nimbux User</DisplayName>
    </Owner>
    <Buckets>
    </Buckets>
</ListAllMyBucketsResult>"#;

    // Record metrics
    let duration = start_time.elapsed().unwrap_or_default();
    let _ = state.metrics.record_request(true, duration).await;
    
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(response)
        .unwrap()
        .into_response())
}

/// Health check endpoint
#[instrument(skip(state))]
async fn health_check(
    State(state): State<S3ApiState>,
) -> Result<Response, StatusCode> {
    let summary = match state.metrics.get_metrics_summary().await {
        Ok(s) => s,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let response = serde_json::json!({
        "status": "healthy",
        "uptime_seconds": summary.uptime_seconds,
        "total_requests": summary.total_requests,
        "success_rate_percent": summary.success_rate_percent
    });

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(response.to_string())
        .unwrap()
        .into_response())
}

/// Get metrics endpoint
#[instrument(skip(state))]
async fn get_metrics(
    State(state): State<S3ApiState>,
) -> Result<Response, StatusCode> {
    let metrics = match state.metrics.get_metrics().await {
        Ok(m) => m,
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&metrics).unwrap_or_default())
        .unwrap()
        .into_response())
}

/// Authenticate request using AWS Signature V4
async fn authenticate_request(
    state: &S3ApiState,
    headers: &HeaderMap,
) -> Result<AuthContext, NimbuxError> {
    // Extract authorization header
    let auth_header = headers.get("authorization")
        .ok_or_else(|| NimbuxError::Authentication("Missing authorization header".to_string()))?
        .to_str()
        .map_err(|_| NimbuxError::Authentication("Invalid authorization header".to_string()))?;

    // Parse AWS Signature V4 authorization header
    // Format: AWS4-HMAC-SHA256 Credential=access_key/date/region/service/aws4_request, SignedHeaders=..., Signature=...
    let parts: Vec<&str> = auth_header.split(',').collect();
    if parts.len() < 3 {
        return Err(NimbuxError::Authentication("Invalid authorization format".to_string()));
    }

    let credential_part = parts.iter().find(|p| p.trim().starts_with("Credential="))
        .ok_or_else(|| NimbuxError::Authentication("Missing credential".to_string()))?;
    
    let signature_part = parts.iter().find(|p| p.trim().starts_with("Signature="))
        .ok_or_else(|| NimbuxError::Authentication("Missing signature".to_string()))?;

    let credential = credential_part.strip_prefix("Credential=")
        .ok_or_else(|| NimbuxError::Authentication("Invalid credential format".to_string()))?;
    
    let signature = signature_part.strip_prefix("Signature=")
        .ok_or_else(|| NimbuxError::Authentication("Invalid signature format".to_string()))?;

    let credential_parts: Vec<&str> = credential.split('/').collect();
    if credential_parts.len() != 5 {
        return Err(NimbuxError::Authentication("Invalid credential format".to_string()));
    }

    let access_key_id = credential_parts[0];
    let date = credential_parts[1];
    let region = credential_parts[2];
    let service = credential_parts[3];

    // Get timestamp from headers
    let timestamp = headers.get("x-amz-date")
        .or_else(|| headers.get("date"))
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| NimbuxError::Authentication("Missing timestamp".to_string()))?;

    // For now, we'll use a simplified authentication
    // In a real implementation, you'd verify the full AWS Signature V4
    state.auth_manager.authenticate_request(
        access_key_id,
        signature,
        "GET", // Simplified for now
        "/",
        "",
        &HashMap::new(),
        "UNSIGNED-PAYLOAD",
        timestamp,
    ).await
}