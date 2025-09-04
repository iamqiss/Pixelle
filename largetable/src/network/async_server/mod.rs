// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Async HTTP server implementation

use crate::{Result, LargetableError};
use crate::config::ServerConfig;
use crate::engine::DatabaseEngine;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Largetable HTTP server
pub struct LargetableServer {
    config: ServerConfig,
    engine: Arc<DatabaseEngine>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

impl LargetableServer {
    /// Create a new server
    pub async fn new(config: ServerConfig) -> Result<Self> {
        config.validate()?;
        
        let engine = Arc::new(DatabaseEngine::with_default_storage_engine(
            config.default_storage_engine.clone(),
        )?);
        
        info!("Created Largetable server on {}:{}", config.host, config.port);
        
        Ok(Self { config, engine })
    }

    /// Run the server
    pub async fn run(self) -> Result<()> {
        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/stats", get(stats_handler))
            .route("/databases", get(list_databases_handler))
            .route("/databases/:db", post(create_database_handler))
            .route("/databases/:db/collections", get(list_collections_handler))
            .route("/databases/:db/collections/:collection", post(create_collection_handler))
            .route("/databases/:db/collections/:collection/documents", post(insert_document_handler))
            .route("/databases/:db/collections/:collection/documents/:id", get(find_document_handler))
            .route("/databases/:db/collections/:collection/query", post(query_handler))
            .with_state(self.engine);

        let listener = tokio::net::TcpListener::bind(format!("{}:{}", self.config.host, self.config.port))
            .await
            .map_err(|e| LargetableError::Network(format!("Failed to bind to address: {}", e)))?;

        info!("🚀 Largetable server running on {}:{}", self.config.host, self.config.port);
        
        axum::serve(listener, app)
            .await
            .map_err(|e| LargetableError::Network(format!("Server error: {}", e)))?;

        Ok(())
    }
}

async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime: 0, // TODO: Implement actual uptime tracking
    })
}

async fn stats_handler(State(engine): State<Arc<DatabaseEngine>>) -> Result<Json<serde_json::Value>, StatusCode> {
    match engine.get_stats().await {
        Ok(stats) => Ok(Json(serde_json::to_value(stats).unwrap())),
        Err(e) => {
            error!("Failed to get stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn list_databases_handler(State(engine): State<Arc<DatabaseEngine>>) -> Result<Json<Vec<String>>, StatusCode> {
    match engine.list_databases().await {
        Ok(databases) => Ok(Json(databases)),
        Err(e) => {
            error!("Failed to list databases: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn create_database_handler(
    State(engine): State<Arc<DatabaseEngine>>,
    Path(db): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match engine.database(db.clone()).await {
        Ok(_) => Ok(Json(serde_json::json!({"status": "created", "database": db}))),
        Err(e) => {
            error!("Failed to create database {}: {}", db, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn list_collections_handler(
    State(engine): State<Arc<DatabaseEngine>>,
    Path(db): Path<String>,
) -> Result<Json<Vec<String>>, StatusCode> {
    match engine.database(db).await {
        Ok(database) => match database.list_collections().await {
            Ok(collections) => Ok(Json(collections)),
            Err(e) => {
                error!("Failed to list collections: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
        Err(e) => {
            error!("Failed to get database: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn create_collection_handler(
    State(engine): State<Arc<DatabaseEngine>>,
    Path((db, collection)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match engine.collection(db, collection.clone()).await {
        Ok(_) => Ok(Json(serde_json::json!({"status": "created", "collection": collection}))),
        Err(e) => {
            error!("Failed to create collection {}: {}", collection, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn insert_document_handler(
    State(engine): State<Arc<DatabaseEngine>>,
    Path((db, collection)): Path<(String, String)>,
    Json(document): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match crate::document::DocumentUtils::from_json(document) {
        Ok(doc) => match engine.insert_document(db, collection, doc).await {
            Ok(id) => Ok(Json(serde_json::json!({"id": id.to_string()}))),
            Err(e) => {
                error!("Failed to insert document: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
        Err(e) => {
            error!("Failed to parse document: {}", e);
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

async fn find_document_handler(
    State(engine): State<Arc<DatabaseEngine>>,
    Path((db, collection, id)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match uuid::Uuid::parse_str(&id) {
        Ok(doc_id) => match engine.find_document_by_id(db, collection, doc_id).await {
            Ok(Some(doc)) => match crate::document::DocumentUtils::to_json(&doc) {
                Ok(json) => Ok(Json(json)),
                Err(e) => {
                    error!("Failed to serialize document: {}", e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            },
            Ok(None) => Err(StatusCode::NOT_FOUND),
            Err(e) => {
                error!("Failed to find document: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
        Err(e) => {
            error!("Invalid document ID: {}", e);
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

async fn query_handler(
    State(engine): State<Arc<DatabaseEngine>>,
    Path((db, collection)): Path<(String, String)>,
    Json(query): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Simple query implementation - in a real system, this would parse the query
    // and convert it to the appropriate Query type
    let largetable_query = crate::query::Query::new();
    
    match engine.query(db, collection, largetable_query).await {
        Ok(result) => {
            let documents: Vec<serde_json::Value> = result.documents
                .into_iter()
                .filter_map(|(_, doc)| crate::document::DocumentUtils::to_json(&doc).ok())
                .collect();
            
            Ok(Json(serde_json::json!({
                "documents": documents,
                "total_count": result.total_count,
                "has_more": result.has_more
            })))
        }
        Err(e) => {
            error!("Failed to execute query: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}