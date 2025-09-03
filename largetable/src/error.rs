// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

use thiserror::Error;

/// Comprehensive error types for all Largetable operations
#[derive(Error, Debug)]
pub enum LargetableError {
    #[error("Storage engine error: {0}")]
    Storage(String),
    
    #[error("Query execution error: {0}")]
    Query(String),
    
    #[error("Index operation error: {0}")]
    Index(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Authentication error: {0}")]
    Auth(String),
    
    #[error("Replication error: {0}")]
    Replication(String),
    
    #[error("Sharding error: {0}")]
    Sharding(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),
    
    #[error("Concurrent access violation: {0}")]
    ConcurrencyViolation(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("BSON error: {0}")]
    Bson(#[from] bson::ser::Error),
}

pub type Result<T> = std::result::Result<T, LargetableError>;

// === RESULT EXTENSIONS FOR TRACING ===
pub trait ResultExt<T> {
    fn trace_err(self) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn trace_err(self) -> Result<T> {
        if let Err(ref e) = self {
            tracing::error!(error = %e, "Operation failed");
        }
        self
    }
}
