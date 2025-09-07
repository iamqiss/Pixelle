// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Custom error types

use thiserror::Error;

/// Core error types for Nimbux operations
#[derive(Error, Debug)]
pub enum NimbuxError {
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Authentication error: {0}")]
    Authentication(String),
    
    #[error("Authorization error: {0}")]
    Authorization(String),
    
    #[error("Object not found: {object_id}")]
    ObjectNotFound { object_id: String },
    
    #[error("Object already exists: {object_id}")]
    ObjectExists { object_id: String },
    
    #[error("Invalid object ID: {object_id}")]
    InvalidObjectId { object_id: String },
    
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    
    #[error("Compression error: {0}")]
    Compression(String),
    
    #[error("Decompression error: {0}")]
    Decompression(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for Nimbux operations
pub type Result<T> = std::result::Result<T, NimbuxError>;

/// Convert anyhow::Error to NimbuxError
impl From<anyhow::Error> for NimbuxError {
    fn from(err: anyhow::Error) -> Self {
        NimbuxError::Internal(err.to_string())
    }
}
