// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Storage engine interface

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::errors::{NimbuxError, Result};

pub mod block;
pub mod compression;
pub mod content_addressable;
pub mod disk;
pub mod memory;
pub mod advanced;
pub mod ai_compression;
pub mod integrity;

// Re-export commonly used types
pub use memory::MemoryStorage;
pub use content_addressable::ContentAddressableStorage;
pub use advanced::{AdvancedStorageBackend, AdvancedObject, AdvancedObjectMetadata, VersioningManager, LifecycleManager, ReplicationManager, EncryptionManager};
pub use ai_compression::{CompressionManager, AICompressionAnalyzer, CompressionAlgorithm, CompressionConfig, CompressionResult};
pub use integrity::{IntegrityManager, IntegrityConfig, ChecksumAlgorithm, IntegrityReport, IntegrityStats};

/// Object metadata stored alongside the data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub checksum: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u64,
    pub tags: HashMap<String, String>,
    pub compression: Option<String>,
}

/// Object data with metadata
#[derive(Debug, Clone)]
pub struct Object {
    pub metadata: ObjectMetadata,
    pub data: Vec<u8>,
}

/// Storage backend trait for pluggable storage implementations
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store an object
    async fn put(&self, object: Object) -> Result<()>;
    
    /// Retrieve an object by ID
    async fn get(&self, id: &str) -> Result<Object>;
    
    /// Delete an object by ID
    async fn delete(&self, id: &str) -> Result<()>;
    
    /// Check if an object exists
    async fn exists(&self, id: &str) -> Result<bool>;
    
    /// List objects with optional filtering
    async fn list(&self, prefix: Option<&str>, limit: Option<usize>) -> Result<Vec<ObjectMetadata>>;
    
    /// Get object metadata without data
    async fn head(&self, id: &str) -> Result<ObjectMetadata>;
    
    /// Get storage statistics
    async fn stats(&self) -> Result<StorageStats>;
}

/// Storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub total_objects: u64,
    pub total_size: u64,
    pub available_space: u64,
    pub used_space: u64,
}

/// Storage engine that manages multiple backends
pub struct StorageEngine {
    backends: HashMap<String, Box<dyn StorageBackend>>,
    default_backend: String,
}

impl StorageEngine {
    pub fn new(default_backend: String) -> Self {
        Self {
            backends: HashMap::new(),
            default_backend,
        }
    }
    
    /// Add a storage backend
    pub fn add_backend(&mut self, name: String, backend: Box<dyn StorageBackend>) {
        self.backends.insert(name, backend);
    }
    
    /// Get the default backend
    fn get_default_backend(&self) -> Result<&dyn StorageBackend> {
        self.backends
            .get(&self.default_backend)
            .map(|b| b.as_ref())
            .ok_or_else(|| NimbuxError::Storage(format!("Default backend '{}' not found", self.default_backend)))
    }
    
    /// Get a specific backend
    fn get_backend(&self, name: &str) -> Result<&dyn StorageBackend> {
        self.backends
            .get(name)
            .map(|b| b.as_ref())
            .ok_or_else(|| NimbuxError::Storage(format!("Backend '{}' not found", name)))
    }
}

#[async_trait]
impl StorageBackend for StorageEngine {
    async fn put(&self, object: Object) -> Result<()> {
        self.get_default_backend()?.put(object).await
    }
    
    async fn get(&self, id: &str) -> Result<Object> {
        self.get_default_backend()?.get(id).await
    }
    
    async fn delete(&self, id: &str) -> Result<()> {
        self.get_default_backend()?.delete(id).await
    }
    
    async fn exists(&self, id: &str) -> Result<bool> {
        self.get_default_backend()?.exists(id).await
    }
    
    async fn list(&self, prefix: Option<&str>, limit: Option<usize>) -> Result<Vec<ObjectMetadata>> {
        self.get_default_backend()?.list(prefix, limit).await
    }
    
    async fn head(&self, id: &str) -> Result<ObjectMetadata> {
        self.get_default_backend()?.head(id).await
    }
    
    async fn stats(&self) -> Result<StorageStats> {
        self.get_default_backend()?.stats().await
    }
}

/// Helper functions for object management
impl Object {
    /// Create a new object with auto-generated ID
    pub fn new(name: String, data: Vec<u8>, content_type: Option<String>) -> Self {
        let id = Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            metadata: ObjectMetadata {
                id: id.clone(),
                name,
                size: data.len() as u64,
                content_type,
                checksum: blake3::hash(&data).to_hex().to_string(),
                created_at: now,
                updated_at: now,
                version: 1,
                tags: HashMap::new(),
                compression: None,
            },
            data,
        }
    }
    
    /// Create an object with a specific ID
    pub fn with_id(id: String, name: String, data: Vec<u8>, content_type: Option<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            metadata: ObjectMetadata {
                id,
                name,
                size: data.len() as u64,
                content_type,
                checksum: blake3::hash(&data).to_hex().to_string(),
                created_at: now,
                updated_at: now,
                version: 1,
                tags: HashMap::new(),
                compression: None,
            },
            data,
        }
    }
    
    /// Update object data and metadata
    pub fn update(&mut self, data: Vec<u8>, content_type: Option<String>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        self.data = data;
        self.metadata.size = self.data.len() as u64;
        self.metadata.checksum = blake3::hash(&self.data).to_hex().to_string();
        self.metadata.updated_at = now;
        self.metadata.version += 1;
        
        if let Some(ct) = content_type {
            self.metadata.content_type = Some(ct);
        }
    }
    
    /// Add a tag to the object
    pub fn add_tag(&mut self, key: String, value: String) {
        self.metadata.tags.insert(key, value);
    }
    
    /// Remove a tag from the object
    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        self.metadata.tags.remove(key)
    }
}
