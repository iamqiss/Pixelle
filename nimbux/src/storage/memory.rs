// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// In-memory storage backend

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{Object, ObjectMetadata, StorageBackend, StorageStats};
use crate::errors::{NimbuxError, Result};

/// In-memory storage backend for testing and development
pub struct MemoryStorage {
    objects: Arc<RwLock<HashMap<String, Object>>>,
    max_size: Option<u64>,
}

impl MemoryStorage {
    /// Create a new memory storage backend
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            max_size: None,
        }
    }
    
    /// Create a new memory storage backend with size limit
    pub fn with_max_size(max_size: u64) -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            max_size: Some(max_size),
        }
    }
    
    /// Calculate current storage usage
    async fn calculate_usage(&self) -> Result<u64> {
        let objects = self.objects.read().await;
        Ok(objects.values().map(|obj| obj.metadata.size).sum())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn put(&self, object: Object) -> Result<()> {
        // Check size limits if configured
        if let Some(max_size) = self.max_size {
            let current_usage = self.calculate_usage().await?;
            if current_usage + object.metadata.size > max_size {
                return Err(NimbuxError::Storage(
                    format!("Storage limit exceeded: {} + {} > {}", 
                           current_usage, object.metadata.size, max_size)
                ));
            }
        }
        
        let mut objects = self.objects.write().await;
        objects.insert(object.metadata.id.clone(), object);
        Ok(())
    }
    
    async fn get(&self, id: &str) -> Result<Object> {
        let objects = self.objects.read().await;
        objects
            .get(id)
            .cloned()
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })
    }
    
    async fn delete(&self, id: &str) -> Result<()> {
        let mut objects = self.objects.write().await;
        objects
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })
    }
    
    async fn exists(&self, id: &str) -> Result<bool> {
        let objects = self.objects.read().await;
        Ok(objects.contains_key(id))
    }
    
    async fn list(&self, prefix: Option<&str>, limit: Option<usize>) -> Result<Vec<ObjectMetadata>> {
        let objects = self.objects.read().await;
        let mut results: Vec<ObjectMetadata> = objects
            .values()
            .filter(|obj| {
                if let Some(prefix) = prefix {
                    obj.metadata.name.starts_with(prefix)
                } else {
                    true
                }
            })
            .map(|obj| obj.metadata.clone())
            .collect();
        
        // Sort by creation time (newest first)
        results.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        
        // Apply limit if specified
        if let Some(limit) = limit {
            results.truncate(limit);
        }
        
        Ok(results)
    }
    
    async fn head(&self, id: &str) -> Result<ObjectMetadata> {
        let objects = self.objects.read().await;
        objects
            .get(id)
            .map(|obj| obj.metadata.clone())
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })
    }
    
    async fn stats(&self) -> Result<StorageStats> {
        let objects = self.objects.read().await;
        let total_objects = objects.len() as u64;
        let total_size: u64 = objects.values().map(|obj| obj.metadata.size).sum();
        
        Ok(StorageStats {
            total_objects,
            total_size,
            available_space: self.max_size.unwrap_or(u64::MAX) - total_size,
            used_space: total_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();
        
        // Test put and get
        let object = Object::new("test.txt".to_string(), b"Hello, World!".to_vec(), Some("text/plain".to_string()));
        let object_id = object.metadata.id.clone();
        
        storage.put(object).await.unwrap();
        
        let retrieved = storage.get(&object_id).await.unwrap();
        assert_eq!(retrieved.metadata.name, "test.txt");
        assert_eq!(retrieved.data, b"Hello, World!");
        
        // Test exists
        assert!(storage.exists(&object_id).await.unwrap());
        
        // Test head
        let metadata = storage.head(&object_id).await.unwrap();
        assert_eq!(metadata.name, "test.txt");
        
        // Test list
        let objects = storage.list(None, None).await.unwrap();
        assert_eq!(objects.len(), 1);
        
        // Test delete
        storage.delete(&object_id).await.unwrap();
        assert!(!storage.exists(&object_id).await.unwrap());
    }
    
    #[tokio::test]
    async fn test_memory_storage_size_limit() {
        let storage = MemoryStorage::with_max_size(100);
        
        // This should work
        let small_object = Object::new("small.txt".to_string(), vec![0u8; 50], None);
        storage.put(small_object).await.unwrap();
        
        // This should fail due to size limit
        let large_object = Object::new("large.txt".to_string(), vec![0u8; 100], None);
        let result = storage.put(large_object).await;
        assert!(result.is_err());
    }
}
