// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Content-addressable storage

use async_trait::async_trait;
use blake3::Hasher;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::{Object, ObjectMetadata, StorageBackend, StorageStats};
use crate::errors::{NimbuxError, Result};

/// Content-addressable storage that deduplicates based on content hash
pub struct ContentAddressableStorage {
    /// Maps content hash to object data
    content_store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    /// Maps object ID to content hash and metadata
    object_index: Arc<RwLock<HashMap<String, (String, ObjectMetadata)>>>,
    /// Maps content hash to reference count for garbage collection
    ref_counts: Arc<RwLock<HashMap<String, u64>>>,
    max_size: Option<u64>,
}

impl ContentAddressableStorage {
    /// Create a new content-addressable storage
    pub fn new() -> Self {
        Self {
            content_store: Arc::new(RwLock::new(HashMap::new())),
            object_index: Arc::new(RwLock::new(HashMap::new())),
            ref_counts: Arc::new(RwLock::new(HashMap::new())),
            max_size: None,
        }
    }
    
    /// Create with size limit
    pub fn with_max_size(max_size: u64) -> Self {
        Self {
            content_store: Arc::new(RwLock::new(HashMap::new())),
            object_index: Arc::new(RwLock::new(HashMap::new())),
            ref_counts: Arc::new(RwLock::new(HashMap::new())),
            max_size: Some(max_size),
        }
    }
    
    /// Calculate content hash for data
    fn calculate_hash(data: &[u8]) -> String {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize().to_hex().to_string()
    }
    
    /// Calculate current storage usage
    async fn calculate_usage(&self) -> Result<u64> {
        let content_store = self.content_store.read().await;
        Ok(content_store.values().map(|data| data.len() as u64).sum())
    }
    
    /// Increment reference count for a content hash
    async fn increment_ref_count(&self, content_hash: &str) {
        let mut ref_counts = self.ref_counts.write().await;
        *ref_counts.entry(content_hash.to_string()).or_insert(0) += 1;
    }
    
    /// Decrement reference count for a content hash
    async fn decrement_ref_count(&self, content_hash: &str) -> Result<()> {
        let mut ref_counts = self.ref_counts.write().await;
        if let Some(count) = ref_counts.get_mut(content_hash) {
            *count -= 1;
            if *count == 0 {
                ref_counts.remove(content_hash);
                // Remove from content store as well
                let mut content_store = self.content_store.write().await;
                content_store.remove(content_hash);
            }
        }
        Ok(())
    }
}

impl Default for ContentAddressableStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for ContentAddressableStorage {
    async fn put(&self, object: Object) -> Result<()> {
        let content_hash = Self::calculate_hash(&object.data);
        
        // Check if this is an update to an existing object
        let existing_hash = {
            let object_index = self.object_index.read().await;
            object_index.get(&object.metadata.id).map(|(hash, _)| hash.clone())
        };
        
        // Check size limits if configured
        if let Some(max_size) = self.max_size {
            let current_usage = self.calculate_usage().await?;
            let additional_size = if existing_hash.is_some() { 0 } else { object.data.len() as u64 };
            if current_usage + additional_size > max_size {
                return Err(NimbuxError::Storage(
                    format!("Storage limit exceeded: {} + {} > {}", 
                           current_usage, additional_size, max_size)
                ));
            }
        }
        
        // Store content if it doesn't exist
        {
            let mut content_store = self.content_store.write().await;
            if !content_store.contains_key(&content_hash) {
                content_store.insert(content_hash.clone(), object.data.clone());
            }
        }
        
        // Update object index
        {
            let mut object_index = self.object_index.write().await;
            object_index.insert(object.metadata.id.clone(), (content_hash.clone(), object.metadata));
        }
        
        // Update reference counts
        if let Some(old_hash) = existing_hash {
            if old_hash != content_hash {
                self.decrement_ref_count(&old_hash).await?;
            }
        }
        self.increment_ref_count(&content_hash).await;
        
        Ok(())
    }
    
    async fn get(&self, id: &str) -> Result<Object> {
        let (content_hash, metadata) = {
            let object_index = self.object_index.read().await;
            let entry = object_index.get(id)
                .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })?;
            (entry.0.clone(), entry.1.clone())
        };
        
        let data = {
            let content_store = self.content_store.read().await;
            content_store.get(&content_hash)
                .ok_or_else(|| NimbuxError::Storage("Content not found in store".to_string()))?
                .clone()
        };
        
        Ok(Object { metadata, data })
    }
    
    async fn delete(&self, id: &str) -> Result<()> {
        let content_hash = {
            let mut object_index = self.object_index.write().await;
            let entry = object_index.remove(id)
                .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })?;
            entry.0
        };
        
        self.decrement_ref_count(&content_hash).await?;
        Ok(())
    }
    
    async fn exists(&self, id: &str) -> Result<bool> {
        let object_index = self.object_index.read().await;
        Ok(object_index.contains_key(id))
    }
    
    async fn list(&self, prefix: Option<&str>, limit: Option<usize>) -> Result<Vec<ObjectMetadata>> {
        let object_index = self.object_index.read().await;
        let mut results: Vec<ObjectMetadata> = object_index
            .values()
            .filter(|(_, metadata)| {
                if let Some(prefix) = prefix {
                    metadata.name.starts_with(prefix)
                } else {
                    true
                }
            })
            .map(|(_, metadata)| metadata.clone())
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
        let object_index = self.object_index.read().await;
        let entry = object_index.get(id)
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })?;
        Ok(entry.1.clone())
    }
    
    async fn stats(&self) -> Result<StorageStats> {
        let object_index = self.object_index.read().await;
        let content_store = self.content_store.read().await;
        
        let total_objects = object_index.len() as u64;
        let total_size: u64 = content_store.values().map(|data| data.len() as u64).sum();
        
        Ok(StorageStats {
            total_objects,
            total_size,
            available_space: self.max_size.unwrap_or(u64::MAX) - total_size,
            used_space: total_size,
        })
    }
}

impl ContentAddressableStorage {
    /// Get deduplication statistics
    pub async fn get_dedup_stats(&self) -> Result<DedupStats> {
        let content_store = self.content_store.read().await;
        let object_index = self.object_index.read().await;
        let ref_counts = self.ref_counts.read().await;
        
        let unique_content_blocks = content_store.len() as u64;
        let total_objects = object_index.len() as u64;
        let total_content_size: u64 = content_store.values().map(|data| data.len() as u64).sum();
        
        let deduplication_ratio = if total_objects > 0 {
            (total_objects as f64 - unique_content_blocks as f64) / total_objects as f64
        } else {
            0.0
        };
        
        Ok(DedupStats {
            unique_content_blocks,
            total_objects,
            total_content_size,
            deduplication_ratio,
            reference_counts: ref_counts.clone(),
        })
    }
}

/// Deduplication statistics
#[derive(Debug, Clone)]
pub struct DedupStats {
    pub unique_content_blocks: u64,
    pub total_objects: u64,
    pub total_content_size: u64,
    pub deduplication_ratio: f64,
    pub reference_counts: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_content_addressable_storage() {
        let storage = ContentAddressableStorage::new();
        
        // Store the same content twice with different IDs
        let data = b"Hello, World!".to_vec();
        let object1 = Object::with_id("obj1".to_string(), "test1.txt".to_string(), data.clone(), None);
        let object2 = Object::with_id("obj2".to_string(), "test2.txt".to_string(), data.clone(), None);
        
        storage.put(object1).await.unwrap();
        storage.put(object2).await.unwrap();
        
        // Both objects should exist
        assert!(storage.exists("obj1").await.unwrap());
        assert!(storage.exists("obj2").await.unwrap());
        
        // Content should be deduplicated
        let stats = storage.get_dedup_stats().await.unwrap();
        assert_eq!(stats.unique_content_blocks, 1);
        assert_eq!(stats.total_objects, 2);
        assert!(stats.deduplication_ratio > 0.0);
        
        // Delete one object
        storage.delete("obj1").await.unwrap();
        assert!(!storage.exists("obj1").await.unwrap());
        assert!(storage.exists("obj2").await.unwrap());
        
        // Content should still exist due to reference counting
        let stats = storage.get_dedup_stats().await.unwrap();
        assert_eq!(stats.unique_content_blocks, 1);
        assert_eq!(stats.total_objects, 1);
        
        // Delete the second object
        storage.delete("obj2").await.unwrap();
        assert!(!storage.exists("obj2").await.unwrap());
        
        // Content should be garbage collected
        let stats = storage.get_dedup_stats().await.unwrap();
        assert_eq!(stats.unique_content_blocks, 0);
        assert_eq!(stats.total_objects, 0);
    }
}
