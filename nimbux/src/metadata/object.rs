// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Object metadata struct & methods

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::errors::{NimbuxError, Result};

/// Object metadata with versioning support
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
    pub parent_id: Option<String>, // For versioning
    pub is_latest: bool,
}

/// Object version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub version: u64,
    pub created_at: u64,
    pub size: u64,
    pub checksum: String,
    pub is_latest: bool,
    pub comment: Option<String>,
}

/// Object metadata manager
pub struct ObjectMetadataManager {
    objects: Arc<RwLock<HashMap<String, ObjectMetadata>>>,
    versions: Arc<RwLock<HashMap<String, Vec<ObjectVersion>>>>,
    name_index: Arc<RwLock<HashMap<String, String>>>, // name -> object_id
}

impl ObjectMetadataManager {
    /// Create a new metadata manager
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(HashMap::new())),
            name_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Create new object metadata
    pub async fn create_object(
        &self,
        name: String,
        size: u64,
        content_type: Option<String>,
        checksum: String,
        tags: Option<HashMap<String, String>>,
    ) -> Result<ObjectMetadata> {
        let id = Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Check if object with same name already exists
        {
            let name_index = self.name_index.read().await;
            if name_index.contains_key(&name) {
                return Err(NimbuxError::ObjectExists { object_id: name });
            }
        }
        
        let metadata = ObjectMetadata {
            id: id.clone(),
            name: name.clone(),
            size,
            content_type,
            checksum,
            created_at: now,
            updated_at: now,
            version: 1,
            tags: tags.unwrap_or_default(),
            compression: None,
            parent_id: None,
            is_latest: true,
        };
        
        // Store metadata
        {
            let mut objects = self.objects.write().await;
            objects.insert(id.clone(), metadata.clone());
        }
        
        // Update name index
        {
            let mut name_index = self.name_index.write().await;
            name_index.insert(name, id.clone());
        }
        
        // Create initial version
        {
            let mut versions = self.versions.write().await;
            versions.insert(id.clone(), vec![ObjectVersion {
                version: 1,
                created_at: now,
                size,
                checksum: metadata.checksum.clone(),
                is_latest: true,
                comment: None,
            }]);
        }
        
        Ok(metadata)
    }
    
    /// Get object metadata by ID
    pub async fn get_object(&self, id: &str) -> Result<ObjectMetadata> {
        let objects = self.objects.read().await;
        objects
            .get(id)
            .cloned()
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })
    }
    
    /// Get object metadata by name
    pub async fn get_object_by_name(&self, name: &str) -> Result<ObjectMetadata> {
        let object_id = {
            let name_index = self.name_index.read().await;
            name_index
                .get(name)
                .cloned()
                .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: name.to_string() })?
        };
        
        self.get_object(&object_id).await
    }
    
    /// Update object metadata
    pub async fn update_object(
        &self,
        id: &str,
        size: u64,
        checksum: String,
        content_type: Option<String>,
        tags: Option<HashMap<String, String>>,
        comment: Option<String>,
    ) -> Result<ObjectMetadata> {
        let mut metadata = {
            let mut objects = self.objects.write().await;
            objects
                .get_mut(id)
                .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })?
                .clone()
        };
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Update metadata
        metadata.size = size;
        metadata.checksum = checksum.clone();
        metadata.updated_at = now;
        metadata.version += 1;
        
        if let Some(ct) = content_type {
            metadata.content_type = Some(ct);
        }
        
        if let Some(new_tags) = tags {
            metadata.tags = new_tags;
        }
        
        // Store updated metadata
        {
            let mut objects = self.objects.write().await;
            objects.insert(id.to_string(), metadata.clone());
        }
        
        // Add new version
        {
            let mut versions = self.versions.write().await;
            if let Some(version_list) = versions.get_mut(id) {
                // Mark previous version as not latest
                for version in version_list.iter_mut() {
                    version.is_latest = false;
                }
                
                // Add new version
                version_list.push(ObjectVersion {
                    version: metadata.version,
                    created_at: now,
                    size,
                    checksum,
                    is_latest: true,
                    comment,
                });
            }
        }
        
        Ok(metadata)
    }
    
    /// Delete object and all its versions
    pub async fn delete_object(&self, id: &str) -> Result<()> {
        let metadata = {
            let mut objects = self.objects.write().await;
            objects
                .remove(id)
                .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })?
        };
        
        // Remove from name index
        {
            let mut name_index = self.name_index.write().await;
            name_index.remove(&metadata.name);
        }
        
        // Remove all versions
        {
            let mut versions = self.versions.write().await;
            versions.remove(id);
        }
        
        Ok(())
    }
    
    /// List all objects
    pub async fn list_objects(&self, prefix: Option<&str>, limit: Option<usize>) -> Result<Vec<ObjectMetadata>> {
        let objects = self.objects.read().await;
        let mut results: Vec<ObjectMetadata> = objects
            .values()
            .filter(|obj| {
                if let Some(prefix) = prefix {
                    obj.name.starts_with(prefix)
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        
        // Sort by creation time (newest first)
        results.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        
        // Apply limit if specified
        if let Some(limit) = limit {
            results.truncate(limit);
        }
        
        Ok(results)
    }
    
    /// Get object versions
    pub async fn get_object_versions(&self, id: &str) -> Result<Vec<ObjectVersion>> {
        let versions = self.versions.read().await;
        versions
            .get(id)
            .cloned()
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })
    }
    
    /// Get specific object version
    pub async fn get_object_version(&self, id: &str, version: u64) -> Result<ObjectVersion> {
        let versions = self.versions.read().await;
        let version_list = versions
            .get(id)
            .ok_or_else(|| NimbuxError::ObjectNotFound { object_id: id.to_string() })?;
        
        version_list
            .iter()
            .find(|v| v.version == version)
            .cloned()
            .ok_or_else(|| NimbuxError::ObjectNotFound { 
                object_id: format!("{}:v{}", id, version) 
            })
    }
    
    /// Restore object to specific version
    pub async fn restore_to_version(&self, id: &str, version: u64) -> Result<ObjectMetadata> {
        let target_version = self.get_object_version(id, version).await?;
        
        // Update current metadata to match the target version
        let mut metadata = self.get_object(id).await?;
        metadata.size = target_version.size;
        metadata.checksum = target_version.checksum;
        metadata.version = target_version.version;
        metadata.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Store updated metadata
        {
            let mut objects = self.objects.write().await;
            objects.insert(id.to_string(), metadata.clone());
        }
        
        // Update version flags
        {
            let mut versions = self.versions.write().await;
            if let Some(version_list) = versions.get_mut(id) {
                for v in version_list.iter_mut() {
                    v.is_latest = v.version == version;
                }
            }
        }
        
        Ok(metadata)
    }
    
    /// Get storage statistics
    pub async fn get_stats(&self) -> Result<MetadataStats> {
        let objects = self.objects.read().await;
        let versions = self.versions.read().await;
        
        let total_objects = objects.len() as u64;
        let total_size: u64 = objects.values().map(|obj| obj.size).sum();
        let total_versions: u64 = versions.values().map(|v| v.len() as u64).sum();
        
        Ok(MetadataStats {
            total_objects,
            total_size,
            total_versions,
            average_versions_per_object: if total_objects > 0 {
                total_versions as f64 / total_objects as f64
            } else {
                0.0
            },
        })
    }
}

impl Default for ObjectMetadataManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataStats {
    pub total_objects: u64,
    pub total_size: u64,
    pub total_versions: u64,
    pub average_versions_per_object: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_object_metadata_creation() {
        let manager = ObjectMetadataManager::new();
        
        let metadata = manager
            .create_object(
                "test.txt".to_string(),
                1024,
                Some("text/plain".to_string()),
                "abc123".to_string(),
                None,
            )
            .await
            .unwrap();
        
        assert_eq!(metadata.name, "test.txt");
        assert_eq!(metadata.version, 1);
        assert!(metadata.is_latest);
    }
    
    #[tokio::test]
    async fn test_object_versioning() {
        let manager = ObjectMetadataManager::new();
        
        // Create object
        let metadata = manager
            .create_object(
                "test.txt".to_string(),
                1024,
                Some("text/plain".to_string()),
                "abc123".to_string(),
                None,
            )
            .await
            .unwrap();
        
        // Update object
        let updated = manager
            .update_object(
                &metadata.id,
                2048,
                "def456".to_string(),
                None,
                None,
                Some("Updated content".to_string()),
            )
            .await
            .unwrap();
        
        assert_eq!(updated.version, 2);
        assert_eq!(updated.size, 2048);
        
        // Check versions
        let versions = manager.get_object_versions(&metadata.id).await.unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions[1].is_latest);
        assert!(!versions[0].is_latest);
    }
    
    #[tokio::test]
    async fn test_object_restore() {
        let manager = ObjectMetadataManager::new();
        
        // Create and update object
        let metadata = manager
            .create_object(
                "test.txt".to_string(),
                1024,
                Some("text/plain".to_string()),
                "abc123".to_string(),
                None,
            )
            .await
            .unwrap();
        
        manager
            .update_object(
                &metadata.id,
                2048,
                "def456".to_string(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        
        // Restore to version 1
        let restored = manager
            .restore_to_version(&metadata.id, 1)
            .await
            .unwrap();
        
        assert_eq!(restored.version, 1);
        assert_eq!(restored.size, 1024);
        assert_eq!(restored.checksum, "abc123");
    }
}
