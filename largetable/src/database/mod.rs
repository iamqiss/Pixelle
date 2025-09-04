// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Core database functionality

pub mod admin;
pub mod catalog;
pub mod migrations;
pub mod namespace;

use crate::{Result, DocumentId, Document, StorageEngine, CollectionName, DatabaseName};
use crate::storage::engines::create_storage_engine;
use crate::storage::StorageEngine as StorageEngineTrait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Main database instance
pub struct Database {
    name: DatabaseName,
    storage_engine: Arc<dyn StorageEngineTrait>,
    collections: Arc<RwLock<HashMap<CollectionName, Arc<Collection>>>>,
}

/// Collection within a database
pub struct Collection {
    name: CollectionName,
    database: DatabaseName,
    storage_engine: Arc<dyn StorageEngineTrait>,
    indexes: Arc<RwLock<HashMap<String, crate::IndexType>>>,
}

impl Database {
    /// Create a new database with specified storage engine
    pub fn new(name: DatabaseName, storage_engine: crate::StorageEngine) -> Result<Self> {
        let engine = create_storage_engine(storage_engine)?;
        
        info!("Created database '{}' with {:?} storage engine", name, storage_engine);
        
        Ok(Self {
            name,
            storage_engine: Arc::new(engine),
            collections: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get or create a collection
    pub async fn collection(&self, name: CollectionName) -> Result<Arc<Collection>> {
        let mut collections = self.collections.write().await;
        
        if let Some(collection) = collections.get(&name) {
            return Ok(collection.clone());
        }
        
        let collection = Arc::new(Collection::new(
            name.clone(),
            self.name.clone(),
            self.storage_engine.clone(),
        ));
        
        collections.insert(name, collection.clone());
        debug!("Created collection '{}' in database '{}'", collection.name, self.name);
        
        Ok(collection)
    }

    /// List all collections in the database
    pub async fn list_collections(&self) -> Result<Vec<CollectionName>> {
        let collections = self.collections.read().await;
        Ok(collections.keys().cloned().collect())
    }

    /// Drop a collection
    pub async fn drop_collection(&self, name: &CollectionName) -> Result<bool> {
        let mut collections = self.collections.write().await;
        let removed = collections.remove(name).is_some();
        
        if removed {
            debug!("Dropped collection '{}' from database '{}'", name, self.name);
        }
        
        Ok(removed)
    }

    /// Get database name
    pub fn name(&self) -> &DatabaseName {
        &self.name
    }
}

impl Collection {
    /// Create a new collection
    pub fn new(
        name: CollectionName,
        database: DatabaseName,
        storage_engine: Arc<dyn StorageEngineTrait>,
    ) -> Self {
        Self {
            name,
            database,
            storage_engine,
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert a document into the collection
    pub async fn insert(&self, mut document: Document) -> Result<DocumentId> {
        let id = if document.id == uuid::Uuid::nil() {
            uuid::Uuid::now_v7() // Generate timestamp-ordered UUID
        } else {
            document.id
        };
        
        let now = chrono::Utc::now().timestamp_micros();
        document.id = id;
        document.created_at = now;
        document.updated_at = now;
        document.version = 1;
        
        self.storage_engine.put(id, document).await?;
        
        debug!("Inserted document with ID: {} into collection '{}'", id, self.name);
        Ok(id)
    }

    /// Find a document by ID
    pub async fn find_by_id(&self, id: &DocumentId) -> Result<Option<Document>> {
        self.storage_engine.get(id).await
    }

    /// Update a document by ID
    pub async fn update_by_id(&self, id: &DocumentId, mut document: Document) -> Result<Option<Document>> {
        // Get existing document to preserve metadata
        if let Some(mut existing) = self.storage_engine.get(id).await? {
            let now = chrono::Utc::now().timestamp_micros();
            
            // Preserve creation time and increment version
            document.id = existing.id;
            document.created_at = existing.created_at;
            document.updated_at = now;
            document.version = existing.version + 1;
            
            self.storage_engine.put(*id, document.clone()).await?;
            
            debug!("Updated document with ID: {} in collection '{}'", id, self.name);
            Ok(Some(document))
        } else {
            Ok(None)
        }
    }

    /// Delete a document by ID
    pub async fn delete_by_id(&self, id: &DocumentId) -> Result<bool> {
        let result = self.storage_engine.delete(id).await?;
        
        if result {
            debug!("Deleted document with ID: {} from collection '{}'", id, self.name);
        }
        
        Ok(result)
    }

    /// Find multiple documents with pagination
    pub async fn find_many(
        &self,
        start: Option<DocumentId>,
        limit: usize,
    ) -> Result<Vec<(DocumentId, Document)>> {
        self.storage_engine.scan(start, limit).await
    }

    /// Count documents in the collection
    pub async fn count(&self) -> Result<usize> {
        let documents = self.storage_engine.scan(None, usize::MAX).await?;
        Ok(documents.len())
    }

    /// Create an index on the collection
    pub async fn create_index(&self, field: String, index_type: crate::IndexType) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        indexes.insert(field, index_type);
        
        debug!("Created index on field '{}' for collection '{}'", field, self.name);
        Ok(())
    }

    /// List all indexes on the collection
    pub async fn list_indexes(&self) -> Result<HashMap<String, crate::IndexType>> {
        let indexes = self.indexes.read().await;
        Ok(indexes.clone())
    }

    /// Get collection name
    pub fn name(&self) -> &CollectionName {
        &self.name
    }

    /// Get database name
    pub fn database(&self) -> &DatabaseName {
        &self.database
    }
}