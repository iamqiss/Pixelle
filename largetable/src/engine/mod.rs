// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Core database engine

pub mod async_runtime;
pub mod memory_pool;
pub mod zero_copy;
pub mod transaction;
pub mod mvcc;
pub mod recovery;

use crate::{Result, DatabaseName, CollectionName, StorageEngine, DocumentId, Document};
use crate::database::Database;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Main database engine that manages multiple databases
pub struct DatabaseEngine {
    databases: Arc<RwLock<HashMap<DatabaseName, Arc<Database>>>>,
    default_storage_engine: StorageEngine,
}

impl DatabaseEngine {
    /// Create a new database engine
    pub fn new() -> Result<Self> {
        Self::with_default_storage_engine(StorageEngine::Lsm)
    }

    /// Create a new database engine with specified default storage engine
    pub fn with_default_storage_engine(default_storage_engine: StorageEngine) -> Result<Self> {
        info!("Initialized Largetable Database Engine with {:?} storage engine", default_storage_engine);
        
        Ok(Self {
            databases: Arc::new(RwLock::new(HashMap::new())),
            default_storage_engine,
        })
    }

    /// Get or create a database
    pub async fn database(&self, name: DatabaseName) -> Result<Arc<Database>> {
        let mut databases = self.databases.write().await;
        
        if let Some(database) = databases.get(&name) {
            return Ok(database.clone());
        }
        
        let database = Arc::new(Database::new(name.clone(), self.default_storage_engine)?);
        databases.insert(name, database.clone());
        
        debug!("Created database: {}", name);
        Ok(database)
    }

    /// List all databases
    pub async fn list_databases(&self) -> Result<Vec<DatabaseName>> {
        let databases = self.databases.read().await;
        Ok(databases.keys().cloned().collect())
    }

    /// Drop a database
    pub async fn drop_database(&self, name: &DatabaseName) -> Result<bool> {
        let mut databases = self.databases.write().await;
        let removed = databases.remove(name).is_some();
        
        if removed {
            debug!("Dropped database: {}", name);
        }
        
        Ok(removed)
    }

    /// Get a collection from a database
    pub async fn collection(&self, database_name: DatabaseName, collection_name: CollectionName) -> Result<Arc<crate::database::Collection>> {
        let database = self.database(database_name).await?;
        database.collection(collection_name).await
    }

    /// Execute a query on a collection
    pub async fn query(
        &self,
        database_name: DatabaseName,
        collection_name: CollectionName,
        query: crate::query::Query,
    ) -> Result<crate::query::QueryResult> {
        let collection = self.collection(database_name, collection_name).await?;
        
        // Get all documents from the collection
        let documents = collection.find_many(None, usize::MAX).await?;
        
        // Execute the query
        query.execute(documents).await
    }

    /// Execute an aggregation pipeline on a collection
    pub async fn aggregate(
        &self,
        database_name: DatabaseName,
        collection_name: CollectionName,
        pipeline: crate::query::AggregationPipeline,
    ) -> Result<Vec<serde_json::Value>> {
        let collection = self.collection(database_name, collection_name).await?;
        
        // Get all documents from the collection
        let documents = collection.find_many(None, usize::MAX).await?;
        
        // Execute the aggregation pipeline
        pipeline.execute(documents).await
    }

    /// Insert a document into a collection
    pub async fn insert_document(
        &self,
        database_name: DatabaseName,
        collection_name: CollectionName,
        document: Document,
    ) -> Result<DocumentId> {
        let collection = self.collection(database_name, collection_name).await?;
        collection.insert(document).await
    }

    /// Find a document by ID
    pub async fn find_document_by_id(
        &self,
        database_name: DatabaseName,
        collection_name: CollectionName,
        id: DocumentId,
    ) -> Result<Option<Document>> {
        let collection = self.collection(database_name, collection_name).await?;
        collection.find_by_id(&id).await
    }

    /// Update a document by ID
    pub async fn update_document_by_id(
        &self,
        database_name: DatabaseName,
        collection_name: CollectionName,
        id: DocumentId,
        document: Document,
    ) -> Result<Option<Document>> {
        let collection = self.collection(database_name, collection_name).await?;
        collection.update_by_id(&id, document).await
    }

    /// Delete a document by ID
    pub async fn delete_document_by_id(
        &self,
        database_name: DatabaseName,
        collection_name: CollectionName,
        id: DocumentId,
    ) -> Result<bool> {
        let collection = self.collection(database_name, collection_name).await?;
        collection.delete_by_id(&id).await
    }

    /// Get database statistics
    pub async fn get_stats(&self) -> Result<DatabaseStats> {
        let databases = self.databases.read().await;
        let mut total_collections = 0;
        let mut total_documents = 0;
        
        for database in databases.values() {
            let collections = database.list_collections().await?;
            total_collections += collections.len();
            
            for collection_name in collections {
                let collection = database.collection(collection_name).await?;
                total_documents += collection.count().await?;
            }
        }
        
        Ok(DatabaseStats {
            total_databases: databases.len(),
            total_collections,
            total_documents,
        })
    }
}

/// Database statistics
#[derive(Debug)]
pub struct DatabaseStats {
    pub total_databases: usize,
    pub total_collections: usize,
    pub total_documents: usize,
}
