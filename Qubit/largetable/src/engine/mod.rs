// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Core database engine with enterprise-grade features

pub mod async_runtime;
pub mod memory_pool;
pub mod zero_copy;
pub mod transaction;
pub mod mvcc;
pub mod recovery;
pub mod connection_pool;
pub mod cache;
pub mod memory_manager;
pub mod auto_scaling;

use crate::{Result, DatabaseName, CollectionName, StorageEngine, DocumentId, Document};
use crate::database::Database;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

// Enterprise-grade features
use connection_pool::{ConnectionPool, ConnectionPoolConfig};
use cache::{MultiLevelCache, CacheConfig};
use memory_manager::{MemoryManager, MemoryConfig};
use auto_scaling::{AutoScalingManager, AutoScalingConfig};

/// Main database engine that manages multiple databases
pub struct DatabaseEngine {
    databases: Arc<RwLock<HashMap<DatabaseName, Arc<Database>>>>,
    default_storage_engine: StorageEngine,
    // Enterprise-grade features
    connection_pool: Arc<ConnectionPool>,
    cache: Arc<MultiLevelCache>,
    memory_manager: Arc<MemoryManager>,
    auto_scaling: Arc<AutoScalingManager>,
}

impl DatabaseEngine {
    /// Create a new database engine
    pub async fn new() -> Result<Self> {
        Self::with_default_storage_engine(StorageEngine::Lsm).await
    }

    /// Create a new database engine with specified default storage engine
    pub async fn with_default_storage_engine(default_storage_engine: StorageEngine) -> Result<Self> {
        info!("Initialized Largetable Database Engine with {:?} storage engine", default_storage_engine);
        
        // Initialize enterprise-grade features
        let connection_pool = Arc::new(ConnectionPool::new(ConnectionPoolConfig::default()).await?);
        let cache = Arc::new(MultiLevelCache::new(CacheConfig::default()).await?);
        let memory_manager = Arc::new(MemoryManager::new(MemoryConfig::default()).await?);
        let auto_scaling = Arc::new(AutoScalingManager::new(AutoScalingConfig::default()).await?);
        
        Ok(Self {
            databases: Arc::new(RwLock::new(HashMap::new())),
            default_storage_engine,
            connection_pool,
            cache,
            memory_manager,
            auto_scaling,
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

    // Enterprise-grade features

    /// Get connection pool statistics
    pub async fn get_connection_pool_stats(&self) -> connection_pool::PoolStats {
        self.connection_pool.get_stats().await
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> cache::CacheStats {
        self.cache.get_stats().await
    }

    /// Get memory manager statistics
    pub async fn get_memory_stats(&self) -> memory_manager::MemoryStats {
        self.memory_manager.get_stats().await
    }

    /// Get auto-scaling statistics
    pub async fn get_auto_scaling_stats(&self) -> auto_scaling::AutoScalingStats {
        self.auto_scaling.get_stats().await
    }

    /// Force garbage collection
    pub async fn force_gc(&self) -> Result<()> {
        self.memory_manager.force_gc().await
    }

    /// Compact memory
    pub async fn compact_memory(&self) -> Result<()> {
        self.memory_manager.compact_memory().await
    }

    /// Clear cache
    pub async fn clear_cache(&self) -> Result<()> {
        self.cache.clear().await
    }

    /// Warm cache with frequently accessed keys
    pub async fn warm_cache(&self, keys: Vec<String>) -> Result<()> {
        self.cache.warm_cache(keys).await
    }

    /// Get a connection from the pool
    pub async fn get_connection(&self) -> Result<connection_pool::PooledConnection> {
        self.connection_pool.get_connection().await
    }

    /// Clean up broken connections
    pub async fn cleanup_connections(&self) -> Result<()> {
        self.connection_pool.cleanup_broken_connections().await;
        Ok(())
    }
}

/// Database statistics
#[derive(Debug)]
pub struct DatabaseStats {
    pub total_databases: usize,
    pub total_collections: usize,
    pub total_documents: usize,
}
