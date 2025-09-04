// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Native Rust client for Largetable

use crate::{Result, DatabaseName, CollectionName, DocumentId, Document, StorageEngine};
use crate::engine::DatabaseEngine;
use crate::query::{Query, QueryBuilder, AggregationPipeline, QueryResult};
use std::sync::Arc;
use tracing::{debug, info};

/// Native Rust client for Largetable
pub struct Client {
    engine: Arc<DatabaseEngine>,
}

impl Client {
    /// Create a new client with default LSM storage engine
    pub fn new() -> Result<Self> {
        Self::with_storage_engine(StorageEngine::Lsm)
    }

    /// Create a new client with specified storage engine
    pub fn with_storage_engine(storage_engine: StorageEngine) -> Result<Self> {
        let engine = Arc::new(DatabaseEngine::with_default_storage_engine(storage_engine)?);
        
        info!("Created Largetable client");
        
        Ok(Self { engine })
    }

    /// Get a database
    pub async fn database(&self, name: DatabaseName) -> Result<Arc<crate::database::Database>> {
        self.engine.database(name).await
    }

    /// List all databases
    pub async fn list_databases(&self) -> Result<Vec<DatabaseName>> {
        self.engine.list_databases().await
    }

    /// Drop a database
    pub async fn drop_database(&self, name: &DatabaseName) -> Result<bool> {
        self.engine.drop_database(name).await
    }

    /// Get a collection
    pub async fn collection(&self, database: DatabaseName, collection: CollectionName) -> Result<Arc<crate::database::Collection>> {
        self.engine.collection(database, collection).await
    }

    /// Insert a document
    pub async fn insert(&self, database: DatabaseName, collection: CollectionName, document: Document) -> Result<DocumentId> {
        self.engine.insert_document(database, collection, document).await
    }

    /// Find a document by ID
    pub async fn find_by_id(&self, database: DatabaseName, collection: CollectionName, id: DocumentId) -> Result<Option<Document>> {
        self.engine.find_document_by_id(database, collection, id).await
    }

    /// Update a document by ID
    pub async fn update_by_id(&self, database: DatabaseName, collection: CollectionName, id: DocumentId, document: Document) -> Result<Option<Document>> {
        self.engine.update_document_by_id(database, collection, id, document).await
    }

    /// Delete a document by ID
    pub async fn delete_by_id(&self, database: DatabaseName, collection: CollectionName, id: DocumentId) -> Result<bool> {
        self.engine.delete_document_by_id(database, collection, id).await
    }

    /// Find multiple documents
    pub async fn find_many(&self, database: DatabaseName, collection: CollectionName, query: Query) -> Result<QueryResult> {
        self.engine.query(database, collection, query).await
    }

    /// Execute aggregation pipeline
    pub async fn aggregate(&self, database: DatabaseName, collection: CollectionName, pipeline: AggregationPipeline) -> Result<Vec<serde_json::Value>> {
        self.engine.aggregate(database, collection, pipeline).await
    }

    /// Get database statistics
    pub async fn stats(&self) -> Result<crate::engine::DatabaseStats> {
        self.engine.get_stats().await
    }

    /// Create a query builder
    pub fn query() -> QueryBuilder {
        QueryBuilder::new()
    }

    /// Create an aggregation pipeline builder
    pub fn aggregate() -> AggregationPipeline {
        AggregationPipeline::new()
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new().expect("Failed to create Largetable client")
    }
}

/// Collection reference for fluent API
pub struct CollectionRef {
    client: Arc<Client>,
    database: DatabaseName,
    collection: CollectionName,
}

impl CollectionRef {
    /// Create a new collection reference
    pub fn new(client: Arc<Client>, database: DatabaseName, collection: CollectionName) -> Self {
        Self {
            client,
            database,
            collection,
        }
    }

    /// Insert a document
    pub async fn insert(&self, document: Document) -> Result<DocumentId> {
        self.client.insert(self.database.clone(), self.collection.clone(), document).await
    }

    /// Find a document by ID
    pub async fn find_by_id(&self, id: DocumentId) -> Result<Option<Document>> {
        self.client.find_by_id(self.database.clone(), self.collection.clone(), id).await
    }

    /// Update a document by ID
    pub async fn update_by_id(&self, id: DocumentId, document: Document) -> Result<Option<Document>> {
        self.client.update_by_id(self.database.clone(), self.collection.clone(), id, document).await
    }

    /// Delete a document by ID
    pub async fn delete_by_id(&self, id: DocumentId) -> Result<bool> {
        self.client.delete_by_id(self.database.clone(), self.collection.clone(), id).await
    }

    /// Find multiple documents
    pub async fn find_many(&self, query: Query) -> Result<QueryResult> {
        self.client.find_many(self.database.clone(), self.collection.clone(), query).await
    }

    /// Execute aggregation pipeline
    pub async fn aggregate(&self, pipeline: AggregationPipeline) -> Result<Vec<serde_json::Value>> {
        self.client.aggregate(self.database.clone(), self.collection.clone(), pipeline).await
    }

    /// Get collection name
    pub fn name(&self) -> &CollectionName {
        &self.collection
    }

    /// Get database name
    pub fn database(&self) -> &DatabaseName {
        &self.database
    }
}

impl Client {
    /// Get a collection reference for fluent API
    pub fn collection_ref(&self, database: DatabaseName, collection: CollectionName) -> CollectionRef {
        CollectionRef::new(Arc::new(Client {
            engine: self.engine.clone(),
        }), database, collection)
    }
}