// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Advanced indexing system for efficient queries

pub mod adaptive;
pub mod btree;
pub mod compound;
pub mod fulltext;
pub mod geospatial;
pub mod graph;
pub mod hash;
pub mod sparse;
pub mod timeseries;
pub mod vector;

use crate::{Result, DocumentId, Document, LargetableError, IndexType, VectorMetric};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Index manager for a collection
pub struct IndexManager {
    indexes: Arc<RwLock<HashMap<String, Box<dyn Index + Send + Sync>>>>,
    collection_name: String,
}

/// Trait for all index types
#[async_trait::async_trait]
pub trait Index: Send + Sync {
    /// Insert a document into the index
    async fn insert(&self, id: DocumentId, doc: &Document) -> Result<()>;
    
    /// Remove a document from the index
    async fn remove(&self, id: &DocumentId) -> Result<()>;
    
    /// Update a document in the index
    async fn update(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()>;
    
    /// Search the index
    async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>>;
    
    /// Get index statistics
    async fn stats(&self) -> Result<IndexStats>;
    
    /// Get index type
    fn index_type(&self) -> IndexType;
}

/// Index query for searching
#[derive(Debug, Clone)]
pub enum IndexQuery {
    /// Exact match query
    Exact {
        field: String,
        value: crate::Value,
    },
    /// Range query
    Range {
        field: String,
        min: Option<crate::Value>,
        max: Option<crate::Value>,
    },
    /// Full-text search query
    FullText {
        field: String,
        query: String,
    },
    /// Vector similarity query
    Vector {
        field: String,
        vector: Vec<f32>,
        limit: usize,
        threshold: Option<f32>,
    },
    /// Geospatial query
    Geospatial {
        field: String,
        center: (f64, f64),
        radius: f64,
    },
    /// Compound query (AND of multiple conditions)
    Compound {
        queries: Vec<IndexQuery>,
    },
}

/// Index statistics
#[derive(Debug)]
pub struct IndexStats {
    pub total_entries: usize,
    pub memory_usage: usize,
    pub index_type: IndexType,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new(collection_name: String) -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            collection_name,
        }
    }

    /// Create an index on a field
    pub async fn create_index(&self, field: String, index_type: IndexType) -> Result<()> {
        let mut indexes = self.indexes.write().await;
        
        if indexes.contains_key(&field) {
            return Err(LargetableError::Index(format!("Index on field '{}' already exists", field)));
        }
        
        let index: Box<dyn Index + Send + Sync> = match index_type {
            IndexType::BTree => Box::new(btree::BTreeIndex::new(field.clone())),
            IndexType::Hash => Box::new(hash::HashIndex::new(field.clone())),
            IndexType::FullText { language, stop_words } => {
                Box::new(fulltext::FullTextIndex::new(field.clone(), language, stop_words))
            }
            IndexType::Vector { dimensions, metric } => {
                Box::new(vector::VectorIndex::new(field.clone(), dimensions, metric))
            }
            IndexType::Geospatial { coordinate_system } => {
                Box::new(geospatial::GeospatialIndex::new(field.clone(), coordinate_system))
            }
            IndexType::TimeSeries { granularity } => {
                Box::new(timeseries::TimeSeriesIndex::new(field.clone(), granularity))
            }
        };
        
        indexes.insert(field.clone(), index);
        
        info!("Created {:?} index on field '{}' for collection '{}'", index_type, field, self.collection_name);
        Ok(())
    }

    /// Drop an index
    pub async fn drop_index(&self, field: &str) -> Result<bool> {
        let mut indexes = self.indexes.write().await;
        let removed = indexes.remove(field).is_some();
        
        if removed {
            info!("Dropped index on field '{}' for collection '{}'", field, self.collection_name);
        }
        
        Ok(removed)
    }

    /// List all indexes
    pub async fn list_indexes(&self) -> Result<Vec<(String, IndexType)>> {
        let indexes = self.indexes.read().await;
        Ok(indexes.iter()
            .map(|(field, index)| (field.clone(), index.index_type()))
            .collect())
    }

    /// Insert a document into all indexes
    pub async fn insert_document(&self, id: DocumentId, doc: &Document) -> Result<()> {
        let indexes = self.indexes.read().await;
        
        for (field, index) in indexes.iter() {
            if let Err(e) = index.insert(id, doc).await {
                error!("Failed to insert document {} into index on field '{}': {}", id, field, e);
                return Err(e);
            }
        }
        
        debug!("Inserted document {} into all indexes", id);
        Ok(())
    }

    /// Remove a document from all indexes
    pub async fn remove_document(&self, id: &DocumentId) -> Result<()> {
        let indexes = self.indexes.read().await;
        
        for (field, index) in indexes.iter() {
            if let Err(e) = index.remove(id).await {
                error!("Failed to remove document {} from index on field '{}': {}", id, field, e);
                return Err(e);
            }
        }
        
        debug!("Removed document {} from all indexes", id);
        Ok(())
    }

    /// Update a document in all indexes
    pub async fn update_document(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()> {
        let indexes = self.indexes.read().await;
        
        for (field, index) in indexes.iter() {
            if let Err(e) = index.update(id, old_doc, new_doc).await {
                error!("Failed to update document {} in index on field '{}': {}", id, field, e);
                return Err(e);
            }
        }
        
        debug!("Updated document {} in all indexes", id);
        Ok(())
    }

    /// Search using indexes
    pub async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>> {
        match query {
            IndexQuery::Compound { queries } => {
                if queries.is_empty() {
                    return Ok(Vec::new());
                }
                
                // Start with the first query result
                let mut result = self.search(&queries[0]).await?;
                
                // Intersect with results from other queries
                for query in &queries[1..] {
                    let query_result = self.search(query).await?;
                    result = result.into_iter()
                        .filter(|id| query_result.contains(id))
                        .collect();
                }
                
                Ok(result)
            }
            _ => {
                // Find the appropriate index for this query
                let field = match query {
                    IndexQuery::Exact { field, .. } => field,
                    IndexQuery::Range { field, .. } => field,
                    IndexQuery::FullText { field, .. } => field,
                    IndexQuery::Vector { field, .. } => field,
                    IndexQuery::Geospatial { field, .. } => field,
                    _ => return Err(LargetableError::Index("Unsupported query type".to_string())),
                };
                
                let indexes = self.indexes.read().await;
                if let Some(index) = indexes.get(field) {
                    index.search(query).await
                } else {
                    Err(LargetableError::Index(format!("No index found for field '{}'", field)))
                }
            }
        }
    }

    /// Get statistics for all indexes
    pub async fn get_stats(&self) -> Result<Vec<(String, IndexStats)>> {
        let indexes = self.indexes.read().await;
        let mut stats = Vec::new();
        
        for (field, index) in indexes.iter() {
            match index.stats().await {
                Ok(index_stats) => stats.push((field.clone(), index_stats)),
                Err(e) => {
                    error!("Failed to get stats for index on field '{}': {}", field, e);
                }
            }
        }
        
        Ok(stats)
    }
}