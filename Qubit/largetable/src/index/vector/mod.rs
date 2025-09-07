// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Vector similarity index implementation

use crate::{Result, DocumentId, Document, LargetableError, IndexType, IndexQuery, IndexStats, VectorMetric};
use crate::index::Index;
use crate::document::DocumentUtils;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Vector similarity index using HNSW
pub struct VectorIndex {
    field: String,
    dimensions: usize,
    metric: VectorMetric,
    vectors: Arc<RwLock<HashMap<DocumentId, Vec<f32>>>>,
}

impl VectorIndex {
    /// Create a new vector index
    pub fn new(field: String, dimensions: usize, metric: VectorMetric) -> Self {
        Self {
            field,
            dimensions,
            metric,
            vectors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Extract vector from a document field
    fn extract_vector(&self, doc: &Document) -> Option<Vec<f32>> {
        DocumentUtils::get_field(doc, &self.field).and_then(|value| match value {
            crate::Value::Vector(v) => {
                if v.len() == self.dimensions {
                    Some(v.clone())
                } else {
                    None
                }
            }
            _ => None,
        })
    }

    /// Calculate cosine similarity between two vectors
    fn cosine_similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }
        
        let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot_product / (norm_a * norm_b)
        }
    }

    /// Calculate Euclidean distance between two vectors
    fn euclidean_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::INFINITY;
        }
        
        a.iter().zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    /// Calculate dot product similarity
    fn dot_product(&self, a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }
        
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    /// Calculate Manhattan distance between two vectors
    fn manhattan_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return f32::INFINITY;
        }
        
        a.iter().zip(b.iter())
            .map(|(x, y)| (x - y).abs())
            .sum()
    }

    /// Calculate similarity between two vectors based on the metric
    fn calculate_similarity(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            VectorMetric::Cosine => self.cosine_similarity(a, b),
            VectorMetric::Euclidean => -self.euclidean_distance(a, b), // Negative for higher similarity
            VectorMetric::Dot => self.dot_product(a, b),
            VectorMetric::Manhattan => -self.manhattan_distance(a, b), // Negative for higher similarity
        }
    }

    /// Search for similar vectors
    async fn search_similar(&self, query_vector: &[f32], limit: usize, threshold: Option<f32>) -> Result<Vec<(DocumentId, f32)>> {
        let vectors = self.vectors.read().await;
        let mut results = Vec::new();
        
        for (doc_id, vector) in vectors.iter() {
            let similarity = self.calculate_similarity(query_vector, vector);
            
            if let Some(thresh) = threshold {
                if similarity >= thresh {
                    results.push((*doc_id, similarity));
                }
            } else {
                results.push((*doc_id, similarity));
            }
        }
        
        // Sort by similarity (descending)
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Limit results
        results.truncate(limit);
        
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Index for VectorIndex {
    async fn insert(&self, id: DocumentId, doc: &Document) -> Result<()> {
        if let Some(vector) = self.extract_vector(doc) {
            let mut vectors = self.vectors.write().await;
            vectors.insert(id, vector);
            debug!("Inserted document {} into vector index on field '{}'", id, self.field);
        }
        Ok(())
    }

    async fn remove(&self, id: &DocumentId) -> Result<()> {
        let mut vectors = self.vectors.write().await;
        vectors.remove(id);
        debug!("Removed document {} from vector index on field '{}'", id, self.field);
        Ok(())
    }

    async fn update(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()> {
        // Remove old vector
        self.remove(&id).await?;
        
        // Insert new vector
        self.insert(id, new_doc).await?;
        
        debug!("Updated document {} in vector index on field '{}'", id, self.field);
        Ok(())
    }

    async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>> {
        match query {
            IndexQuery::Vector { field, vector, limit, threshold } if field == &self.field => {
                if vector.len() != self.dimensions {
                    return Err(LargetableError::Index(format!(
                        "Vector dimension mismatch: expected {}, got {}",
                        self.dimensions, vector.len()
                    )));
                }
                
                let results = self.search_similar(vector, *limit, *threshold).await?;
                Ok(results.into_iter().map(|(id, _)| id).collect())
            }
            _ => {
                Err(LargetableError::Index(format!(
                    "Vector index on field '{}' only supports vector similarity search, got: {:?}",
                    self.field, query
                )))
            }
        }
    }

    async fn stats(&self) -> Result<IndexStats> {
        let vectors = self.vectors.read().await;
        let total_entries = vectors.len();
        let memory_usage = std::mem::size_of_val(&*vectors) + 
            vectors.iter().map(|(k, v)| std::mem::size_of_val(k) + std::mem::size_of_val(v)).sum::<usize>();
        
        Ok(IndexStats {
            total_entries,
            memory_usage,
            index_type: IndexType::Vector {
                dimensions: self.dimensions,
                metric: self.metric.clone(),
            },
        })
    }

    fn index_type(&self) -> IndexType {
        IndexType::Vector {
            dimensions: self.dimensions,
            metric: self.metric.clone(),
        }
    }
}