// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! B-Tree index implementation

use crate::{Result, DocumentId, Document, LargetableError, IndexType, IndexQuery, IndexStats};
use crate::index::Index;
use crate::document::DocumentUtils;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// B-Tree index for ordered data
pub struct BTreeIndex {
    field: String,
    index: Arc<RwLock<BTreeMap<IndexKey, Vec<DocumentId>>>>,
}

/// Key for the B-Tree index
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Timestamp(i64),
}

impl BTreeIndex {
    /// Create a new B-Tree index
    pub fn new(field: String) -> Self {
        Self {
            field,
            index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Extract the index key from a document
    fn extract_key(&self, doc: &Document) -> Option<IndexKey> {
        DocumentUtils::get_field(doc, &self.field).map(|value| match value {
            crate::Value::Null => IndexKey::Null,
            crate::Value::Bool(b) => IndexKey::Bool(*b),
            crate::Value::Int32(i) => IndexKey::Int(*i as i64),
            crate::Value::Int64(i) => IndexKey::Int(*i),
            crate::Value::UInt64(u) => IndexKey::Int(*u as i64),
            crate::Value::Float32(f) => IndexKey::Float(*f as f64),
            crate::Value::Float64(f) => IndexKey::Float(*f),
            crate::Value::String(s) => IndexKey::String(s.clone()),
            crate::Value::Timestamp(t) => IndexKey::Timestamp(*t),
            _ => IndexKey::String(value.to_string()),
        })
    }

    /// Convert a Value to an IndexKey
    fn value_to_key(&self, value: &crate::Value) -> IndexKey {
        match value {
            crate::Value::Null => IndexKey::Null,
            crate::Value::Bool(b) => IndexKey::Bool(*b),
            crate::Value::Int32(i) => IndexKey::Int(*i as i64),
            crate::Value::Int64(i) => IndexKey::Int(*i),
            crate::Value::UInt64(u) => IndexKey::Int(*u as i64),
            crate::Value::Float32(f) => IndexKey::Float(*f as f64),
            crate::Value::Float64(f) => IndexKey::Float(*f),
            crate::Value::String(s) => IndexKey::String(s.clone()),
            crate::Value::Timestamp(t) => IndexKey::Timestamp(*t),
            _ => IndexKey::String(value.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl Index for BTreeIndex {
    async fn insert(&self, id: DocumentId, doc: &Document) -> Result<()> {
        if let Some(key) = self.extract_key(doc) {
            let mut index = self.index.write().await;
            index.entry(key).or_default().push(id);
            debug!("Inserted document {} into B-Tree index on field '{}'", id, self.field);
        }
        Ok(())
    }

    async fn remove(&self, id: &DocumentId) -> Result<()> {
        let mut index = self.index.write().await;
        
        // Find and remove the document ID from all keys
        let mut keys_to_remove = Vec::new();
        
        for (key, ids) in index.iter_mut() {
            if let Some(pos) = ids.iter().position(|&x| x == *id) {
                ids.remove(pos);
                if ids.is_empty() {
                    keys_to_remove.push(key.clone());
                }
                break;
            }
        }
        
        // Remove empty keys
        for key in keys_to_remove {
            index.remove(&key);
        }
        
        debug!("Removed document {} from B-Tree index on field '{}'", id, self.field);
        Ok(())
    }

    async fn update(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()> {
        // Remove from old key
        self.remove(&id).await?;
        
        // Insert with new key
        self.insert(id, new_doc).await?;
        
        debug!("Updated document {} in B-Tree index on field '{}'", id, self.field);
        Ok(())
    }

    async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>> {
        let index = self.index.read().await;
        let mut results = Vec::new();
        
        match query {
            IndexQuery::Exact { field, value } if field == &self.field => {
                let key = self.value_to_key(value);
                if let Some(ids) = index.get(&key) {
                    results.extend(ids.iter().cloned());
                }
            }
            IndexQuery::Range { field, min, max } if field == &self.field => {
                let min_key = min.as_ref().map(|v| self.value_to_key(v));
                let max_key = max.as_ref().map(|v| self.value_to_key(v));
                
                for (key, ids) in index.range(
                    min_key.as_ref().map(|k| (k, std::ops::Bound::Included(k)))
                        .unwrap_or((&IndexKey::Null, std::ops::Bound::Unbounded))
                    ..max_key.as_ref().map(|k| (k, std::ops::Bound::Excluded(k)))
                        .unwrap_or((&IndexKey::String("\u{FFFF}".to_string()), std::ops::Bound::Unbounded))
                ) {
                    results.extend(ids.iter().cloned());
                }
            }
            _ => {
                return Err(LargetableError::Index(format!(
                    "B-Tree index on field '{}' does not support query type: {:?}",
                    self.field, query
                )));
            }
        }
        
        debug!("B-Tree search on field '{}' returned {} results", self.field, results.len());
        Ok(results)
    }

    async fn stats(&self) -> Result<IndexStats> {
        let index = self.index.read().await;
        let total_entries = index.values().map(|ids| ids.len()).sum();
        let memory_usage = std::mem::size_of_val(&*index) + 
            index.iter().map(|(k, v)| std::mem::size_of_val(k) + std::mem::size_of_val(v)).sum::<usize>();
        
        Ok(IndexStats {
            total_entries,
            memory_usage,
            index_type: IndexType::BTree,
        })
    }

    fn index_type(&self) -> IndexType {
        IndexType::BTree
    }
}