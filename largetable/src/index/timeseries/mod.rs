// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Time series index implementation

use crate::{Result, DocumentId, Document, LargetableError, IndexType, IndexQuery, IndexStats};
use crate::index::Index;
use crate::document::DocumentUtils;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Time series index for temporal data
pub struct TimeSeriesIndex {
    field: String,
    granularity: String,
    time_index: Arc<RwLock<BTreeMap<i64, Vec<DocumentId>>>>,
}

impl TimeSeriesIndex {
    /// Create a new time series index
    pub fn new(field: String, granularity: String) -> Self {
        Self {
            field,
            granularity,
            time_index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Extract timestamp from a document field
    fn extract_timestamp(&self, doc: &Document) -> Option<i64> {
        DocumentUtils::get_field(doc, &self.field).and_then(|value| match value {
            crate::Value::Timestamp(t) => Some(t),
            crate::Value::Int64(i) => Some(i),
            crate::Value::Int32(i) => Some(i as i64),
            crate::Value::String(s) => {
                // Try to parse as timestamp
                s.parse().ok()
            }
            _ => None,
        })
    }

    /// Round timestamp to granularity
    fn round_to_granularity(&self, timestamp: i64) -> i64 {
        match self.granularity.as_str() {
            "second" => timestamp,
            "minute" => timestamp - (timestamp % 60),
            "hour" => timestamp - (timestamp % 3600),
            "day" => timestamp - (timestamp % 86400),
            "week" => timestamp - (timestamp % 604800),
            "month" => {
                // Approximate month as 30 days
                timestamp - (timestamp % 2592000)
            }
            _ => timestamp,
        }
    }

    /// Search for documents in a time range
    async fn search_time_range(&self, start: Option<i64>, end: Option<i64>) -> Result<Vec<DocumentId>> {
        let time_index = self.time_index.read().await;
        let mut results = Vec::new();
        
        let start_key = start.unwrap_or(i64::MIN);
        let end_key = end.unwrap_or(i64::MAX);
        
        for (timestamp, doc_ids) in time_index.range(start_key..=end_key) {
            results.extend(doc_ids.iter().cloned());
        }
        
        Ok(results)
    }
}

#[async_trait::async_trait]
impl Index for TimeSeriesIndex {
    async fn insert(&self, id: DocumentId, doc: &Document) -> Result<()> {
        if let Some(timestamp) = self.extract_timestamp(doc) {
            let rounded_timestamp = self.round_to_granularity(timestamp);
            let mut time_index = self.time_index.write().await;
            time_index.entry(rounded_timestamp).or_default().push(id);
            debug!("Inserted document {} into time series index on field '{}'", id, self.field);
        }
        Ok(())
    }

    async fn remove(&self, id: &DocumentId) -> Result<()> {
        let mut time_index = self.time_index.write().await;
        
        // Find and remove the document ID from all timestamps
        let mut timestamps_to_remove = Vec::new();
        
        for (timestamp, doc_ids) in time_index.iter_mut() {
            if let Some(pos) = doc_ids.iter().position(|&x| x == *id) {
                doc_ids.remove(pos);
                if doc_ids.is_empty() {
                    timestamps_to_remove.push(*timestamp);
                }
                break;
            }
        }
        
        // Remove empty timestamps
        for timestamp in timestamps_to_remove {
            time_index.remove(&timestamp);
        }
        
        debug!("Removed document {} from time series index on field '{}'", id, self.field);
        Ok(())
    }

    async fn update(&self, id: DocumentId, old_doc: &Document, new_doc: &Document) -> Result<()> {
        // Remove old timestamp
        self.remove(&id).await?;
        
        // Insert new timestamp
        self.insert(id, new_doc).await?;
        
        debug!("Updated document {} in time series index on field '{}'", id, self.field);
        Ok(())
    }

    async fn search(&self, query: &IndexQuery) -> Result<Vec<DocumentId>> {
        match query {
            IndexQuery::Range { field, min, max } if field == &self.field => {
                let start = min.as_ref().and_then(|v| match v {
                    crate::Value::Timestamp(t) => Some(*t),
                    crate::Value::Int64(i) => Some(*i),
                    crate::Value::Int32(i) => Some(*i as i64),
                    _ => None,
                });
                let end = max.as_ref().and_then(|v| match v {
                    crate::Value::Timestamp(t) => Some(*t),
                    crate::Value::Int64(i) => Some(*i),
                    crate::Value::Int32(i) => Some(*i as i64),
                    _ => None,
                });
                
                self.search_time_range(start, end).await
            }
            _ => {
                Err(LargetableError::Index(format!(
                    "Time series index on field '{}' only supports range queries, got: {:?}",
                    self.field, query
                )))
            }
        }
    }

    async fn stats(&self) -> Result<IndexStats> {
        let time_index = self.time_index.read().await;
        let total_entries = time_index.values().map(|ids| ids.len()).sum();
        let memory_usage = std::mem::size_of_val(&*time_index) + 
            time_index.iter().map(|(k, v)| std::mem::size_of_val(k) + std::mem::size_of_val(v)).sum::<usize>();
        
        Ok(IndexStats {
            total_entries,
            memory_usage,
            index_type: IndexType::TimeSeries {
                granularity: self.granularity.clone(),
            },
        })
    }

    fn index_type(&self) -> IndexType {
        IndexType::TimeSeries {
            granularity: self.granularity.clone(),
        }
    }
}