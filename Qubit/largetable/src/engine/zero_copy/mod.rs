// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Zero-copy data structures and operations

pub mod serialization;
pub mod deserialization;
pub mod simd;
pub mod memory_layout;
pub mod buffer_pool;

use crate::{Result, Document, Value, DocumentId};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Zero-copy document pool for high-performance operations
pub struct ZeroCopyPool {
    pool: Arc<RwLock<Vec<Document>>>,
    max_size: usize,
}

impl ZeroCopyPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Arc::new(RwLock::new(Vec::with_capacity(max_size))),
            max_size,
        }
    }

    /// Get a document from the pool, creating a new one if needed
    pub async fn get_document(&self) -> Document {
        let mut pool = self.pool.write().await;
        pool.pop().unwrap_or_else(|| Document::new())
    }

    /// Return a document to the pool for reuse
    pub async fn return_document(&self, mut doc: Document) {
        let mut pool = self.pool.write().await;
        if pool.len() < self.max_size {
            doc.clear();
            pool.push(doc);
        }
    }
}

impl Document {
    /// Create a new empty document
    pub fn new() -> Self {
        use std::collections::HashMap;
        use uuid::Uuid;
        use chrono::Utc;

        Self {
            id: Uuid::new_v7(),
            fields: HashMap::new(),
            version: 1,
            created_at: Utc::now().timestamp_micros(),
            updated_at: Utc::now().timestamp_micros(),
        }
    }

    /// Clear document fields for reuse
    pub fn clear(&mut self) {
        self.fields.clear();
        self.version = 1;
        self.updated_at = chrono::Utc::now().timestamp_micros();
    }
}
