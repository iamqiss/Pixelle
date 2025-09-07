// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! B-Tree storage engine - read-optimized

use crate::storage::StorageEngine;
use crate::{Result, DocumentId, Document, LargetableError};
use async_trait::async_trait;
use redb::{Database, ReadableTable, TableDefinition, WriteableTable};
use rkyv::{to_bytes, from_bytes};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

const DOCUMENTS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("documents");

/// B-Tree storage engine using Redb
pub struct BTreeEngine {
    db: Arc<RwLock<Database>>,
}

impl BTreeEngine {
    /// Create a new B-Tree engine with Redb backend
    pub fn new() -> Result<Self> {
        Self::with_path("largetable_btree.redb")
    }

    /// Create B-Tree engine with custom data path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)
            .map_err(|e| LargetableError::Storage(format!("Failed to create Redb database: {}", e)))?;
        
        info!("B-Tree Engine initialized with Redb backend");
        
        Ok(Self {
            db: Arc::new(RwLock::new(db)),
        })
    }

    /// Serialize document to bytes using zero-copy serialization
    fn serialize_document(&self, doc: &Document) -> Result<Vec<u8>> {
        to_bytes::<_, 1024>(doc)
            .map_err(|e| LargetableError::Serialization(format!("Failed to serialize document: {}", e)))
    }

    /// Deserialize bytes to document using zero-copy deserialization
    fn deserialize_document(&self, data: &[u8]) -> Result<Document> {
        from_bytes::<Document>(data)
            .map_err(|e| LargetableError::Serialization(format!("Failed to deserialize document: {}", e)))
    }

    /// Convert DocumentId to bytes for Redb key
    fn id_to_bytes(&self, id: &DocumentId) -> Vec<u8> {
        id.as_bytes().to_vec()
    }

    /// Convert bytes to DocumentId
    fn bytes_to_id(&self, data: &[u8]) -> Result<DocumentId> {
        if data.len() != 16 {
            return Err(LargetableError::Serialization("Invalid DocumentId length".to_string()));
        }
        
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(data);
        Ok(DocumentId::from_bytes(bytes))
    }
}

#[async_trait]
impl StorageEngine for BTreeEngine {
    async fn get(&self, id: &DocumentId) -> Result<Option<Document>> {
        let db = self.db.read().await;
        let key = self.id_to_bytes(id);
        
        let read_txn = db.begin_read()
            .map_err(|e| LargetableError::Storage(format!("Failed to begin read transaction: {}", e)))?;
        
        let table = read_txn.open_table(DOCUMENTS_TABLE)
            .map_err(|e| LargetableError::Storage(format!("Failed to open table: {}", e)))?;
        
        match table.get(&key) {
            Ok(Some(data)) => {
                debug!("Retrieved document with ID: {}", id);
                self.deserialize_document(&data.value()).map(Some)
            }
            Ok(None) => {
                debug!("Document not found with ID: {}", id);
                Ok(None)
            }
            Err(e) => {
                error!("Failed to get document {}: {}", id, e);
                Err(LargetableError::Storage(format!("Get operation failed: {}", e)))
            }
        }
    }
    
    async fn put(&self, id: DocumentId, doc: Document) -> Result<()> {
        let db = self.db.write().await;
        let key = self.id_to_bytes(&id);
        let value = self.serialize_document(&doc)?;
        
        let write_txn = db.begin_write()
            .map_err(|e| LargetableError::Storage(format!("Failed to begin write transaction: {}", e)))?;
        
        {
            let mut table = write_txn.open_table(DOCUMENTS_TABLE)
                .map_err(|e| LargetableError::Storage(format!("Failed to open table: {}", e)))?;
            
            table.insert(&key, &value)
                .map_err(|e| LargetableError::Storage(format!("Failed to insert document: {}", e)))?;
        }
        
        write_txn.commit()
            .map_err(|e| LargetableError::Storage(format!("Failed to commit transaction: {}", e)))?;
        
        debug!("Stored document with ID: {}", id);
        Ok(())
    }
    
    async fn delete(&self, id: &DocumentId) -> Result<bool> {
        let db = self.db.write().await;
        let key = self.id_to_bytes(id);
        
        let write_txn = db.begin_write()
            .map_err(|e| LargetableError::Storage(format!("Failed to begin write transaction: {}", e)))?;
        
        {
            let mut table = write_txn.open_table(DOCUMENTS_TABLE)
                .map_err(|e| LargetableError::Storage(format!("Failed to open table: {}", e)))?;
            
            table.remove(&key)
                .map_err(|e| LargetableError::Storage(format!("Failed to remove document: {}", e)))?;
        }
        
        write_txn.commit()
            .map_err(|e| LargetableError::Storage(format!("Failed to commit transaction: {}", e)))?;
        
        debug!("Deleted document with ID: {}", id);
        Ok(true)
    }
    
    async fn scan(&self, start: Option<DocumentId>, limit: usize) -> Result<Vec<(DocumentId, Document)>> {
        let db = self.db.read().await;
        let mut results = Vec::new();
        let mut count = 0;
        
        let read_txn = db.begin_read()
            .map_err(|e| LargetableError::Storage(format!("Failed to begin read transaction: {}", e)))?;
        
        let table = read_txn.open_table(DOCUMENTS_TABLE)
            .map_err(|e| LargetableError::Storage(format!("Failed to open table: {}", e)))?;
        
        let range = if let Some(start_id) = start {
            let start_key = self.id_to_bytes(&start_id);
            table.range(start_key..)
        } else {
            table.iter()
        };
        
        for item in range {
            if count >= limit {
                break;
            }
            
            match item {
                Ok((key, value)) => {
                    let id = self.bytes_to_id(&key.value())?;
                    let doc = self.deserialize_document(&value.value())?;
                    results.push((id, doc));
                    count += 1;
                }
                Err(e) => {
                    error!("Iterator error during scan: {}", e);
                    return Err(LargetableError::Storage(format!("Scan operation failed: {}", e)));
                }
            }
        }
        
        debug!("Scanned {} documents", results.len());
        Ok(results)
    }
}