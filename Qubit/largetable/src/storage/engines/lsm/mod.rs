// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! LSM Tree storage engine - write-optimized

use crate::storage::StorageEngine;
use crate::{Result, DocumentId, Document, LargetableError};
use async_trait::async_trait;
use rocksdb::{DB, Options, WriteOptions, ReadOptions, IteratorMode};
use rkyv::{to_bytes, from_bytes};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// LSM Tree storage engine using RocksDB
pub struct LsmEngine {
    db: Arc<RwLock<DB>>,
    write_options: WriteOptions,
    read_options: ReadOptions,
}

impl LsmEngine {
    /// Create a new LSM engine with RocksDB backend
    pub fn new() -> Result<Self> {
        Self::with_path("largetable_lsm")
    }

    /// Create LSM engine with custom data path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_use_fsync(false);
        opts.set_max_background_jobs(4);
        opts.set_bytes_per_sync(1048576);
        opts.set_wal_bytes_per_sync(1048576);
        
        // Optimize for write-heavy workloads
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(1);
        
        // Optimize for read performance
        opts.set_max_open_files(1000);
        opts.set_use_direct_reads(true);
        opts.set_use_direct_io_for_flush_and_compaction(true);
        
        // Bloom filter for point lookups
        opts.set_bloom_locality(1);
        
        let db = DB::open(&opts, path)
            .map_err(|e| LargetableError::Storage(format!("Failed to open RocksDB: {}", e)))?;
        
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);
        write_opts.disable_wal(false);
        
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(true);
        
        info!("LSM Engine initialized with RocksDB backend");
        
        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            write_options: write_opts,
            read_options: read_opts,
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

    /// Convert DocumentId to bytes for RocksDB key
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
impl StorageEngine for LsmEngine {
    async fn get(&self, id: &DocumentId) -> Result<Option<Document>> {
        let db = self.db.read().await;
        let key = self.id_to_bytes(id);
        
        match db.get_opt(&key, &self.read_options) {
            Ok(Some(data)) => {
                debug!("Retrieved document with ID: {}", id);
                self.deserialize_document(&data).map(Some)
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
        
        match db.put_opt(&key, &value, &self.write_options) {
            Ok(_) => {
                debug!("Stored document with ID: {}", id);
                Ok(())
            }
            Err(e) => {
                error!("Failed to put document {}: {}", id, e);
                Err(LargetableError::Storage(format!("Put operation failed: {}", e)))
            }
        }
    }
    
    async fn delete(&self, id: &DocumentId) -> Result<bool> {
        let db = self.db.write().await;
        let key = self.id_to_bytes(id);
        
        match db.delete_opt(&key, &self.write_options) {
            Ok(_) => {
                debug!("Deleted document with ID: {}", id);
                Ok(true)
            }
            Err(e) => {
                error!("Failed to delete document {}: {}", id, e);
                Err(LargetableError::Storage(format!("Delete operation failed: {}", e)))
            }
        }
    }
    
    async fn scan(&self, start: Option<DocumentId>, limit: usize) -> Result<Vec<(DocumentId, Document)>> {
        let db = self.db.read().await;
        let mut results = Vec::new();
        let mut count = 0;
        
        let iter_mode = if let Some(start_id) = start {
            IteratorMode::From(&self.id_to_bytes(&start_id), rocksdb::Direction::Forward)
        } else {
            IteratorMode::Start
        };
        
        let mut iter = db.iterator_opt(iter_mode, &self.read_options);
        
        while let Some(item) = iter.next() {
            if count >= limit {
                break;
            }
            
            match item {
                Ok((key, value)) => {
                    let id = self.bytes_to_id(&key)?;
                    let doc = self.deserialize_document(&value)?;
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
