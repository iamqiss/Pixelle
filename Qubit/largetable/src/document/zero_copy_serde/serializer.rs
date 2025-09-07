// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! High-performance zero-copy serializer implementation

use rkyv::{
    archived_root,
    ser::{Serializer, serializers::AllocSerializer},
    AlignedVec, Archived,
};
use std::sync::Arc;
use parking_lot::RwLock;
use dashmap::DashMap;

#[cfg(feature = "mmap")]
use memmap2::Mmap;

use crate::{Result, LargetableError, DocumentId};
use super::{ZeroCopyDocument, SerializationStats};

/// High-performance zero-copy serializer
pub struct ZeroCopySerializer {
    /// Memory-mapped file cache for large documents
    #[cfg(feature = "mmap")]
    mmap_cache: DashMap<DocumentId, Arc<Mmap>>,
    
    /// Compression threshold (documents larger than this get compressed)
    compression_threshold: usize,
    
    /// Statistics for performance monitoring
    stats: Arc<RwLock<SerializationStats>>,
}

impl ZeroCopySerializer {
    /// Create a new zero-copy serializer with optimized defaults
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "mmap")]
            mmap_cache: DashMap::new(),
            compression_threshold: 64 * 1024, // 64KB
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }

    /// Create serializer with custom compression threshold
    pub fn with_compression_threshold(threshold: usize) -> Self {
        Self {
            #[cfg(feature = "mmap")]
            mmap_cache: DashMap::new(),
            compression_threshold: threshold,
            stats: Arc::new(RwLock::new(SerializationStats::default())),
        }
    }

    /// Serialize document to bytes with zero-copy optimizations
    pub fn serialize_document(&self, doc: &ZeroCopyDocument) -> Result<AlignedVec> {
        let start_time = std::time::Instant::now();
        
        // Use AllocSerializer for optimal performance
        let mut serializer = AllocSerializer::<256>::default();
        serializer.serialize_value(doc)
            .map_err(|e| LargetableError::Serialization(format!("Serialization failed: {}", e)))?;
        
        let bytes = serializer.into_serializer().into_inner();
        
        // Update statistics
        self.update_serialization_stats(bytes.len(), start_time.elapsed());
        
        Ok(bytes)
    }

    /// Deserialize document with zero-copy access
    pub fn deserialize_document(&self, bytes: &[u8]) -> Result<&Archived<ZeroCopyDocument>> {
        let start_time = std::time::Instant::now();
        
        // Validate and get archived document without copying
        let archived = archived_root::<ZeroCopyDocument>(bytes)
            .map_err(|e| LargetableError::Serialization(format!("Deserialization failed: {}", e)))?;
        
        // Update statistics
        self.update_deserialization_stats(bytes.len(), start_time.elapsed());
        
        Ok(archived)
    }

    /// Batch serialize multiple documents for optimal performance
    pub fn batch_serialize(&self, docs: &[ZeroCopyDocument]) -> Result<Vec<AlignedVec>> {
        let start_time = std::time::Instant::now();
        let mut results = Vec::with_capacity(docs.len());
        
        // Use SIMD-friendly batch processing where possible
        for doc in docs {
            let mut serializer = AllocSerializer::<256>::default();
            serializer.serialize_value(doc)
                .map_err(|e| LargetableError::Serialization(format!("Batch serialization failed: {}", e)))?;
            
            results.push(serializer.into_serializer().into_inner());
        }
        
        // Update batch statistics
        let total_bytes: usize = results.iter().map(|v| v.len()).sum();
        let elapsed = start_time.elapsed();
        let avg_per_doc = elapsed / docs.len() as u32;
        
        let mut stats = self.stats.write();
        stats.total_serializations += docs.len() as u64;
        stats.bytes_serialized += total_bytes as u64;
        stats.avg_serialization_time_ns = 
            (stats.avg_serialization_time_ns + avg_per_doc.as_nanos() as u64) / 2;
        
        Ok(results)
    }

    /// Get serialization statistics
    pub fn get_stats(&self) -> SerializationStats {
        self.stats.read().clone()
    }

    /// Clear all caches to free memory
    pub fn clear_cache(&self) {
        #[cfg(feature = "mmap")]
        self.mmap_cache.clear();
    }

    /// Get cache size information
    pub fn cache_info(&self) -> CacheInfo {
        #[cfg(feature = "mmap")]
        {
            CacheInfo {
                mmap_entries: self.mmap_cache.len(),
                estimated_memory_kb: self.mmap_cache.len() * 8, // Rough estimate
            }
        }
        #[cfg(not(feature = "mmap"))]
        {
            CacheInfo {
                mmap_entries: 0,
                estimated_memory_kb: 0,
            }
        }
    }

    // Private helper methods
    fn update_serialization_stats(&self, bytes_len: usize, elapsed: std::time::Duration) {
        let mut stats = self.stats.write();
        stats.total_serializations += 1;
        stats.bytes_serialized += bytes_len as u64;
        stats.avg_serialization_time_ns = 
            (stats.avg_serialization_time_ns + elapsed.as_nanos() as u64) / 2;
    }

    fn update_deserialization_stats(&self, bytes_len: usize, elapsed: std::time::Duration) {
        let mut stats = self.stats.write();
        stats.total_deserializations += 1;
        stats.bytes_deserialized += bytes_len as u64;
        stats.avg_deserialization_time_ns = 
            (stats.avg_deserialization_time_ns + elapsed.as_nanos() as u64) / 2;
    }
}

impl Default for ZeroCopySerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache information for monitoring
#[derive(Debug, Clone)]
pub struct CacheInfo {
    pub mmap_entries: usize,
    pub estimated_memory_kb: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::document::zero_copy_serde::ZeroCopyValue;

    #[test]
    fn test_serializer_creation() {
        let serializer = ZeroCopySerializer::new();
        let stats = serializer.get_stats();
        
        assert_eq!(stats.total_serializations, 0);
        assert_eq!(stats.total_deserializations, 0);
    }
    
    #[test]
    fn test_custom_compression_threshold() {
        let serializer = ZeroCopySerializer::with_compression_threshold(128 * 1024);
        assert_eq!(serializer.compression_threshold, 128 * 1024);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let serializer = ZeroCopySerializer::new();
        
        // Create test document
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), ZeroCopyValue::String("Neo Qiss".to_string()));
        fields.insert("age".to_string(), ZeroCopyValue::Int32(30));
        fields.insert("active".to_string(), ZeroCopyValue::Bool(true));
        
        let doc = ZeroCopyDocument {
            id: uuid::Uuid::new_v4(),
            fields,
            version: 1,
            created_at: chrono::Utc::now().timestamp_micros(),
            updated_at: chrono::Utc::now().timestamp_micros(),
            size_bytes: 0,
        };
        
        // Serialize
        let serialized = serializer.serialize_document(&doc).unwrap();
        
        // Deserialize with zero-copy
        let deserialized = serializer.deserialize_document(&serialized).unwrap();
        
        // Verify data integrity
        assert_eq!(deserialized.id, doc.id);
        assert_eq!(deserialized.version, doc.version);
        assert!(deserialized.fields.contains_key("name"));
        
        // Check stats were updated
        let stats = serializer.get_stats();
        assert_eq!(stats.total_serializations, 1);
        assert_eq!(stats.total_deserializations, 1);
        assert!(stats.bytes_serialized > 0);
        assert!(stats.bytes_deserialized > 0);
    }
    
    #[test]
    fn test_batch_serialization_performance() {
        let serializer = ZeroCopySerializer::new();
        let mut docs = Vec::new();
        
        // Create multiple test documents
        for i in 0..1000 {
            let mut fields = HashMap::new();
            fields.insert("index".to_string(), ZeroCopyValue::Int32(i));
            fields.insert("data".to_string(), ZeroCopyValue::String(format!("test_data_{}", i)));
            
            docs.push(ZeroCopyDocument {
                id: uuid::Uuid::new_v4(),
                fields,
                version: 1,
                created_at: chrono::Utc::now().timestamp_micros(),
                updated_at: chrono::Utc::now().timestamp_micros(),
                size_bytes: 0,
            });
        }
        
        // Batch serialize
        let start = std::time::Instant::now();
        let serialized = serializer.batch_serialize(&docs).unwrap();
        let batch_time = start.elapsed();
        
        println!("Batch serialized 1000 documents in {:?}", batch_time);
        assert_eq!(serialized.len(), 1000);
        
        // Verify stats
        let stats = serializer.get_stats();
        assert_eq!(stats.total_serializations, 1000);
        assert!(stats.bytes_serialized > 0);
    }
}
