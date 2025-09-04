// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Memory layout optimizations for cache efficiency

use crate::{Result, Document, Value, DocumentId};
use std::collections::HashMap;
use std::alloc::{Layout, GlobalAlloc, System};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Cache-aligned memory allocator for optimal performance
pub struct CacheAlignedAllocator;

unsafe impl GlobalAlloc for CacheAlignedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Align to cache line size (64 bytes)
        let aligned_layout = layout.align_to(64).unwrap_or(layout);
        System.alloc(aligned_layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let aligned_layout = layout.align_to(64).unwrap_or(layout);
        System.dealloc(ptr, aligned_layout);
    }
}

/// Memory pool for efficient document allocation
pub struct DocumentMemoryPool {
    blocks: Vec<DocumentBlock>,
    free_blocks: Vec<usize>,
    block_size: usize,
    total_allocated: AtomicUsize,
}

struct DocumentBlock {
    data: Vec<u8>,
    in_use: bool,
    document_count: usize,
}

impl DocumentMemoryPool {
    const BLOCK_SIZE: usize = 64 * 1024; // 64KB blocks
    const CACHE_LINE_SIZE: usize = 64;

    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            free_blocks: Vec::new(),
            block_size: Self::BLOCK_SIZE,
            total_allocated: AtomicUsize::new(0),
        }
    }

    /// Allocate space for a document
    pub fn allocate_document(&mut self, estimated_size: usize) -> Result<DocumentAllocation> {
        // Find a suitable block
        for &block_idx in &self.free_blocks {
            if let Some(block) = self.blocks.get_mut(block_idx) {
                if !block.in_use && block.data.len() >= estimated_size {
                    block.in_use = true;
                    block.document_count += 1;
                    self.total_allocated.fetch_add(estimated_size, Ordering::Relaxed);
                    
                    return Ok(DocumentAllocation {
                        block_idx,
                        offset: 0,
                        size: estimated_size,
                    });
                }
            }
        }

        // Create new block if needed
        let block_size = std::cmp::max(estimated_size, Self::BLOCK_SIZE);
        let new_block = DocumentBlock {
            data: vec![0u8; block_size],
            in_use: true,
            document_count: 1,
        };

        let block_idx = self.blocks.len();
        self.blocks.push(new_block);
        self.total_allocated.fetch_add(estimated_size, Ordering::Relaxed);

        Ok(DocumentAllocation {
            block_idx,
            offset: 0,
            size: estimated_size,
        })
    }

    /// Deallocate a document
    pub fn deallocate_document(&mut self, allocation: DocumentAllocation) {
        if let Some(block) = self.blocks.get_mut(allocation.block_idx) {
            block.document_count -= 1;
            if block.document_count == 0 {
                block.in_use = false;
                self.free_blocks.push(allocation.block_idx);
            }
            self.total_allocated.fetch_sub(allocation.size, Ordering::Relaxed);
        }
    }

    /// Get memory usage statistics
    pub fn get_stats(&self) -> MemoryPoolStats {
        MemoryPoolStats {
            total_blocks: self.blocks.len(),
            free_blocks: self.free_blocks.len(),
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            block_size: self.block_size,
        }
    }
}

/// Document allocation handle
#[derive(Debug, Clone)]
pub struct DocumentAllocation {
    block_idx: usize,
    offset: usize,
    size: usize,
}

/// Memory pool statistics
#[derive(Debug)]
pub struct MemoryPoolStats {
    pub total_blocks: usize,
    pub free_blocks: usize,
    pub total_allocated: usize,
    pub block_size: usize,
}

/// Cache-friendly document storage with optimized memory layout
pub struct CacheOptimizedDocument {
    // Store frequently accessed fields first (hot path)
    pub id: DocumentId,
    pub version: u64,
    pub created_at: i64,
    pub updated_at: i64,
    
    // Store fields in a cache-friendly way
    pub fields: HashMap<String, Value>,
    
    // Padding to align to cache line
    _padding: [u8; 8],
}

impl CacheOptimizedDocument {
    pub fn new() -> Self {
        use uuid::Uuid;
        use chrono::Utc;

        Self {
            id: Uuid::new_v7(),
            version: 1,
            created_at: Utc::now().timestamp_micros(),
            updated_at: Utc::now().timestamp_micros(),
            fields: HashMap::new(),
            _padding: [0; 8],
        }
    }

    /// Convert to regular Document
    pub fn into_document(self) -> Document {
        Document {
            id: self.id,
            fields: self.fields,
            version: self.version,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    /// Create from regular Document
    pub fn from_document(doc: Document) -> Self {
        Self {
            id: doc.id,
            fields: doc.fields,
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
            _padding: [0; 8],
        }
    }
}

/// SIMD-optimized value storage
pub struct SimdOptimizedValue {
    // Store numeric values in SIMD-friendly format
    pub numeric_data: Vec<f32>,
    pub string_data: Vec<String>,
    pub binary_data: Vec<Vec<u8>>,
    
    // Type information for each value
    pub types: Vec<ValueType>,
}

#[derive(Debug, Clone, Copy)]
pub enum ValueType {
    Null,
    Bool,
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    String,
    Binary,
    Document,
    Array,
    Timestamp,
    ObjectId,
    Vector,
    Decimal128,
}

impl SimdOptimizedValue {
    pub fn new() -> Self {
        Self {
            numeric_data: Vec::new(),
            string_data: Vec::new(),
            binary_data: Vec::new(),
            types: Vec::new(),
        }
    }

    /// Add a value with SIMD optimization
    pub fn add_value(&mut self, value: Value) {
        match value {
            Value::Float32(f) => {
                self.numeric_data.push(f as f32);
                self.types.push(ValueType::Float32);
            },
            Value::Float64(f) => {
                self.numeric_data.push(f as f32);
                self.types.push(ValueType::Float64);
            },
            Value::Int32(i) => {
                self.numeric_data.push(i as f32);
                self.types.push(ValueType::Int32);
            },
            Value::Int64(i) => {
                self.numeric_data.push(i as f32);
                self.types.push(ValueType::Int64);
            },
            Value::String(s) => {
                self.string_data.push(s);
                self.types.push(ValueType::String);
            },
            Value::Binary(b) => {
                self.binary_data.push(b);
                self.types.push(ValueType::Binary);
            },
            _ => {
                // For complex types, store as-is
                self.types.push(ValueType::Null);
            }
        }
    }

    /// Get numeric values as SIMD-friendly slice
    pub fn get_numeric_slice(&self) -> &[f32] {
        &self.numeric_data
    }
}

/// Memory-mapped document collection for zero-copy access
pub struct MemoryMappedCollection {
    mmap: memmap2::Mmap,
    index: HashMap<DocumentId, DocumentOffset>,
    next_offset: usize,
}

#[derive(Debug, Clone)]
struct DocumentOffset {
    offset: usize,
    length: usize,
}

impl MemoryMappedCollection {
    pub fn new(path: &std::path::Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        
        Ok(Self {
            mmap,
            index: HashMap::new(),
            next_offset: 0,
        })
    }

    /// Insert a document with zero-copy
    pub fn insert_document(&mut self, document: &Document) -> Result<()> {
        // Serialize document
        let mut serializer = crate::engine::zero_copy::serialization::ZeroCopySerializer::new();
        let serialized = serializer.serialize_document(document)?;
        
        // Store in memory-mapped file
        let offset = self.next_offset;
        let length = serialized.len();
        
        if offset + length > self.mmap.len() {
            return Err(crate::LargetableError::InvalidInput("Insufficient space".to_string()));
        }
        
        unsafe {
            std::ptr::copy_nonoverlapping(
                serialized.as_ptr(),
                self.mmap.as_ptr().add(offset),
                length
            );
        }
        
        // Update index
        self.index.insert(document.id, DocumentOffset { offset, length });
        self.next_offset += length;
        
        Ok(())
    }

    /// Get a document with zero-copy
    pub fn get_document(&self, id: &DocumentId) -> Result<Option<Document>> {
        if let Some(offset_info) = self.index.get(id) {
            let slice = &self.mmap[offset_info.offset..offset_info.offset + offset_info.length];
            let mut deserializer = crate::engine::zero_copy::serialization::ZeroCopyDeserializer::new(slice);
            Ok(Some(deserializer.deserialize_document()?))
        } else {
            Ok(None)
        }
    }
}

/// NUMA-aware memory allocation
pub struct NumaAwareAllocator {
    node_allocators: Vec<DocumentMemoryPool>,
    current_node: usize,
}

impl NumaAwareAllocator {
    pub fn new(num_nodes: usize) -> Self {
        Self {
            node_allocators: (0..num_nodes).map(|_| DocumentMemoryPool::new()).collect(),
            current_node: 0,
        }
    }

    /// Allocate on the current NUMA node
    pub fn allocate_document(&mut self, estimated_size: usize) -> Result<DocumentAllocation> {
        let allocation = self.node_allocators[self.current_node].allocate_document(estimated_size)?;
        self.current_node = (self.current_node + 1) % self.node_allocators.len();
        Ok(allocation)
    }

    /// Allocate on a specific NUMA node
    pub fn allocate_document_on_node(&mut self, node: usize, estimated_size: usize) -> Result<DocumentAllocation> {
        if node >= self.node_allocators.len() {
            return Err(crate::LargetableError::InvalidInput("Invalid NUMA node".to_string()));
        }
        self.node_allocators[node].allocate_document(estimated_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool() {
        let mut pool = DocumentMemoryPool::new();
        let allocation = pool.allocate_document(1024).unwrap();
        assert_eq!(allocation.size, 1024);
        
        let stats = pool.get_stats();
        assert!(stats.total_allocated > 0);
    }

    #[test]
    fn test_cache_optimized_document() {
        let mut doc = CacheOptimizedDocument::new();
        doc.fields.insert("test".to_string(), Value::String("value".to_string()));
        
        let regular_doc = doc.into_document();
        assert_eq!(regular_doc.fields.get("test").unwrap(), &Value::String("value".to_string()));
    }

    #[test]
    fn test_simd_optimized_value() {
        let mut simd_value = SimdOptimizedValue::new();
        simd_value.add_value(Value::Float32(1.0));
        simd_value.add_value(Value::Float32(2.0));
        
        assert_eq!(simd_value.get_numeric_slice(), &[1.0, 2.0]);
    }
}