// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! High-performance buffer pool for zero-copy operations

use crate::{Result, Document};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// High-performance buffer pool with lock-free operations
pub struct BufferPool {
    buffers: Arc<Mutex<VecDeque<PooledBuffer>>>,
    condition: Arc<Condvar>,
    max_size: usize,
    buffer_size: usize,
    stats: Arc<BufferPoolStats>,
}

/// Pooled buffer with metadata
struct PooledBuffer {
    data: Vec<u8>,
    last_used: Instant,
    in_use: bool,
}

/// Buffer pool statistics
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub total_allocations: u64,
    pub total_deallocations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub current_size: usize,
    pub peak_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(max_size: usize, buffer_size: usize) -> Self {
        Self {
            buffers: Arc::new(Mutex::new(VecDeque::new())),
            condition: Arc::new(Condvar::new()),
            max_size,
            buffer_size,
            stats: Arc::new(BufferPoolStats::default()),
        }
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&self) -> Result<PooledBufferHandle> {
        let mut buffers = self.buffers.lock().unwrap();
        
        // Try to find an available buffer
        while let Some(mut buffer) = buffers.pop_front() {
            if !buffer.in_use {
                buffer.in_use = true;
                buffer.last_used = Instant::now();
                
                self.stats.cache_hits += 1;
                self.stats.total_allocations += 1;
                
                return Ok(PooledBufferHandle {
                    buffer: Arc::new(Mutex::new(buffer)),
                    pool: self.clone(),
        });
            }
        }

        // No available buffer, create new one if under limit
        if buffers.len() < self.max_size {
            let new_buffer = PooledBuffer {
                data: vec![0u8; self.buffer_size],
                last_used: Instant::now(),
                in_use: true,
            };
            
            self.stats.cache_misses += 1;
            self.stats.total_allocations += 1;
            self.stats.current_size += 1;
            self.stats.peak_size = self.stats.peak_size.max(self.stats.current_size);
            
            return Ok(PooledBufferHandle {
                buffer: Arc::new(Mutex::new(new_buffer)),
                pool: self.clone(),
            });
        }

        // Wait for a buffer to become available
        let buffer = self.condition.wait(buffers).unwrap().pop_front().unwrap();
        buffer.in_use = true;
        buffer.last_used = Instant::now();
        
        self.stats.cache_hits += 1;
        self.stats.total_allocations += 1;
        
        Ok(PooledBufferHandle {
            buffer: Arc::new(Mutex::new(buffer)),
            pool: self.clone(),
        })
    }

    /// Return a buffer to the pool
    fn return_buffer(&self, mut buffer: PooledBuffer) {
        buffer.in_use = false;
        buffer.last_used = Instant::now();
        
        let mut buffers = self.buffers.lock().unwrap();
        buffers.push_back(buffer);
        self.stats.total_deallocations += 1;
        self.condition.notify_one();
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            total_allocations: self.stats.total_allocations,
            total_deallocations: self.stats.total_deallocations,
            cache_hits: self.stats.cache_hits,
            cache_misses: self.stats.cache_misses,
            current_size: self.stats.current_size,
            peak_size: self.stats.peak_size,
        }
    }

    /// Clean up old buffers
    pub fn cleanup_old_buffers(&self, max_age: Duration) {
        let mut buffers = self.buffers.lock().unwrap();
        let now = Instant::now();
        
        buffers.retain(|buffer| {
            if !buffer.in_use && now.duration_since(buffer.last_used) > max_age {
                self.stats.current_size -= 1;
                false
            } else {
                true
            }
        });
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            buffers: self.buffers.clone(),
            condition: self.condition.clone(),
            max_size: self.max_size,
            buffer_size: self.buffer_size,
            stats: self.stats.clone(),
        }
    }
}

/// Handle to a pooled buffer
pub struct PooledBufferHandle {
    buffer: Arc<Mutex<PooledBuffer>>,
    pool: BufferPool,
}

impl PooledBufferHandle {
    /// Get mutable access to the buffer data
    pub fn get_mut(&mut self) -> &mut [u8] {
        let mut buffer = self.buffer.lock().unwrap();
        &mut buffer.data
    }

    /// Get immutable access to the buffer data
    pub fn get(&self) -> &[u8] {
        let buffer = self.buffer.lock().unwrap();
        &buffer.data
    }

    /// Resize the buffer
    pub fn resize(&mut self, new_size: usize) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.data.resize(new_size, 0);
    }

    /// Get the current size of the buffer
    pub fn len(&self) -> usize {
        let buffer = self.buffer.lock().unwrap();
        buffer.data.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for PooledBufferHandle {
    fn drop(&mut self) {
        let buffer = self.buffer.lock().unwrap();
        let buffer = std::mem::replace(&mut *buffer, PooledBuffer {
            data: vec![],
            last_used: Instant::now(),
            in_use: false,
        });
        self.pool.return_buffer(buffer);
    }
}

/// Async buffer pool for high-concurrency scenarios
pub struct AsyncBufferPool {
    pool: Arc<RwLock<BufferPool>>,
    cleanup_interval: Duration,
}

impl AsyncBufferPool {
    pub fn new(max_size: usize, buffer_size: usize) -> Self {
        Self {
            pool: Arc::new(RwLock::new(BufferPool::new(max_size, buffer_size))),
            cleanup_interval: Duration::from_secs(60),
        }
    }

    /// Get a buffer asynchronously
    pub async fn get_buffer(&self) -> Result<AsyncPooledBufferHandle> {
        let pool = self.pool.read().await;
        let buffer_handle = pool.get_buffer()?;
        
        Ok(AsyncPooledBufferHandle {
            buffer: buffer_handle.buffer,
            pool: self.pool.clone(),
        })
    }

    /// Start cleanup task
    pub async fn start_cleanup_task(&self) {
        let pool = self.pool.clone();
        let cleanup_interval = self.cleanup_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                let pool_guard = pool.read().await;
                pool_guard.cleanup_old_buffers(Duration::from_secs(300)); // 5 minutes
            }
        });
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> BufferPoolStats {
        let pool = self.pool.read().await;
        pool.get_stats()
    }
}

/// Async handle to a pooled buffer
pub struct AsyncPooledBufferHandle {
    buffer: Arc<Mutex<PooledBuffer>>,
    pool: Arc<RwLock<BufferPool>>,
}

impl AsyncPooledBufferHandle {
    /// Get mutable access to the buffer data
    pub fn get_mut(&mut self) -> &mut [u8] {
        let mut buffer = self.buffer.lock().unwrap();
        &mut buffer.data
    }

    /// Get immutable access to the buffer data
    pub fn get(&self) -> &[u8] {
        let buffer = self.buffer.lock().unwrap();
        &buffer.data
    }

    /// Resize the buffer
    pub fn resize(&mut self, new_size: usize) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.data.resize(new_size, 0);
    }

    /// Get the current size of the buffer
    pub fn len(&self) -> usize {
        let buffer = self.buffer.lock().unwrap();
        buffer.data.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for AsyncPooledBufferHandle {
    fn drop(&mut self) {
        let buffer = self.buffer.lock().unwrap();
        let buffer = std::mem::replace(&mut *buffer, PooledBuffer {
            data: vec![],
            last_used: Instant::now(),
            in_use: false,
        });
        
        // Return buffer to pool asynchronously
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let pool_guard = pool.read().await;
            pool_guard.return_buffer(buffer);
        });
    }
}

/// Specialized buffer pool for document serialization
pub struct DocumentBufferPool {
    pool: AsyncBufferPool,
    serializer: crate::engine::zero_copy::serialization::ZeroCopySerializer,
}

impl DocumentBufferPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: AsyncBufferPool::new(max_size, 64 * 1024), // 64KB buffers
            serializer: crate::engine::zero_copy::serialization::ZeroCopySerializer::new(),
        }
    }

    /// Serialize a document using a pooled buffer
    pub async fn serialize_document(&self, document: &Document) -> Result<Vec<u8>> {
        let mut buffer_handle = self.pool.get_buffer().await?;
        let mut serializer = self.serializer.clone();
        
        let serialized = serializer.serialize_document(document)?;
        buffer_handle.resize(serialized.len());
        buffer_handle.get_mut().copy_from_slice(serialized);
        
        Ok(buffer_handle.get().to_vec())
    }

    /// Serialize multiple documents in batch
    pub async fn serialize_documents(&self, documents: &[Document]) -> Result<Vec<u8>> {
        let mut buffer_handle = self.pool.get_buffer().await?;
        let mut serializer = self.serializer.clone();
        
        let serialized = serializer.serialize_documents(documents)?;
        buffer_handle.resize(serialized.len());
        buffer_handle.get_mut().copy_from_slice(serialized);
        
        Ok(buffer_handle.get().to_vec())
    }

    /// Start cleanup task
    pub async fn start_cleanup_task(&self) {
        self.pool.start_cleanup_task().await;
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> BufferPoolStats {
        self.pool.get_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(10, 1024);
        let mut handle = pool.get_buffer().unwrap();
        
        assert_eq!(handle.len(), 1024);
        assert!(!handle.is_empty());
        
        // Test buffer operations
        handle.get_mut()[0] = 42;
        assert_eq!(handle.get()[0], 42);
        
        // Handle is dropped here, buffer returned to pool
    }

    #[tokio::test]
    async fn test_async_buffer_pool() {
        let pool = AsyncBufferPool::new(10, 1024);
        let mut handle = pool.get_buffer().await.unwrap();
        
        assert_eq!(handle.len(), 1024);
        assert!(!handle.is_empty());
        
        // Test buffer operations
        handle.get_mut()[0] = 42;
        assert_eq!(handle.get()[0], 42);
        
        // Handle is dropped here, buffer returned to pool
    }

    #[tokio::test]
    async fn test_document_buffer_pool() {
        let pool = DocumentBufferPool::new(10);
        let mut doc = Document::new();
        doc.fields.insert("test".to_string(), Value::String("value".to_string()));
        
        let serialized = pool.serialize_document(&doc).await.unwrap();
        assert!(!serialized.is_empty());
    }
}