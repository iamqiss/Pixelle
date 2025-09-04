// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Advanced compression and deduplication

use std::collections::HashMap;
use std::sync::Arc;
use std::io::Write;
use serde::{Deserialize, Serialize};
use tracing::{info, debug, warn};
use blake3::Hasher;
use zstd::encode_all;
use flate2::{Compression, write::GzEncoder};
use lz4_flex::compress;

use crate::errors::{NimbuxError, Result};

/// Compression algorithms supported by Nimbux
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Zstd,
    Lz4,
    Auto, // Automatically choose best algorithm
}

/// Compression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    pub original_size: u64,
    pub compressed_size: u64,
    pub compression_ratio: f64,
    pub algorithm_used: CompressionAlgorithm,
    pub compression_time_ms: u64,
}

/// Content-addressable storage with advanced compression
pub struct CompressionEngine {
    /// Cache of compressed content by hash
    compressed_cache: Arc<tokio::sync::RwLock<HashMap<String, CompressedChunk>>>,
    /// Statistics tracking
    stats: Arc<tokio::sync::RwLock<CompressionStats>>,
}

/// Compressed data chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressedChunk {
    pub hash: String,
    pub data: Vec<u8>,
    pub algorithm: CompressionAlgorithm,
    pub original_size: u64,
    pub compressed_size: u64,
    pub reference_count: u64,
}

impl CompressionEngine {
    /// Create a new compression engine
    pub fn new() -> Self {
        Self {
            compressed_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            stats: Arc::new(tokio::sync::RwLock::new(CompressionStats {
                original_size: 0,
                compressed_size: 0,
                compression_ratio: 0.0,
                algorithm_used: CompressionAlgorithm::None,
                compression_time_ms: 0,
            })),
        }
    }

    /// Compress data using the specified algorithm
    pub async fn compress_data(
        &self,
        data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<CompressedChunk> {
        let start_time = std::time::Instant::now();
        
        // Calculate content hash
        let hash = self.calculate_hash(data);
        
        // Check if already compressed
        {
            let cache = self.compressed_cache.read().await;
            if let Some(chunk) = cache.get(&hash) {
                // Increment reference count
                let mut chunk = chunk.clone();
                chunk.reference_count += 1;
                
                // Update cache
                drop(cache);
                let mut cache = self.compressed_cache.write().await;
                cache.insert(hash.clone(), chunk.clone());
                
                debug!("Reused compressed chunk: {} (refs: {})", hash, chunk.reference_count);
                return Ok(chunk);
            }
        }

        // Compress data
        let (compressed_data, used_algorithm) = match algorithm {
            CompressionAlgorithm::None => (data.to_vec(), CompressionAlgorithm::None),
            CompressionAlgorithm::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(data)
                    .map_err(|e| NimbuxError::Compression(format!("Gzip compression failed: {}", e)))?;
                let compressed = encoder.finish()
                    .map_err(|e| NimbuxError::Compression(format!("Gzip finish failed: {}", e)))?;
                (compressed, CompressionAlgorithm::Gzip)
            }
            CompressionAlgorithm::Zstd => {
                let compressed = encode_all(data, 3)
                    .map_err(|e| NimbuxError::Compression(format!("Zstd compression failed: {}", e)))?;
                (compressed, CompressionAlgorithm::Zstd)
            }
            CompressionAlgorithm::Lz4 => {
                let compressed = compress(data);
                (compressed, CompressionAlgorithm::Lz4)
            }
            CompressionAlgorithm::Auto => {
                // Try all algorithms and choose the best one
                let mut best_ratio = 0.0;
                let mut best_data = data.to_vec();
                let mut best_algorithm = CompressionAlgorithm::None;

                // Test Gzip
                let gzip_data = {
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(data).ok();
                    encoder.finish().unwrap_or_default()
                };
                let gzip_ratio = 1.0 - (gzip_data.len() as f64 / data.len() as f64);
                if gzip_ratio > best_ratio {
                    best_ratio = gzip_ratio;
                    best_data = gzip_data;
                    best_algorithm = CompressionAlgorithm::Gzip;
                }

                // Test Zstd
                if let Ok(zstd_data) = encode_all(data, 3) {
                    let zstd_ratio = 1.0 - (zstd_data.len() as f64 / data.len() as f64);
                    if zstd_ratio > best_ratio {
                        best_ratio = zstd_ratio;
                        best_data = zstd_data;
                        best_algorithm = CompressionAlgorithm::Zstd;
                    }
                }

                // Test Lz4
                let lz4_data = compress(data);
                let lz4_ratio = 1.0 - (lz4_data.len() as f64 / data.len() as f64);
                if lz4_ratio > best_ratio {
                    best_ratio = lz4_ratio;
                    best_data = lz4_data;
                    best_algorithm = CompressionAlgorithm::Lz4;
                }

                (best_data, best_algorithm)
            }
        };

        let compression_time = start_time.elapsed().as_millis() as u64;
        let compression_ratio = 1.0 - (compressed_data.len() as f64 / data.len() as f64);

        let chunk = CompressedChunk {
            hash: hash.clone(),
            data: compressed_data,
            algorithm: used_algorithm,
            original_size: data.len() as u64,
            compressed_size: compressed_data.len() as u64,
            reference_count: 1,
        };

        // Store in cache
        {
            let mut cache = self.compressed_cache.write().await;
            cache.insert(hash.clone(), chunk.clone());
        }

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.original_size += data.len() as u64;
            stats.compressed_size += chunk.compressed_size;
            stats.compression_ratio = if stats.original_size > 0 {
                1.0 - (stats.compressed_size as f64 / stats.original_size as f64)
            } else {
                0.0
            };
            stats.algorithm_used = used_algorithm;
            stats.compression_time_ms += compression_time;
        }

        debug!("Compressed data: {} -> {} (ratio: {:.2}%, algorithm: {:?}, time: {}ms)",
               data.len(), chunk.compressed_size, compression_ratio * 100.0, used_algorithm, compression_time);

        Ok(chunk)
    }

    /// Decompress data
    pub async fn decompress_data(&self, chunk: &CompressedChunk) -> Result<Vec<u8>> {
        match chunk.algorithm {
            CompressionAlgorithm::None => Ok(chunk.data.clone()),
            CompressionAlgorithm::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;
                
                let mut decoder = GzDecoder::new(&chunk.data[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)
                    .map_err(|e| NimbuxError::Compression(format!("Gzip decompression failed: {}", e)))?;
                Ok(decompressed)
            }
            CompressionAlgorithm::Zstd => {
                zstd::decode_all(&chunk.data[..])
                    .map_err(|e| NimbuxError::Compression(format!("Zstd decompression failed: {}", e)))
            }
            CompressionAlgorithm::Lz4 => {
                lz4_flex::decompress(&chunk.data, chunk.original_size as usize)
                    .map_err(|e| NimbuxError::Compression(format!("Lz4 decompression failed: {}", e)))
            }
            CompressionAlgorithm::Auto => {
                // This shouldn't happen for stored chunks
                Err(NimbuxError::Compression("Auto algorithm not supported for decompression".to_string()))
            }
        }
    }

    /// Get compressed chunk by hash
    pub async fn get_compressed_chunk(&self, hash: &str) -> Result<Option<CompressedChunk>> {
        let cache = self.compressed_cache.read().await;
        Ok(cache.get(hash).cloned())
    }

    /// Remove reference to compressed chunk
    pub async fn remove_reference(&self, hash: &str) -> Result<()> {
        let mut cache = self.compressed_cache.write().await;
        if let Some(chunk) = cache.get_mut(hash) {
            chunk.reference_count = chunk.reference_count.saturating_sub(1);
            if chunk.reference_count == 0 {
                cache.remove(hash);
                debug!("Removed compressed chunk: {}", hash);
            }
        }
        Ok(())
    }

    /// Get compression statistics
    pub async fn get_stats(&self) -> Result<CompressionStats> {
        let stats = self.stats.read().await;
        Ok(stats.clone())
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        let cache = self.compressed_cache.read().await;
        let total_chunks = cache.len();
        let total_references: u64 = cache.values().map(|c| c.reference_count).sum();
        let total_compressed_size: u64 = cache.values().map(|c| c.compressed_size).sum();
        let total_original_size: u64 = cache.values().map(|c| c.original_size).sum();

        Ok(CacheStats {
            total_chunks,
            total_references,
            total_compressed_size,
            total_original_size,
            space_saved: total_original_size.saturating_sub(total_compressed_size),
            deduplication_ratio: if total_original_size > 0 {
                total_references as f64 / total_chunks as f64
            } else {
                0.0
            },
        })
    }

    /// Calculate content hash
    fn calculate_hash(&self, data: &[u8]) -> String {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize().to_hex().to_string()
    }

    /// Clean up unused chunks
    pub async fn cleanup_unused_chunks(&self) -> Result<usize> {
        let mut cache = self.compressed_cache.write().await;
        let initial_count = cache.len();
        
        cache.retain(|_, chunk| chunk.reference_count > 0);
        
        let removed_count = initial_count - cache.len();
        if removed_count > 0 {
            info!("Cleaned up {} unused compressed chunks", removed_count);
        }
        
        Ok(removed_count)
    }
}

/// Cache statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub total_chunks: usize,
    pub total_references: u64,
    pub total_compressed_size: u64,
    pub total_original_size: u64,
    pub space_saved: u64,
    pub deduplication_ratio: f64,
}

impl Default for CompressionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Smart compression analyzer
pub struct CompressionAnalyzer {
    sample_size: usize,
    min_size_for_compression: usize,
}

impl CompressionAnalyzer {
    /// Create a new compression analyzer
    pub fn new() -> Self {
        Self {
            sample_size: 1024, // 1KB sample
            min_size_for_compression: 512, // Don't compress files smaller than 512 bytes
        }
    }

    /// Analyze data to determine if compression is beneficial
    pub fn should_compress(&self, data: &[u8]) -> bool {
        if data.len() < self.min_size_for_compression {
            return false;
        }

        // Sample the data to analyze entropy
        let sample = if data.len() > self.sample_size {
            &data[..self.sample_size]
        } else {
            data
        };

        // Calculate entropy
        let entropy = self.calculate_entropy(sample);
        
        // If entropy is low, data is likely already compressed or highly repetitive
        entropy > 0.7 // Threshold for compression benefit
    }

    /// Calculate Shannon entropy of data
    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        let mut counts = [0u32; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }

        let length = data.len() as f64;
        let mut entropy = 0.0;

        for &count in &counts {
            if count > 0 {
                let probability = count as f64 / length;
                entropy -= probability * probability.log2();
            }
        }

        entropy / 8.0 // Normalize to 0-1 range
    }

    /// Recommend compression algorithm based on data characteristics
    pub fn recommend_algorithm(&self, data: &[u8]) -> CompressionAlgorithm {
        if !self.should_compress(data) {
            return CompressionAlgorithm::None;
        }

        // For small files, use LZ4 (fast)
        if data.len() < 1024 * 1024 { // 1MB
            return CompressionAlgorithm::Lz4;
        }

        // For medium files, use Zstd (good balance)
        if data.len() < 10 * 1024 * 1024 { // 10MB
            return CompressionAlgorithm::Zstd;
        }

        // For large files, use Gzip (good compression)
        CompressionAlgorithm::Gzip
    }
}

impl Default for CompressionAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}