// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Compression utilities

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::errors::{NimbuxError, Result};

/// Supported compression algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
    Gzip,
    Lz4,
}

impl CompressionAlgorithm {
    /// Get the default compression level for the algorithm
    pub fn default_level(&self) -> i32 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => 3,
            CompressionAlgorithm::Gzip => 6,
            CompressionAlgorithm::Lz4 => 1,
        }
    }
    
    /// Get the maximum compression level for the algorithm
    pub fn max_level(&self) -> i32 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => 22,
            CompressionAlgorithm::Gzip => 9,
            CompressionAlgorithm::Lz4 => 16,
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub level: i32,
    pub min_size: usize,
    pub max_ratio: f64,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            min_size: 1024, // Only compress objects larger than 1KB
            max_ratio: 0.8,  // Only compress if ratio is better than 80%
        }
    }
}

/// Compression result
#[derive(Debug, Clone)]
pub struct CompressionResult {
    pub compressed_data: Vec<u8>,
    pub algorithm: CompressionAlgorithm,
    pub original_size: usize,
    pub compressed_size: usize,
    pub ratio: f64,
}

/// Compression utilities
pub struct CompressionEngine {
    config: CompressionConfig,
}

impl CompressionEngine {
    /// Create a new compression engine with default config
    pub fn new() -> Self {
        Self {
            config: CompressionConfig::default(),
        }
    }
    
    /// Create a new compression engine with custom config
    pub fn with_config(config: CompressionConfig) -> Self {
        Self { config }
    }
    
    /// Compress data using the configured algorithm
    pub fn compress(&self, data: &[u8]) -> Result<CompressionResult> {
        // Skip compression for small objects
        if data.len() < self.config.min_size {
            return Ok(CompressionResult {
                compressed_data: data.to_vec(),
                algorithm: CompressionAlgorithm::None,
                original_size: data.len(),
                compressed_size: data.len(),
                ratio: 1.0,
            });
        }
        
        let compressed_data = match self.config.algorithm {
            CompressionAlgorithm::None => data.to_vec(),
            CompressionAlgorithm::Zstd => {
                zstd::encode_all(data, self.config.level)
                    .map_err(|e| NimbuxError::Compression(format!("Zstd compression failed: {}", e)))?
            }
            CompressionAlgorithm::Gzip => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;
                
                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.config.level as u32));
                encoder.write_all(data)
                    .map_err(|e| NimbuxError::Compression(format!("Gzip compression failed: {}", e)))?;
                encoder.finish()
                    .map_err(|e| NimbuxError::Compression(format!("Gzip compression failed: {}", e)))?
            }
            CompressionAlgorithm::Lz4 => {
                lz4_flex::compress(data)
            }
        };
        
        let compressed_size = compressed_data.len();
        let ratio = compressed_size as f64 / data.len() as f64;
        
        // Only use compression if it meets our criteria
        if ratio < self.config.max_ratio {
            Ok(CompressionResult {
                compressed_data,
                algorithm: self.config.algorithm,
                original_size: data.len(),
                compressed_size,
                ratio,
            })
        } else {
            Ok(CompressionResult {
                compressed_data: data.to_vec(),
                algorithm: CompressionAlgorithm::None,
                original_size: data.len(),
                compressed_size: data.len(),
                ratio: 1.0,
            })
        }
    }
    
    /// Decompress data
    pub fn decompress(&self, data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        match algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Zstd => {
                zstd::decode_all(data)
                    .map_err(|e| NimbuxError::Decompression(format!("Zstd decompression failed: {}", e)))
            }
            CompressionAlgorithm::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;
                
                let mut decoder = GzDecoder::new(data);
                let mut result = Vec::new();
                decoder.read_to_end(&mut result)
                    .map_err(|e| NimbuxError::Decompression(format!("Gzip decompression failed: {}", e)))?;
                Ok(result)
            }
            CompressionAlgorithm::Lz4 => {
                lz4_flex::decompress_size_prepended(data)
                    .map_err(|e| NimbuxError::Decompression(format!("LZ4 decompression failed: {}", e)))
            }
        }
    }
    
    /// Get compression statistics for multiple data samples
    pub fn analyze_compression(&self, samples: &[&[u8]]) -> CompressionAnalysis {
        let mut total_original = 0;
        let mut total_compressed = 0;
        let mut compression_ratios = Vec::new();
        let mut algorithm_usage = HashMap::new();
        
        for sample in samples {
            match self.compress(sample) {
                Ok(result) => {
                    total_original += result.original_size;
                    total_compressed += result.compressed_size;
                    compression_ratios.push(result.ratio);
                    *algorithm_usage.entry(result.algorithm).or_insert(0) += 1;
                }
                Err(_) => {
                    // If compression fails, count as no compression
                    total_original += sample.len();
                    total_compressed += sample.len();
                    compression_ratios.push(1.0);
                    *algorithm_usage.entry(CompressionAlgorithm::None).or_insert(0) += 1;
                }
            }
        }
        
        let average_ratio = if !compression_ratios.is_empty() {
            compression_ratios.iter().sum::<f64>() / compression_ratios.len() as f64
        } else {
            1.0
        };
        
        CompressionAnalysis {
            total_samples: samples.len(),
            total_original_size: total_original,
            total_compressed_size: total_compressed,
            average_compression_ratio: average_ratio,
            space_saved: total_original - total_compressed,
            algorithm_usage,
        }
    }
}

impl Default for CompressionEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Compression analysis results
#[derive(Debug, Clone)]
pub struct CompressionAnalysis {
    pub total_samples: usize,
    pub total_original_size: usize,
    pub total_compressed_size: usize,
    pub average_compression_ratio: f64,
    pub space_saved: usize,
    pub algorithm_usage: HashMap<CompressionAlgorithm, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_basic() {
        let engine = CompressionEngine::new();
        let data = b"Hello, World! This is a test string for compression.".repeat(100);
        
        let result = engine.compress(&data).unwrap();
        assert!(result.compressed_size < result.original_size);
        assert!(result.ratio < 1.0);
        
        // Test decompression
        let decompressed = engine.decompress(&result.compressed_data, result.algorithm).unwrap();
        assert_eq!(decompressed, data);
    }
    
    #[test]
    fn test_compression_small_data() {
        let engine = CompressionEngine::new();
        let data = b"small";
        
        let result = engine.compress(data).unwrap();
        // Small data should not be compressed
        assert_eq!(result.algorithm, CompressionAlgorithm::None);
        assert_eq!(result.compressed_data, data);
    }
    
    #[test]
    fn test_compression_analysis() {
        let engine = CompressionEngine::new();
        let samples = vec![
            b"Hello, World!".as_slice(),
            b"This is a test".as_slice(),
            b"Another sample data".as_slice(),
        ];
        
        let analysis = engine.analyze_compression(&samples);
        assert_eq!(analysis.total_samples, 3);
        assert!(analysis.average_compression_ratio <= 1.0);
    }
}
