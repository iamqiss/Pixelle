// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// AI-driven compression with smart algorithm selection

use std::collections::HashMap;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::errors::{NimbuxError, Result};

/// Compression algorithm types supported by Nimbux
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CompressionAlgorithm {
    Gzip,
    Zstd,
    Lz4,
    Brotli,
    Lzma,
    Bzip2,
    Snappy,
    Zlib,
    Deflate,
    Lz77,
    Custom(String),
}

impl CompressionAlgorithm {
    pub fn as_str(&self) -> &str {
        match self {
            CompressionAlgorithm::Gzip => "gzip",
            CompressionAlgorithm::Zstd => "zstd",
            CompressionAlgorithm::Lz4 => "lz4",
            CompressionAlgorithm::Brotli => "brotli",
            CompressionAlgorithm::Lzma => "lzma",
            CompressionAlgorithm::Bzip2 => "bzip2",
            CompressionAlgorithm::Snappy => "snappy",
            CompressionAlgorithm::Zlib => "zlib",
            CompressionAlgorithm::Deflate => "deflate",
            CompressionAlgorithm::Lz77 => "lz77",
            CompressionAlgorithm::Custom(name) => name,
        }
    }
    
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "gzip" => Some(CompressionAlgorithm::Gzip),
            "zstd" => Some(CompressionAlgorithm::Zstd),
            "lz4" => Some(CompressionAlgorithm::Lz4),
            "brotli" => Some(CompressionAlgorithm::Brotli),
            "lzma" => Some(CompressionAlgorithm::Lzma),
            "bzip2" => Some(CompressionAlgorithm::Bzip2),
            "snappy" => Some(CompressionAlgorithm::Snappy),
            "zlib" => Some(CompressionAlgorithm::Zlib),
            "deflate" => Some(CompressionAlgorithm::Deflate),
            "lz77" => Some(CompressionAlgorithm::Lz77),
            _ => Some(CompressionAlgorithm::Custom(s.to_string())),
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub level: u32,
    pub threshold: u64,
    pub max_compression_time_ms: u64,
    pub enable_ai_selection: bool,
    pub fallback_algorithm: Option<CompressionAlgorithm>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            level: 3,
            threshold: 1024, // 1KB minimum size to compress
            max_compression_time_ms: 5000, // 5 seconds max
            enable_ai_selection: true,
            fallback_algorithm: Some(CompressionAlgorithm::Gzip),
        }
    }
}

/// Compression result with detailed metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionResult {
    pub algorithm: CompressionAlgorithm,
    pub original_size: u64,
    pub compressed_size: u64,
    pub compression_ratio: f64,
    pub compression_time_ms: u64,
    pub decompression_time_ms: u64,
    pub compression_speed_mbps: f64,
    pub decompression_speed_mbps: f64,
    pub quality_score: f64,
    pub energy_efficiency: f64,
    pub memory_usage: u64,
    pub success: bool,
    pub error: Option<String>,
}

/// Data characteristics for AI analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCharacteristics {
    pub entropy: f64,
    pub repetition_ratio: f64,
    pub pattern_density: f64,
    pub byte_frequency: HashMap<u8, u64>,
    pub data_type: DataType,
    pub structure_complexity: f64,
    pub compressibility_score: f64,
    pub optimal_algorithm: Option<CompressionAlgorithm>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Text,
    Binary,
    Image,
    Video,
    Audio,
    Archive,
    Database,
    Log,
    Json,
    Xml,
    Csv,
    Unknown,
}

/// AI compression analyzer
pub struct AICompressionAnalyzer {
    models: HashMap<String, CompressionModel>,
    performance_history: HashMap<String, Vec<CompressionResult>>,
    learning_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionModel {
    pub id: String,
    pub name: String,
    pub algorithm: CompressionAlgorithm,
    pub accuracy: f64,
    pub training_data_size: u64,
    pub last_updated: DateTime<Utc>,
    pub parameters: HashMap<String, f64>,
}

impl AICompressionAnalyzer {
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
            performance_history: HashMap::new(),
            learning_enabled: true,
        }
    }
    
    /// Analyze data characteristics to determine optimal compression
    pub async fn analyze_data(&self, data: &[u8]) -> Result<DataCharacteristics> {
        let entropy = self.calculate_entropy(data);
        let repetition_ratio = self.calculate_repetition_ratio(data);
        let pattern_density = self.calculate_pattern_density(data);
        let byte_frequency = self.calculate_byte_frequency(data);
        let data_type = self.detect_data_type(data);
        let structure_complexity = self.calculate_structure_complexity(data);
        let compressibility_score = self.calculate_compressibility_score(data);
        
        Ok(DataCharacteristics {
            entropy,
            repetition_ratio,
            pattern_density,
            byte_frequency,
            data_type,
            structure_complexity,
            compressibility_score,
            optimal_algorithm: None, // Will be determined by AI model
        })
    }
    
    /// Select optimal compression algorithm using AI
    pub async fn select_optimal_algorithm(
        &self,
        data: &[u8],
        characteristics: &DataCharacteristics,
        requirements: &CompressionRequirements,
    ) -> Result<CompressionAlgorithm> {
        if !self.learning_enabled {
            return Ok(CompressionAlgorithm::Zstd);
        }
        
        // Use AI models to predict optimal algorithm
        let mut scores = HashMap::new();
        
        for (model_id, model) in &self.models {
            if let Some(score) = self.predict_compression_score(model, characteristics, requirements).await? {
                scores.insert(model_id.clone(), score);
            }
        }
        
        // Select algorithm with highest score
        let best_model = scores.iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(id, _)| id);
        
        match best_model {
            Some(model_id) => {
                if let Some(model) = self.models.get(model_id) {
                    Ok(model.algorithm.clone())
                } else {
                    Ok(CompressionAlgorithm::Zstd)
                }
            }
            None => Ok(CompressionAlgorithm::Zstd),
        }
    }
    
    /// Compress data with AI-optimized algorithm selection
    pub async fn compress_with_ai(
        &mut self,
        data: &[u8],
        config: &CompressionConfig,
    ) -> Result<CompressionResult> {
        let start_time = Instant::now();
        
        // Analyze data characteristics
        let characteristics = self.analyze_data(data).await?;
        
        // Determine requirements
        let requirements = CompressionRequirements {
            max_compression_time_ms: config.max_compression_time_ms,
            min_compression_ratio: 0.1, // At least 10% compression
            priority: CompressionPriority::Balanced,
        };
        
        // Select optimal algorithm
        let algorithm = if config.enable_ai_selection {
            self.select_optimal_algorithm(data, &characteristics, &requirements).await?
        } else {
            config.algorithm.clone()
        };
        
        // Compress data
        let compression_result = self.compress_data(data, &algorithm, config.level).await?;
        
        // Record performance for learning
        if self.learning_enabled {
            self.record_performance(&algorithm, &compression_result).await;
        }
        
        Ok(compression_result)
    }
    
    /// Compress data with specific algorithm
    async fn compress_data(
        &self,
        data: &[u8],
        algorithm: &CompressionAlgorithm,
        level: u32,
    ) -> Result<CompressionResult> {
        let start_time = Instant::now();
        let original_size = data.len() as u64;
        
        let compressed_data = match algorithm {
            CompressionAlgorithm::Gzip => {
                self.compress_gzip(data, level)?
            }
            CompressionAlgorithm::Zstd => {
                self.compress_zstd(data, level)?
            }
            CompressionAlgorithm::Lz4 => {
                self.compress_lz4(data, level)?
            }
            CompressionAlgorithm::Brotli => {
                self.compress_brotli(data, level)?
            }
            CompressionAlgorithm::Lzma => {
                self.compress_lzma(data, level)?
            }
            CompressionAlgorithm::Bzip2 => {
                self.compress_bzip2(data, level)?
            }
            CompressionAlgorithm::Snappy => {
                self.compress_snappy(data)?
            }
            CompressionAlgorithm::Zlib => {
                self.compress_zlib(data, level)?
            }
            CompressionAlgorithm::Deflate => {
                self.compress_deflate(data, level)?
            }
            CompressionAlgorithm::Lz77 => {
                self.compress_lz77(data)?
            }
            CompressionAlgorithm::Custom(_) => {
                return Err(NimbuxError::Compression("Custom compression algorithm not implemented".to_string()));
            }
        };
        
        let compression_time = start_time.elapsed();
        let compressed_size = compressed_data.len() as u64;
        let compression_ratio = compressed_size as f64 / original_size as f64;
        
        // Test decompression speed
        let decompress_start = Instant::now();
        let _ = self.decompress_data(&compressed_data, algorithm)?;
        let decompression_time = decompress_start.elapsed();
        
        let compression_speed = (original_size as f64 / 1024.0 / 1024.0) / (compression_time.as_secs_f64());
        let decompression_speed = (original_size as f64 / 1024.0 / 1024.0) / (decompression_time.as_secs_f64());
        
        Ok(CompressionResult {
            algorithm: algorithm.clone(),
            original_size,
            compressed_size,
            compression_ratio,
            compression_time_ms: compression_time.as_millis() as u64,
            decompression_time_ms: decompression_time.as_millis() as u64,
            compression_speed_mbps: compression_speed,
            decompression_speed_mbps: decompression_speed,
            quality_score: self.calculate_quality_score(compression_ratio, compression_time.as_millis() as u64),
            energy_efficiency: self.calculate_energy_efficiency(compression_ratio, compression_time.as_millis() as u64),
            memory_usage: self.estimate_memory_usage(original_size),
            success: true,
            error: None,
        })
    }
    
    /// Decompress data
    async fn decompress_data(
        &self,
        compressed_data: &[u8],
        algorithm: &CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        match algorithm {
            CompressionAlgorithm::Gzip => self.decompress_gzip(compressed_data),
            CompressionAlgorithm::Zstd => self.decompress_zstd(compressed_data),
            CompressionAlgorithm::Lz4 => self.decompress_lz4(compressed_data),
            CompressionAlgorithm::Brotli => self.decompress_brotli(compressed_data),
            CompressionAlgorithm::Lzma => self.decompress_lzma(compressed_data),
            CompressionAlgorithm::Bzip2 => self.decompress_bzip2(compressed_data),
            CompressionAlgorithm::Snappy => self.decompress_snappy(compressed_data),
            CompressionAlgorithm::Zlib => self.decompress_zlib(compressed_data),
            CompressionAlgorithm::Deflate => self.decompress_deflate(compressed_data),
            CompressionAlgorithm::Lz77 => self.decompress_lz77(compressed_data),
            CompressionAlgorithm::Custom(_) => {
                Err(NimbuxError::Compression("Custom decompression algorithm not implemented".to_string()))
            }
        }
    }
    
    // Individual compression implementations
    fn compress_gzip(&self, data: &[u8], level: u32) -> Result<Vec<u8>> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
    
    fn compress_zstd(&self, data: &[u8], level: u32) -> Result<Vec<u8>> {
        use zstd::encode_all;
        Ok(encode_all(data, level as i32)?)
    }
    
    fn compress_lz4(&self, data: &[u8], _level: u32) -> Result<Vec<u8>> {
        use lz4_flex::compress;
        Ok(compress(data))
    }
    
    fn compress_brotli(&self, data: &[u8], level: u32) -> Result<Vec<u8>> {
        use brotli::enc::BrotliEncoderParams;
        use brotli::enc::encode;
        
        let mut params = BrotliEncoderParams::default();
        params.quality = level as i32;
        
        let mut output = Vec::new();
        encode(data, &mut output, &params)?;
        Ok(output)
    }
    
    fn compress_lzma(&self, data: &[u8], _level: u32) -> Result<Vec<u8>> {
        // TODO: Implement LZMA compression
        Err(NimbuxError::Compression("LZMA compression not yet implemented".to_string()))
    }
    
    fn compress_bzip2(&self, data: &[u8], level: u32) -> Result<Vec<u8>> {
        use bzip2::write::BzEncoder;
        use bzip2::Compression;
        use std::io::Write;
        
        let mut encoder = BzEncoder::new(Vec::new(), Compression::new(level as u32));
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
    
    fn compress_snappy(&self, data: &[u8]) -> Result<Vec<u8>> {
        // TODO: Implement Snappy compression
        Err(NimbuxError::Compression("Snappy compression not yet implemented".to_string()))
    }
    
    fn compress_zlib(&self, data: &[u8], level: u32) -> Result<Vec<u8>> {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(level));
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
    
    fn compress_deflate(&self, data: &[u8], level: u32) -> Result<Vec<u8>> {
        use flate2::write::DeflateEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(level));
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
    
    fn compress_lz77(&self, data: &[u8]) -> Result<Vec<u8>> {
        // TODO: Implement LZ77 compression
        Err(NimbuxError::Compression("LZ77 compression not yet implemented".to_string()))
    }
    
    // Decompression implementations
    fn decompress_gzip(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::GzDecoder;
        use std::io::Read;
        
        let mut decoder = GzDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)?;
        Ok(result)
    }
    
    fn decompress_zstd(&self, data: &[u8]) -> Result<Vec<u8>> {
        use zstd::decode_all;
        Ok(decode_all(data)?)
    }
    
    fn decompress_lz4(&self, data: &[u8]) -> Result<Vec<u8>> {
        use lz4_flex::decompress;
        Ok(decompress(data, data.len() * 4)?) // Estimate decompressed size
    }
    
    fn decompress_brotli(&self, data: &[u8]) -> Result<Vec<u8>> {
        use brotli::Decompressor;
        use std::io::Read;
        
        let mut decoder = Decompressor::new(data, 4096);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)?;
        Ok(result)
    }
    
    fn decompress_lzma(&self, _data: &[u8]) -> Result<Vec<u8>> {
        Err(NimbuxError::Compression("LZMA decompression not yet implemented".to_string()))
    }
    
    fn decompress_bzip2(&self, data: &[u8]) -> Result<Vec<u8>> {
        use bzip2::read::BzDecoder;
        use std::io::Read;
        
        let mut decoder = BzDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)?;
        Ok(result)
    }
    
    fn decompress_snappy(&self, _data: &[u8]) -> Result<Vec<u8>> {
        Err(NimbuxError::Compression("Snappy decompression not yet implemented".to_string()))
    }
    
    fn decompress_zlib(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::ZlibDecoder;
        use std::io::Read;
        
        let mut decoder = ZlibDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)?;
        Ok(result)
    }
    
    fn decompress_deflate(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::DeflateDecoder;
        use std::io::Read;
        
        let mut decoder = DeflateDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)?;
        Ok(result)
    }
    
    fn decompress_lz77(&self, _data: &[u8]) -> Result<Vec<u8>> {
        Err(NimbuxError::Compression("LZ77 decompression not yet implemented".to_string()))
    }
    
    // Analysis methods
    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        
        let mut frequency = [0u64; 256];
        for &byte in data {
            frequency[byte as usize] += 1;
        }
        
        let mut entropy = 0.0;
        let data_len = data.len() as f64;
        
        for &count in &frequency {
            if count > 0 {
                let probability = count as f64 / data_len;
                entropy -= probability * probability.log2();
            }
        }
        
        entropy
    }
    
    fn calculate_repetition_ratio(&self, data: &[u8]) -> f64 {
        if data.len() < 2 {
            return 0.0;
        }
        
        let mut repeated_bytes = 0;
        for i in 1..data.len() {
            if data[i] == data[i - 1] {
                repeated_bytes += 1;
            }
        }
        
        repeated_bytes as f64 / (data.len() - 1) as f64
    }
    
    fn calculate_pattern_density(&self, data: &[u8]) -> f64 {
        // Simple pattern detection - look for repeated sequences
        let mut patterns = 0;
        let min_pattern_len = 3;
        
        for len in min_pattern_len..=data.len() / 2 {
            for i in 0..=data.len() - len * 2 {
                let pattern = &data[i..i + len];
                for j in i + len..=data.len() - len {
                    if &data[j..j + len] == pattern {
                        patterns += 1;
                    }
                }
            }
        }
        
        patterns as f64 / data.len() as f64
    }
    
    fn calculate_byte_frequency(&self, data: &[u8]) -> HashMap<u8, u64> {
        let mut frequency = HashMap::new();
        for &byte in data {
            *frequency.entry(byte).or_insert(0) += 1;
        }
        frequency
    }
    
    fn detect_data_type(&self, data: &[u8]) -> DataType {
        if data.is_empty() {
            return DataType::Unknown;
        }
        
        // Check for common file signatures
        if data.starts_with(b"\x89PNG") {
            return DataType::Image;
        }
        if data.starts_with(b"\xFF\xD8\xFF") {
            return DataType::Image;
        }
        if data.starts_with(b"GIF8") {
            return DataType::Image;
        }
        if data.starts_with(b"RIFF") && data.len() > 8 && &data[8..12] == b"AVI " {
            return DataType::Video;
        }
        if data.starts_with(b"\x1F\x8B") {
            return DataType::Archive;
        }
        if data.starts_with(b"PK\x03\x04") {
            return DataType::Archive;
        }
        
        // Check if it's text
        if data.iter().all(|&b| b.is_ascii() && (b.is_ascii_alphanumeric() || b.is_ascii_whitespace() || b.is_ascii_punctuation())) {
            if data.starts_with(b"{") || data.starts_with(b"[") {
                return DataType::Json;
            }
            if data.starts_with(b"<") {
                return DataType::Xml;
            }
            if data.contains(&b',') && data.contains(&b'\n') {
                return DataType::Csv;
            }
            return DataType::Text;
        }
        
        DataType::Binary
    }
    
    fn calculate_structure_complexity(&self, data: &[u8]) -> f64 {
        // Calculate structural complexity based on patterns and organization
        let entropy = self.calculate_entropy(data);
        let pattern_density = self.calculate_pattern_density(data);
        
        // Higher entropy and lower pattern density = higher complexity
        entropy * (1.0 - pattern_density)
    }
    
    fn calculate_compressibility_score(&self, data: &[u8]) -> f64 {
        let entropy = self.calculate_entropy(data);
        let repetition_ratio = self.calculate_repetition_ratio(data);
        let pattern_density = self.calculate_pattern_density(data);
        
        // Lower entropy, higher repetition, higher pattern density = more compressible
        (1.0 - entropy / 8.0) * (0.5 + repetition_ratio) * (0.5 + pattern_density)
    }
    
    fn calculate_quality_score(&self, compression_ratio: f64, compression_time_ms: u64) -> f64 {
        // Quality score based on compression ratio and speed
        let ratio_score = compression_ratio;
        let speed_score = 1.0 / (1.0 + compression_time_ms as f64 / 1000.0);
        
        (ratio_score + speed_score) / 2.0
    }
    
    fn calculate_energy_efficiency(&self, compression_ratio: f64, compression_time_ms: u64) -> f64 {
        // Energy efficiency = compression ratio / time
        compression_ratio / (1.0 + compression_time_ms as f64 / 1000.0)
    }
    
    fn estimate_memory_usage(&self, data_size: u64) -> u64 {
        // Estimate memory usage for compression (typically 2-3x data size)
        data_size * 3
    }
    
    async fn predict_compression_score(
        &self,
        model: &CompressionModel,
        characteristics: &DataCharacteristics,
        requirements: &CompressionRequirements,
    ) -> Result<Option<f64>> {
        // TODO: Implement AI model prediction
        // For now, return a simple heuristic score
        let base_score = characteristics.compressibility_score;
        let time_penalty = if requirements.max_compression_time_ms < 1000 { 0.1 } else { 0.0 };
        
        Ok(Some(base_score - time_penalty))
    }
    
    async fn record_performance(&mut self, algorithm: &CompressionAlgorithm, result: &CompressionResult) {
        let key = algorithm.as_str().to_string();
        self.performance_history
            .entry(key)
            .or_insert_with(Vec::new)
            .push(result.clone());
        
        // Keep only last 1000 results per algorithm
        if let Some(history) = self.performance_history.get_mut(algorithm.as_str()) {
            if history.len() > 1000 {
                history.drain(0..history.len() - 1000);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionRequirements {
    pub max_compression_time_ms: u64,
    pub min_compression_ratio: f64,
    pub priority: CompressionPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionPriority {
    Speed,
    Ratio,
    Balanced,
    Quality,
}

/// Compression manager for handling all compression operations
pub struct CompressionManager {
    analyzer: AICompressionAnalyzer,
    config: CompressionConfig,
}

impl CompressionManager {
    pub fn new(config: CompressionConfig) -> Self {
        Self {
            analyzer: AICompressionAnalyzer::new(),
            config,
        }
    }
    
    /// Compress data with AI optimization
    pub async fn compress(&mut self, data: &[u8]) -> Result<CompressionResult> {
        self.analyzer.compress_with_ai(data, &self.config).await
    }
    
    /// Decompress data
    pub async fn decompress(&self, compressed_data: &[u8], algorithm: &CompressionAlgorithm) -> Result<Vec<u8>> {
        self.analyzer.decompress_data(compressed_data, algorithm).await
    }
    
    /// Get compression statistics
    pub fn get_statistics(&self) -> CompressionStatistics {
        let mut total_compressions = 0;
        let mut total_original_size = 0;
        let mut total_compressed_size = 0;
        let mut algorithm_stats = HashMap::new();
        
        for (algorithm, results) in &self.analyzer.performance_history {
            let count = results.len() as u64;
            let original_size: u64 = results.iter().map(|r| r.original_size).sum();
            let compressed_size: u64 = results.iter().map(|r| r.compressed_size).sum();
            let avg_ratio: f64 = results.iter().map(|r| r.compression_ratio).sum::<f64>() / count as f64;
            let avg_time: f64 = results.iter().map(|r| r.compression_time_ms).sum::<u64>() as f64 / count as f64;
            
            algorithm_stats.insert(algorithm.clone(), AlgorithmStats {
                count,
                total_original_size: original_size,
                total_compressed_size: compressed_size,
                average_ratio: avg_ratio,
                average_time_ms: avg_time,
                success_rate: results.iter().filter(|r| r.success).count() as f64 / count as f64,
            });
            
            total_compressions += count;
            total_original_size += original_size;
            total_compressed_size += compressed_size;
        }
        
        CompressionStatistics {
            total_compressions,
            total_original_size,
            total_compressed_size,
            overall_compression_ratio: if total_original_size > 0 {
                total_compressed_size as f64 / total_original_size as f64
            } else {
                0.0
            },
            algorithm_stats,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStatistics {
    pub total_compressions: u64,
    pub total_original_size: u64,
    pub total_compressed_size: u64,
    pub overall_compression_ratio: f64,
    pub algorithm_stats: HashMap<String, AlgorithmStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmStats {
    pub count: u64,
    pub total_original_size: u64,
    pub total_compressed_size: u64,
    pub average_ratio: f64,
    pub average_time_ms: f64,
    pub success_rate: f64,
}