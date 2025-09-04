// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Serialization performance statistics and monitoring

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Serialization performance statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SerializationStats {
    pub total_serializations: u64,
    pub total_deserializations: u64,
    pub bytes_serialized: u64,
    pub bytes_deserialized: u64,
    pub avg_serialization_time_ns: u64,
    pub avg_deserialization_time_ns: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub compression_ratio: f32,
    pub error_count: u64,
}

impl SerializationStats {
    /// Calculate throughput in operations per second
    pub fn serialization_ops_per_sec(&self, elapsed: Duration) -> f64 {
        if elapsed.as_secs_f64() > 0.0 {
            self.total_serializations as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        }
    }
    
    /// Calculate deserialization throughput in operations per second
    pub fn deserialization_ops_per_sec(&self, elapsed: Duration) -> f64 {
        if elapsed.as_secs_f64() > 0.0 {
            self.total_deserializations as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        }
    }
    
    /// Calculate serialization throughput in MB/s
    pub fn serialization_mbps(&self, elapsed: Duration) -> f64 {
        if elapsed.as_secs_f64() > 0.0 {
            (self.bytes_serialized as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
        } else {
            0.0
        }
    }
    
    /// Calculate deserialization throughput in MB/s
    pub fn deserialization_mbps(&self, elapsed: Duration) -> f64 {
        if elapsed.as_secs_f64() > 0.0 {
            (self.bytes_deserialized as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
        } else {
            0.0
        }
    }
    
    /// Calculate cache hit ratio
    pub fn cache_hit_ratio(&self) -> f32 {
        let total_requests = self.cache_hits + self.cache_misses;
        if total_requests > 0 {
            self.cache_hits as f32 / total_requests as f32
        } else {
            0.0
        }
    }
    
    /// Calculate average document size for serialization
    pub fn avg_serialized_document_size(&self) -> f64 {
        if self.total_serializations > 0 {
            self.bytes_serialized as f64 / self.total_serializations as f64
        } else {
            0.0
        }
    }
    
    /// Calculate average document size for deserialization
    pub fn avg_deserialized_document_size(&self) -> f64 {
        if self.total_deserializations > 0 {
            self.bytes_deserialized as f64 / self.total_deserializations as f64
        } else {
            0.0
        }
    }
    
    /// Calculate error rate
    pub fn error_rate(&self) -> f32 {
        let total_operations = self.total_serializations + self.total_deserializations;
        if total_operations > 0 {
            self.error_count as f32 / total_operations as f32
        } else {
            0.0
        }
    }
    
    /// Reset all statistics
    pub fn reset(&mut self) {
        *self = SerializationStats::default();
    }
    
    /// Merge statistics from another instance
    pub fn merge(&mut self, other: &SerializationStats) {
        self.total_serializations += other.total_serializations;
        self.total_deserializations += other.total_deserializations;
        self.bytes_serialized += other.bytes_serialized;
        self.bytes_deserialized += other.bytes_deserialized;
        self.cache_hits += other.cache_hits;
        self.cache_misses += other.cache_misses;
        self.error_count += other.error_count;
        
        // Average the timing statistics
        if other.total_serializations > 0 {
            self.avg_serialization_time_ns = 
                (self.avg_serialization_time_ns + other.avg_serialization_time_ns) / 2;
        }
        
        if other.total_deserializations > 0 {
            self.avg_deserialization_time_ns = 
                (self.avg_deserialization_time_ns + other.avg_deserialization_time_ns) / 2;
        }
        
        // Average compression ratio
        self.compression_ratio = (self.compression_ratio + other.compression_ratio) / 2.0;
    }
}

/// Performance benchmark results comparing to MongoDB BSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub largetable_serialization_ns: u64,
    pub largetable_deserialization_ns: u64,
    pub mongodb_bson_serialization_ns: u64,
    pub mongodb_bson_deserialization_ns: u64,
    pub speedup_serialization: f32,
    pub speedup_deserialization: f32,
    pub memory_savings_percent: f32,
    pub document_count: usize,
    pub avg_document_size_bytes: usize,
}

impl BenchmarkResults {
    /// Create new benchmark results
    pub fn new(
        largetable_ser: u64,
        largetable_deser: u64,
        mongodb_ser: u64,
        mongodb_deser: u64,
        doc_count: usize,
        doc_size: usize,
    ) -> Self {
        let ser_speedup = if largetable_ser > 0 {
            mongodb_ser as f32 / largetable_ser as f32
        } else {
            0.0
        };
        
        let deser_speedup = if largetable_deser > 0 {
            mongodb_deser as f32 / largetable_deser as f32
        } else {
            0.0
        };
        
        Self {
            largetable_serialization_ns: largetable_ser,
            largetable_deserialization_ns: largetable_deser,
            mongodb_bson_serialization_ns: mongodb_ser,
            mongodb_bson_deserialization_ns: mongodb_deser,
            speedup_serialization: ser_speedup,
            speedup_deserialization: deser_speedup,
            memory_savings_percent: 0.0, // Would be calculated based on actual memory usage
            document_count: doc_count,
            avg_document_size_bytes: doc_size,
        }
    }
    
    /// Print benchmark results in a nice format
    pub fn print_results(&self) {
        println!("🚀 Zero-Copy Serialization Benchmark Results");
        println!("============================================");
        println!("📊 Test Configuration:");
        println!("   Documents: {}", self.document_count);
        println!("   Avg Size:  {} bytes", self.avg_document_size_bytes);
        println!();
        println!("⚡ Performance Comparison:");
        println!("   Serialization Speedup:   {:.2}x faster", self.speedup_serialization);
        println!("   Deserialization Speedup: {:.2}x faster", self.speedup_deserialization);
        println!("   Memory Savings:          {:.1}%", self.memory_savings_percent);
        println!();
        println!("🕐 Detailed Timings (nanoseconds):");
        println!("   Largetable Serialization:   {:>10}", self.largetable_serialization_ns);
        println!("   MongoDB BSON Serialization: {:>10}", self.mongodb_bson_serialization_ns);
        println!("   Largetable Deserialization: {:>10}", self.largetable_deserialization_ns);
        println!("   MongoDB BSON Deserialization: {:>10}", self.mongodb_bson_deserialization_ns);
    }
}

/// Statistics collector for continuous monitoring
pub struct StatsCollector {
    start_time: Instant,
    snapshots: Vec<(Instant, SerializationStats)>,
}

impl StatsCollector {
    /// Create a new statistics collector
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            snapshots: Vec::new(),
        }
    }
    
    /// Take a snapshot of current statistics
    pub fn snapshot(&mut self, stats: &SerializationStats) {
        self.snapshots.push((Instant::now(), stats.clone()));
    }
    
    /// Get statistics over a time window
    pub fn get_windowed_stats(&self, window: Duration) -> Option<SerializationStats> {
        let now = Instant::now();
        let cutoff = now - window;
        
        // Find snapshots within the window
        let recent_snapshots: Vec<_> = self.snapshots
            .iter()
            .filter(|(time, _)| *time >= cutoff)
            .collect();
        
        if recent_snapshots.is_empty() {
            return None;
        }
        
        // Calculate windowed statistics
        let mut windowed_stats = SerializationStats::default();
        for (_, stats) in recent_snapshots {
            windowed_stats.merge(stats);
        }
        
        Some(windowed_stats)
    }
    
    /// Get total elapsed time since collection started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
    
    /// Clear old snapshots to prevent memory growth
    pub fn cleanup_old_snapshots(&mut self, retention: Duration) {
        let now = Instant::now();
        // Retain only snapshots newer than (now - retention)
        self.snapshots
            .retain(|(time, _)| *time >= now - retention);
    }
}
