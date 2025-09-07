// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Enterprise-grade example demonstrating high-traffic, high-concurrency features

use largetable::{
    engine::{DatabaseEngine, StorageEngine},
    config::enterprise::{EnterpriseConfigManager, PerformancePreset},
    document::DocumentBuilder,
    query::{QueryBuilder, SortDirection},
    Result,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn, error, debug};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting Largetable Enterprise Example");
    
    // Load enterprise configuration
    let config_manager = EnterpriseConfigManager::with_preset(
        PerformancePreset::HighTraffic
    )?;
    let config = config_manager.get_config().await;
    
    info!("Loaded enterprise configuration: {:?}", config.performance);
    
    // Create database engine with enterprise features
    let engine = DatabaseEngine::with_default_storage_engine(
        StorageEngine::Lsm
    ).await?;
    
    info!("Database engine initialized with enterprise features");
    
    // Demonstrate connection pooling
    await connection_pooling_demo(&engine).await?;
    
    // Demonstrate caching
    await caching_demo(&engine).await?;
    
    // Demonstrate memory management
    await memory_management_demo(&engine).await?;
    
    // Demonstrate auto-scaling
    await auto_scaling_demo(&engine).await?;
    
    // Demonstrate monitoring
    await monitoring_demo(&engine).await?;
    
    // Demonstrate high-concurrency operations
    await high_concurrency_demo(&engine).await?;
    
    // Demonstrate performance benchmarks
    await performance_benchmarks(&engine).await?;
    
    info!("Enterprise example completed successfully");
    Ok(())
}

/// Demonstrate high-performance connection pooling
async fn connection_pooling_demo(engine: &DatabaseEngine) -> Result<()> {
    info!("=== Connection Pooling Demo ===");
    
    // Get connection pool statistics
    let pool_stats = engine.get_connection_pool_stats().await;
    info!("Connection pool stats: {:?}", pool_stats);
    
    // Simulate high-concurrency connection usage
    let mut handles = Vec::new();
    
    for i in 0..100 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let connection = engine_clone.get_connection().await?;
            info!("Got connection {}: {}", i, connection.id());
            
            // Simulate some work
            sleep(Duration::from_millis(10)).await;
            
            // Connection is automatically returned to pool when dropped
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }
    
    // Wait for all connections to complete
    for handle in handles {
        handle.await??;
    }
    
    // Get updated statistics
    let updated_stats = engine.get_connection_pool_stats().await;
    info!("Updated connection pool stats: {:?}", updated_stats);
    
    Ok(())
}

/// Demonstrate multi-level caching
async fn caching_demo(engine: &DatabaseEngine) -> Result<()> {
    info!("=== Multi-Level Caching Demo ===");
    
    // Get cache statistics
    let cache_stats = engine.get_cache_stats().await;
    info!("Cache stats: {:?}", cache_stats);
    
    // Create some test data
    let test_documents = vec![
        DocumentBuilder::new()
            .string("name", "John Doe")
            .int("age", 30)
            .string("email", "john@example.com")
            .build(),
        DocumentBuilder::new()
            .string("name", "Jane Smith")
            .int("age", 25)
            .string("email", "jane@example.com")
            .build(),
    ];
    
    // Insert documents (will be cached)
    for (i, doc) in test_documents.iter().enumerate() {
        let doc_id = engine.insert_document(
            "test_db".to_string(),
            "users".to_string(),
            doc.clone()
        ).await?;
        info!("Inserted document {} with ID: {}", i, doc_id);
    }
    
    // Perform repeated reads to demonstrate caching
    let start_time = Instant::now();
    
    for _ in 0..1000 {
        let _doc = engine.find_document_by_id(
            "test_db".to_string(),
            "users".to_string(),
            "doc_1".to_string()
        ).await?;
    }
    
    let elapsed = start_time.elapsed();
    info!("1000 reads completed in {:?} (cached)", elapsed);
    
    // Warm cache with frequently accessed keys
    let frequent_keys = vec![
        "frequent_key_1".to_string(),
        "frequent_key_2".to_string(),
        "frequent_key_3".to_string(),
    ];
    
    engine.warm_cache(frequent_keys).await?;
    info!("Cache warmed with frequent keys");
    
    // Get updated cache statistics
    let updated_stats = engine.get_cache_stats().await;
    info!("Updated cache stats: {:?}", updated_stats);
    
    Ok(())
}

/// Demonstrate advanced memory management
async fn memory_management_demo(engine: &DatabaseEngine) -> Result<()> {
    info!("=== Memory Management Demo ===");
    
    // Get memory statistics
    let memory_stats = engine.get_memory_stats().await;
    info!("Memory stats: {:?}", memory_stats);
    
    // Simulate memory-intensive operations
    let mut large_documents = Vec::new();
    
    for i in 0..1000 {
        let large_doc = DocumentBuilder::new()
            .string("id", &format!("large_doc_{}", i))
            .string("data", &"x".repeat(10000)) // 10KB of data
            .array("numbers", (0..1000).collect())
            .build();
        
        large_documents.push(large_doc);
    }
    
    info!("Created {} large documents", large_documents.len());
    
    // Insert large documents
    for doc in large_documents {
        engine.insert_document(
            "test_db".to_string(),
            "large_data".to_string(),
            doc
        ).await?;
    }
    
    // Get memory statistics after insertion
    let memory_stats_after = engine.get_memory_stats().await;
    info!("Memory stats after insertion: {:?}", memory_stats_after);
    
    // Force garbage collection
    engine.force_gc().await?;
    info!("Forced garbage collection");
    
    // Compact memory
    engine.compact_memory().await?;
    info!("Memory compaction completed");
    
    // Get final memory statistics
    let final_memory_stats = engine.get_memory_stats().await;
    info!("Final memory stats: {:?}", final_memory_stats);
    
    Ok(())
}

/// Demonstrate intelligent auto-scaling
async fn auto_scaling_demo(engine: &DatabaseEngine) -> Result<()> {
    info!("=== Auto-Scaling Demo ===");
    
    // Get auto-scaling statistics
    let scaling_stats = engine.get_auto_scaling_stats().await;
    info!("Auto-scaling stats: {:?}", scaling_stats);
    
    // Simulate high load to trigger scaling
    let mut handles = Vec::new();
    
    for i in 0..1000 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            // Simulate CPU-intensive operation
            let start = Instant::now();
            while start.elapsed() < Duration::from_millis(100) {
                // Busy work
                let _ = (0..1000).sum::<usize>();
            }
            
            // Perform database operation
            let _ = engine_clone.find_document_by_id(
                "test_db".to_string(),
                "users".to_string(),
                format!("doc_{}", i % 10)
            ).await;
            
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }
    
    // Wait for some operations to complete
    for handle in handles.into_iter().take(100) {
        handle.await??;
    }
    
    // Get updated scaling statistics
    let updated_scaling_stats = engine.get_auto_scaling_stats().await;
    info!("Updated auto-scaling stats: {:?}", updated_scaling_stats);
    
    Ok(())
}

/// Demonstrate real-time monitoring
async fn monitoring_demo(engine: &DatabaseEngine) -> Result<()> {
    info!("=== Monitoring Demo ===");
    
    // Get all statistics
    let pool_stats = engine.get_connection_pool_stats().await;
    let cache_stats = engine.get_cache_stats().await;
    let memory_stats = engine.get_memory_stats().await;
    let scaling_stats = engine.get_auto_scaling_stats().await;
    
    info!("=== Real-Time Statistics ===");
    info!("Connection Pool: {:?}", pool_stats);
    info!("Cache: {:?}", cache_stats);
    info!("Memory: {:?}", memory_stats);
    info!("Auto-Scaling: {:?}", scaling_stats);
    
    // Simulate monitoring over time
    for i in 0..5 {
        info!("Monitoring iteration {}", i);
        
        // Perform some operations
        for j in 0..100 {
            let _ = engine.find_document_by_id(
                "test_db".to_string(),
                "users".to_string(),
                format!("doc_{}", j)
            ).await;
        }
        
        // Get updated statistics
        let updated_pool_stats = engine.get_connection_pool_stats().await;
        let updated_cache_stats = engine.get_cache_stats().await;
        
        info!("Updated stats - Pool: {:?}, Cache: {:?}", 
              updated_pool_stats, updated_cache_stats);
        
        sleep(Duration::from_secs(1)).await;
    }
    
    Ok(())
}

/// Demonstrate high-concurrency operations
async fn high_concurrency_demo(engine: &DatabaseEngine) -> Result<()> {
    info!("=== High-Concurrency Demo ===");
    
    let start_time = Instant::now();
    
    // Simulate 10,000 concurrent operations
    let mut handles = Vec::new();
    
    for i in 0..10000 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            // Random operation type
            match i % 4 {
                0 => {
                    // Read operation
                    let _ = engine_clone.find_document_by_id(
                        "test_db".to_string(),
                        "users".to_string(),
                        format!("doc_{}", i % 10)
                    ).await;
                }
                1 => {
                    // Write operation
                    let doc = DocumentBuilder::new()
                        .string("name", &format!("user_{}", i))
                        .int("value", i as i64)
                        .build();
                    
                    let _ = engine_clone.insert_document(
                        "test_db".to_string(),
                        "concurrent_users".to_string(),
                        doc
                    ).await;
                }
                2 => {
                    // Query operation
                    let query = QueryBuilder::new()
                        .filter(serde_json::json!({
                            "name": { "$regex": "user" }
                        }))
                        .limit(10)
                        .build();
                    
                    let _ = engine_clone.query(
                        "test_db".to_string(),
                        "users".to_string(),
                        query
                    ).await;
                }
                _ => {
                    // Update operation
                    let doc = DocumentBuilder::new()
                        .string("name", &format!("updated_user_{}", i))
                        .int("updated_value", i as i64)
                        .build();
                    
                    let _ = engine_clone.update_document_by_id(
                        "test_db".to_string(),
                        "users".to_string(),
                        format!("doc_{}", i % 10),
                        doc
                    ).await;
                }
            }
            
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    let mut completed = 0;
    for handle in handles {
        if handle.await?.is_ok() {
            completed += 1;
        }
    }
    
    let elapsed = start_time.elapsed();
    info!("Completed {} concurrent operations in {:?}", completed, elapsed);
    info!("Throughput: {:.2} ops/sec", completed as f64 / elapsed.as_secs_f64());
    
    Ok(())
}

/// Demonstrate performance benchmarks
async fn performance_benchmarks(engine: &DatabaseEngine) -> Result<()> {
    info!("=== Performance Benchmarks ===");
    
    // Benchmark 1: Write throughput
    let write_start = Instant::now();
    let mut write_handles = Vec::new();
    
    for i in 0..10000 {
        let engine_clone = engine.clone();
        let handle = tokio::spawn(async move {
            let doc = DocumentBuilder::new()
                .string("benchmark_id", &format!("bench_{}", i))
                .int("value", i as i64)
                .string("data", &"benchmark_data".repeat(100))
                .build();
            
            engine_clone.insert_document(
                "benchmark_db".to_string(),
                "benchmark_collection".to_string(),
                doc
            ).await
        });
        write_handles.push(handle);
    }
    
    for handle in write_handles {
        handle.await??;
    }
    
    let write_elapsed = write_start.elapsed();
    let write_throughput = 10000.0 / write_elapsed.as_secs_f64();
    info!("Write throughput: {:.2} writes/sec", write_throughput);
    
    // Benchmark 2: Read latency
    let read_start = Instant::now();
    let mut read_latencies = Vec::new();
    
    for i in 0..1000 {
        let start = Instant::now();
        let _ = engine.find_document_by_id(
            "benchmark_db".to_string(),
            "benchmark_collection".to_string(),
            format!("bench_{}", i % 1000)
        ).await?;
        read_latencies.push(start.elapsed());
    }
    
    let read_elapsed = read_start.elapsed();
    let avg_read_latency = read_latencies.iter().sum::<Duration>() / read_latencies.len() as u32;
    let read_throughput = 1000.0 / read_elapsed.as_secs_f64();
    
    info!("Read throughput: {:.2} reads/sec", read_throughput);
    info!("Average read latency: {:?}", avg_read_latency);
    
    // Benchmark 3: Memory efficiency
    let memory_stats = engine.get_memory_stats().await;
    let memory_efficiency = memory_stats.current_usage as f64 / memory_stats.peak_usage as f64;
    info!("Memory efficiency: {:.2}%", memory_efficiency * 100.0);
    
    // Benchmark 4: Cache hit rate
    let cache_stats = engine.get_cache_stats().await;
    info!("Cache hit rate: {:.2}%", cache_stats.hit_rate * 100.0);
    
    info!("=== Benchmark Summary ===");
    info!("Write Throughput: {:.2} writes/sec", write_throughput);
    info!("Read Throughput: {:.2} reads/sec", read_throughput);
    info!("Average Read Latency: {:?}", avg_read_latency);
    info!("Memory Efficiency: {:.2}%", memory_efficiency * 100.0);
    info!("Cache Hit Rate: {:.2}%", cache_stats.hit_rate * 100.0);
    
    Ok(())
}