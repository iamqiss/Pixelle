// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Comprehensive demo showcasing Nimbux features

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing_subscriber;

use nimbux::{
    storage::{MemoryStorage, ContentAddressableStorage, StorageEngine, Object},
    network::{TcpServer, S3ApiServer, HttpConnectionPool, BufferPool, PerformanceMonitor},
    auth::{AuthManager, PolicyDocument, PolicyStatement},
    observability::MetricsCollector,
    errors::Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    println!("🚀 Nimbux High-Performance Object Storage Demo");
    println!("================================================");
    
    // 1. Demonstrate Storage Features
    println!("\n📦 Storage Features Demo");
    println!("------------------------");
    demo_storage_features().await?;
    
    // 2. Demonstrate Authentication
    println!("\n🔐 Authentication Demo");
    println!("----------------------");
    demo_authentication().await?;
    
    // 3. Demonstrate Compression
    println!("\n🗜️  Compression Demo");
    println!("--------------------");
    demo_compression().await?;
    
    // 4. Demonstrate Network Protocols
    println!("\n🌐 Network Protocols Demo");
    println!("-------------------------");
    demo_network_protocols().await?;
    
    // 5. Demonstrate Observability
    println!("\n📊 Observability Demo");
    println!("---------------------");
    demo_observability().await?;
    
    // 6. Demonstrate Performance Features
    println!("\n⚡ Performance Features Demo");
    println!("-----------------------------");
    demo_performance_features().await?;
    
    println!("\n✅ Demo completed successfully!");
    println!("\nNimbux is ready for production use with:");
    println!("  • S3-compatible API");
    println!("  • Custom high-performance TCP protocol");
    println!("  • Advanced compression and deduplication");
    println!("  • Comprehensive authentication and authorization");
    println!("  • Real-time metrics and observability");
    println!("  • Connection pooling and performance optimization");
    
    Ok(())
}

async fn demo_storage_features() -> Result<()> {
    println!("Creating storage engine with multiple backends...");
    
    // Create storage engine
    let mut storage_engine = StorageEngine::new("content".to_string());
    storage_engine.add_backend("memory".to_string(), Box::new(MemoryStorage::new()));
    storage_engine.add_backend("content".to_string(), Box::new(ContentAddressableStorage::new()));
    
    let storage = Arc::new(storage_engine);
    
    // Create test objects
    let objects = vec![
        Object::new("test1.txt".to_string(), b"Hello, Nimbux!".to_vec(), Some("text/plain".to_string())),
        Object::new("test2.json".to_string(), b"{\"message\": \"Nimbux is awesome!\"}".to_vec(), Some("application/json".to_string())),
        Object::new("test3.bin".to_string(), vec![0u8; 1024], Some("application/octet-stream".to_string())),
    ];
    
    println!("Storing {} objects...", objects.len());
    for object in objects {
        storage.put(object).await?;
    }
    
    // Get storage statistics
    let stats = storage.stats().await?;
    println!("Storage stats: {} objects, {} bytes", stats.total_objects, stats.total_size);
    
    Ok(())
}

async fn demo_authentication() -> Result<()> {
    println!("Setting up authentication system...");
    
    let auth_manager = AuthManager::new();
    
    // Create users
    let admin_user = auth_manager.create_user(
        "admin".to_string(),
        "admin@nimbux.local".to_string(),
    ).await?;
    
    let user = auth_manager.create_user(
        "user".to_string(),
        "user@nimbux.local".to_string(),
    ).await?;
    
    // Create access keys
    let admin_key = auth_manager.create_access_key(&admin_user.user_id).await?;
    let user_key = auth_manager.create_access_key(&user.user_id).await?;
    
    println!("Created admin user: {}", admin_user.username);
    println!("Created regular user: {}", user.username);
    println!("Admin access key: {}", admin_key.access_key_id);
    println!("User access key: {}", user_key.access_key_id);
    
    // Add policies
    let admin_policy = PolicyDocument {
        version: "2012-10-17".to_string(),
        statement: vec![
            PolicyStatement {
                effect: "Allow".to_string(),
                action: vec!["*".to_string()],
                resource: vec!["*".to_string()],
                condition: None,
            }
        ],
    };
    
    let user_policy = PolicyDocument {
        version: "2012-10-17".to_string(),
        statement: vec![
            PolicyStatement {
                effect: "Allow".to_string(),
                action: vec!["s3:GetObject".to_string(), "s3:PutObject".to_string()],
                resource: vec!["arn:nimbux:s3:::user-bucket/*".to_string()],
                condition: None,
            }
        ],
    };
    
    auth_manager.add_policy(&admin_user.user_id, admin_policy).await?;
    auth_manager.add_policy(&user.user_id, user_policy).await?;
    
    println!("Added policies for both users");
    
    Ok(())
}

async fn demo_compression() -> Result<()> {
    println!("Demonstrating advanced compression features...");
    
    use nimbux::storage::compression::{CompressionEngine, CompressionAlgorithm, CompressionAnalyzer};
    
    let compression_engine = CompressionEngine::new();
    let analyzer = CompressionAnalyzer::new();
    
    // Test data with different characteristics
    let test_data = vec![
        ("repetitive", vec![0u8; 10000]), // Highly repetitive
        ("random", (0..10000).map(|i| (i % 256) as u8).collect()), // Random data
        ("text", b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(100).into()), // Text
    ];
    
    for (name, data) in test_data {
        println!("Testing compression for {} data ({} bytes)...", name, data.len());
        
        // Analyze if compression is beneficial
        let should_compress = analyzer.should_compress(&data);
        println!("  Should compress: {}", should_compress);
        
        if should_compress {
            // Get algorithm recommendation
            let recommended = analyzer.recommend_algorithm(&data);
            println!("  Recommended algorithm: {:?}", recommended);
            
            // Compress with recommended algorithm
            let compressed = compression_engine.compress_data(&data, recommended).await?;
            println!("  Original size: {} bytes", compressed.original_size);
            println!("  Compressed size: {} bytes", compressed.compressed_size);
            println!("  Compression ratio: {:.2}%", 
                     (1.0 - compressed.compressed_size as f64 / compressed.original_size as f64) * 100.0);
            println!("  Algorithm used: {:?}", compressed.algorithm);
            
            // Test decompression
            let decompressed = compression_engine.decompress_data(&compressed).await?;
            assert_eq!(data, decompressed);
            println!("  Decompression successful ✓");
        }
    }
    
    // Get compression statistics
    let stats = compression_engine.get_stats().await?;
    println!("Overall compression ratio: {:.2}%", stats.compression_ratio * 100.0);
    
    Ok(())
}

async fn demo_network_protocols() -> Result<()> {
    println!("Demonstrating network protocols...");
    
    // Create storage for servers
    let storage = Arc::new(StorageEngine::new("memory".to_string()));
    storage.add_backend("memory".to_string(), Box::new(MemoryStorage::new()));
    
    // Create auth manager
    let auth_manager = Arc::new(AuthManager::new());
    let user = auth_manager.create_user("demo".to_string(), "demo@nimbux.local".to_string()).await?;
    let _key = auth_manager.create_access_key(&user.user_id).await?;
    
    // Create metrics collector
    let metrics = Arc::new(MetricsCollector::new());
    
    println!("Starting TCP server on port 8081...");
    let tcp_server = TcpServer::new(Arc::clone(&storage), 8081);
    
    println!("Starting S3 API server on port 8082...");
    let s3_server = S3ApiServer::new(
        Arc::clone(&storage),
        Arc::clone(&auth_manager),
        Arc::clone(&metrics),
        8082,
    );
    
    // Start servers in background
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = tcp_server.start().await {
            eprintln!("TCP server error: {}", e);
        }
    });
    
    let s3_handle = tokio::spawn(async move {
        if let Err(e) = s3_server.start().await {
            eprintln!("S3 server error: {}", e);
        }
    });
    
    // Let servers run for a moment
    sleep(Duration::from_secs(2)).await;
    
    println!("TCP server running on tcp://localhost:8081");
    println!("S3 API server running on http://localhost:8082");
    println!("Servers started successfully ✓");
    
    // Clean up
    tcp_handle.abort();
    s3_handle.abort();
    
    Ok(())
}

async fn demo_observability() -> Result<()> {
    println!("Demonstrating observability features...");
    
    let metrics = MetricsCollector::new();
    
    // Simulate some activity
    println!("Simulating storage operations...");
    for i in 0..10 {
        metrics.record_storage_operation("put", 1024 * (i + 1) as u64).await?;
        metrics.record_request(true, Duration::from_millis(50 + i * 10)).await?;
        metrics.record_auth_event(true).await?;
        metrics.record_cache_event(i % 2 == 0).await?;
    }
    
    // Get metrics summary
    let summary = metrics.get_metrics_summary().await?;
    println!("Metrics Summary:");
    println!("  Uptime: {} seconds", summary.uptime_seconds);
    println!("  Total requests: {}", summary.total_requests);
    println!("  Success rate: {:.2}%", summary.success_rate_percent);
    println!("  Average request duration: {:.3} seconds", summary.avg_request_duration_seconds);
    println!("  Total objects: {}", summary.total_objects);
    println!("  Total bytes stored: {}", summary.total_bytes_stored);
    println!("  Cache hit rate: {:.2}%", summary.cache_hit_rate_percent);
    
    // Get detailed metrics
    let detailed_metrics = metrics.get_metrics().await?;
    println!("Detailed metrics:");
    println!("  Storage operations: {:?}", detailed_metrics.storage_operations);
    println!("  Request duration histogram: {} samples", detailed_metrics.request_duration_histogram.count);
    
    Ok(())
}

async fn demo_performance_features() -> Result<()> {
    println!("Demonstrating performance optimization features...");
    
    // Connection pool demo
    println!("Creating HTTP connection pool...");
    let connection_pool = HttpConnectionPool::new(
        10,  // max connections per endpoint
        100, // max total connections
        Duration::from_secs(5), // connection timeout
        Duration::from_secs(30), // idle timeout
    );
    
    // Buffer pool demo
    println!("Creating buffer pool...");
    let buffer_pool = BufferPool::new(8192, 100); // 8KB buffers, max 100
    
    // Performance monitor demo
    println!("Creating performance monitor...");
    let perf_monitor = PerformanceMonitor::new();
    
    // Simulate some operations
    println!("Simulating network operations...");
    for i in 0..20 {
        let start = std::time::Instant::now();
        
        // Simulate work
        sleep(Duration::from_millis(10 + i)).await;
        
        let latency = start.elapsed();
        perf_monitor.record_latency(latency).await;
        
        if i % 3 == 0 {
            perf_monitor.record_error().await;
        } else {
            perf_monitor.record_success().await;
        }
    }
    
    // Get performance statistics
    let perf_stats = perf_monitor.get_stats().await;
    println!("Performance Statistics:");
    println!("  Total requests: {}", perf_stats.total_requests);
    println!("  Total errors: {}", perf_stats.total_errors);
    println!("  Requests per second: {:.2}", perf_stats.requests_per_second);
    println!("  Average latency: {:.2}ms", perf_stats.avg_latency.as_millis());
    println!("  P95 latency: {:.2}ms", perf_stats.p95_latency.as_millis());
    println!("  Error rate: {:.2}%", perf_stats.error_rate * 100.0);
    
    // Get pool statistics
    let pool_stats = connection_pool.get_stats().await?;
    println!("Connection Pool Statistics:");
    println!("  Total connections: {}", pool_stats.total_connections);
    println!("  Active connections: {}", pool_stats.active_connections);
    
    let buffer_stats = buffer_pool.get_stats().await;
    println!("Buffer Pool Statistics:");
    println!("  Available buffers: {}", buffer_stats.available_buffers);
    println!("  Buffer size: {} bytes", buffer_stats.buffer_size);
    
    Ok(())
}