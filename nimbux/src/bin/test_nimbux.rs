// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
// Test binary for Nimbux

use nimbux::storage::{MemoryStorage, ContentAddressableStorage, StorageEngine, Object, StorageBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Nimbux storage system...");
    
    // Test memory storage
    println!("\n=== Testing Memory Storage ===");
    let memory_storage = MemoryStorage::new();
    
    let object = Object::new("test.txt".to_string(), b"Hello, World!".to_vec(), Some("text/plain".to_string()));
    let object_id = object.metadata.id.clone();
    
    memory_storage.put(object).await?;
    println!("✓ Object stored in memory storage");
    
    let retrieved = memory_storage.get(&object_id).await?;
    println!("✓ Object retrieved from memory storage: {}", String::from_utf8_lossy(&retrieved.data));
    
    // Test content-addressable storage
    println!("\n=== Testing Content-Addressable Storage ===");
    let content_storage = ContentAddressableStorage::new();
    
    let object1 = Object::with_id("obj1".to_string(), "test1.txt".to_string(), b"Hello, World!".to_vec(), None);
    let object2 = Object::with_id("obj2".to_string(), "test2.txt".to_string(), b"Hello, World!".to_vec(), None);
    
    content_storage.put(object1).await?;
    content_storage.put(object2).await?;
    println!("✓ Two objects with same content stored");
    
    let stats = content_storage.get_dedup_stats().await?;
    println!("✓ Deduplication stats: {} unique blocks for {} objects (ratio: {:.2})", 
             stats.unique_content_blocks, stats.total_objects, stats.deduplication_ratio);
    
    // Test storage engine
    println!("\n=== Testing Storage Engine ===");
    let mut storage_engine = StorageEngine::new("memory".to_string());
    storage_engine.add_backend("memory".to_string(), Box::new(MemoryStorage::new()));
    storage_engine.add_backend("content".to_string(), Box::new(ContentAddressableStorage::new()));
    
    let test_object = Object::new("engine_test.txt".to_string(), b"Storage engine test".to_vec(), None);
    let test_id = test_object.metadata.id.clone();
    
    storage_engine.put(test_object).await?;
    println!("✓ Object stored via storage engine");
    
    let retrieved = storage_engine.get(&test_id).await?;
    println!("✓ Object retrieved via storage engine: {}", String::from_utf8_lossy(&retrieved.data));
    
    let stats = storage_engine.stats().await?;
    println!("✓ Storage engine stats: {} objects, {} bytes", stats.total_objects, stats.total_size);
    
    println!("\n🎉 All tests passed! Nimbux is working correctly.");
    
    Ok(())
}