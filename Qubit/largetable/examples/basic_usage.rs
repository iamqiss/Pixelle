// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Basic usage example for Largetable

use largetable::{Client, DocumentBuilder, Value, StorageEngine};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create a client with LSM storage engine
    let client = Client::with_storage_engine(StorageEngine::Lsm)?;
    
    println!("🚀 Largetable Basic Usage Example");
    println!("=================================");

    // Create a document using the fluent API
    let document = DocumentBuilder::new()
        .string("name", "John Doe")
        .int("age", 30)
        .float("salary", 75000.50)
        .bool("active", true)
        .array("skills", vec![
            Value::String("Rust".to_string()),
            Value::String("Python".to_string()),
            Value::String("JavaScript".to_string()),
        ])
        .build();

    println!("📄 Created document: {:?}", document);

    // Insert the document
    let doc_id = client
        .insert("test_db".to_string(), "users".to_string(), document)
        .await?;
    
    println!("✅ Inserted document with ID: {}", doc_id);

    // Find the document by ID
    if let Some(found_doc) = client
        .find_by_id("test_db".to_string(), "users".to_string(), doc_id)
        .await?
    {
        println!("🔍 Found document: {:?}", found_doc);
    }

    // Create another document
    let document2 = DocumentBuilder::new()
        .string("name", "Jane Smith")
        .int("age", 25)
        .float("salary", 65000.00)
        .bool("active", true)
        .array("skills", vec![
            Value::String("Java".to_string()),
            Value::String("Go".to_string()),
        ])
        .build();

    let doc_id2 = client
        .insert("test_db".to_string(), "users".to_string(), document2)
        .await?;

    println!("✅ Inserted second document with ID: {}", doc_id2);

    // Query all documents
    let query = largetable::query::QueryBuilder::new()
        .limit(10)
        .build();

    let results = client
        .find_many("test_db".to_string(), "users".to_string(), query)
        .await?;

    println!("📊 Query results: {} documents found", results.documents.len());
    for (id, doc) in &results.documents {
        println!("  - ID: {}, Name: {:?}", id, doc.fields.get("name"));
    }

    // Create a filter query
    let filter_query = largetable::query::QueryBuilder::new()
        .filter(json!({
            "age": { "$gte": 30 }
        }))
        .build();

    let filtered_results = client
        .find_many("test_db".to_string(), "users".to_string(), filter_query)
        .await?;

    println!("🔍 Filtered results (age >= 30): {} documents", filtered_results.documents.len());

    // Test aggregation pipeline
    let mut accumulators = std::collections::HashMap::new();
    accumulators.insert("avg_age".to_string(), largetable::query::Accumulator::Avg("age".to_string()));
    accumulators.insert("count".to_string(), largetable::query::Accumulator::Count);
    accumulators.insert("max_salary".to_string(), largetable::query::Accumulator::Max("salary".to_string()));

    let aggregation = largetable::query::AggregationPipeline::new()
        .group("active".to_string(), accumulators);

    let agg_results = client
        .aggregate("test_db".to_string(), "users".to_string(), aggregation)
        .await?;

    println!("📈 Aggregation results: {:?}", agg_results);

    // Update a document
    let mut updated_doc = DocumentBuilder::new()
        .string("name", "John Doe Updated")
        .int("age", 31)
        .float("salary", 80000.00)
        .bool("active", true)
        .array("skills", vec![
            Value::String("Rust".to_string()),
            Value::String("Python".to_string()),
            Value::String("JavaScript".to_string()),
            Value::String("TypeScript".to_string()),
        ])
        .build();

    if let Some(updated) = client
        .update_by_id("test_db".to_string(), "users".to_string(), doc_id, updated_doc)
        .await?
    {
        println!("🔄 Updated document: {:?}", updated);
    }

    // Get database statistics
    let stats = client.stats().await?;
    println!("📊 Database statistics:");
    println!("  - Databases: {}", stats.total_databases);
    println!("  - Collections: {}", stats.total_collections);
    println!("  - Documents: {}", stats.total_documents);

    // Test different storage engines
    println!("\n🔄 Testing different storage engines...");

    // Test B-Tree engine
    let btree_client = Client::with_storage_engine(StorageEngine::BTree)?;
    let btree_doc = DocumentBuilder::new()
        .string("engine", "B-Tree")
        .string("optimization", "Read-heavy workloads")
        .build();

    let btree_id = btree_client
        .insert("engines".to_string(), "test".to_string(), btree_doc)
        .await?;
    println!("✅ B-Tree engine test: {}", btree_id);

    // Test Columnar engine
    let columnar_client = Client::with_storage_engine(StorageEngine::Columnar)?;
    let columnar_doc = DocumentBuilder::new()
        .string("engine", "Columnar")
        .string("optimization", "Analytics workloads")
        .build();

    let columnar_id = columnar_client
        .insert("engines".to_string(), "test".to_string(), columnar_doc)
        .await?;
    println!("✅ Columnar engine test: {}", columnar_id);

    // Test Graph engine
    let graph_client = Client::with_storage_engine(StorageEngine::Graph)?;
    let graph_doc = DocumentBuilder::new()
        .string("engine", "Graph")
        .string("optimization", "Relationship-heavy workloads")
        .build();

    let graph_id = graph_client
        .insert("engines".to_string(), "test".to_string(), graph_doc)
        .await?;
    println!("✅ Graph engine test: {}", graph_id);

    println!("\n🎉 All tests completed successfully!");
    println!("Largetable is ready for production use! 🚀");

    Ok(())
}