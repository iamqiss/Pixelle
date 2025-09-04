// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Comprehensive demonstration of Largetable features

use largetable::{
    Client, DocumentBuilder, Value, StorageEngine, IndexType, VectorMetric,
    query::{QueryBuilder, SortDirection, AggregationPipeline, Accumulator},
    engine::transaction::{TransactionManager, TransactionOperation},
};
use serde_json::json;
use std::collections::HashMap;
use tracing::{info, debug};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🚀 Largetable Comprehensive Demo");
    println!("================================");

    // Test different storage engines
    test_storage_engines().await?;
    
    // Test document operations
    test_document_operations().await?;
    
    // Test querying and aggregation
    test_querying_and_aggregation().await?;
    
    // Test indexing
    test_indexing().await?;
    
    // Test transactions
    test_transactions().await?;
    
    // Test HTTP API (if server is running)
    test_http_api().await?;
    
    println!("\n🎉 All demos completed successfully!");
    println!("Largetable is ready for production use! 🚀");
    
    Ok(())
}

async fn test_storage_engines() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📦 Testing Storage Engines");
    println!("---------------------------");

    let engines = vec![
        ("LSM (Write-optimized)", StorageEngine::Lsm),
        ("B-Tree (Read-optimized)", StorageEngine::BTree),
        ("Columnar (Analytics)", StorageEngine::Columnar),
        ("Graph (Relationships)", StorageEngine::Graph),
    ];

    for (name, engine_type) in engines {
        println!("Testing {} engine...", name);
        
        let client = Client::with_storage_engine(engine_type)?;
        
        // Create a test document
        let doc = DocumentBuilder::new()
            .string("engine", name)
            .string("type", format!("{:?}", engine_type))
            .int("performance_score", 95)
            .bool("optimized", true)
            .build();
        
        // Insert document
        let doc_id = client
            .insert("engines".to_string(), "test".to_string(), doc)
            .await?;
        
        // Retrieve document
        if let Some(retrieved_doc) = client
            .find_by_id("engines".to_string(), "test".to_string(), doc_id)
            .await?
        {
            println!("  ✅ {}: Inserted and retrieved document {}", name, doc_id);
            debug!("Document: {:?}", retrieved_doc);
        }
    }
    
    Ok(())
}

async fn test_document_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📄 Testing Document Operations");
    println!("------------------------------");

    let client = Client::new()?;
    
    // Create a complex document
    let mut user_doc = DocumentBuilder::new()
        .string("name", "Alice Johnson")
        .string("email", "alice@example.com")
        .int("age", 28)
        .float("salary", 85000.0)
        .bool("active", true)
        .array("skills", vec![
            Value::String("Rust".to_string()),
            Value::String("Python".to_string()),
            Value::String("JavaScript".to_string()),
        ])
        .document("address", DocumentBuilder::new()
            .string("street", "123 Main St")
            .string("city", "San Francisco")
            .string("state", "CA")
            .string("zip", "94105")
            .build())
        .vector("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5])
        .build();
    
    // Insert document
    let doc_id = client
        .insert("users".to_string(), "profiles".to_string(), user_doc)
        .await?;
    
    println!("✅ Inserted user document: {}", doc_id);
    
    // Update document
    let mut updated_doc = DocumentBuilder::new()
        .string("name", "Alice Johnson-Smith")
        .string("email", "alice.smith@example.com")
        .int("age", 29)
        .float("salary", 90000.0)
        .bool("active", true)
        .array("skills", vec![
            Value::String("Rust".to_string()),
            Value::String("Python".to_string()),
            Value::String("JavaScript".to_string()),
            Value::String("Go".to_string()),
        ])
        .build();
    
    if let Some(updated) = client
        .update_by_id("users".to_string(), "profiles".to_string(), doc_id, updated_doc)
        .await?
    {
        println!("✅ Updated user document: {}", doc_id);
        debug!("Updated document: {:?}", updated);
    }
    
    // Delete document
    let deleted = client
        .delete_by_id("users".to_string(), "profiles".to_string(), doc_id)
        .await?;
    
    if deleted {
        println!("✅ Deleted user document: {}", doc_id);
    }
    
    Ok(())
}

async fn test_querying_and_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔍 Testing Querying and Aggregation");
    println!("------------------------------------");

    let client = Client::new()?;
    
    // Insert sample data
    let employees = vec![
        ("Alice", "Engineering", 28, 85000.0),
        ("Bob", "Engineering", 32, 95000.0),
        ("Charlie", "Sales", 25, 65000.0),
        ("Diana", "Engineering", 29, 90000.0),
        ("Eve", "Marketing", 27, 70000.0),
    ];
    
    for (name, department, age, salary) in employees {
        let doc = DocumentBuilder::new()
            .string("name", name)
            .string("department", department)
            .int("age", age)
            .float("salary", salary)
            .bool("active", true)
            .build();
        
        client
            .insert("company".to_string(), "employees".to_string(), doc)
            .await?;
    }
    
    println!("✅ Inserted {} employee documents", employees.len());
    
    // Test filtering query
    let filter_query = QueryBuilder::new()
        .filter(json!({
            "department": "Engineering",
            "age": { "$gte": 28 }
        }))
        .sort("salary".to_string(), SortDirection::Descending)
        .limit(10)
        .build();
    
    let results = client
        .find_many("company".to_string(), "employees".to_string(), filter_query)
        .await?;
    
    println!("✅ Filter query returned {} engineering employees aged 28+", results.documents.len());
    
    // Test aggregation pipeline
    let mut accumulators = HashMap::new();
    accumulators.insert("avg_salary".to_string(), Accumulator::Avg("salary".to_string()));
    accumulators.insert("max_salary".to_string(), Accumulator::Max("salary".to_string()));
    accumulators.insert("min_salary".to_string(), Accumulator::Min("salary".to_string()));
    accumulators.insert("count".to_string(), Accumulator::Count);
    
    let aggregation = AggregationPipeline::new()
        .group("department".to_string(), accumulators)
        .sort(vec![
            largetable::query::SortField {
                field: "avg_salary".to_string(),
                direction: SortDirection::Descending,
            }
        ]);
    
    let agg_results = client
        .aggregate("company".to_string(), "employees".to_string(), aggregation)
        .await?;
    
    println!("✅ Aggregation results by department:");
    for result in agg_results {
        println!("  {:?}", result);
    }
    
    Ok(())
}

async fn test_indexing() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Testing Indexing System");
    println!("--------------------------");

    let client = Client::new()?;
    let collection = client.collection("test_db".to_string(), "indexed_collection".to_string()).await?;
    
    // Create different types of indexes
    println!("Creating indexes...");
    
    // B-Tree index for range queries
    collection.create_index("age".to_string(), IndexType::BTree).await?;
    println!("✅ Created B-Tree index on 'age'");
    
    // Hash index for exact matches
    collection.create_index("email".to_string(), IndexType::Hash).await?;
    println!("✅ Created Hash index on 'email'");
    
    // Full-text search index
    collection.create_index("description".to_string(), IndexType::FullText {
        language: "en".to_string(),
        stop_words: vec!["the".to_string(), "and".to_string(), "or".to_string()],
    }).await?;
    println!("✅ Created Full-text index on 'description'");
    
    // Vector index for AI/ML
    collection.create_index("embedding".to_string(), IndexType::Vector {
        dimensions: 5,
        metric: VectorMetric::Cosine,
    }).await?;
    println!("✅ Created Vector index on 'embedding'");
    
    // Geospatial index
    collection.create_index("location".to_string(), IndexType::Geospatial {
        coordinate_system: "WGS84".to_string(),
    }).await?;
    println!("✅ Created Geospatial index on 'location'");
    
    // Time-series index
    collection.create_index("timestamp".to_string(), IndexType::TimeSeries {
        granularity: "minute".to_string(),
    }).await?;
    println!("✅ Created Time-series index on 'timestamp'");
    
    // List all indexes
    let indexes = collection.list_indexes().await?;
    println!("✅ Total indexes created: {}", indexes.len());
    
    for (field, index_type) in indexes {
        println!("  - {}: {:?}", field, index_type);
    }
    
    Ok(())
}

async fn test_transactions() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Testing Transactions");
    println!("----------------------");

    let client = Client::new()?;
    let tx_manager = TransactionManager::new();
    
    // Begin transaction
    let tx_id = tx_manager.begin_transaction().await?;
    println!("✅ Started transaction: {}", tx_id);
    
    // Add multiple operations to transaction
    let user_doc = DocumentBuilder::new()
        .string("name", "Transaction User")
        .string("email", "tx@example.com")
        .int("age", 30)
        .build();
    
    let profile_doc = DocumentBuilder::new()
        .string("bio", "Transaction test user")
        .string("location", "Test City")
        .build();
    
    // Add insert operations
    tx_manager.add_operation(tx_id, TransactionOperation::Insert {
        database: "tx_test".to_string(),
        collection: "users".to_string(),
        document: user_doc,
    }).await?;
    
    tx_manager.add_operation(tx_id, TransactionOperation::Insert {
        database: "tx_test".to_string(),
        collection: "profiles".to_string(),
        document: profile_doc,
    }).await?;
    
    println!("✅ Added 2 operations to transaction");
    
    // Get transaction info
    let transaction = tx_manager.get_transaction(tx_id).await?;
    let tx_info = transaction.read().await;
    println!("✅ Transaction has {} operations", tx_info.operations().len());
    
    // Commit transaction
    tx_manager.commit_transaction(tx_id).await?;
    println!("✅ Committed transaction: {}", tx_id);
    
    // Test transaction abort
    let tx_id2 = tx_manager.begin_transaction().await?;
    println!("✅ Started second transaction: {}", tx_id2);
    
    tx_manager.abort_transaction(tx_id2).await?;
    println!("✅ Aborted transaction: {}", tx_id2);
    
    // Test cleanup
    let active_count = tx_manager.active_transaction_count().await;
    println!("✅ Active transactions: {}", active_count);
    
    Ok(())
}

async fn test_http_api() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🌐 Testing HTTP API");
    println!("-------------------");

    // Note: This assumes the server is running
    // In a real scenario, you would start the server in a separate process
    
    println!("ℹ️  HTTP API testing requires a running server");
    println!("   Start the server with: cargo run --bin largetable");
    println!("   Then test endpoints like:");
    println!("   - GET  http://localhost:27017/health");
    println!("   - GET  http://localhost:27017/stats");
    println!("   - POST http://localhost:27017/databases/test/collections/users/documents");
    
    Ok(())
}

// Helper function to create sample data
async fn create_sample_data(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let sample_users = vec![
        ("Alice", "alice@example.com", 28, "Engineering"),
        ("Bob", "bob@example.com", 32, "Engineering"),
        ("Charlie", "charlie@example.com", 25, "Sales"),
        ("Diana", "diana@example.com", 29, "Marketing"),
        ("Eve", "eve@example.com", 27, "Engineering"),
    ];
    
    for (name, email, age, department) in sample_users {
        let doc = DocumentBuilder::new()
            .string("name", name)
            .string("email", email)
            .int("age", age)
            .string("department", department)
            .bool("active", true)
            .vector("embedding", vec![0.1, 0.2, 0.3, 0.4, 0.5])
            .build();
        
        client
            .insert("demo".to_string(), "users".to_string(), doc)
            .await?;
    }
    
    Ok(())
}