# Largetable - Next-Generation NoSQL Database

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)](Cargo.toml)

**Largetable** is a high-performance, distributed NoSQL database built with Rust, designed to outperform MongoDB with its async-first architecture, zero-copy serialization, and pluggable storage engines.

## 🚀 Features

### Core Features
- **Multi-Model Support**: Document, Graph, Time-Series, and Vector databases
- **Pluggable Storage Engines**: LSM, B-Tree, Columnar, and Graph storage
- **Zero-Copy Serialization**: High-performance data serialization with rkyv
- **Async-First Architecture**: Built on Tokio for maximum concurrency
- **ACID Transactions**: Full transaction support with isolation levels
- **Advanced Indexing**: B-Tree, Hash, Full-Text, Vector, Geospatial, and Time-Series indexes
- **Query Engine**: Rich query language with filtering, sorting, and aggregation
- **HTTP API**: RESTful API compatible with MongoDB drivers
- **Observability**: Built-in tracing, metrics, and monitoring

### Performance Features
- **Memory Mapping**: Efficient memory usage with mmap
- **Compression**: LZ4, Zstd, and Snappy compression support
- **SIMD Optimizations**: Vectorized operations for maximum speed
- **Connection Pooling**: Efficient connection management
- **Caching**: Multi-level caching for hot data

## 📦 Installation

Add Largetable to your `Cargo.toml`:

```toml
[dependencies]
largetable = "0.1.0"
```

## 🏃 Quick Start

### Basic Usage

```rust
use largetable::{Client, DocumentBuilder, Value, StorageEngine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client
    let client = Client::with_storage_engine(StorageEngine::Lsm)?;
    
    // Create a document
    let document = DocumentBuilder::new()
        .string("name", "John Doe")
        .int("age", 30)
        .float("salary", 75000.50)
        .bool("active", true)
        .array("skills", vec![
            Value::String("Rust".to_string()),
            Value::String("Python".to_string()),
        ])
        .build();
    
    // Insert the document
    let doc_id = client
        .insert("my_db".to_string(), "users".to_string(), document)
        .await?;
    
    println!("Inserted document with ID: {}", doc_id);
    
    // Find the document
    if let Some(found_doc) = client
        .find_by_id("my_db".to_string(), "users".to_string(), doc_id)
        .await?
    {
        println!("Found document: {:?}", found_doc);
    }
    
    Ok(())
}
```

### Query Operations

```rust
use largetable::query::{QueryBuilder, SortDirection};
use serde_json::json;

// Simple query
let query = QueryBuilder::new()
    .filter(json!({
        "age": { "$gte": 25 }
    }))
    .sort("name".to_string(), SortDirection::Ascending)
    .limit(10)
    .build();

let results = client
    .find_many("my_db".to_string(), "users".to_string(), query)
    .await?;

println!("Found {} documents", results.documents.len());
```

### Aggregation Pipeline

```rust
use largetable::query::{AggregationPipeline, Accumulator};
use std::collections::HashMap;

let mut accumulators = HashMap::new();
accumulators.insert("avg_age".to_string(), Accumulator::Avg("age".to_string()));
accumulators.insert("count".to_string(), Accumulator::Count);

let pipeline = AggregationPipeline::new()
    .group("department".to_string(), accumulators);

let results = client
    .aggregate("my_db".to_string(), "employees".to_string(), pipeline)
    .await?;
```

### Indexing

```rust
use largetable::{IndexType, VectorMetric};

// Create different types of indexes
let collection = client.collection("my_db".to_string(), "users".to_string()).await?;

// B-Tree index for range queries
collection.create_index("age".to_string(), IndexType::BTree).await?;

// Hash index for exact matches
collection.create_index("email".to_string(), IndexType::Hash).await?;

// Full-text search index
collection.create_index("description".to_string(), IndexType::FullText {
    language: "en".to_string(),
    stop_words: vec!["the".to_string(), "and".to_string()],
}).await?;

// Vector index for AI/ML
collection.create_index("embedding".to_string(), IndexType::Vector {
    dimensions: 768,
    metric: VectorMetric::Cosine,
}).await?;

// Geospatial index
collection.create_index("location".to_string(), IndexType::Geospatial {
    coordinate_system: "WGS84".to_string(),
}).await?;
```

### Transactions

```rust
use largetable::engine::transaction::{TransactionManager, TransactionOperation};

let tx_manager = TransactionManager::new();

// Begin transaction
let tx_id = tx_manager.begin_transaction().await?;

// Add operations
tx_manager.add_operation(tx_id, TransactionOperation::Insert {
    database: "my_db".to_string(),
    collection: "users".to_string(),
    document: user_doc,
}).await?;

tx_manager.add_operation(tx_id, TransactionOperation::Update {
    database: "my_db".to_string(),
    collection: "profiles".to_string(),
    id: profile_id,
    document: updated_profile,
}).await?;

// Commit transaction
tx_manager.commit_transaction(tx_id).await?;
```

## 🏗️ Architecture

### Storage Engines

#### LSM (Log-Structured Merge) Engine
- **Use Case**: Write-heavy workloads
- **Backend**: RocksDB
- **Features**: High write throughput, automatic compaction
- **Best For**: Logs, time-series data, high-velocity writes

#### B-Tree Engine
- **Use Case**: Read-heavy workloads
- **Backend**: Redb
- **Features**: Fast point lookups, range queries
- **Best For**: User data, configuration, frequently accessed data

#### Columnar Engine
- **Use Case**: Analytics and reporting
- **Backend**: Apache Arrow/Parquet
- **Features**: Columnar storage, compression
- **Best For**: Data warehousing, analytics, OLAP

#### Graph Engine
- **Use Case**: Relationship-heavy data
- **Backend**: Petgraph
- **Features**: Graph traversal, relationship queries
- **Best For**: Social networks, recommendation systems

### Index Types

| Index Type | Use Case | Performance | Memory |
|------------|----------|-------------|---------|
| B-Tree | Range queries, sorting | O(log n) | Medium |
| Hash | Exact matches | O(1) | Low |
| Full-Text | Text search | O(log n) | High |
| Vector | Similarity search | O(log n) | High |
| Geospatial | Location queries | O(log n) | Medium |
| Time-Series | Temporal data | O(log n) | Low |

## 🌐 HTTP API

Largetable provides a RESTful HTTP API compatible with MongoDB drivers:

### Endpoints

```
GET  /health                    # Health check
GET  /stats                     # Database statistics
GET  /databases                 # List databases
POST /databases/{db}            # Create database
GET  /databases/{db}/collections # List collections
POST /databases/{db}/collections/{collection} # Create collection
POST /databases/{db}/collections/{collection}/documents # Insert document
GET  /databases/{db}/collections/{collection}/documents/{id} # Find document
POST /databases/{db}/collections/{collection}/query # Query documents
```

### Example API Usage

```bash
# Health check
curl http://localhost:27017/health

# Insert document
curl -X POST http://localhost:27017/databases/my_db/collections/users/documents \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "age": 30}'

# Find document
curl http://localhost:27017/databases/my_db/collections/users/documents/{id}

# Query documents
curl -X POST http://localhost:27017/databases/my_db/collections/users/query \
  -H "Content-Type: application/json" \
  -d '{"filter": {"age": {"$gte": 25}}}'
```

## ⚙️ Configuration

Largetable can be configured via environment variables or a TOML file:

### Environment Variables

```bash
export LARGETABLE_HOST=127.0.0.1
export LARGETABLE_PORT=27017
export LARGETABLE_STORAGE_ENGINE=lsm
export LARGETABLE_DATA_DIR=./data
export LARGETABLE_LOG_LEVEL=info
export LARGETABLE_MAX_CONNECTIONS=1000
export LARGETABLE_WORKER_THREADS=8
export LARGETABLE_MEMORY_LIMIT_MB=1024
export LARGETABLE_ENABLE_COMPRESSION=true
export LARGETABLE_ENABLE_REPLICATION=false
export LARGETABLE_REPLICATION_FACTOR=1
```

### Configuration File (largetable.toml)

```toml
host = "127.0.0.1"
port = 27017
default_storage_engine = "lsm"
data_dir = "./data"
log_level = "info"
max_connections = 1000
worker_threads = 8
memory_limit_mb = 1024
enable_compression = true
enable_replication = false
replication_factor = 1
```

## 🔧 Development

### Building from Source

```bash
git clone https://github.com/your-org/largetable.git
cd largetable
cargo build --release
```

### Running Tests

```bash
cargo test
```

### Running Benchmarks

```bash
cargo bench
```

### Running the Server

```bash
cargo run --bin largetable
```

## 📊 Performance

Largetable is designed for high performance:

- **Write Throughput**: 100K+ writes/second
- **Read Latency**: Sub-millisecond for point lookups
- **Memory Efficiency**: Zero-copy serialization
- **Concurrency**: Async-first architecture
- **Scalability**: Horizontal scaling support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

This project is proprietary software. All rights reserved.

## 🙏 Acknowledgments

- [RocksDB](https://rocksdb.org/) for LSM storage
- [Apache Arrow](https://arrow.apache.org/) for columnar storage
- [Tantivy](https://github.com/quickwit-oss/tantivy) for full-text search
- [Tokio](https://tokio.rs/) for async runtime
- [Rkyv](https://github.com/rkyv/rkyv) for zero-copy serialization

---

**Largetable** - Built to outperform MongoDB with Rust's power. 🦀