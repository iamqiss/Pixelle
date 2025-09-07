// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Core types optimized for performance and zero-copy operations

use rkyv::{Archive, Deserialize, Serialize as RkyvSerialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::HashMap;
use uuid::Uuid;
use bytecheck::CheckBytes;

/// High-performance document ID using UUID v7 (timestamp-ordered)
pub type DocumentId = Uuid;

/// Collection identifier
pub type CollectionName = String;

/// Database identifier
pub type DatabaseName = String;

/// Timestamp in microseconds since epoch
pub type Timestamp = i64;

/// Zero-copy document representation
#[derive(Debug, Clone, Archive, RkyvSerialize, Deserialize, SerdeSerialize, SerdeDeserialize)]
#[archive(check_bytes)]
pub struct Document {
    pub id: DocumentId,
    pub fields: HashMap<String, Value>,
    pub version: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// High-performance value type with zero-copy support
#[derive(Debug, Clone, Archive, RkyvSerialize, Deserialize, SerdeSerialize, SerdeDeserialize)]
#[archive(check_bytes)]
pub enum Value {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Document(Document),
    Array(Vec<Value>),
    Timestamp(Timestamp),
    ObjectId(DocumentId),
    /// Vector embedding for AI/ML applications
    Vector(Vec<f32>),
    /// Decimal for financial applications
    Decimal128([u8; 16]),
}

/// Storage engine selection
#[derive(Debug, Clone, Copy, SerdeSerialize, SerdeDeserialize)]
pub enum StorageEngine {
    /// LSM Tree - optimized for writes
    Lsm,
    /// B-Tree - optimized for reads
    BTree,
    /// Columnar - optimized for analytics
    Columnar,
    /// Graph - optimized for relationships
    Graph,
}

/// Index type specification
#[derive(Debug, Clone, SerdeSerialize, SerdeDeserialize)]
pub enum IndexType {
    /// Standard B-Tree index
    BTree,
    /// Hash index for exact matches
    Hash,
    /// Full-text search index
    FullText {
        language: String,
        stop_words: Vec<String>,
    },
    /// Vector similarity index
    Vector {
        dimensions: usize,
        metric: VectorMetric,
    },
    /// Geospatial index
    Geospatial {
        coordinate_system: String,
    },
    /// Time-series optimized index
    TimeSeries {
        granularity: String,
    },
}

/// Vector similarity metrics
#[derive(Debug, Clone, SerdeSerialize, SerdeDeserialize)]
pub enum VectorMetric {
    Cosine,
    Euclidean,
    Dot,
    Manhattan,
}
