// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Zero-copy data types optimized for direct memory access

use rkyv::{Archive, Deserialize, Serialize as RkyvSerialize, AlignedVec};
use bytecheck::CheckBytes;
use std::collections::HashMap;
use crate::DocumentId;

/// Zero-copy document that can be accessed directly from memory/disk
#[derive(Archive, RkyvSerialize, Deserialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct ZeroCopyDocument {
    pub id: DocumentId,
    pub fields: HashMap<String, ZeroCopyValue>,
    pub version: u64,
    pub created_at: i64,
    pub updated_at: i64,
    /// Document size in bytes for quick memory calculations
    pub size_bytes: u32,
}

/// Zero-copy value type optimized for direct memory access
#[derive(Archive, RkyvSerialize, Deserialize, Debug, Clone)]
#[archive(check_bytes)]
pub enum ZeroCopyValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    /// String stored with length prefix for zero-copy access
    String(String),
    /// Binary data with size optimization
    Binary(Vec<u8>),
    /// Nested document
    Document(ZeroCopyDocument),
    /// Array with optimized layout
    Array(Vec<ZeroCopyValue>),
    Timestamp(i64),
    ObjectId(DocumentId),
    /// Vector embeddings with SIMD alignment
    #[cfg(feature = "vector")]
    Vector(AlignedVec<f32>),
    /// Decimal128 for financial data
    Decimal128([u8; 16]),
}

impl ZeroCopyDocument {
    /// Create a new zero-copy document with current timestamp
    pub fn new(id: DocumentId, fields: HashMap<String, ZeroCopyValue>) -> Self {
        let now = chrono::Utc::now().timestamp_micros();
        Self {
            id,
            fields,
            version: 1,
            created_at: now,
            updated_at: now,
            size_bytes: 0, // Will be calculated during serialization
        }
    }
    
    /// Update a field in the document and increment version
    pub fn update_field(&mut self, key: String, value: ZeroCopyValue) {
        self.fields.insert(key, value);
        self.version += 1;
        self.updated_at = chrono::Utc::now().timestamp_micros();
    }
    
    /// Get estimated memory size of the document
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<Self>() + 
        self.fields.iter().map(|(k, v)| {
            k.len() + v.estimate_size()
        }).sum::<usize>()
    }
}

impl ZeroCopyValue {
    /// Get estimated memory size of the value
    pub fn estimate_size(&self) -> usize {
        match self {
            ZeroCopyValue::Null => 0,
            ZeroCopyValue::Bool(_) => 1,
            ZeroCopyValue::Int32(_) => 4,
            ZeroCopyValue::Int64(_) | ZeroCopyValue::UInt64(_) => 8,
            ZeroCopyValue::Float32(_) => 4,
            ZeroCopyValue::Float64(_) => 8,
            ZeroCopyValue::String(s) => s.len(),
            ZeroCopyValue::Binary(b) => b.len(),
            ZeroCopyValue::Document(d) => d.estimate_size(),
            ZeroCopyValue::Array(arr) => arr.iter().map(|v| v.estimate_size()).sum(),
            ZeroCopyValue::Timestamp(_) => 8,
            ZeroCopyValue::ObjectId(_) => 16,
            #[cfg(feature = "vector")]
            ZeroCopyValue::Vector(v) => v.len() * 4,
            ZeroCopyValue::Decimal128(_) => 16,
        }
    }
    
    /// Check if this value is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(self,
            ZeroCopyValue::Int32(_) |
            ZeroCopyValue::Int64(_) |
            ZeroCopyValue::UInt64(_) |
            ZeroCopyValue::Float32(_) |
            ZeroCopyValue::Float64(_)
        )
    }
    
    /// Check if this value is a string-like type
    pub fn is_string_like(&self) -> bool {
        matches!(self, ZeroCopyValue::String(_))
    }
    
    /// Check if this value is a container type
    pub fn is_container(&self) -> bool {
        matches!(self,
            ZeroCopyValue::Document(_) |
            ZeroCopyValue::Array(_)
        )
    }
}
