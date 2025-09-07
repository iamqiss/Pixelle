// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! BSON serialization/deserialization for Largetable
//!
//! High-performance BSON support with zero-copy and SIMD optimizations.
//! Supports: serialization, deserialization, type conversions, and validation.

pub mod serializer;
pub mod deserializer;
pub mod types;
pub mod utils;

pub use serializer::{to_bson_bytes, to_bson_bytes_simd};
pub use deserializer::{from_bson_bytes, from_bson_bytes_simd};
pub use types::{BsonValue, BsonDocument};
pub use utils::BsonError;

/// Convenience function: serialize a document to BSON
pub fn serialize_document(doc: &crate::Document) -> Result<Vec<u8>, BsonError> {
    to_bson_bytes(doc)
}

/// Convenience function: deserialize BSON bytes into a document
pub fn deserialize_document(bytes: &[u8]) -> Result<crate::Document, BsonError> {
    from_bson_bytes(bytes)
}
