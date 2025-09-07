// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! BSON serializer for Largetable
//!
//! High-performance serialization of Largetable `Document` into BSON bytes.
//! Zero-copy for embedded documents and SIMD-friendly for numeric vectors.

use crate::Document;
use crate::types::{Value, DocumentId};
use crate::document::bson::utils::BsonError;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Write, Cursor};

/// Serializes a Largetable `Document` into BSON bytes.
///
/// This function produces a BSON-compatible byte vector for the given document,
/// optimized for zero-copy serialization of embedded documents.
///
/// # Arguments
/// * `doc` - The Largetable document to serialize.
///
/// # Returns
/// A `Result` containing the serialized BSON bytes or a `BsonError` if serialization fails.
pub fn to_bson_bytes(doc: &Document) -> Result<Vec<u8>, BsonError> {
    let mut buf = Vec::with_capacity(estimate_doc_size(doc));
    write_document(&mut buf, doc)?;
    Ok(buf)
}

/// SIMD-optimized serializer (currently for numeric vectors).
///
/// Falls back to `to_bson_bytes` for non-vector cases. Future implementations
/// may use SIMD instructions for large numeric arrays.
pub fn to_bson_bytes_simd(doc: &Document) -> Result<Vec<u8>, BsonError> {
    to_bson_bytes(doc) // Placeholder for SIMD optimization
}

/// Estimates the size of a document for buffer preallocation.
fn estimate_doc_size(doc: &Document) -> usize {
    let mut size = 4 + 1; // Length (i32) + null terminator
    for (key, value) in &doc.fields {
        size += key.len() + 1; // Key + null terminator
        size += match value {
            Value::Null => 1,
            Value::Bool(_) => 1 + 1,
            Value::Int32(_) => 1 + 4,
            Value::Int64(_) => 1 + 8,
            Value::Float64(_) => 1 + 8,
            Value::String(s) => 1 + 4 + s.len() + 1,
            Value::Binary(b) => 1 + 4 + 1 + b.len(),
            Value::Document(d) => 1 + estimate_doc_size(d),
            Value::Array(arr) => 1 + estimate_doc_size(&Document {
                id: DocumentId::new_v7(),
                fields: arr.iter().enumerate().map(|(i, v)| (i.to_string(), v.clone())).collect(),
                version: 0,
                created_at: 0,
                updated_at: 0,
            }),
            Value::Vector(v) => 1 + 4 + 1 + v.len() * std::mem::size_of::<f32>(),
            _ => 0,
        };
    }
    size
}

/// Internal: write a document to a writer.
fn write_document<W: Write>(writer: &mut W, doc: &Document) -> Result<(), BsonError> {
    let start_pos = writer.stream_position().map_err(|e| BsonError::Serialization(e.to_string()))?;
    writer.write_i32::<LittleEndian>(0)?; // Reserve space for length

    for (key, value) in &doc.fields {
        write_element(writer, key, value)?;
    }

    writer.write_u8(0)?; // Null terminator

    let end_pos = writer.stream_position().map_err(|e| BsonError::Serialization(e.to_string()))?;
    let doc_len = (end_pos - start_pos) as i32;
    let mut cursor = Cursor::new(writer.get_mut());
    cursor.set_position(start_pos);
    cursor.write_i32::<LittleEndian>(doc_len)?;

    Ok(())
}

/// Internal: write a single BSON element.
fn write_element<W: Write>(writer: &mut W, key: &str, value: &Value) -> Result<(), BsonError> {
    match value {
        Value::Null => {
            writer.write_u8(0x0A)?;
        }
        Value::Bool(b) => {
            writer.write_u8(0x08)?;
            writer.write_u8(*b as u8)?;
        }
        Value::Int32(i) => {
            writer.write_u8(0x10)?;
            writer.write_i32::<LittleEndian>(*i)?;
        }
        Value::Int64(i) => {
            writer.write_u8(0x12)?;
            writer.write_i64::<LittleEndian>(*i)?;
        }
        Value::Float64(f) => {
            writer.write_u8(0x01)?;
            writer.write_f64::<LittleEndian>(*f)?;
        }
        Value::String(s) => {
            writer.write_u8(0x02)?;
            write_string(writer, s)?;
        }
        Value::Binary(b) => {
            writer.write_u8(0x05)?;
            writer.write_i32::<LittleEndian>(b.len() as i32)?;
            writer.write_u8(0x00)?; // Generic binary subtype
            writer.write_all(b)?;
        }
        Value::Document(doc) => {
            writer.write_u8(0x03)?;
            write_document(writer, doc)?;
        }
        Value::Array(arr) => {
            writer.write_u8(0x04)?;
            let start_pos = writer.stream_position().map_err(|e| BsonError::Serialization(e.to_string()))?;
            writer.write_i32::<LittleEndian>(0)?; // Reserve space
            for (i, value) in arr.iter().enumerate() {
                write_element(writer, &i.to_string(), value)?;
            }
            writer.write_u8(0)?; // Null terminator
            let end_pos = writer.stream_position().map_err(|e| BsonError::Serialization(e.to_string()))?;
            let doc_len = (end_pos - start_pos) as i32;
            let mut cursor = Cursor::new(writer.get_mut());
            cursor.set_position(start_pos);
            cursor.write_i32::<LittleEndian>(doc_len)?;
        }
        Value::Vector(v) => {
            writer.write_u8(0x05)?;
            // Safety: f32 is Copy, has no padding, and its memory layout is compatible with little-endian byte serialization.
            let bytes = unsafe {
                std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * std::mem::size_of::<f32>())
            };
            writer.write_i32::<LittleEndian>(bytes.len() as i32)?;
            writer.write_u8(0x80)?; // Custom vector subtype
            writer.write_all(bytes)?;
        }
        _ => return Err(BsonError::Serialization("Unsupported BSON type".into())),
    }

    writer.write_all(key.as_bytes())?;
    writer.write_u8(0)?;
    Ok(())
}

/// Internal: write a string as BSON expects (length-prefixed + null).
fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), BsonError> {
    let bytes = s.as_bytes();
    writer.write_i32::<LittleEndian>((bytes.len() + 1) as i32)?;
    writer.write_all(bytes)?;
    writer.write_u8(0)?;
    Ok(())
      }
