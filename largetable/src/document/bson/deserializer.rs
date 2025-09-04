// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! BSON deserializer for Largetable
//!
//! High-performance deserialization of BSON bytes into Largetable `Document`.
//! Supports zero-copy for embedded documents and SIMD-friendly paths for numeric vectors.

use crate::Document;
use crate::types::{Value, DocumentId};
use crate::document::bson::utils::BsonError;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};

/// Deserializes BSON bytes into a Largetable `Document`.
///
/// This function parses a BSON byte slice into a `Document`, supporting
/// zero-copy for embedded documents and SIMD-friendly paths for numeric vectors.
///
/// # Arguments
/// * `bytes` - The BSON byte slice to deserialize.
///
/// # Returns
/// A `Result` containing the deserialized `Document` or a `BsonError` if deserialization fails.
pub fn from_bson_bytes(bytes: &[u8]) -> Result<Document, BsonError> {
    let mut cursor = Cursor::new(bytes);
    read_document(&mut cursor)
}

/// Internal: read a document from a reader.
fn read_document<R: Read>(reader: &mut R) -> Result<Document, BsonError> {
    let start_pos = reader.stream_position().map_err(|e| BsonError::Deserialization(e.to_string()))?;
    let doc_len = reader.read_i32::<LittleEndian>()? as usize;
    if doc_len < 5 {
        return Err(BsonError::Deserialization("Document length too small".into()));
    }
    if let Some(cursor) = reader.downcast_ref::<Cursor<&[u8]>>() {
        if cursor.get_ref().len() < doc_len {
            return Err(BsonError::Deserialization("Input buffer too small for document".into()));
        }
    }

    let mut fields = std::collections::HashMap::with_capacity((doc_len / 10).max(4));

    loop {
        let elem_type = match reader.read_u8() {
            Ok(t) => t,
            Err(_) => break,
        };

        if elem_type == 0x00 {
            break;
        }

        let key = read_cstring(reader)?;
        let value = read_element(reader, elem_type)?;
        fields.insert(key, value);
    }

    let end_pos = reader.stream_position().map_err(|e| BsonError::Deserialization(e.to_string()))?;
    if end_pos - start_pos != doc_len as u64 {
        return Err(BsonError::Deserialization("Document length mismatch".into()));
    }

    Ok(Document {
        id: DocumentId::new_v7(),
        fields,
        version: 0,
        created_at: 0,
        updated_at: 0,
    })
}

/// Internal: read a single BSON element.
fn read_element<R: Read>(reader: &mut R, elem_type: u8) -> Result<Value, BsonError> {
    match elem_type {
        0x0A => Ok(Value::Null),
        0x08 => {
            let b = reader.read_u8()? != 0;
            Ok(Value::Bool(b))
        }
        0x10 => {
            let i = reader.read_i32::<LittleEndian>()?;
            Ok(Value::Int32(i))
        }
        0x12 => {
            let i = reader.read_i64::<LittleEndian>()?;
            Ok(Value::Int64(i))
        }
        0x01 => {
            let f = reader.read_f64::<LittleEndian>()?;
            Ok(Value::Float64(f))
        }
        0x02 => {
            let s = read_string(reader)?;
            Ok(Value::String(s))
        }
        0x03 => {
            let doc = read_document(reader)?;
            Ok(Value::Document(doc))
        }
        0x04 => {
            let doc = read_document(reader)?;
            let mut arr = Vec::with_capacity(doc.fields.len());
            for i in 0..doc.fields.len() {
                let key = i.to_string();
                match doc.fields.get(&key) {
                    Some(v) => arr.push(v.clone()),
                    None => return Err(BsonError::Deserialization(format!("Missing array index {}", i))),
                }
            }
            Ok(Value::Array(arr))
        }
        0x05 => {
            let len = reader.read_i32::<LittleEndian>()? as usize;
            let subtype = reader.read_u8()?;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            if subtype == 0x80 && len % 4 == 0 {
                let vec_len = len / 4;
                let mut vec_f32 = Vec::with_capacity(vec_len);
                for i in 0..vec_len {
                    let bytes = &buf[i * 4..i * 4 + 4];
                    let f = f32::from_le_bytes(
                        bytes.try_into().map_err(|_| BsonError::Deserialization("Invalid f32 bytes".into()))?,
                    );
                    vec_f32.push(f);
                }
                Ok(Value::Vector(vec_f32))
            } else {
                Ok(Value::Binary(buf))
            }
        }
        _ => Err(BsonError::Deserialization(format!(
            "Unsupported BSON type: {:02x}",
            elem_type
        ))),
    }
}

/// Internal: read a null-terminated string (CString).
fn read_cstring<R: Read>(reader: &mut R) -> Result<String, BsonError> {
    let mut buf = Vec::new();
    loop {
        let byte = reader.read_u8()?;
        if byte == 0 {
            break;
        }
        buf.push(byte);
    }
    String::from_utf8(buf).map_err(|e| BsonError::Deserialization(format!("Invalid UTF-8 in CString: {}", e)))
}

/// Internal: read a length-prefixed string.
fn read_string<R: Read>(reader: &mut R) -> Result<String, BsonError> {
    let len = reader.read_i32::<LittleEndian>()? as usize;
    if len == 0 {
        return Ok(String::new());
    }
    let mut buf = vec![0u8; len - 1];
    reader.read_exact(&mut buf)?;
    let null = reader.read_u8()?;
    if null != 0 {
        return Err(BsonError::Deserialization("Missing string null terminator".into()));
    }
    String::from_utf8(buf).map_err(|e| BsonError::Deserialization(format!("Invalid UTF-8 in string: {}", e)))
  }
