// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Type conversions between standard and zero-copy document types

use rkyv::Archived;
use std::collections::HashMap;
use crate::{Document, Value};
use super::{ZeroCopyDocument, ZeroCopyValue};

/// Convert from standard Document to ZeroCopyDocument
impl From<Document> for ZeroCopyDocument {
    fn from(doc: Document) -> Self {
        let mut zero_copy_fields = HashMap::new();
        let mut total_size = 0usize;
        
        for (key, value) in doc.fields {
            let zero_copy_value = ZeroCopyValue::from(value);
            total_size += key.len() + zero_copy_value.estimate_size();
            zero_copy_fields.insert(key, zero_copy_value);
        }
        
        Self {
            id: doc.id,
            fields: zero_copy_fields,
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
            size_bytes: total_size as u32,
        }
    }
}

/// Convert from ZeroCopyDocument to standard Document
impl From<ZeroCopyDocument> for Document {
    fn from(doc: ZeroCopyDocument) -> Self {
        let mut standard_fields = HashMap::new();
        
        for (key, value) in doc.fields {
            standard_fields.insert(key, Value::from(value));
        }
        
        Self {
            id: doc.id,
            fields: standard_fields,
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
        }
    }
}

/// Convert from archived ZeroCopyDocument to standard Document
impl From<&Archived<ZeroCopyDocument>> for Document {
    fn from(archived: &Archived<ZeroCopyDocument>) -> Self {
        let mut fields = HashMap::new();
        
        for (k, v) in &archived.fields {
            fields.insert(k.to_string(), Value::from(v));
        }
        
        Self {
            id: archived.id,
            fields,
            version: archived.version,
            created_at: archived.created_at,
            updated_at: archived.updated_at,
        }
    }
}

/// Convert from standard Value to ZeroCopyValue
impl From<Value> for ZeroCopyValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => ZeroCopyValue::Null,
            Value::Bool(b) => ZeroCopyValue::Bool(b),
            Value::Int32(i) => ZeroCopyValue::Int32(i),
            Value::Int64(i) => ZeroCopyValue::Int64(i),
            Value::UInt64(u) => ZeroCopyValue::UInt64(u),
            Value::Float32(f) => ZeroCopyValue::Float32(f),
            Value::Float64(f) => ZeroCopyValue::Float64(f),
            Value::String(s) => ZeroCopyValue::String(s),
            Value::Binary(b) => ZeroCopyValue::Binary(b),
            Value::Document(d) => ZeroCopyValue::Document(ZeroCopyDocument::from(d)),
            Value::Array(arr) => {
                let zero_copy_arr: Vec<ZeroCopyValue> = arr
                    .into_iter()
                    .map(ZeroCopyValue::from)
                    .collect();
                ZeroCopyValue::Array(zero_copy_arr)
            },
            Value::Timestamp(t) => ZeroCopyValue::Timestamp(t),
            Value::ObjectId(id) => ZeroCopyValue::ObjectId(id),
            Value::Vector(v) => {
                #[cfg(feature = "vector")]
                {
                    use rkyv::AlignedVec;
                    ZeroCopyValue::Vector(AlignedVec::from(v))
                }
                #[cfg(not(feature = "vector"))]
                {
                    // Store as binary data if vector feature is not enabled
                    let bytes: Vec<u8> = v.into_iter()
                        .flat_map(|f| f.to_le_bytes())
                        .collect();
                    ZeroCopyValue::Binary(bytes)
                }
            },
            Value::Decimal128(d) => ZeroCopyValue::Decimal128(d),
        }
    }
}

/// Convert from ZeroCopyValue to standard Value
impl From<ZeroCopyValue> for Value {
    fn from(value: ZeroCopyValue) -> Self {
        match value {
            ZeroCopyValue::Null => Value::Null,
            ZeroCopyValue::Bool(b) => Value::Bool(b),
            ZeroCopyValue::Int32(i) => Value::Int32(i),
            ZeroCopyValue::Int64(i) => Value::Int64(i),
            ZeroCopyValue::UInt64(u) => Value::UInt64(u),
            ZeroCopyValue::Float32(f) => Value::Float32(f),
            ZeroCopyValue::Float64(f) => Value::Float64(f),
            ZeroCopyValue::String(s) => Value::String(s),
            ZeroCopyValue::Binary(b) => Value::Binary(b),
            ZeroCopyValue::Document(d) => Value::Document(Document::from(d)),
            ZeroCopyValue::Array(arr) => {
                let standard_arr: Vec<Value> = arr
                    .into_iter()
                    .map(Value::from)
                    .collect();
                Value::Array(standard_arr)
            },
            ZeroCopyValue::Timestamp(t) => Value::Timestamp(t),
            ZeroCopyValue::ObjectId(id) => Value::ObjectId(id),
            #[cfg(feature = "vector")]
            ZeroCopyValue::Vector(v) => Value::Vector(v.to_vec()),
            ZeroCopyValue::Decimal128(d) => Value::Decimal128(d),
        }
    }
}

/// Convert from archived ZeroCopyValue to standard Value
impl From<&Archived<ZeroCopyValue>> for Value {
    fn from(archived: &Archived<ZeroCopyValue>) -> Self {
        match archived {
            rkyv::Archived::<ZeroCopyValue>::Null => Value::Null,
            rkyv::Archived::<ZeroCopyValue>::Bool(b) => Value::Bool(*b),
            rkyv::Archived::<ZeroCopyValue>::Int32(i) => Value::Int32(*i),
            rkyv::Archived::<ZeroCopyValue>::Int64(i) => Value::Int64(*i),
            rkyv::Archived::<ZeroCopyValue>::UInt64(u) => Value::UInt64(*u),
            rkyv::Archived::<ZeroCopyValue>::Float32(f) => Value::Float32(*f),
            rkyv::Archived::<ZeroCopyValue>::Float64(f) => Value::Float64(*f),
            rkyv::Archived::<ZeroCopyValue>::String(s) => Value::String(s.to_string()),
            rkyv::Archived::<ZeroCopyValue>::Binary(b) => Value::Binary(b.to_vec()),
            rkyv::Archived::<ZeroCopyValue>::Document(d) => Value::Document(Document::from(d)),
            rkyv::Archived::<ZeroCopyValue>::Array(arr) => {
                let standard_arr: Vec<Value> = arr
                    .iter()
                    .map(Value::from)
                    .collect();
                Value::Array(standard_arr)
            },
            rkyv::Archived::<ZeroCopyValue>::Timestamp(t) => Value::Timestamp(*t),
            rkyv::Archived::<ZeroCopyValue>::ObjectId(id) => Value::ObjectId(*id),
            #[cfg(feature = "vector")]
            rkyv::Archived::<ZeroCopyValue>::Vector(v) => Value::Vector(v.to_vec()),
            rkyv::Archived::<ZeroCopyValue>::Decimal128(d) => Value::Decimal128(*d),
        }
    }
}

/// Batch conversion utilities for high-performance scenarios
pub struct BatchConverter;

impl BatchConverter {
    /// Convert multiple standard documents to zero-copy documents
    pub fn documents_to_zero_copy(docs: Vec<Document>) -> Vec<ZeroCopyDocument> {
        docs.into_iter()
            .map(ZeroCopyDocument::from)
            .collect()
    }
    
    /// Convert multiple zero-copy documents to standard documents
    pub fn zero_copy_to_documents(docs: Vec<ZeroCopyDocument>) -> Vec<Document> {
        docs.into_iter()
            .map(Document::from)
            .collect()
    }
    
    /// Convert archived documents to standard documents with parallel processing
    #[cfg(feature = "rayon")]
    pub fn archived_to_documents_parallel(
        archived_docs: Vec<&Archived<ZeroCopyDocument>>
    ) -> Vec<Document> {
        use rayon::prelude::*;
        
        archived_docs
            .par_iter()
            .map(|&archived| Document::from(archived))
            .collect()
    }
    
    /// Convert standard documents to zero-copy with parallel processing
    #[cfg(feature = "rayon")]
    pub fn documents_to_zero_copy_parallel(docs: Vec<Document>) -> Vec<ZeroCopyDocument> {
        use rayon::prelude::*;
        
        docs.into_par_iter()
            .map(ZeroCopyDocument::from)
            .collect()
    }
}

/// Specialized converters for specific use cases
pub struct SpecializedConverters;

impl SpecializedConverters {
    /// Convert a document while preserving only specific fields (projection)
    pub fn project_to_zero_copy(
        doc: &Document,
        fields: &[&str]
    ) -> ZeroCopyDocument {
        let mut projected_fields = HashMap::new();
        let mut total_size = 0usize;
        
        for &field_name in fields {
            if let Some(value) = doc.fields.get(field_name) {
                let zero_copy_value = ZeroCopyValue::from(value.clone());
                total_size += field_name.len() + zero_copy_value.estimate_size();
                projected_fields.insert(field_name.to_string(), zero_copy_value);
            }
        }
        
        ZeroCopyDocument {
            id: doc.id,
            fields: projected_fields,
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
            size_bytes: total_size as u32,
        }
    }
    
    /// Convert only the top-level fields, without nested document conversion
    pub fn shallow_to_zero_copy(doc: &Document) -> ZeroCopyDocument {
        let mut fields = HashMap::new();
        let mut total_size = 0usize;
        
        for (key, value) in &doc.fields {
            let zero_copy_value = match value {
                Value::Document(_) => ZeroCopyValue::Null, // Skip nested documents
                Value::Array(arr) if arr.iter().any(|v| matches!(v, Value::Document(_))) => {
                    // Skip arrays containing documents
                    ZeroCopyValue::Null
                },
                other => ZeroCopyValue::from(other.clone()),
            };
            
            total_size += key.len() + zero_copy_value.estimate_size();
            fields.insert(key.clone(), zero_copy_value);
        }
        
        ZeroCopyDocument {
            id: doc.id,
            fields,
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
            size_bytes: total_size as u32,
        }
    }
    
    /// Convert with type validation and error handling
    pub fn safe_to_zero_copy(doc: &Document) -> Result<ZeroCopyDocument, ConversionError> {
        let mut fields = HashMap::new();
        let mut total_size = 0usize;
        
        for (key, value) in &doc.fields {
            // Validate field name
            if key.is_empty() || key.len() > 1024 {
                return Err(ConversionError::InvalidFieldName(key.clone()));
            }
            
            // Validate value size
            let estimated_size = estimate_value_size(value);
            if estimated_size > 16 * 1024 * 1024 { // 16MB limit per field
                return Err(ConversionError::ValueTooLarge {
                    field: key.clone(),
                    size: estimated_size,
                });
            }
            
            let zero_copy_value = ZeroCopyValue::from(value.clone());
            total_size += key.len() + zero_copy_value.estimate_size();
            fields.insert(key.clone(), zero_copy_value);
        }
        
        Ok(ZeroCopyDocument {
            id: doc.id,
            fields,
            version: doc.version,
            created_at: doc.created_at,
            updated_at: doc.updated_at,
            size_bytes: total_size as u32,
        })
    }
}

/// Conversion errors
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("Invalid field name: {0}")]
    InvalidFieldName(String),
    
    #[error("Value too large for field {field}: {size} bytes")]
    ValueTooLarge {
        field: String,
        size: usize,
    },
    
    #[error("Unsupported value type in field: {0}")]
    UnsupportedType(String),
    
    #[error("Document too large: {0} bytes")]
    DocumentTooLarge(usize),
}

/// Estimate the serialized size of a value
fn estimate_value_size(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 1,
        Value::Int32(_) => 4,
        Value::Int64(_) | Value::UInt64(_) => 8,
        Value::Float32(_) => 4,
        Value::Float64(_) => 8,
        Value::String(s) => s.len() + 4, // Length prefix
        Value::Binary(b) => b.len() + 4,
        Value::Document(d) => {
            d.fields.iter()
                .map(|(k, v)| k.len() + estimate_value_size(v))
                .sum::<usize>() + 16 // Metadata overhead
        },
        Value::Array(arr) => {
            arr.iter()
                .map(estimate_value_size)
                .sum::<usize>() + 4 // Length prefix
        },
        Value::Timestamp(_) => 8,
        Value::ObjectId(_) => 16,
        Value::Vector(v) => v.len() * 4 + 4,
        Value::Decimal128(_) => 16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use std::collections::HashMap;

    #[test]
    fn test_document_conversion_roundtrip() {
        // Create a standard document
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("Neo Qiss".to_string()));
        fields.insert("age".to_string(), Value::Int32(30));
        fields.insert("active".to_string(), Value::Bool(true));
        
        let original = Document {
            id: Uuid::new_v4(),
            fields,
            version: 1,
            created_at: chrono::Utc::now().timestamp_micros(),
            updated_at: chrono::Utc::now().timestamp_micros(),
        };
        
        // Convert to zero-copy and back
        let zero_copy = ZeroCopyDocument::from(original.clone());
        let converted_back = Document::from(zero_copy);
        
        // Verify integrity
        assert_eq!(original.id, converted_back.id);
        assert_eq!(original.version, converted_back.version);
        assert_eq!(original.fields.len(), converted_back.fields.len());
        
        // Check specific fields
        match (&original.fields["name"], &converted_back.fields["name"]) {
            (Value::String(orig), Value::String(conv)) => assert_eq!(orig, conv),
            _ => panic!("String field conversion failed"),
        }
    }
    
    #[test]
    fn test_batch_conversion() {
        let docs = vec![
            Document {
                id: Uuid::new_v4(),
                fields: [("test".to_string(), Value::Int32(1))].into(),
                version: 1,
                created_at: 0,
                updated_at: 0,
            },
            Document {
                id: Uuid::new_v4(),
                fields: [("test".to_string(), Value::Int32(2))].into(),
                version: 1,
                created_at: 0,
                updated_at: 0,
            },
        ];
        
        let zero_copy_docs = BatchConverter::documents_to_zero_copy(docs.clone());
        assert_eq!(zero_copy_docs.len(), 2);
        
        let converted_back = BatchConverter::zero_copy_to_documents(zero_copy_docs);
        assert_eq!(converted_back.len(), 2);
        
        // Verify first document
        if let (Some(Value::Int32(orig)), Some(Value::Int32(conv))) = (
            docs[0].fields.get("test"),
            converted_back[0].fields.get("test")
        ) {
            assert_eq!(orig, conv);
        } else {
            panic!("Batch conversion failed");
        }
    }
    
    #[test]
    fn test_projection_conversion() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::String("Neo".to_string()));
        fields.insert("age".to_string(), Value::Int32(30));
        fields.insert("secret".to_string(), Value::String("hidden".to_string()));
        
        let doc = Document {
            id: Uuid::new_v4(),
            fields,
            version: 1,
            created_at: 0,
            updated_at: 0,
        };
        
        // Project only name and age
        let projected = SpecializedConverters::project_to_zero_copy(&doc, &["name", "age"]);
        
        assert_eq!(projected.fields.len(), 2);
        assert!(projected.fields.contains_key("name"));
        assert!(projected.fields.contains_key("age"));
        assert!(!projected.fields.contains_key("secret"));
    }
    
    #[test]
    fn test_safe_conversion_with_validation() {
        let mut fields = HashMap::new();
        fields.insert("valid".to_string(), Value::String("ok".to_string()));
        
        let doc = Document {
            id: Uuid::new_v4(),
            fields,
            version: 1,
            created_at: 0,
            updated_at: 0,
        };
        
        let result = SpecializedConverters::safe_to_zero_copy(&doc);
        assert!(result.is_ok());
        
        // Test with invalid field name
        let mut bad_fields = HashMap::new();
        bad_fields.insert("".to_string(), Value::String("empty key".to_string()));
        
        let bad_doc = Document {
            id: Uuid::new_v4(),
            fields: bad_fields,
            version: 1,
            created_at: 0,
            updated_at: 0,
        };
        
        let result = SpecializedConverters::safe_to_zero_copy(&bad_doc);
        assert!(matches!(result, Err(ConversionError::InvalidFieldName(_))));
    }
  }
