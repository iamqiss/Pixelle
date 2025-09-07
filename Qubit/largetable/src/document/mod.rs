// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Document operations and utilities

pub mod bson;
pub mod schema;
pub mod validation;
pub mod versioning;
pub mod zero_copy_serde;

use crate::{Result, DocumentId, Document, Value, LargetableError};
use serde_json::{Value as JsonValue, Map as JsonMap};
use std::collections::HashMap;
use tracing::{debug, error};

/// Document builder for creating documents with fluent API
pub struct DocumentBuilder {
    id: Option<DocumentId>,
    fields: HashMap<String, Value>,
}

impl DocumentBuilder {
    /// Create a new document builder
    pub fn new() -> Self {
        Self {
            id: None,
            fields: HashMap::new(),
        }
    }

    /// Set the document ID
    pub fn id(mut self, id: DocumentId) -> Self {
        self.id = Some(id);
        self
    }

    /// Add a field to the document
    pub fn field(mut self, key: String, value: Value) -> Self {
        self.fields.insert(key, value);
        self
    }

    /// Add a string field
    pub fn string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), Value::String(value.into()));
        self
    }

    /// Add an integer field
    pub fn int(mut self, key: impl Into<String>, value: i64) -> Self {
        self.fields.insert(key.into(), Value::Int64(value));
        self
    }

    /// Add a float field
    pub fn float(mut self, key: impl Into<String>, value: f64) -> Self {
        self.fields.insert(key.into(), Value::Float64(value));
        self
    }

    /// Add a boolean field
    pub fn bool(mut self, key: impl Into<String>, value: bool) -> Self {
        self.fields.insert(key.into(), Value::Bool(value));
        self
    }

    /// Add a null field
    pub fn null(mut self, key: impl Into<String>) -> Self {
        self.fields.insert(key.into(), Value::Null);
        self
    }

    /// Add an array field
    pub fn array(mut self, key: impl Into<String>, values: Vec<Value>) -> Self {
        self.fields.insert(key.into(), Value::Array(values));
        self
    }

    /// Add a nested document field
    pub fn document(mut self, key: impl Into<String>, doc: Document) -> Self {
        self.fields.insert(key.into(), Value::Document(doc));
        self
    }

    /// Add a vector field for AI/ML applications
    pub fn vector(mut self, key: impl Into<String>, vector: Vec<f32>) -> Self {
        self.fields.insert(key.into(), Value::Vector(vector));
        self
    }

    /// Build the document
    pub fn build(self) -> Document {
        let now = chrono::Utc::now().timestamp_micros();
        let id = self.id.unwrap_or_else(|| uuid::Uuid::now_v7());
        
        Document {
            id,
            fields: self.fields,
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }
}

impl Default for DocumentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Document utilities and operations
pub struct DocumentUtils;

impl DocumentUtils {
    /// Convert a document to JSON
    pub fn to_json(doc: &Document) -> Result<JsonValue> {
        let mut json = JsonMap::new();
        
        json.insert("_id".to_string(), JsonValue::String(doc.id.to_string()));
        json.insert("_version".to_string(), JsonValue::Number(doc.version.into()));
        json.insert("_created_at".to_string(), JsonValue::Number(doc.created_at.into()));
        json.insert("_updated_at".to_string(), JsonValue::Number(doc.updated_at.into()));
        
        for (key, value) in &doc.fields {
            json.insert(key.clone(), Self::value_to_json(value)?);
        }
        
        Ok(JsonValue::Object(json))
    }

    /// Convert JSON to a document
    pub fn from_json(json: JsonValue) -> Result<Document> {
        let mut doc = DocumentBuilder::new();
        
        if let JsonValue::Object(map) = json {
            for (key, value) in map {
                match key.as_str() {
                    "_id" => {
                        if let JsonValue::String(id_str) = value {
                            let id = uuid::Uuid::parse_str(&id_str)
                                .map_err(|e| LargetableError::Serialization(format!("Invalid document ID: {}", e)))?;
                            doc = doc.id(id);
                        }
                    }
                    "_version" => {
                        // Skip version, will be set during build
                    }
                    "_created_at" => {
                        // Skip created_at, will be set during build
                    }
                    "_updated_at" => {
                        // Skip updated_at, will be set during build
                    }
                    _ => {
                        doc = doc.field(key, Self::json_to_value(value)?);
                    }
                }
            }
        }
        
        Ok(doc.build())
    }

    /// Convert a Value to JSON
    fn value_to_json(value: &Value) -> Result<JsonValue> {
        match value {
            Value::Null => Ok(JsonValue::Null),
            Value::Bool(b) => Ok(JsonValue::Bool(*b)),
            Value::Int32(i) => Ok(JsonValue::Number((*i).into())),
            Value::Int64(i) => Ok(JsonValue::Number((*i).into())),
            Value::UInt64(u) => Ok(JsonValue::Number((*u).into())),
            Value::Float32(f) => Ok(JsonValue::Number(serde_json::Number::from_f64(*f as f64).unwrap_or(0.into()))),
            Value::Float64(f) => Ok(JsonValue::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into()))),
            Value::String(s) => Ok(JsonValue::String(s.clone())),
            Value::Binary(b) => Ok(JsonValue::Array(b.iter().map(|&x| JsonValue::Number(x.into())).collect())),
            Value::Document(d) => Self::to_json(d),
            Value::Array(arr) => {
                let json_arr: Result<Vec<JsonValue>> = arr.iter().map(Self::value_to_json).collect();
                Ok(JsonValue::Array(json_arr?))
            }
            Value::Timestamp(t) => Ok(JsonValue::Number((*t).into())),
            Value::ObjectId(id) => Ok(JsonValue::String(id.to_string())),
            Value::Vector(v) => {
                let json_arr: Vec<JsonValue> = v.iter().map(|&f| JsonValue::Number(serde_json::Number::from_f64(f as f64).unwrap_or(0.into()))).collect();
                Ok(JsonValue::Array(json_arr))
            }
            Value::Decimal128(bytes) => {
                // Convert decimal to string representation
                Ok(JsonValue::String(format!("{:?}", bytes)))
            }
        }
    }

    /// Convert JSON to a Value
    fn json_to_value(json: JsonValue) -> Result<Value> {
        match json {
            JsonValue::Null => Ok(Value::Null),
            JsonValue::Bool(b) => Ok(Value::Bool(b)),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int64(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float64(f))
                } else {
                    Ok(Value::String(n.to_string()))
                }
            }
            JsonValue::String(s) => Ok(Value::String(s)),
            JsonValue::Array(arr) => {
                let values: Result<Vec<Value>> = arr.into_iter().map(Self::json_to_value).collect();
                Ok(Value::Array(values?))
            }
            JsonValue::Object(map) => {
                let mut fields = HashMap::new();
                for (key, value) in map {
                    fields.insert(key, Self::json_to_value(value)?);
                }
                Ok(Value::Document(Document {
                    id: uuid::Uuid::now_v7(),
                    fields,
                    version: 1,
                    created_at: chrono::Utc::now().timestamp_micros(),
                    updated_at: chrono::Utc::now().timestamp_micros(),
                }))
            }
        }
    }

    /// Get a field value from a document
    pub fn get_field(doc: &Document, field_path: &str) -> Option<&Value> {
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = &doc.fields;
        
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                return current.get(*part);
            }
            
            if let Some(Value::Document(nested_doc)) = current.get(*part) {
                current = &nested_doc.fields;
            } else {
                return None;
            }
        }
        
        None
    }

    /// Set a field value in a document
    pub fn set_field(doc: &mut Document, field_path: &str, value: Value) -> Result<()> {
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = &mut doc.fields;
        
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                current.insert(part.to_string(), value);
                return Ok(());
            }
            
            if let Some(Value::Document(nested_doc)) = current.get_mut(*part) {
                current = &mut nested_doc.fields;
            } else {
                // Create nested document if it doesn't exist
                let nested_doc = Document {
                    id: uuid::Uuid::now_v7(),
                    fields: HashMap::new(),
                    version: 1,
                    created_at: chrono::Utc::now().timestamp_micros(),
                    updated_at: chrono::Utc::now().timestamp_micros(),
                };
                current.insert(part.to_string(), Value::Document(nested_doc));
                
                if let Some(Value::Document(nested_doc)) = current.get_mut(*part) {
                    current = &mut nested_doc.fields;
                }
            }
        }
        
        Ok(())
    }

    /// Check if a document matches a filter
    pub fn matches_filter(doc: &Document, filter: &JsonValue) -> Result<bool> {
        match filter {
            JsonValue::Object(filter_map) => {
                for (key, expected_value) in filter_map {
                    if let Some(actual_value) = Self::get_field(doc, key) {
                        if !Self::value_matches(actual_value, expected_value)? {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Check if a value matches a JSON value
    fn value_matches(value: &Value, json: &JsonValue) -> Result<bool> {
        match (value, json) {
            (Value::Null, JsonValue::Null) => Ok(true),
            (Value::Bool(b), JsonValue::Bool(jb)) => Ok(b == jb),
            (Value::Int64(i), JsonValue::Number(jn)) => Ok(Some(*i) == jn.as_i64()),
            (Value::Float64(f), JsonValue::Number(jn)) => Ok(Some(*f) == jn.as_f64()),
            (Value::String(s), JsonValue::String(js)) => Ok(s == js),
            (Value::Array(arr), JsonValue::Array(jarr)) => {
                if arr.len() != jarr.len() {
                    return Ok(false);
                }
                for (v, jv) in arr.iter().zip(jarr.iter()) {
                    if !Self::value_matches(v, jv)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}