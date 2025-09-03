// ===========================================
// Largetable - Distributed NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================
//! Core types used throughout Largetable

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Document ID type
pub type DocumentId = Uuid;

/// Collection name
pub type CollectionName = String;

/// Database name  
pub type DatabaseName = String;

/// BSON-like document representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub fields: HashMap<String, Value>,
}

/// Value types supported in documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Double(f64),
    String(String),
    Binary(Vec<u8>),
    Document(Document),
    Array(Vec<Value>),
    Timestamp(i64),
    ObjectId(DocumentId),
}
