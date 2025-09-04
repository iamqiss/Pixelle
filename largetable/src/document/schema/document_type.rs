// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Document schema definitions for Largetable
//!
//! Defines the structure and versioning of document schemas.

use super::field::{Field, FieldType};
use super::ValidationError;
use crate::Document;
use std::collections::BTreeMap;

/// Represents a Largetable document schema.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentSchema {
    /// The name of the schema (e.g., "user", "product").
    pub name: String,
    /// The version of the schema.
    pub version: SchemaVersion,
    /// The fields in the schema, stored in a BTreeMap to preserve order.
    pub fields: BTreeMap<String, Field>,
}

impl DocumentSchema {
    /// Creates a new schema with the given name and version.
    pub fn new(name: impl Into<String>, version: SchemaVersion) -> Self {
        DocumentSchema {
            name: name.into(),
            version,
            fields: BTreeMap::new(),
        }
    }

    /// Adds a field to the schema.
    pub fn add_field(mut self, field: Field) -> Self {
        self.fields.insert(field.name.clone(), field);
        self
    }

    /// Validates a document against the schema.
    pub fn validate(&self, doc: &Document) -> Result<(), ValidationError> {
        super::validate_document(doc, self)
    }

    /// Checks if the schema is compatible with another schema (e.g., same major version).
    pub fn is_compatible(&self, other: &Self) -> bool {
        self.version.major == other.version.major
    }
}

/// Represents a schema version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaVersion {
    /// Major version (breaking changes).
    pub major: u32,
    /// Minor version (backward-compatible changes).
    pub minor: u32,
}

impl SchemaVersion {
    /// Creates a new schema version.
    pub fn new(major: u32, minor: u32) -> Self {
        SchemaVersion { major, minor }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Document, DocumentId, Value};
    use super::super::field::{Field, FieldType, FieldConstraints};
    use std::collections::HashMap;

    #[test]
    fn test_document_schema() {
        let schema = DocumentSchema::new("user", SchemaVersion::new(1, 0))
            .add_field(Field {
                name: "name".into(),
                field_type: FieldType::String,
                constraints: FieldConstraints::new().required().max_length(50),
            })
            .add_field(Field {
                name: "age".into(),
                field_type: FieldType::Int32,
                constraints: FieldConstraints::new().min(18.0),
            });

        let valid_doc = Document {
            id: DocumentId::new_v7(),
            fields: HashMap::from([
                ("name".into(), Value::String("Alice".into())),
                ("age".into(), Value::Int32(25)),
            ]),
            version: 0,
            created_at: 0,
            updated_at: 0,
        };

        assert!(schema.validate(&valid_doc).is_ok());

        let other_schema = DocumentSchema::new("user", SchemaVersion::new(1, 1));
        assert!(schema.is_compatible(&other_schema));

        let incompatible_schema = DocumentSchema::new("user", SchemaVersion::new(2, 0));
        assert!(!schema.is_compatible(&incompatible_schema));
    }
}
