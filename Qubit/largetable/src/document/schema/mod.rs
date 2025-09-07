// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Schema definitions for Largetable documents
//!
//! This module defines the document schema, including
//! field types, constraints, and validation logic.

use crate::{Document, Value};
use thiserror::Error;

pub mod field;
pub mod document_type;

pub use field::{Field, FieldType, FieldConstraints};
pub use document_type::{DocumentSchema, SchemaVersion};

/// Errors encountered during schema validation.
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Type mismatch for field {0}: expected {1}, found {2}")]
    TypeMismatch(String, String, String),

    #[error("Constraint violation for field {0}: {1}")]
    ConstraintViolation(String, String),

    #[error("Invalid nested document: {0}")]
    InvalidNestedDocument(String),
}

/// Validates a document against a schema.
///
/// # Arguments
/// * `doc` - The document to validate.
/// * `schema` - The schema to validate against.
///
/// # Returns
/// `Ok(())` if the document is valid, or a `ValidationError` if validation fails.
pub fn validate_document(doc: &Document, schema: &DocumentSchema) -> Result<(), ValidationError> {
    // Check required fields
    for (name, field) in &schema.fields {
        if field.constraints.required && !doc.fields.contains_key(name) {
            return Err(ValidationError::MissingField(name.clone()));
        }
    }

    // Validate field types and constraints
    for (name, value) in &doc.fields {
        let field = schema.fields.get(name).ok_or_else(|| {
            ValidationError::ConstraintViolation(name.clone(), "Unknown field".into())
        })?;

        match (&field.field_type, value) {
            (FieldType::Null, Value::Null) => Ok(()),
            (FieldType::Bool, Value::Bool(_)) => Ok(()),
            (FieldType::Int32, Value::Int32(_)) => {
                if let Some(min) = field.constraints.min {
                    if let Value::Int32(v) = value {
                        if (*v as f64) < min {
                            return Err(ValidationError::ConstraintViolation(
                                name.clone(),
                                format!("Value below min {}", min),
                            ));
                        }
                    }
                }
                Ok(())
            }
            (FieldType::Int64, Value::Int64(_)) => {
                if let Some(min) = field.constraints.min {
                    if let Value::Int64(v) = value {
                        if (*v as f64) < min {
                            return Err(ValidationError::ConstraintViolation(
                                name.clone(),
                                format!("Value below min {}", min),
                            ));
                        }
                    }
                }
                Ok(())
            }
            (FieldType::Float64, Value::Float64(_)) => {
                if let Some(min) = field.constraints.min {
                    if let Value::Float64(v) = value {
                        if *v < min {
                            return Err(ValidationError::ConstraintViolation(
                                name.clone(),
                                format!("Value below min {}", min),
                            ));
                        }
                    }
                }
                Ok(())
            }
            (FieldType::String, Value::String(s)) => {
                if let Some(max_len) = field.constraints.max_length {
                    if s.len() > max_len {
                        return Err(ValidationError::ConstraintViolation(
                            name.clone(),
                            format!("String length exceeds {}", max_len),
                        ));
                    }
                }
                if let Some(regex) = &field.constraints.regex {
                    let re = regex::Regex::new(regex).map_err(|e| {
                        ValidationError::ConstraintViolation(name.clone(), format!("Invalid regex: {}", e))
                    })?;
                    if !re.is_match(s) {
                        return Err(ValidationError::ConstraintViolation(
                            name.clone(),
                            format!("String does not match regex {}", regex),
                        ));
                    }
                }
                Ok(())
            }
            (FieldType::Binary, Value::Binary(b)) => {
                if let Some(max_len) = field.constraints.max_length {
                    if b.len() > max_len {
                        return Err(ValidationError::ConstraintViolation(
                            name.clone(),
                            format!("Binary length exceeds {}", max_len),
                        ));
                    }
                }
                Ok(())
            }
            (FieldType::Document(nested_schema), Value::Document(nested_doc)) => {
                validate_document(nested_doc, nested_schema)
                    .map_err(|e| ValidationError::InvalidNestedDocument(e.to_string()))
            }
            (FieldType::Array(element_type), Value::Array(arr)) => {
                for (i, value) in arr.iter().enumerate() {
                    let temp_field = Field {
                        name: format!("{}[{}]", name, i),
                        field_type: *element_type.clone(),
                        constraints: field.constraints.clone(),
                    };
                    let temp_schema = DocumentSchema {
                        name: format!("{}[{}]", name, i),
                        version: schema.version.clone(),
                        fields: std::collections::BTreeMap::from([(temp_field.name.clone(), temp_field)]),
                    };
                    let temp_doc = Document {
                        id: doc.id.clone(),
                        fields: std::collections::HashMap::from([(temp_field.name.clone(), value.clone())]),
                        version: doc.version,
                        created_at: doc.created_at,
                        updated_at: doc.updated_at,
                    };
                    validate_document(&temp_doc, &temp_schema)?;
                }
                Ok(())
            }
            (FieldType::Vector, Value::Vector(v)) => {
                if let Some(max_len) = field.constraints.max_length {
                    if v.len() > max_len {
                        return Err(ValidationError::ConstraintViolation(
                            name.clone(),
                            format!("Vector length exceeds {}", max_len),
                        ));
                    }
                }
                if let Some(min) = field.constraints.min {
                    for &val in v {
                        if val < min as f32 {
                            return Err(ValidationError::ConstraintViolation(
                                name.clone(),
                                format!("Vector value below min {}", min),
                            ));
                        }
                    }
                }
                Ok(())
            }
            _ => Err(ValidationError::TypeMismatch(
                name.clone(),
                format!("{:?}", field.field_type),
                format!("{:?}", value),
            )),
        }?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Document, DocumentId, Value};
    use std::collections::HashMap;

    #[test]
    fn test_validate_document() {
        let schema = DocumentSchema {
            name: "user".into(),
            version: SchemaVersion::new(1, 0),
            fields: std::collections::BTreeMap::from([
                (
                    "name".into(),
                    Field {
                        name: "name".into(),
                        field_type: FieldType::String,
                        constraints: FieldConstraints {
                            required: true,
                            max_length: Some(50),
                            regex: Some(r"^[A-Za-z]+$".into()),
                            ..FieldConstraints::new()
                        },
                    },
                ),
                (
                    "age".into(),
                    Field {
                        name: "age".into(),
                        field_type: FieldType::Int32,
                        constraints: FieldConstraints {
                            min: Some(18.0),
                            ..FieldConstraints::new()
                        },
                    },
                ),
            ]),
        };

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

        let invalid_doc_missing_field = Document {
            id: DocumentId::new_v7(),
            fields: HashMap::from([("age".into(), Value::Int32(25))]),
            version: 0,
            created_at: 0,
            updated_at: 0,
        };

        let invalid_doc_type_mismatch = Document {
            id: DocumentId::new_v7(),
            fields: HashMap::from([
                ("name".into(), Value::Int32(123)),
                ("age".into(), Value::Int32(25)),
            ]),
            version: 0,
            created_at: 0,
            updated_at: 0,
        };

        let invalid_doc_constraint = Document {
            id: DocumentId::new_v7(),
            fields: HashMap::from([
                ("name".into(), Value::String("Alice123".into())),
                ("age".into(), Value::Int32(15)),
            ]),
            version: 0,
            created_at: 0,
            updated_at: 0,
        };

        assert!(validate_document(&valid_doc, &schema).is_ok());
        assert!(matches!(
            validate_document(&invalid_doc_missing_field, &schema),
            Err(ValidationError::MissingField(_))
        ));
        assert!(matches!(
            validate_document(&invalid_doc_type_mismatch, &schema),
            Err(ValidationError::TypeMismatch(_, _, _))
        ));
        assert!(matches!(
            validate_document(&invalid_doc_constraint, &schema),
            Err(ValidationError::ConstraintViolation(_, _))
        ));
    }
                              }
