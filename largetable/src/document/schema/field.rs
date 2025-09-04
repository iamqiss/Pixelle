// ===========================================
// Largetable - Next-Generation NoSQL Database
// (c) 2025 Neo Qiss. All Rights Reserved.
// Built to outperform MongoDB with Rust's power.
// ===========================================

//! Field definitions for Largetable document schemas
//!
//! Defines the structure and constraints for individual fields in a schema.

use crate::Value;
use super::document_type::DocumentSchema;

/// Represents a field in a Largetable document schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// The name of the field.
    pub name: String,
    /// The expected type of the field.
    pub field_type: FieldType,
    /// Constraints on the field’s value (e.g., required, max length).
    pub constraints: FieldConstraints,
}

/// Defines the type of a field in a schema.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Null,
    Bool,
    Int32,
    Int64,
    Float64,
    String,
    Binary,
    Document(DocumentSchema),
    Array(Box<FieldType>),
    Vector,
}

impl FieldType {
    /// Checks if a value matches the field type.
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (FieldType::Null, Value::Null) => true,
            (FieldType::Bool, Value::Bool(_)) => true,
            (FieldType::Int32, Value::Int32(_)) => true,
            (FieldType::Int64, Value::Int64(_)) => true,
            (FieldType::Float64, Value::Float64(_)) => true,
            (FieldType::String, Value::String(_)) => true,
            (FieldType::Binary, Value::Binary(_)) => true,
            (FieldType::Document(_), Value::Document(_)) => true,
            (FieldType::Array(element_type), Value::Array(arr)) => {
                arr.iter().all(|v| element_type.matches(v))
            }
            (FieldType::Vector, Value::Vector(_)) => true,
            _ => false,
        }
    }
}

/// Defines constraints for a field.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldConstraints {
    /// Whether the field is required.
    pub required: bool,
    /// Default value if the field is missing.
    pub default: Option<Value>,
    /// Minimum value for numeric fields.
    pub min: Option<f64>,
    /// Maximum value for numeric fields.
    pub max: Option<f64>,
    /// Maximum length for strings, arrays, or binary data.
    pub max_length: Option<usize>,
    /// Regex pattern for strings.
    pub regex: Option<String>,
}

impl FieldConstraints {
    /// Creates a new set of constraints with default values.
    pub fn new() -> Self {
        FieldConstraints {
            required: false,
            default: None,
            min: None,
            max: None,
            max_length: None,
            regex: None,
        }
    }

    /// Marks the field as required.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Sets a default value for the field.
    pub fn default_value(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    /// Sets a minimum value for numeric fields.
    pub fn min(mut self, min: f64) -> Self {
        self.min = Some(min);
        self
    }

    /// Sets a maximum value for numeric fields.
    pub fn max(mut self, max: f64) -> Self {
        self.max = Some(max);
        self
    }

    /// Sets a maximum length for strings, arrays, or binary data.
    pub fn max_length(mut self, max_length: usize) -> Self {
        self.max_length = Some(max_length);
        self
    }

    /// Sets a regex pattern for strings.
    pub fn regex(mut self, regex: impl Into<String>) -> Self {
        self.regex = Some(regex.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn test_field_type_matches() {
        let string_type = FieldType::String;
        let int_type = FieldType::Int32;
        let array_type = FieldType::Array(Box::new(FieldType::Int32));

        assert!(string_type.matches(&Value::String("test".into())));
        assert!(!string_type.matches(&Value::Int32(123)));

        assert!(int_type.matches(&Value::Int32(123)));
        assert!(!int_type.matches(&Value::String("test".into())));

        assert!(array_type.matches(&Value::Array(vec![Value::Int32(1), Value::Int32(2)])));
        assert!(!array_type.matches(&Value::Array(vec![Value::String("test".into())])));
    }

    #[test]
    fn test_field_constraints_builder() {
        let constraints = FieldConstraints::new()
            .required()
            .max_length(50)
            .regex("^[A-Za-z]+$");

        assert!(constraints.required);
        assert_eq!(constraints.max_length, Some(50));
        assert_eq!(constraints.regex, Some("^[A-Za-z]+$".into()));
    }
  }
