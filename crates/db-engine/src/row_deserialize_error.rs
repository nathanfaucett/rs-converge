#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(feature = "std")]
use std::string::{String, ToString};

use core::fmt;

/// Error type for row deserialization failures.
///
/// Provides detailed context about what went wrong when deserializing a row into a typed struct.
#[derive(Debug, Clone, PartialEq)]
pub enum RowDeserializeError {
  /// Column name not found in the schema
  ColumnNotFound { column: String },

  /// Type mismatch: expected type doesn't match actual value
  TypeMismatch {
    column: String,
    expected_type: String,
    actual_value: String,
  },

  /// Required field is missing or NULL
  MissingRequiredField { column: String },

  /// Serde deserialization error
  SerdeError { message: String },

  /// Schema validation failed
  SchemaError { message: String },
}

impl fmt::Display for RowDeserializeError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      RowDeserializeError::ColumnNotFound { column } => {
        write!(f, "Column '{}' not found in schema", column)
      }
      RowDeserializeError::TypeMismatch {
        column,
        expected_type,
        actual_value,
      } => {
        write!(
          f,
          "Column '{}' expects {}, but got: {}",
          column, expected_type, actual_value
        )
      }
      RowDeserializeError::MissingRequiredField { column } => {
        write!(f, "Required field '{}' is NULL or missing", column)
      }
      RowDeserializeError::SerdeError { message } => {
        write!(f, "Deserialization error: {}", message)
      }
      RowDeserializeError::SchemaError { message } => {
        write!(f, "Schema error: {}", message)
      }
    }
  }
}

#[cfg(feature = "std")]
impl std::error::Error for RowDeserializeError {}

#[cfg(not(feature = "std"))]
impl core::error::Error for RowDeserializeError {}

impl serde::de::Error for RowDeserializeError {
  fn custom<T>(msg: T) -> Self
  where
    T: fmt::Display,
  {
    RowDeserializeError::SerdeError {
      message: msg.to_string(),
    }
  }
}

impl RowDeserializeError {
  pub fn column_not_found(column: impl Into<String>) -> Self {
    RowDeserializeError::ColumnNotFound {
      column: column.into(),
    }
  }

  pub fn type_mismatch(
    column: impl Into<String>,
    expected_type: impl Into<String>,
    actual_value: impl Into<String>,
  ) -> Self {
    RowDeserializeError::TypeMismatch {
      column: column.into(),
      expected_type: expected_type.into(),
      actual_value: actual_value.into(),
    }
  }

  pub fn missing_required_field(column: impl Into<String>) -> Self {
    RowDeserializeError::MissingRequiredField {
      column: column.into(),
    }
  }

  pub fn serde_error(message: impl Into<String>) -> Self {
    RowDeserializeError::SerdeError {
      message: message.into(),
    }
  }

  pub fn schema_error(message: impl Into<String>) -> Self {
    RowDeserializeError::SchemaError {
      message: message.into(),
    }
  }
}
