use alloc::{string::String, vec::Vec};

use crate::{JsonValue, Row, Value, ValueType};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromRowError {
    MissingColumn(String),
    DuplicateColumn(String),
    MissingValue(String),
    InvalidValue {
        column: String,
        expected: ValueType,
        actual: ValueType,
    },
}

impl core::fmt::Display for FromRowError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingColumn(column) => write!(formatter, "missing column {column}"),
            Self::DuplicateColumn(column) => write!(formatter, "duplicate column {column}"),
            Self::MissingValue(column) => write!(formatter, "missing value for column {column}"),
            Self::InvalidValue {
                column,
                expected,
                actual,
            } => write!(
                formatter,
                "invalid value for column {column}: expected {expected:?}, got {actual:?}"
            ),
        }
    }
}

pub trait FromValue: Sized {
    const TYPE: ValueType;

    fn from_value(value: &Value) -> Option<Self>;
}

pub trait FromRow: Sized {
    fn from_row(row: &Row, columns: &[&str]) -> Result<Self, FromRowError>;
}

pub fn value<'a>(row: &'a Row, columns: &[&str], column: &str) -> Result<&'a Value, FromRowError> {
    let mut indices = columns
        .iter()
        .enumerate()
        .filter_map(|(index, name)| (*name == column).then_some(index));
    let index = indices
        .next()
        .ok_or_else(|| FromRowError::MissingColumn(String::from(column)))?;
    if indices.next().is_some() {
        return Err(FromRowError::DuplicateColumn(String::from(column)));
    }
    row.values
        .get(index)
        .ok_or_else(|| FromRowError::MissingValue(String::from(column)))
}

pub fn decode<T: FromValue>(value: &Value, column: &str) -> Result<T, FromRowError> {
    T::from_value(value).ok_or_else(|| FromRowError::InvalidValue {
        column: String::from(column),
        expected: T::TYPE,
        actual: value.r#type(),
    })
}

macro_rules! from_value {
    ($type:ty, $value_type:ident, $method:ident) => {
        impl FromValue for $type {
            const TYPE: ValueType = ValueType::$value_type;

            fn from_value(value: &Value) -> Option<Self> {
                value.$method()
            }
        }
    };
}

from_value!(Uuid, Uuid, to_uuid);
from_value!(bool, Bool, to_bool);
from_value!(i64, Integer, to_integer);
from_value!(f64, Float, to_float);
from_value!(String, Text, to_text);
from_value!(Vec<u8>, Blob, to_blob);
from_value!(JsonValue, Json, to_json);

impl FromValue for Value {
    const TYPE: ValueType = ValueType::Null;

    fn from_value(value: &Value) -> Option<Self> {
        Some(value.clone())
    }
}

impl<T: FromValue> FromValue for Option<T> {
    const TYPE: ValueType = T::TYPE;

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(None),
            value => T::from_value(value).map(Some),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_values_and_null_options() {
        assert_eq!(decode::<i64>(&Value::Integer(1), "id"), Ok(1));
        assert_eq!(decode::<Option<String>>(&Value::Null, "name"), Ok(None));
        assert_eq!(
            decode::<bool>(&Value::Integer(1), "active"),
            Err(FromRowError::InvalidValue {
                column: String::from("active"),
                expected: ValueType::Bool,
                actual: ValueType::Integer,
            })
        );
    }
}
