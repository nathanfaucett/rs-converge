#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{boxed::Box, format, string::ToString};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use value::{Value, ValueType};

pub type ColumnSchemaIndex = u32;

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct ColumnSchema {
    pub name: String,
    pub r#type: ValueType,
    pub default: Value,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct IndexSchema {
    pub name: String,
    pub table_name: String,
    pub column_indices: Vec<ColumnSchemaIndex>,
    pub unique: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
    feature = "wasm",
    derive(tsify::Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn validate_uuid_primary_key(&self) -> Result<(), &'static str> {
        let mut primary_keys = self.columns.iter().filter(|column| column.primary_key);
        let Some(primary_key) = primary_keys.next() else {
            return Err("Table requires a UUID primary key");
        };
        if primary_keys.next().is_some() {
            return Err("Table requires exactly one primary key");
        }
        if primary_key.r#type != ValueType::Uuid {
            return Err("Primary key must have UUID type");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "std"))]
    use alloc::vec;
    #[cfg(feature = "std")]
    use std::vec;

    use super::*;

    fn column(value_type: ValueType, primary_key: bool) -> ColumnSchema {
        ColumnSchema {
            name: String::new(),
            r#type: value_type,
            default: Value::Null,
            primary_key,
        }
    }

    #[test]
    fn validates_one_uuid_primary_key() {
        let valid = TableSchema {
            name: String::new(),
            columns: vec![column(ValueType::Uuid, true)],
        };
        assert_eq!(valid.validate_uuid_primary_key(), Ok(()));

        for columns in [
            vec![column(ValueType::Text, false)],
            vec![column(ValueType::Integer, true)],
            vec![column(ValueType::Uuid, true), column(ValueType::Uuid, true)],
        ] {
            assert!(
                TableSchema {
                    name: String::new(),
                    columns
                }
                .validate_uuid_primary_key()
                .is_err()
            );
        }
    }
}
