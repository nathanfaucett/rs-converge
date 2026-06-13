#[cfg(all(not(feature = "std"), feature = "wasm"))]
use alloc::{boxed::Box, format, string::ToString};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use db_value::ValueType;

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
  pub primary_key: Vec<ColumnSchemaIndex>,
}
