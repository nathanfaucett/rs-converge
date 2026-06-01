#[cfg(not(feature = "std"))]
use alloc::{format, string::ToString, vec::Vec};

use serde::de::DeserializeOwned;

use crate::{ResultColumn, Row, TableSchema};

use super::errors::RowDeserializeError;
use super::row_deserializer::RowDeserializer;

pub trait FromRow: DeserializeOwned {
  fn from_row(schema: &TableSchema, row: &Row) -> Result<Self, RowDeserializeError> {
    let cols: Vec<ResultColumn> = schema
      .columns
      .iter()
      .enumerate()
      .map(|(i, c)| ResultColumn {
        name: c.name.clone(),
        source_table: None,
        source_column_index: Some(i as u8),
      })
      .collect();
    Self::from_named_row(&cols, row)
  }

  fn from_named_row(columns: &[ResultColumn], row: &Row) -> Result<Self, RowDeserializeError> {
    if columns.len() != row.len() {
      return Err(RowDeserializeError::SchemaError(format!(
        "columns metadata length {} does not match row length {}",
        columns.len(),
        row.len()
      )));
    }
    let deser = RowDeserializer::new(columns, row);
    Self::deserialize(deser).map_err(|e| RowDeserializeError::SchemaError(e.to_string()))
  }
}

impl<T: DeserializeOwned> FromRow for T {}
