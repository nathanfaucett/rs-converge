#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};
#[cfg(feature = "std")]
use std::{string::String, vec::Vec};

use crate::key_encoding::{DefaultEncoding, KeyEncoding};
use crate::{EngineKey, EngineType, EngineValue, PrimaryKey};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
  pub name: String,
  pub data_type: EngineType,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexSchema {
  pub name: String,
  pub table_name: String,
  pub column_indices: Vec<usize>,
  pub unique: bool,
}

impl IndexSchema {
  pub fn make_entry_key(&self, index_key: &EngineKey, row_pk_key: &EngineKey) -> EngineKey {
    let mut composite = Vec::with_capacity(4 + index_key.len() + row_pk_key.len());
    composite.extend_from_slice(&(index_key.len() as u32).to_be_bytes());
    composite.extend_from_slice(index_key);
    composite.extend_from_slice(row_pk_key);
    composite
  }

  pub fn split_entry_key(
    &self,
    composite: &EngineKey,
  ) -> Result<(EngineKey, EngineKey), SchemaError> {
    if composite.len() < 4 {
      return Err(SchemaError::SchemaMismatch(
        "invalid index entry key".into(),
      ));
    }

    let len = u32::from_be_bytes([composite[0], composite[1], composite[2], composite[3]]) as usize;
    if composite.len() < 4 + len {
      return Err(SchemaError::SchemaMismatch(
        "invalid index entry key".into(),
      ));
    }

    let index_key = composite[4..4 + len].to_vec();
    let row_pk_key = composite[4 + len..].to_vec();
    Ok((index_key, row_pk_key))
  }

  pub fn validate_for_table(&self, table: &TableSchema) -> Result<(), SchemaError> {
    if self.table_name != table.name {
      return Err(SchemaError::SchemaMismatch(
        "index table name mismatch".into(),
      ));
    }
    if self.column_indices.is_empty() {
      return Err(SchemaError::SchemaMismatch(
        "index must have columns".into(),
      ));
    }
    for index in &self.column_indices {
      if *index >= table.columns.len() {
        return Err(SchemaError::SchemaMismatch(
          "index column out of bounds".into(),
        ));
      }
    }
    Ok(())
  }

  pub fn key_for(&self, row: &[EngineValue]) -> Result<EngineKey, SchemaError> {
    let mut values = Vec::with_capacity(self.column_indices.len());
    for &column_index in &self.column_indices {
      let value = row
        .get(column_index)
        .ok_or_else(|| SchemaError::SchemaMismatch("index column missing".into()))?;
      values.push(value.clone());
    }
    Ok(DefaultEncoding::encode_values(&values))
  }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
  pub name: String,
  pub columns: Vec<ColumnSchema>,
  pub primary_key: Vec<usize>,
}

impl TableSchema {
  pub fn validate_primary_key_definition(&self) -> Result<(), SchemaError> {
    if self.primary_key.is_empty() {
      return Err(SchemaError::PrimaryKeyMissing);
    }
    for index in &self.primary_key {
      if *index >= self.columns.len() {
        return Err(SchemaError::SchemaMismatch(
          "primary key column out of bounds".into(),
        ));
      }
    }
    Ok(())
  }

  pub fn validate_row(&self, row: &[EngineValue]) -> Result<(), SchemaError> {
    if row.len() != self.columns.len() {
      return Err(SchemaError::SchemaMismatch("row length mismatch".into()));
    }
    for (value, column) in row.iter().zip(self.columns.iter()) {
      if !column.data_type.matches_value(value) {
        return Err(SchemaError::TypeMismatch(format!(
          "column '{}' expects {:?}",
          column.name, column.data_type
        )));
      }
    }
    Ok(())
  }

  pub fn primary_key(&self, row: &[EngineValue]) -> Result<PrimaryKey, SchemaError> {
    self.validate_primary_key_definition()?;
    if self.primary_key.len() != 1 {
      return Err(SchemaError::SchemaMismatch(
        "only single-column UUID primary keys are supported".into(),
      ));
    }
    let value = row
      .get(self.primary_key[0])
      .ok_or_else(|| SchemaError::SchemaMismatch("primary key value missing".into()))?;

    match value {
      EngineValue::Uuid(bytes) => Ok(PrimaryKey::new(*bytes)),
      _ => Err(SchemaError::TypeMismatch("primary key must be UUID".into())),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
  SchemaMismatch(String),
  TypeMismatch(String),
  PrimaryKeyMissing,
}

impl From<String> for SchemaError {
  fn from(value: String) -> Self {
    SchemaError::SchemaMismatch(value)
  }
}
