#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};
#[cfg(feature = "std")]
use std::{string::String, vec::Vec};

use crate::key_encoding::{DefaultEncoding, KeyEncoding};
use crate::schema::SchemaError;
use crate::{EngineKey, EngineValue, IndexSchema, TableSchema};

pub const TABLE_SCHEMA_TREE: &str = "table_schema";
pub const INDEX_SCHEMA_TREE: &str = "index_schema";

pub fn row_tree(table_name: &str) -> String {
  format!("t:{}", table_name)
}

pub fn index_tree(index_name: &str) -> String {
  format!("i:{}", index_name)
}

pub fn table_schema_entry_key(table_name: impl AsRef<str>) -> EngineKey {
  DefaultEncoding::encode_values(&[EngineValue::Text(table_name.as_ref().to_string())])
}

pub fn index_schema_entry_key(index_name: impl AsRef<str>) -> EngineKey {
  DefaultEncoding::encode_values(&[EngineValue::Text(index_name.as_ref().to_string())])
}

pub fn encode_table_schema(schema: &TableSchema) -> Vec<EngineValue> {
  let columns_blob = encode_columns(&schema.columns);
  let primary_key_blob = encode_column_indices(&schema.primary_key);
  vec![
    EngineValue::Text(schema.name.clone()),
    EngineValue::Blob(columns_blob),
    EngineValue::Blob(primary_key_blob),
  ]
}

pub fn decode_table_schema_rows(
  rows: Vec<Vec<EngineValue>>,
) -> Result<Vec<TableSchema>, SchemaError> {
  let mut schemas = Vec::with_capacity(rows.len());
  for row in rows {
    if row.len() != 3 {
      return Err(SchemaError::SchemaMismatch(
        "invalid table schema row".into(),
      ));
    }

    let name = match &row[0] {
      EngineValue::Text(s) => s.clone(),
      _ => return Err(SchemaError::SchemaMismatch("invalid table name".into())),
    };

    let columns = match &row[1] {
      EngineValue::Blob(bytes) => decode_columns(bytes)?,
      _ => return Err(SchemaError::SchemaMismatch("invalid columns blob".into())),
    };

    let primary_key = match &row[2] {
      EngineValue::Blob(bytes) => decode_column_indices(bytes)?,
      _ => {
        return Err(SchemaError::SchemaMismatch(
          "invalid primary key blob".into(),
        ));
      }
    };

    schemas.push(TableSchema {
      name,
      columns,
      primary_key,
    });
  }
  Ok(schemas)
}

pub fn encode_index_schema(schema: &IndexSchema) -> Vec<EngineValue> {
  let column_indices_blob = encode_column_indices(&schema.column_indices);
  vec![
    EngineValue::Text(schema.name.clone()),
    EngineValue::Text(schema.table_name.clone()),
    EngineValue::Blob(column_indices_blob),
    EngineValue::Integer(if schema.unique { 1 } else { 0 }),
  ]
}

pub fn decode_index_schema_rows(
  rows: Vec<Vec<EngineValue>>,
) -> Result<Vec<IndexSchema>, SchemaError> {
  let mut schemas = Vec::with_capacity(rows.len());
  for row in rows {
    if row.len() != 4 {
      return Err(SchemaError::SchemaMismatch(
        "invalid index schema row".into(),
      ));
    }

    let name = match &row[0] {
      EngineValue::Text(s) => s.clone(),
      _ => return Err(SchemaError::SchemaMismatch("invalid index name".into())),
    };

    let table_name = match &row[1] {
      EngineValue::Text(s) => s.clone(),
      _ => {
        return Err(SchemaError::SchemaMismatch(
          "invalid index table name".into(),
        ));
      }
    };

    let column_indices = match &row[2] {
      EngineValue::Blob(bytes) => decode_column_indices(bytes)?,
      _ => {
        return Err(SchemaError::SchemaMismatch(
          "invalid column indices blob".into(),
        ));
      }
    };

    let unique = match &row[3] {
      EngineValue::Integer(v) => *v != 0,
      _ => return Err(SchemaError::SchemaMismatch("invalid unique flag".into())),
    };

    schemas.push(IndexSchema {
      name,
      table_name,
      column_indices,
      unique,
    });
  }
  Ok(schemas)
}

fn encode_columns(columns: &[crate::ColumnSchema]) -> Vec<u8> {
  let mut out = Vec::new();
  out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
  for column in columns {
    out.extend_from_slice(&(column.name.len() as u32).to_be_bytes());
    out.extend_from_slice(column.name.as_bytes());
    out.push(column.data_type.as_u8());
  }
  out
}

fn decode_columns(bytes: &[u8]) -> Result<Vec<crate::ColumnSchema>, SchemaError> {
  if bytes.len() < 4 {
    return Err(SchemaError::SchemaMismatch("invalid columns blob".into()));
  }

  let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
  let mut index = 4;
  let mut columns = Vec::with_capacity(count);

  for _ in 0..count {
    if bytes.len() < index + 4 {
      return Err(SchemaError::SchemaMismatch("invalid columns blob".into()));
    }
    let name_len = u32::from_be_bytes([
      bytes[index],
      bytes[index + 1],
      bytes[index + 2],
      bytes[index + 3],
    ]) as usize;
    index += 4;
    if bytes.len() < index + name_len + 1 {
      return Err(SchemaError::SchemaMismatch("invalid columns blob".into()));
    }
    let name = String::from_utf8(bytes[index..index + name_len].to_vec())
      .map_err(|_| SchemaError::SchemaMismatch("invalid column name".into()))?;
    index += name_len;
    let data_type = crate::EngineType::from_u8(bytes[index])?;
    index += 1;
    columns.push(crate::ColumnSchema { name, data_type });
  }

  Ok(columns)
}

fn encode_column_indices(indices: &[usize]) -> Vec<u8> {
  let mut out = Vec::new();
  out.extend_from_slice(&(indices.len() as u32).to_be_bytes());
  for index in indices {
    out.extend_from_slice(&(*index as u32).to_be_bytes());
  }
  out
}

fn decode_column_indices(bytes: &[u8]) -> Result<Vec<usize>, SchemaError> {
  if bytes.len() < 4 {
    return Err(SchemaError::SchemaMismatch("invalid index list".into()));
  }

  let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
  let mut index = 4;
  let mut result = Vec::with_capacity(count);

  for _ in 0..count {
    if bytes.len() < index + 4 {
      return Err(SchemaError::SchemaMismatch("invalid index list".into()));
    }
    let value = u32::from_be_bytes([
      bytes[index],
      bytes[index + 1],
      bytes[index + 2],
      bytes[index + 3],
    ]) as usize;
    index += 4;
    result.push(value);
  }

  Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreKey {
  TableRow {
    table_name: String,
    primary_key: EngineKey,
  },
}

pub fn encode_store_key(bytes: &mut Vec<u8>, key: &StoreKey) {
  bytes.push(1);
  match key {
    StoreKey::TableRow {
      table_name,
      primary_key,
    } => {
      bytes.push(1);
      bytes.extend_from_slice(&(table_name.len() as u32).to_be_bytes());
      bytes.extend_from_slice(table_name.as_bytes());
      bytes.extend_from_slice(&(primary_key.len() as u32).to_be_bytes());
      bytes.extend_from_slice(primary_key);
    }
  }
}

pub fn decode_store_key(bytes: &[u8]) -> Result<StoreKey, crate::schema::SchemaError> {
  if bytes.len() < 2 {
    return Err(crate::schema::SchemaError::SchemaMismatch(
      "truncated store key".into(),
    ));
  }
  let version = bytes[0];
  if version != 1 {
    return Err(crate::schema::SchemaError::SchemaMismatch(
      "unsupported store key version".into(),
    ));
  }
  let variant = bytes[1];
  let mut index = 2;
  match variant {
    1 => {
      if bytes.len() < index + 4 {
        return Err(crate::schema::SchemaError::SchemaMismatch(
          "truncated table name".into(),
        ));
      }
      let name_len = u32::from_be_bytes([
        bytes[index],
        bytes[index + 1],
        bytes[index + 2],
        bytes[index + 3],
      ]) as usize;
      index += 4;
      if bytes.len() < index + name_len + 4 {
        return Err(crate::schema::SchemaError::SchemaMismatch(
          "truncated store key".into(),
        ));
      }
      let table_name = String::from_utf8(bytes[index..index + name_len].to_vec())
        .map_err(|_| crate::schema::SchemaError::SchemaMismatch("invalid table name".into()))?;
      index += name_len;
      let pk_len = u32::from_be_bytes([
        bytes[index],
        bytes[index + 1],
        bytes[index + 2],
        bytes[index + 3],
      ]) as usize;
      index += 4;
      if bytes.len() < index + pk_len {
        return Err(crate::schema::SchemaError::SchemaMismatch(
          "truncated primary key".into(),
        ));
      }
      let primary_key = bytes[index..index + pk_len].to_vec();
      Ok(StoreKey::TableRow {
        table_name,
        primary_key,
      })
    }
    _ => Err(crate::schema::SchemaError::SchemaMismatch(
      "unknown store key variant".into(),
    )),
  }
}
