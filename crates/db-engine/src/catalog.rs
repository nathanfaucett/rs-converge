#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, string::String, vec::Vec};

use crate::{ColumnIndex, ColumnSchema, IndexSchema, Row, TableSchema, Value, ValueType};

pub const ENGINE_TABLES: &str = "tables";
pub const ENGINE_INDICES: &str = "indices";
pub const ENGINE_TABLE_FIELDS: &str = "table_fields";
pub const ENGINE_INDEX_FIELDS: &str = "index_fields";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFieldRow {
  pub table_name: String,
  pub column_index: ColumnIndex,
  pub column_name: String,
  pub value_type: ValueType,
  pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRow {
  pub index_name: String,
  pub table_name: String,
  pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexFieldRow {
  pub index_name: String,
  pub field_order: u8,
  pub column_index: ColumnIndex,
}

pub fn table_key(table_name: &str) -> Vec<Value> {
  vec![Value::Text(table_name.to_owned())]
}

pub fn index_key(index_name: &str) -> Vec<Value> {
  vec![Value::Text(index_name.to_owned())]
}

pub fn table_field_key(table_name: &str, column_index: u8) -> Vec<Value> {
  vec![
    Value::Text(table_name.to_owned()),
    Value::Integer(i64::from(column_index)),
  ]
}

pub fn index_field_key(index_name: &str, field_order: u8) -> Vec<Value> {
  vec![
    Value::Text(index_name.to_owned()),
    Value::Integer(i64::from(field_order)),
  ]
}

pub fn encode_table_row(table_name: &str) -> Row {
  vec![Value::Text(table_name.to_owned())]
}

pub fn decode_table_row(row: &Row) -> Option<String> {
  let name = row.first()?;

  match name {
    Value::Text(value) => Some(value.clone()),
    _ => None,
  }
}

pub fn encode_table_field_row(table_name: &str, field: &TableFieldRow) -> Row {
  vec![
    Value::Text(table_name.to_owned()),
    Value::Integer(i64::from(field.column_index)),
    Value::Text(field.column_name.clone()),
    Value::Type(field.value_type),
    Value::Bool(field.primary_key),
  ]
}

pub fn decode_table_field_row(row: &Row) -> Option<TableFieldRow> {
  let table_name = row.first()?.as_text()?.to_owned();
  let column_index = row.get(1)?.as_integer()? as ColumnIndex;
  let column_name = row.get(2)?.as_text()?.to_owned();
  let value_type = *row.get(3)?.as_type()?;
  let primary_key = row.get(4)?.as_bool()?;

  Some(TableFieldRow {
    table_name,
    column_index,
    column_name,
    value_type,
    primary_key,
  })
}

pub fn encode_index_row(index: &IndexSchema) -> Row {
  vec![
    Value::Text(index.name.clone()),
    Value::Text(index.table_name.clone()),
    Value::Bool(index.unique),
  ]
}

pub fn decode_index_row(row: &Row) -> Option<IndexRow> {
  let index_name = row.first()?.as_text()?.to_owned();
  let table_name = row.get(1)?.as_text()?.to_owned();
  let unique = row.get(2)?.as_bool()?;

  Some(IndexRow {
    index_name,
    table_name,
    unique,
  })
}

pub fn encode_index_field_row(index_name: &str, field: &IndexFieldRow) -> Row {
  vec![
    Value::Text(index_name.to_owned()),
    Value::Integer(i64::from(field.field_order)),
    Value::Integer(i64::from(field.column_index)),
  ]
}

pub fn decode_index_field_row(row: &Row) -> Option<IndexFieldRow> {
  let index_name = row.first()?.as_text()?.to_owned();
  let field_order = row.get(1)?.as_integer()? as ColumnIndex;
  let column_index = row.get(2)?.as_integer()? as ColumnIndex;

  Some(IndexFieldRow {
    index_name,
    field_order,
    column_index,
  })
}

pub fn table_schema_from_rows(table_name: &str, rows: &[TableFieldRow]) -> TableSchema {
  let mut sorted_rows = rows.to_vec();
  sorted_rows.sort_by_key(|row| row.column_index);

  let columns = sorted_rows
    .iter()
    .map(|row| ColumnSchema {
      name: row.column_name.clone(),
      r#type: row.value_type,
    })
    .collect();

  let primary_key_index = sorted_rows
    .iter()
    .filter(|row| row.primary_key)
    .map(|row| row.column_index)
    .collect();

  TableSchema {
    name: table_name.to_owned(),
    columns,
    primary_key_index,
  }
}

pub fn index_schema_from_rows(index: IndexRow, rows: &[IndexFieldRow]) -> IndexSchema {
  let mut sorted_rows = rows.to_vec();
  sorted_rows.sort_by_key(|row| row.field_order);

  let column_indices = sorted_rows.iter().map(|row| row.column_index).collect();

  IndexSchema {
    name: index.index_name,
    table_name: index.table_name,
    column_indices,
    unique: index.unique,
  }
}
