//! Shared store-level codec helpers (sink + decode helpers)
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

use db_core::{
  BufferSink, Cursor, DecodeError, canonical_f64_bits_into_sink, decode_bool, decode_bytes,
  decode_len, decode_string, decode_usize, encode_bool_into_sink, encode_bytes_into_sink,
  encode_i64_into_sink, encode_len_into_sink, encode_string_into_sink, encode_usize_into_sink,
  encode_with_version,
};

use crate::engine_types::{EngineType, EngineValue};
use crate::key_encoding::{DefaultEncoding, RowEncoding};
use crate::schema::{ColumnSchema, IndexSchema, TableSchema};
use crate::store::{StoreKey, StoreValue};

// Engine type/value/key/row codec (moved from db-core) -----------------

pub fn encode_engine_type_into_sink<S: BufferSink>(sink: &mut S, value: &EngineType) {
  let tag = match value {
    EngineType::Integer => 0,
    EngineType::Float => 1,
    EngineType::Text => 2,
    EngineType::Blob => 3,
    EngineType::Uuid => 4,
    EngineType::Json => 5,
  };
  sink.push_bytes(&[tag]);
}

pub fn decode_engine_type(cursor: &mut Cursor<'_>) -> Result<EngineType, DecodeError> {
  match cursor.read_u8()? {
    0 => Ok(EngineType::Integer),
    1 => Ok(EngineType::Float),
    2 => Ok(EngineType::Text),
    3 => Ok(EngineType::Blob),
    4 => Ok(EngineType::Uuid),
    5 => Ok(EngineType::Json),
    _ => Err(DecodeError::Malformed),
  }
}

fn encode_engine_value_null<S: BufferSink>(sink: &mut S) {
  sink.push_bytes(&[0]);
}

fn encode_engine_value_integer<S: BufferSink>(sink: &mut S, integer: i64) {
  sink.push_bytes(&[1]);
  encode_i64_into_sink(sink, integer);
}

fn encode_engine_value_float<S: BufferSink>(sink: &mut S, float: f64) {
  sink.push_bytes(&[2]);
  canonical_f64_bits_into_sink(sink, float);
}

fn encode_engine_value_text<S: BufferSink>(sink: &mut S, text: &str) {
  sink.push_bytes(&[3]);
  encode_string_into_sink(sink, text);
}

fn encode_engine_value_blob<S: BufferSink>(sink: &mut S, bytes: &[u8]) {
  sink.push_bytes(&[4]);
  encode_bytes_into_sink(sink, bytes);
}

fn encode_engine_value_uuid<S: BufferSink>(sink: &mut S, bytes: &[u8]) {
  sink.push_bytes(&[5]);
  sink.push_bytes(bytes);
}

fn encode_engine_value_json<S: BufferSink>(sink: &mut S, json: &str) {
  sink.push_bytes(&[6]);
  encode_string_into_sink(sink, json);
}

pub fn encode_engine_value_into_sink<S: BufferSink>(sink: &mut S, value: &EngineValue) {
  match value {
    EngineValue::Null => encode_engine_value_null(sink),
    EngineValue::Integer(integer) => encode_engine_value_integer(sink, *integer),
    EngineValue::Float(float) => encode_engine_value_float(sink, *float),
    EngineValue::Text(text) => encode_engine_value_text(sink, text),
    EngineValue::Blob(bytes) => encode_engine_value_blob(sink, bytes),
    EngineValue::Uuid(bytes) => encode_engine_value_uuid(sink, bytes),
    EngineValue::Json(json) => encode_engine_value_json(sink, json),
  }
}

fn decode_vec<T, F>(cursor: &mut Cursor<'_>, mut decode: F) -> Result<Vec<T>, DecodeError>
where
  F: FnMut(&mut Cursor<'_>) -> Result<T, DecodeError>,
{
  let len = decode_len(cursor)?;
  let mut out = Vec::with_capacity(len);
  for _ in 0..len {
    out.push(decode(cursor)?);
  }
  Ok(out)
}

pub fn decode_engine_value(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  crate::key_encoding::decode_engine_value(cursor)
}

// EngineRow and EngineKey are now encoded bytes; encoding/decoding moved to key_encoding.rs
// These legacy functions are no longer used.

// Schema codec ---------------------------------------------------------

pub fn encode_column_schema_into_sink<S: BufferSink>(sink: &mut S, value: &ColumnSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_engine_type_into_sink(sink, &value.data_type);
}

pub fn encode_table_schema_into_sink<S: BufferSink>(sink: &mut S, value: &TableSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_len_into_sink(sink, value.columns.len());
  for column in &value.columns {
    encode_column_schema_into_sink(sink, column);
  }
  encode_len_into_sink(sink, value.primary_key.len());
  for index in &value.primary_key {
    encode_usize_into_sink(sink, *index);
  }
}

pub fn encode_index_schema_into_sink<S: BufferSink>(sink: &mut S, value: &IndexSchema) {
  encode_string_into_sink(sink, &value.name);
  encode_string_into_sink(sink, &value.table_name);
  encode_len_into_sink(sink, value.column_indices.len());
  for index in &value.column_indices {
    encode_usize_into_sink(sink, *index);
  }
  encode_bool_into_sink(sink, value.unique);
}

pub fn decode_column_schema(cursor: &mut Cursor<'_>) -> Result<ColumnSchema, DecodeError> {
  Ok(ColumnSchema {
    name: decode_string(cursor)?,
    data_type: decode_engine_type(cursor)?,
  })
}

pub fn decode_table_schema(cursor: &mut Cursor<'_>) -> Result<TableSchema, DecodeError> {
  let name = decode_string(cursor)?;
  let columns = decode_vec(cursor, decode_column_schema)?;
  let primary_key = decode_vec(cursor, decode_usize)?;

  Ok(TableSchema {
    name,
    columns,
    primary_key,
  })
}

pub fn decode_index_schema(cursor: &mut Cursor<'_>) -> Result<IndexSchema, DecodeError> {
  let name = decode_string(cursor)?;
  let table_name = decode_string(cursor)?;
  let column_indices = decode_vec(cursor, decode_usize)?;
  let unique = decode_bool(cursor)?;

  Ok(IndexSchema {
    name,
    table_name,
    column_indices,
    unique,
  })
}

pub fn encode_store_key_into_sink<S: BufferSink>(sink: &mut S, value: &StoreKey) {
  match value {
    StoreKey::TableRow {
      table_name,
      primary_key,
    } => {
      sink.push_bytes(&[0]);
      encode_string_into_sink(sink, table_name);
      encode_bytes_into_sink(sink, primary_key);
    }
    StoreKey::IndexEntry {
      index_name,
      index_key,
      row_pk,
    } => {
      sink.push_bytes(&[1]);
      encode_string_into_sink(sink, index_name);
      encode_bytes_into_sink(sink, index_key);
      encode_bytes_into_sink(sink, row_pk);
    }
    StoreKey::TableSchema { table_name } => {
      sink.push_bytes(&[2]);
      encode_string_into_sink(sink, table_name);
    }
    StoreKey::IndexSchema { index_name } => {
      sink.push_bytes(&[3]);
      encode_string_into_sink(sink, index_name);
    }
  }
}

pub fn encode_store_value_into_sink<S: BufferSink>(sink: &mut S, value: &StoreValue) {
  match value {
    StoreValue::Row(row) => {
      sink.push_bytes(&[0]);
      // EngineRow is Vec<EngineValue>; encode to bytes
      let encoded = DefaultEncoding::encode_values(row);
      encode_bytes_into_sink(sink, &encoded);
    }
    StoreValue::IndexEntry => sink.push_bytes(&[1]),
    StoreValue::TableSchema(schema) => {
      sink.push_bytes(&[2]);
      encode_table_schema_into_sink(sink, schema);
    }
    StoreValue::IndexSchema(schema) => {
      sink.push_bytes(&[3]);
      encode_index_schema_into_sink(sink, schema);
    }
  }
}

// Decoding helpers (cursor-based) --------------------------------------
type StoreKeyDecodeFn = fn(&mut Cursor<'_>) -> Result<StoreKey, DecodeError>;

const STORE_KEY_DECODERS: [StoreKeyDecodeFn; 4] = [
  decode_table_row_key,
  decode_index_entry_key,
  decode_table_schema_key,
  decode_index_schema_key,
];

pub fn decode_store_key(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  decode_store_key_impl(cursor)
}

fn decode_store_key_impl(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  decode_store_key_body(cursor)
}

fn decode_store_key_body(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  let tag = cursor.read_u8()?;
  STORE_KEY_DECODERS
    .get(tag as usize)
    .ok_or(DecodeError::Malformed)?(cursor)
}

fn decode_table_row_key(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  Ok(StoreKey::table_row(
    decode_string(cursor)?,
    decode_bytes(cursor)?, // EngineKey is now bytes
  ))
}

fn decode_index_entry_key(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  Ok(StoreKey::index_entry(
    decode_string(cursor)?,
    decode_bytes(cursor)?, // EngineKey is now bytes
    decode_bytes(cursor)?, // EngineKey is now bytes
  ))
}

fn decode_table_schema_key(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  Ok(StoreKey::table_schema(decode_string(cursor)?))
}

fn decode_index_schema_key(cursor: &mut Cursor<'_>) -> Result<StoreKey, DecodeError> {
  Ok(StoreKey::index_schema(decode_string(cursor)?))
}

type StoreValueDecodeFn = fn(&mut Cursor<'_>) -> Result<StoreValue, DecodeError>;

const STORE_VALUE_DECODERS: [StoreValueDecodeFn; 4] = [
  decode_row_value,
  decode_index_entry_value,
  decode_table_schema_value,
  decode_index_schema_value,
];

pub fn decode_store_value(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  decode_store_value_impl(cursor)
}

fn decode_store_value_impl(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  decode_store_value_body(cursor)
}

fn decode_store_value_body(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  let tag = cursor.read_u8()?;
  STORE_VALUE_DECODERS
    .get(tag as usize)
    .ok_or(DecodeError::Malformed)?(cursor)
}

fn decode_row_value(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  let bytes = decode_bytes(cursor)?;
  let row = DefaultEncoding::decode_values(&bytes).map_err(|_e| DecodeError::Malformed)?;
  Ok(StoreValue::Row(row))
}

fn decode_index_entry_value(_: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  Ok(StoreValue::IndexEntry)
}

fn decode_table_schema_value(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  Ok(StoreValue::TableSchema(decode_table_schema(cursor)?))
}

fn decode_index_schema_value(cursor: &mut Cursor<'_>) -> Result<StoreValue, DecodeError> {
  Ok(StoreValue::IndexSchema(decode_index_schema(cursor)?))
}

// Convenience owned-vector helpers (keep small and allocation-friendly)
pub fn encode_store_key(buffer: &mut Vec<u8>, value: &StoreKey) {
  encode_with_version(buffer, |b| encode_store_key_into_sink(b, value));
}

pub fn encode_store_value(buffer: &mut Vec<u8>, value: &StoreValue) {
  encode_with_version(buffer, |b| encode_store_value_into_sink(b, value));
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::engine_types::EngineValue;

  #[test]
  fn encode_engine_value_into_sink_variants() {
    let uuid = [1_u8; 16];
    let values = vec![
      EngineValue::Null,
      EngineValue::Integer(42),
      EngineValue::Float(3.14),
      EngineValue::Text("hello".into()),
      EngineValue::Blob(vec![0, 1, 2, 3]),
      EngineValue::Uuid(uuid),
      EngineValue::Json("{\"x\":1}".into()),
    ];

    for value in values {
      let mut out = Vec::new();
      encode_engine_value_into_sink(&mut out, &value);
      assert!(!out.is_empty(), "encoded bytes should not be empty");
    }
  }
}
