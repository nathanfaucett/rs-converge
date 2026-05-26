#[cfg(not(feature = "std"))]
use alloc::string::ToString;

use serde::de::{DeserializeSeed, MapAccess, Visitor};

use crate::{EngineRow, EngineValue, TableSchema, query::ResultColumn};

use super::row_deserialize_error::RowDeserializeError;

/// Deserialize a row into a typed value using serde.
pub fn deserialize_row<'a, T: serde::de::Deserialize<'a>>(
  schema: &'a TableSchema,
  row: &'a EngineRow,
) -> Result<T, RowDeserializeError> {
  let deserializer = RowDeserializer::new(schema, row);
  T::deserialize(deserializer)
}

pub fn deserialize_named_row<'a, T: serde::de::Deserialize<'a>>(
  columns: &'a [ResultColumn],
  row: &'a EngineRow,
) -> Result<T, RowDeserializeError> {
  let deserializer = NamedRowDeserializer::new(columns, row);
  T::deserialize(deserializer)
}

/// Main deserializer that wraps a row and schema
struct RowDeserializer<'a> {
  schema: &'a TableSchema,
  row: &'a EngineRow,
}

impl<'a> RowDeserializer<'a> {
  fn new(schema: &'a TableSchema, row: &'a EngineRow) -> Self {
    RowDeserializer { schema, row }
  }

  fn get_column_index(&self, name: &str) -> Result<usize, RowDeserializeError> {
    let name_lower = name.to_lowercase();
    self
      .schema
      .columns
      .iter()
      .position(|col| col.name.to_lowercase() == name_lower)
      .ok_or_else(|| RowDeserializeError::column_not_found(name))
  }
}

struct NamedRowDeserializer<'a> {
  columns: &'a [ResultColumn],
  row: &'a EngineRow,
}

impl<'a> NamedRowDeserializer<'a> {
  fn new(columns: &'a [ResultColumn], row: &'a EngineRow) -> Self {
    Self { columns, row }
  }

  fn get_column_index(&self, name: &str) -> Result<usize, RowDeserializeError> {
    let name_lower = name.to_lowercase();
    self
      .columns
      .iter()
      .position(|column| column.name.to_lowercase() == name_lower)
      .ok_or_else(|| RowDeserializeError::column_not_found(name))
  }
}

impl<'de> serde::Deserializer<'de> for RowDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    Err(RowDeserializeError::serde_error("not supported"))
  }

  fn deserialize_struct<V>(
    self,
    _name: &'static str,
    fields: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    visitor.visit_map(RowMapAccess {
      deserializer: self,
      fields,
      field_index: 0,
    })
  }

  serde::forward_to_deserialize_any! {
    bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
    byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
    map enum identifier ignored_any
  }
}

impl<'de> serde::Deserializer<'de> for NamedRowDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    Err(RowDeserializeError::serde_error("not supported"))
  }

  fn deserialize_struct<V>(
    self,
    _name: &'static str,
    fields: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    visitor.visit_map(NamedRowMapAccess {
      deserializer: self,
      fields,
      field_index: 0,
    })
  }

  serde::forward_to_deserialize_any! {
    bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
    byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
    map enum identifier ignored_any
  }
}

struct RowMapAccess<'de> {
  deserializer: RowDeserializer<'de>,
  fields: &'static [&'static str],
  field_index: usize,
}

impl<'de> MapAccess<'de> for RowMapAccess<'de> {
  type Error = RowDeserializeError;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
  where
    K: DeserializeSeed<'de>,
  {
    if self.field_index < self.fields.len() {
      let field_name = self.fields[self.field_index];
      // Provide the field name that serde expects
      let key = seed
        .deserialize(serde::de::value::StrDeserializer::<RowDeserializeError>::new(field_name))?;
      Ok(Some(key))
    } else {
      Ok(None)
    }
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
  where
    V: DeserializeSeed<'de>,
  {
    // Find the column index matching this field name (case-insensitive)
    let field_name = self.fields[self.field_index];
    let column_index = self.deserializer.get_column_index(field_name)?;

    if column_index < self.deserializer.row.len() {
      let value = &self.deserializer.row[column_index];
      let result = seed.deserialize(EngineValueDeserializer(value))?;
      self.field_index += 1;
      Ok(result)
    } else {
      Err(RowDeserializeError::schema_error("row index out of bounds"))
    }
  }
}

struct NamedRowMapAccess<'de> {
  deserializer: NamedRowDeserializer<'de>,
  fields: &'static [&'static str],
  field_index: usize,
}

impl<'de> MapAccess<'de> for NamedRowMapAccess<'de> {
  type Error = RowDeserializeError;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
  where
    K: DeserializeSeed<'de>,
  {
    if self.field_index < self.fields.len() {
      let field_name = self.fields[self.field_index];
      let key = seed
        .deserialize(serde::de::value::StrDeserializer::<RowDeserializeError>::new(field_name))?;
      Ok(Some(key))
    } else {
      Ok(None)
    }
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
  where
    V: DeserializeSeed<'de>,
  {
    let field_name = self.fields[self.field_index];
    let column_index = self.deserializer.get_column_index(field_name)?;

    if column_index < self.deserializer.row.len() {
      let value = &self.deserializer.row[column_index];
      let result = seed.deserialize(EngineValueDeserializer(value))?;
      self.field_index += 1;
      Ok(result)
    } else {
      Err(RowDeserializeError::schema_error("row index out of bounds"))
    }
  }
}

struct EngineValueDeserializer<'a>(&'a EngineValue);

impl<'de> serde::Deserializer<'de> for EngineValueDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Null => visitor.visit_none(),
      EngineValue::Integer(i) => visitor.visit_i64(*i),
      EngineValue::Float(f) => visitor.visit_f64(*f),
      EngineValue::Text(s) => visitor.visit_str(s),
      EngineValue::Uuid(bytes) => visitor.visit_bytes(bytes),
      EngineValue::Blob(bytes) => visitor.visit_bytes(bytes),
      EngineValue::Json(s) => {
        let mut de = serde_json::Deserializer::from_str(s);
        serde::Deserializer::deserialize_any(&mut de, visitor)
          .map_err(|e| RowDeserializeError::serde_error(e.to_string()))
      }
    }
  }

  fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Integer(i) => visitor.visit_bool(*i != 0),
      EngineValue::Text(s) => visitor.visit_bool(s == "true" || s == "1"),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Integer(i) => visitor.visit_i64(*i),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Integer(i) => {
        if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
          visitor.visit_i32(*i as i32)
        } else {
          Err(RowDeserializeError::serde_error("i32 out of range"))
        }
      }
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Integer(i) => {
        if *i >= 0 {
          visitor.visit_u64(*i as u64)
        } else {
          Err(RowDeserializeError::serde_error("negative value for u64"))
        }
      }
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Float(f) => visitor.visit_f64(*f),
      EngineValue::Integer(i) => visitor.visit_f64(*i as f64),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Text(s) => visitor.visit_str(s),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Text(s) => visitor.visit_str(s),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Null => visitor.visit_none(),
      _ => visitor.visit_some(self),
    }
  }

  fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Uuid(bytes) => visitor.visit_bytes(bytes),
      EngineValue::Blob(bytes) => visitor.visit_bytes(bytes),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Uuid(bytes) => visitor.visit_byte_buf(bytes.to_vec()),
      EngineValue::Blob(bytes) => visitor.visit_byte_buf(bytes.clone()),
      EngineValue::Null => visitor.visit_none(),
      _ => Err(RowDeserializeError::serde_error("type mismatch")),
    }
  }

  fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
  where
    V: Visitor<'de>,
  {
    match self.0 {
      EngineValue::Uuid(bytes) => visitor.visit_seq(serde::de::value::SeqDeserializer::new(
        bytes.iter().cloned(),
      )),
      EngineValue::Blob(bytes) => visitor.visit_seq(serde::de::value::SeqDeserializer::new(
        bytes.iter().cloned(),
      )),
      _ => Err(RowDeserializeError::serde_error("not a sequence")),
    }
  }

  serde::forward_to_deserialize_any! {
    i8 i16 u8 u16 u32 f32 char unit unit_struct
    newtype_struct tuple tuple_struct map struct enum identifier
    ignored_any
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde::Deserialize;

  #[test]
  fn deserialize_bool_accepts_integer_and_text_forms() {
    let int_true = bool::deserialize(EngineValueDeserializer(&EngineValue::Integer(1)))
      .expect("integer bool should deserialize");
    let text_true = bool::deserialize(EngineValueDeserializer(&EngineValue::Text("true".into())))
      .expect("text bool should deserialize");
    let text_false = bool::deserialize(EngineValueDeserializer(&EngineValue::Text("0".into())))
      .expect("text false should deserialize");

    assert!(int_true);
    assert!(text_true);
    assert!(!text_false);
  }

  #[test]
  fn deserialize_i32_checks_range() {
    let ok = i32::deserialize(EngineValueDeserializer(&EngineValue::Integer(123)))
      .expect("in-range i32 should deserialize");
    assert_eq!(ok, 123);

    let err = i32::deserialize(EngineValueDeserializer(&EngineValue::Integer(i64::MAX)))
      .expect_err("out-of-range i32 should fail");
    assert!(err.to_string().contains("i32 out of range"));
  }
}
