#[cfg(not(feature = "std"))]
use alloc::borrow::ToOwned;

use serde::de;

use super::errors::RowDeserializeError;
use super::value_deserializer::ValueDeserializer;
use crate::{QueryResultColumn, Row};

pub(crate) struct RowMapAccess<'de> {
  pub(crate) cols: &'de [QueryResultColumn],
  pub(crate) row: &'de Row,
  pub(crate) pos: usize,
}

impl<'de> RowMapAccess<'de> {
  pub(crate) fn new(cols: &'de [QueryResultColumn], row: &'de Row) -> Self {
    RowMapAccess { cols, row, pos: 0 }
  }
}

impl<'de> de::MapAccess<'de> for RowMapAccess<'de> {
  type Error = RowDeserializeError;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, RowDeserializeError>
  where
    K: de::DeserializeSeed<'de>,
  {
    if self.pos >= self.cols.len() {
      return Ok(None);
    }
    let key = &self.cols[self.pos].name;
    Ok(Some(seed.deserialize(
      super::key_deserializer::KeyDeserializer::new(key),
    )?))
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::DeserializeSeed<'de>,
  {
    if self.pos >= self.cols.len() {
      return Err(RowDeserializeError::SchemaError(
        "no value for key".to_owned(),
      ));
    }
    let idx = self.pos;
    self.pos += 1;
    let v = &self.row[idx];
    let dv = ValueDeserializer::new(v);
    seed.deserialize(dv)
  }
}
