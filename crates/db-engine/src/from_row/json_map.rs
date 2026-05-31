#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, vec::Vec};

use serde::de;

use super::errors::RowDeserializeError;
use super::key_deserializer::KeyDeserializer;

pub(crate) struct JsonMap<'de> {
  pub(crate) keys: Vec<&'de String>,
  pub(crate) map: &'de serde_json::Map<String, serde_json::Value>,
  pub(crate) pos: usize,
}

impl<'de> JsonMap<'de> {
  pub(crate) fn new(map: &'de serde_json::Map<String, serde_json::Value>) -> Self {
    let keys = map.keys().collect::<Vec<_>>();
    JsonMap { keys, map, pos: 0 }
  }
}

impl<'de> de::MapAccess<'de> for JsonMap<'de> {
  type Error = RowDeserializeError;

  fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, RowDeserializeError>
  where
    K: de::DeserializeSeed<'de>,
  {
    if self.pos >= self.keys.len() {
      return Ok(None);
    }
    let k = self.keys[self.pos];
    self.pos += 1;
    Ok(Some(seed.deserialize(KeyDeserializer::new(k))?))
  }

  fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::DeserializeSeed<'de>,
  {
    let idx = self.pos - 1;
    let key = self.keys[idx];
    let v = self
      .map
      .get(key)
      .ok_or_else(|| RowDeserializeError::SchemaError("missing json map value".to_owned()))?;
    let dv = super::json_value_deserializer::JsonValueDeserializer::new(v);
    seed.deserialize(dv)
  }
}
