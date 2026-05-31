use serde::de;

use super::errors::RowDeserializeError;
use super::json_value_deserializer::JsonValueDeserializer;

pub(crate) struct JsonSeq<'de> {
  pub(crate) slice: &'de [serde_json::Value],
  pub(crate) pos: usize,
}

impl<'de> JsonSeq<'de> {
  pub(crate) fn new(slice: &'de [serde_json::Value]) -> Self {
    JsonSeq { slice, pos: 0 }
  }
}

impl<'de> de::SeqAccess<'de> for JsonSeq<'de> {
  type Error = RowDeserializeError;

  fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, RowDeserializeError>
  where
    T: de::DeserializeSeed<'de>,
  {
    if self.pos >= self.slice.len() {
      return Ok(None);
    }
    let dv = JsonValueDeserializer::new(&self.slice[self.pos]);
    self.pos += 1;
    Ok(Some(seed.deserialize(dv)?))
  }
}
