use serde::de;

use super::errors::RowDeserializeError;

pub(crate) struct KeyDeserializer<'de> {
  pub(crate) key: &'de str,
}

impl<'de> KeyDeserializer<'de> {
  pub(crate) fn new(key: &'de str) -> Self {
    KeyDeserializer { key }
  }
}

impl<'de> de::Deserializer<'de> for KeyDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    visitor.visit_str(self.key)
  }

  forward_to_deserialize_any!();
}
