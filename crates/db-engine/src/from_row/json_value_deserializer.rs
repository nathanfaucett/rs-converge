#[cfg(not(feature = "std"))]
use alloc::borrow::ToOwned;

use serde::de;

use super::errors::RowDeserializeError;
use super::json_map::JsonMap;
use super::json_seq::JsonSeq;

pub(crate) struct JsonValueDeserializer<'de> {
  pub(crate) v: &'de serde_json::Value,
}

impl<'de> JsonValueDeserializer<'de> {
  pub(crate) fn new(v: &'de serde_json::Value) -> Self {
    JsonValueDeserializer { v }
  }
}

impl<'de> de::Deserializer<'de> for JsonValueDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    match self.v {
      serde_json::Value::Null => visitor.visit_unit(),
      serde_json::Value::Bool(b) => visitor.visit_bool(*b),
      serde_json::Value::Number(n) => {
        if let Some(i) = n.as_i64() {
          visitor.visit_i64(i)
        } else if let Some(u) = n.as_u64() {
          visitor.visit_u64(u)
        } else if let Some(f) = n.as_f64() {
          visitor.visit_f64(f)
        } else {
          Err(RowDeserializeError::SchemaError(
            "invalid number".to_owned(),
          ))
        }
      }
      serde_json::Value::String(s) => visitor.visit_str(s),
      serde_json::Value::Array(arr) => visitor.visit_seq(JsonSeq::new(arr.as_slice())),
      serde_json::Value::Object(map) => visitor.visit_map(JsonMap::new(map)),
    }
  }

  fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    match self.v {
      serde_json::Value::Null => visitor.visit_none(),
      _ => visitor.visit_some(self),
    }
  }

  forward_to_deserialize_any_no_option!();
}
