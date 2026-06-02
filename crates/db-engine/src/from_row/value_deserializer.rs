#[cfg(not(feature = "std"))]
use alloc::string::ToString;

use serde::de;

use super::errors::RowDeserializeError;
use super::json_value_deserializer::JsonValueDeserializer;
use crate::value::Value;

pub(crate) struct ValueDeserializer<'de> {
  pub(crate) v: &'de Value,
}

impl<'de> ValueDeserializer<'de> {
  pub(crate) fn new(v: &'de Value) -> Self {
    ValueDeserializer { v }
  }
}

impl<'de> de::Deserializer<'de> for ValueDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    match self.v {
      Value::Null => visitor.visit_unit(),
      // TODO use visit_enum
      Value::Type(t) => unimplemented!("Type deserialization is not supported yet: {:?}", t),
      Value::Uuid(u) => visitor.visit_str(&u.to_string()),
      Value::Bool(b) => visitor.visit_bool(*b),
      Value::Integer(i) => visitor.visit_i64(*i),
      Value::Float(f) => visitor.visit_f64(*f),
      Value::Text(s) => visitor.visit_str(s),
      Value::Blob(b) => visitor.visit_bytes(b.as_slice()),
      Value::Json(j) => {
        let jd = JsonValueDeserializer::new(j);
        jd.deserialize_any(visitor)
      }
    }
  }

  fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    match self.v {
      Value::Null => visitor.visit_none(),
      _ => visitor.visit_some(self),
    }
  }

  forward_to_deserialize_any_no_option!();
}
