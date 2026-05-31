use serde::de;

use super::errors::RowDeserializeError;
use super::row_map_access::RowMapAccess;
use super::value_deserializer::ValueDeserializer;

pub(crate) struct RowDeserializer<'de> {
  pub(crate) cols: &'de [crate::ResultColumn],
  pub(crate) row: &'de crate::Row,
}

impl<'de> RowDeserializer<'de> {
  pub(crate) fn new(cols: &'de [crate::ResultColumn], row: &'de crate::Row) -> Self {
    RowDeserializer { cols, row }
  }
}

impl<'de> de::Deserializer<'de> for RowDeserializer<'de> {
  type Error = RowDeserializeError;

  fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    if self.cols.len() == 1 {
      let v = &self.row[0];
      ValueDeserializer::new(v).deserialize_any(visitor)
    } else {
      visitor.visit_map(RowMapAccess::new(self.cols, self.row))
    }
  }

  fn deserialize_struct<V>(
    self,
    _name: &'static str,
    _fields: &'static [&'static str],
    visitor: V,
  ) -> Result<V::Value, RowDeserializeError>
  where
    V: de::Visitor<'de>,
  {
    visitor.visit_map(RowMapAccess::new(self.cols, self.row))
  }

  forward_to_deserialize_any_no_struct!();
}
