// forward_to_deserialize_any: forward everything to deserialize_any
macro_rules! forward_to_deserialize_any {
  () => {
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_unit_struct<V>(
      self,
      _name: &'static str,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_newtype_struct<V>(
      self,
      _name: &'static str,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_tuple_struct<V>(
      self,
      _name: &'static str,
      _len: usize,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
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
      self.deserialize_any(visitor)
    }
    fn deserialize_enum<V>(
      self,
      _name: &'static str,
      _variants: &'static [&'static str],
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
  };
}

// forward_to_deserialize_any_no_option: like above but omit deserialize_option
macro_rules! forward_to_deserialize_any_no_option {
  () => {
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_unit_struct<V>(
      self,
      _name: &'static str,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_newtype_struct<V>(
      self,
      _name: &'static str,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_tuple_struct<V>(
      self,
      _name: &'static str,
      _len: usize,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
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
      self.deserialize_any(visitor)
    }
    fn deserialize_enum<V>(
      self,
      _name: &'static str,
      _variants: &'static [&'static str],
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
  };
}

// forward_to_deserialize_any_no_struct: like forward_to_deserialize_any but omit deserialize_struct
macro_rules! forward_to_deserialize_any_no_struct {
  () => {
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_unit_struct<V>(
      self,
      _name: &'static str,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_newtype_struct<V>(
      self,
      _name: &'static str,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_tuple_struct<V>(
      self,
      _name: &'static str,
      _len: usize,
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_enum<V>(
      self,
      _name: &'static str,
      _variants: &'static [&'static str],
      visitor: V,
    ) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, RowDeserializeError>
    where
      V: de::Visitor<'de>,
    {
      self.deserialize_any(visitor)
    }
  };
}
