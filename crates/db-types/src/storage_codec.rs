#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use db_core::{KeyCodec, ValueCodec};

use crate::{
  EngineKey, EngineRow,
  key_encoding::{DefaultEncoding, RowEncoding},
};

fn encode_key_versioned(value: &EngineKey) -> Vec<u8> {
  // EngineKey bytes already use the canonical versioned format.
  value.clone()
}

fn decode_key_versioned(data: &[u8]) -> Result<EngineKey, db_core::DecodeError> {
  // EngineKey bytes are already stored in canonical versioned form.
  Ok(data.to_vec())
}

fn decode_row_versioned(data: &[u8]) -> Result<EngineRow, db_core::DecodeError> {
  // EngineRow bytes already use the canonical versioned row format.
  <DefaultEncoding as RowEncoding>::decode_values(data)
    .map_err(|_e| db_core::DecodeError::Malformed)
}

fn encode_row_versioned(value: &EngineRow) -> Vec<u8> {
  // RowEncoding already emits the canonical versioned row format.
  <DefaultEncoding as RowEncoding>::encode_values(value)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineKeyCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineRowCodec;

macro_rules! impl_engine_value_codec {
  ($codec:ty, $value:ty, $encode:ident, $decode:ident, $expect:literal) => {
    impl ValueCodec<$value> for $codec {
      type Bytes<'a>
        = Vec<u8>
      where
        Self: 'a,
        $value: 'a;

      fn encode<'a>(value: &'a $value) -> Self::Bytes<'a> {
        $encode(value)
      }

      fn decode(data: &[u8]) -> $value {
        $decode(data).expect($expect)
      }

      fn decode_checked(data: &[u8]) -> Result<$value, db_core::DecodeError> {
        $decode(data)
      }
    }
  };
}

impl_engine_value_codec!(
  EngineKeyCodec,
  EngineKey,
  encode_key_versioned,
  decode_key_versioned,
  "decode engine key failed"
);

impl KeyCodec<EngineKey> for EngineKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> core::cmp::Ordering {
    let left = <Self as ValueCodec<EngineKey>>::decode(left);
    let right = <Self as ValueCodec<EngineKey>>::decode(right);
    left.cmp(&right)
  }
}

impl_engine_value_codec!(
  EngineRowCodec,
  EngineRow,
  encode_row_versioned,
  decode_row_versioned,
  "decode engine row failed"
);

#[cfg(test)]
mod tests {
  use super::*;
  use crate::EngineValue;
  use crate::key_encoding::{DefaultEncoding, KeyEncoding};

  #[test]
  fn engine_key_round_trips() {
    let value = <DefaultEncoding as KeyEncoding>::encode_values(&[
      EngineValue::Integer(1),
      EngineValue::Text("Alice".into()),
    ]);

    let encoded = <EngineKeyCodec as ValueCodec<EngineKey>>::encode_to_vec(&value);
    let decoded =
      <EngineKeyCodec as ValueCodec<EngineKey>>::decode_checked(&encoded).expect("decode failed");

    assert_eq!(decoded, value);
  }

  #[cfg(not(feature = "std"))]
  use alloc::vec;

  #[test]
  fn engine_row_round_trips() {
    let value = vec![
      EngineValue::Integer(1),
      EngineValue::Text("Alice".into()),
      EngineValue::Blob(vec![1, 2, 3]),
    ];

    let encoded = <EngineRowCodec as ValueCodec<EngineRow>>::encode_to_vec(&value);
    let decoded =
      <EngineRowCodec as ValueCodec<EngineRow>>::decode_checked(&encoded).expect("decode failed");

    assert_eq!(decoded, value);
  }

  #[test]
  fn key_compare_matches_engine_key_ordering() {
    let left = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Integer(2)]);
    let right = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Float(3.0)]);

    let left_encoded = <EngineKeyCodec as ValueCodec<EngineKey>>::encode_to_vec(&left);
    let right_encoded = <EngineKeyCodec as ValueCodec<EngineKey>>::encode_to_vec(&right);

    assert_eq!(
      EngineKeyCodec::compare(&left_encoded, &right_encoded),
      left.cmp(&right)
    );
  }
}
