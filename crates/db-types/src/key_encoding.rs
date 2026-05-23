#![allow(clippy::manual_async_fn)]

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

use db_core::{
  BufferSink, Cursor, DecodeError, canonical_f64_bits_into_sink, decode_bytes, decode_len,
  decode_string, decode_with_version, encode_bytes_into_sink, encode_i64_into_sink,
  encode_len_into_sink, encode_string_into_sink, encode_with_version,
};

use crate::engine_types::EngineValue;

/// Encodes semantic EngineValues to orderable bytes preserving ordering.
pub trait KeyEncoding {
  /// Encode a list of EngineValues to bytes where byte-order matches semantic order.
  fn encode_values(values: &[EngineValue]) -> Vec<u8>;

  /// Decode bytes back to EngineValues. Fails if format is invalid.
  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, DecodeError>;
}

/// Encodes semantic EngineValues (rows) to bytes.
pub trait RowEncoding {
  /// Encode row values to bytes.
  fn encode_values(values: &[EngineValue]) -> Vec<u8>;

  /// Decode bytes back to EngineValues.
  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, DecodeError>;
}

/// Default encoding implementation using versioned format.
pub struct DefaultEncoding;

// ============================================================================
// Engine value encoding helpers (moved from codec.rs)
// ============================================================================

pub(crate) fn encode_engine_value_into_sink<S: BufferSink>(sink: &mut S, value: &EngineValue) {
  match value {
    EngineValue::Null => sink.push_bytes(&[0]),
    EngineValue::Integer(integer) => {
      sink.push_bytes(&[1]);
      encode_i64_into_sink(sink, *integer);
    }
    EngineValue::Float(float) => {
      sink.push_bytes(&[2]);
      canonical_f64_bits_into_sink(sink, *float);
    }
    EngineValue::Text(text) => {
      sink.push_bytes(&[3]);
      encode_string_into_sink(sink, text);
    }
    EngineValue::Blob(bytes) => {
      sink.push_bytes(&[4]);
      encode_bytes_into_sink(sink, bytes);
    }
    EngineValue::Uuid(bytes) => {
      sink.push_bytes(&[5]);
      sink.push_bytes(bytes);
    }
    EngineValue::Json(json) => {
      sink.push_bytes(&[6]);
      encode_string_into_sink(sink, json);
    }
  }
}

type DecodeFn = fn(&mut Cursor<'_>) -> Result<EngineValue, DecodeError>;

const ENGINE_VALUE_DECODERS: [DecodeFn; 7] = [
  decode_null,
  decode_integer,
  decode_float,
  decode_text,
  decode_blob,
  decode_uuid,
  decode_json,
];

pub(crate) fn decode_engine_value(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  let tag = cursor.read_u8()?;
  ENGINE_VALUE_DECODERS
    .get(tag as usize)
    .ok_or(DecodeError::Malformed)?(cursor)
}

fn decode_null(_: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  Ok(EngineValue::Null)
}

fn decode_integer(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  Ok(EngineValue::Integer(cursor.read_i64()?))
}

fn decode_float(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  Ok(EngineValue::Float(f64::from_bits(cursor.read_u64()?)))
}

fn decode_text(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  Ok(EngineValue::Text(decode_string(cursor)?))
}

fn decode_blob(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  Ok(EngineValue::Blob(decode_bytes(cursor)?))
}

fn decode_uuid(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  let bytes = cursor.read_exact(16)?;
  let mut value = [0_u8; 16];
  value.copy_from_slice(bytes);
  Ok(EngineValue::Uuid(value))
}

fn decode_json(cursor: &mut Cursor<'_>) -> Result<EngineValue, DecodeError> {
  Ok(EngineValue::Json(decode_string(cursor)?))
}

fn decode_vec<T, F>(cursor: &mut Cursor<'_>, mut decode: F) -> Result<Vec<T>, DecodeError>
where
  F: FnMut(&mut Cursor<'_>) -> Result<T, DecodeError>,
{
  let len = decode_len(cursor)?;
  let mut out = Vec::with_capacity(len);
  for _ in 0..len {
    out.push(decode(cursor)?);
  }
  Ok(out)
}

// ============================================================================
// KeyEncoding implementation
// ============================================================================

impl KeyEncoding for DefaultEncoding {
  fn encode_values(values: &[EngineValue]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_with_version(&mut out, |sink| {
      encode_len_into_sink(sink, values.len());
      for value in values {
        encode_engine_value_into_sink(sink, value);
      }
    });
    out
  }

  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, DecodeError> {
    decode_with_version(bytes, |cursor| decode_vec(cursor, decode_engine_value))
  }
}

// ============================================================================
// RowEncoding implementation
// ============================================================================

impl RowEncoding for DefaultEncoding {
  fn encode_values(values: &[EngineValue]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_with_version(&mut out, |sink| {
      encode_len_into_sink(sink, values.len());
      for value in values {
        encode_engine_value_into_sink(sink, value);
      }
    });
    out
  }

  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, DecodeError> {
    decode_with_version(bytes, |cursor| decode_vec(cursor, decode_engine_value))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_encode_decode_scalar() {
    let values = vec![EngineValue::Integer(42)];
    let encoded = <DefaultEncoding as KeyEncoding>::encode_values(&values);
    let decoded = <DefaultEncoding as KeyEncoding>::decode_values(&encoded).unwrap();
    assert_eq!(values, decoded);
  }

  #[test]
  fn test_encode_decode_tuple() {
    let values = vec![EngineValue::Integer(1), EngineValue::Text("hello".into())];
    let encoded = <DefaultEncoding as KeyEncoding>::encode_values(&values);
    let decoded = <DefaultEncoding as KeyEncoding>::decode_values(&encoded).unwrap();
    assert_eq!(values, decoded);
  }

  #[test]
  fn test_byte_order_integers() {
    let val1 = vec![EngineValue::Integer(10)];
    let val2 = vec![EngineValue::Integer(20)];
    let bytes1 = <DefaultEncoding as KeyEncoding>::encode_values(&val1);
    let bytes2 = <DefaultEncoding as KeyEncoding>::encode_values(&val2);
    // Note: version byte is same, so just compare the rest
    assert!(bytes1 < bytes2, "Encoded integers should preserve order");
  }

  #[test]
  fn test_byte_order_tuples() {
    let val1 = vec![EngineValue::Integer(1), EngineValue::Text("a".into())];
    let val2 = vec![EngineValue::Integer(1), EngineValue::Text("b".into())];
    let bytes1 = <DefaultEncoding as KeyEncoding>::encode_values(&val1);
    let bytes2 = <DefaultEncoding as KeyEncoding>::encode_values(&val2);
    assert!(bytes1 < bytes2, "Encoded tuples should preserve order");
  }

  #[test]
  fn test_null_ordering() {
    let val1 = vec![EngineValue::Null];
    let val2 = vec![EngineValue::Integer(0)];
    let bytes1 = <DefaultEncoding as KeyEncoding>::encode_values(&val1);
    let bytes2 = <DefaultEncoding as KeyEncoding>::encode_values(&val2);
    // Null tag is 0, Integer tag is 1, so null should sort first
    assert!(bytes1 < bytes2, "Null should sort before Integer");
  }
}
