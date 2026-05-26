#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};
#[cfg(feature = "std")]
use std::{string::String, vec::Vec};

use core::cmp::Ordering;

use crate::{EngineKey, EngineRow, EngineValue};
use db_core::{FastKeyCodec, KeyCodec, KeyScratch, ValueCodec};

pub trait KeyEncoding {
  fn encode_values(values: &[EngineValue]) -> EngineKey;
  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, String>;
}

pub trait RowEncoding {
  fn encode_values(values: &[EngineValue]) -> Vec<u8>;
  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, String>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultEncoding;

impl KeyEncoding for DefaultEncoding {
  fn encode_values(values: &[EngineValue]) -> EngineKey {
    let mut bytes = Vec::new();
    for value in values {
      bytes.push(value.tag());
      value.write_payload(&mut bytes);
    }
    bytes
  }

  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, String> {
    let mut values = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
      let tag = bytes[index];
      index += 1;
      let (value, used) = EngineValue::read_payload(tag, &bytes[index..])?;
      values.push(value);
      index += used;
    }

    Ok(values)
  }
}

impl RowEncoding for DefaultEncoding {
  fn encode_values(values: &[EngineValue]) -> Vec<u8> {
    <DefaultEncoding as KeyEncoding>::encode_values(values)
  }

  fn decode_values(bytes: &[u8]) -> Result<Vec<EngineValue>, String> {
    <DefaultEncoding as KeyEncoding>::decode_values(bytes)
  }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineKeyCodec;

impl ValueCodec<EngineKey> for EngineKeyCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    EngineKey: 'a;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a EngineKey) -> Self::Bytes<'a> {
    value.clone()
  }

  fn decode(data: &[u8]) -> EngineKey {
    data.to_vec()
  }

  fn decode_checked(data: &[u8]) -> Result<EngineKey, db_core::DecodeError> {
    Ok(data.to_vec())
  }
}

impl KeyCodec<EngineKey> for EngineKeyCodec {
  fn compare(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
  }
}

impl FastKeyCodec<EngineKey> for EngineKeyCodec {
  fn encode_into(&self, value: &EngineKey, scratch: &mut KeyScratch) {
    scratch.push_bytes(value);
  }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EngineRowCodec;

impl ValueCodec<EngineRow> for EngineRowCodec {
  type Bytes<'a>
    = Vec<u8>
  where
    Self: 'a,
    EngineRow: 'a;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a EngineRow) -> Self::Bytes<'a> {
    <DefaultEncoding as RowEncoding>::encode_values(value)
  }

  fn decode(data: &[u8]) -> EngineRow {
    <DefaultEncoding as RowEncoding>::decode_values(data)
      .unwrap_or_else(|error| panic!("failed to decode EngineRow: {}", error))
  }

  fn decode_checked(data: &[u8]) -> Result<EngineRow, db_core::DecodeError> {
    <DefaultEncoding as RowEncoding>::decode_values(data)
      .map_err(|_| db_core::DecodeError::InvalidData)
  }
}
