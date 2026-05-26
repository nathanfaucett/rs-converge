#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use core::cmp::Ordering;
use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
  Truncated,
  InvalidData,
  UnsupportedVersion(u8),
}

#[derive(Debug, Clone)]
pub struct KeyScratch {
  pub buf: Vec<u8>,
}

impl KeyScratch {
  pub fn with_capacity(capacity: usize) -> Self {
    Self {
      buf: Vec::with_capacity(capacity),
    }
  }

  pub fn push_bytes(&mut self, bytes: &[u8]) {
    self.buf.extend_from_slice(bytes);
  }
}

pub trait ValueCodec<T> {
  type Bytes<'a>
  where
    Self: 'a,
    T: 'a;

  fn fixed_width() -> Option<usize> {
    None
  }

  fn encode<'a>(value: &'a T) -> Self::Bytes<'a>;

  fn decode(data: &[u8]) -> T;

  fn decode_checked(data: &[u8]) -> Result<T, DecodeError>;

  fn encode_to_vec<'a>(value: &'a T) -> Vec<u8>
  where
    Self: Sized + 'a,
    Self::Bytes<'a>: AsRef<[u8]>,
  {
    Self::encode(value).as_ref().to_vec()
  }
}

pub trait KeyCodec<T> {
  fn compare(left: &[u8], right: &[u8]) -> Ordering;
}

pub trait FastKeyCodec<T>: KeyCodec<T> {
  fn encode_into(&self, value: &T, scratch: &mut KeyScratch);

  fn compare_encoded(left: &[u8], right: &[u8]) -> Ordering {
    Self::compare(left, right)
  }
}

impl fmt::Display for DecodeError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DecodeError::Truncated => write!(f, "truncated data"),
      DecodeError::InvalidData => write!(f, "invalid data"),
      DecodeError::UnsupportedVersion(v) => write!(f, "unsupported version: {}", v),
    }
  }
}

pub fn decode_with_version<T, F>(data: &[u8], decode: F) -> Result<T, DecodeError>
where
  F: Fn(&[u8]) -> Result<T, DecodeError>,
{
  if data.is_empty() {
    return Err(DecodeError::Truncated);
  }

  let version = data[0];
  match version {
    1 => decode(&data[1..]),
    version => Err(DecodeError::UnsupportedVersion(version)),
  }
}
