#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  vec::Vec,
};
#[cfg(feature = "std")]
use std::{string::String, vec::Vec};

use crate::key_encoding::{DefaultEncoding, KeyEncoding};
use core::cmp::Ordering;
use core::hash::{Hash, Hasher};
use db_core::BTreeError;
use thiserror::Error;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum EngineValue {
  Null,
  Integer(i64),
  Float(f64),
  Text(String),
  Blob(Vec<u8>),
  Json(String),
  Uuid([u8; 16]),
}

impl EngineValue {
  pub(crate) fn tag(&self) -> u8 {
    match self {
      EngineValue::Null => 0,
      EngineValue::Integer(_) => 1,
      EngineValue::Float(_) => 2,
      EngineValue::Text(_) => 3,
      EngineValue::Blob(_) => 4,
      EngineValue::Json(_) => 5,
      EngineValue::Uuid(_) => 6,
    }
  }

  pub(crate) fn write_payload(&self, out: &mut Vec<u8>) {
    match self {
      EngineValue::Null => {}
      EngineValue::Integer(value) => out.extend_from_slice(&value.to_be_bytes()),
      EngineValue::Float(value) => out.extend_from_slice(&value.to_bits().to_be_bytes()),
      EngineValue::Text(text) | EngineValue::Json(text) => {
        out.extend_from_slice(&(text.len() as u32).to_be_bytes());
        out.extend_from_slice(text.as_bytes());
      }
      EngineValue::Blob(bytes) => {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
      }
      EngineValue::Uuid(bytes) => out.extend_from_slice(bytes),
    }
  }

  pub(crate) fn read_payload(tag: u8, bytes: &[u8]) -> Result<(EngineValue, usize), String> {
    match tag {
      0 => Ok((EngineValue::Null, 0)),
      1 => {
        if bytes.len() < 8 {
          Err("truncated integer payload".into())
        } else {
          let mut buf = [0u8; 8];
          buf.copy_from_slice(&bytes[..8]);
          Ok((EngineValue::Integer(i64::from_be_bytes(buf)), 8))
        }
      }
      2 => {
        if bytes.len() < 8 {
          Err("truncated float payload".into())
        } else {
          let mut buf = [0u8; 8];
          buf.copy_from_slice(&bytes[..8]);
          Ok((
            EngineValue::Float(f64::from_bits(u64::from_be_bytes(buf))),
            8,
          ))
        }
      }
      3 | 5 => {
        if bytes.len() < 4 {
          return Err("truncated string payload".into());
        }
        let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + len {
          return Err("truncated string payload".into());
        }
        let value = String::from_utf8(bytes[4..4 + len].to_vec())
          .map_err(|_| "invalid utf8 string payload".to_string())?;
        let kind = if tag == 3 {
          EngineValue::Text(value)
        } else {
          EngineValue::Json(value)
        };
        Ok((kind, 4 + len))
      }
      4 => {
        if bytes.len() < 4 {
          return Err("truncated blob payload".into());
        }
        let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + len {
          return Err("truncated blob payload".into());
        }
        Ok((EngineValue::Blob(bytes[4..4 + len].to_vec()), 4 + len))
      }
      6 => {
        if bytes.len() < 16 {
          Err("truncated uuid payload".into())
        } else {
          let mut uuid = [0u8; 16];
          uuid.copy_from_slice(&bytes[..16]);
          Ok((EngineValue::Uuid(uuid), 16))
        }
      }
      _ => Err("unknown EngineValue tag".into()),
    }
  }
}

impl PartialEq for EngineValue {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (EngineValue::Null, EngineValue::Null) => true,
      (EngineValue::Integer(a), EngineValue::Integer(b)) => a == b,
      (EngineValue::Float(a), EngineValue::Float(b)) => a.to_bits() == b.to_bits(),
      (EngineValue::Text(a), EngineValue::Text(b)) => a == b,
      (EngineValue::Blob(a), EngineValue::Blob(b)) => a == b,
      (EngineValue::Json(a), EngineValue::Json(b)) => a == b,
      (EngineValue::Uuid(a), EngineValue::Uuid(b)) => a == b,
      _ => false,
    }
  }
}

impl Eq for EngineValue {}

impl Hash for EngineValue {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.tag().hash(state);
    match self {
      EngineValue::Null => {}
      EngineValue::Integer(value) => value.hash(state),
      EngineValue::Float(value) => value.to_bits().hash(state),
      EngineValue::Text(text) | EngineValue::Json(text) => text.hash(state),
      EngineValue::Blob(bytes) => bytes.hash(state),
      EngineValue::Uuid(bytes) => bytes.hash(state),
    }
  }
}

impl PartialOrd for EngineValue {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for EngineValue {
  fn cmp(&self, other: &Self) -> Ordering {
    let tag_cmp = self.tag().cmp(&other.tag());
    if tag_cmp != Ordering::Equal {
      return tag_cmp;
    }

    match (self, other) {
      (EngineValue::Null, EngineValue::Null) => Ordering::Equal,
      (EngineValue::Integer(a), EngineValue::Integer(b)) => a.cmp(b),
      (EngineValue::Float(a), EngineValue::Float(b)) => a.to_bits().cmp(&b.to_bits()),
      (EngineValue::Text(a), EngineValue::Text(b)) => a.cmp(b),
      (EngineValue::Json(a), EngineValue::Json(b)) => a.cmp(b),
      (EngineValue::Blob(a), EngineValue::Blob(b)) => a.cmp(b),
      (EngineValue::Uuid(a), EngineValue::Uuid(b)) => a.cmp(b),
      _ => Ordering::Equal,
    }
  }
}

pub type EngineKey = Vec<u8>;
pub type EngineRow = Vec<EngineValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum EngineType {
  Null = 0,
  Integer = 1,
  Float = 2,
  Text = 3,
  Blob = 4,
  Json = 5,
  Uuid = 6,
}

impl EngineType {
  pub fn as_u8(&self) -> u8 {
    *self as u8
  }

  pub fn from_u8(tag: u8) -> Result<Self, String> {
    match tag {
      0 => Ok(EngineType::Null),
      1 => Ok(EngineType::Integer),
      2 => Ok(EngineType::Float),
      3 => Ok(EngineType::Text),
      4 => Ok(EngineType::Blob),
      5 => Ok(EngineType::Json),
      6 => Ok(EngineType::Uuid),
      _ => Err("unknown EngineType".into()),
    }
  }

  pub fn matches_value(&self, value: &EngineValue) -> bool {
    match (self, value) {
      (EngineType::Null, EngineValue::Null) => true,
      (EngineType::Integer, EngineValue::Integer(_)) => true,
      (EngineType::Float, EngineValue::Float(_)) => true,
      (EngineType::Text, EngineValue::Text(_)) => true,
      (EngineType::Blob, EngineValue::Blob(_)) => true,
      (EngineType::Json, EngineValue::Json(_)) => true,
      (EngineType::Uuid, EngineValue::Uuid(_)) => true,
      _ => false,
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PrimaryKey {
  bytes: [u8; 16],
}

impl PrimaryKey {
  pub fn new(bytes: [u8; 16]) -> Self {
    Self { bytes }
  }

  pub fn as_bytes(&self) -> &[u8; 16] {
    &self.bytes
  }

  pub fn to_engine_key(&self) -> EngineKey {
    DefaultEncoding::encode_values(&[EngineValue::Uuid(self.bytes)])
  }
}

impl From<[u8; 16]> for PrimaryKey {
  fn from(bytes: [u8; 16]) -> Self {
    Self::new(bytes)
  }
}

#[derive(Error, Debug)]
pub enum EngineError {
  #[error("table not found: {0}")]
  TableNotFound(String),

  #[error("index not found: {0}")]
  IndexNotFound(String),

  #[error("table already exists: {0}")]
  DuplicateTable(String),

  #[error("index already exists: {0}")]
  DuplicateIndex(String),

  #[error("duplicate primary key: {0:?}")]
  DuplicatePrimaryKey(PrimaryKey),

  #[error("unique index violation: {0}")]
  UniqueIndexViolation(String),

  #[error("schema mismatch: {0}")]
  SchemaMismatch(String),

  #[error("type mismatch: {0}")]
  TypeMismatch(String),

  #[error("query limit exceeded: {0}")]
  QueryLimitExceeded(String),

  #[error("query not supported: {0}")]
  QueryNotSupported(String),

  #[error("primary key missing")]
  PrimaryKeyMissing,

  #[error("unsupported index type")]
  UnsupportedIndexType,

  #[error("storage error: {0}")]
  StoreError(#[from] BTreeError),
}

impl From<crate::schema::SchemaError> for EngineError {
  fn from(e: crate::schema::SchemaError) -> Self {
    match e {
      crate::schema::SchemaError::SchemaMismatch(s) => EngineError::SchemaMismatch(s),
      crate::schema::SchemaError::TypeMismatch(s) => EngineError::TypeMismatch(s),
      crate::schema::SchemaError::PrimaryKeyMissing => EngineError::PrimaryKeyMissing,
    }
  }
}
