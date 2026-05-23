use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(feature = "wasm")]
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format};
#[cfg(feature = "std")]
use std::string::{String, ToString};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::vec::Vec;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub enum EngineType {
  Integer,
  Float,
  Text,
  Uuid,
  Blob,
  Json,
}

#[derive(Debug, Clone)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi), serde(untagged))]
pub enum EngineValue {
  Integer(i64),
  Float(f64),
  Text(String),
  Uuid([u8; 16]),
  Blob(Vec<u8>),
  Json(#[cfg_attr(feature = "wasm", serde(serialize_with = "serialize_json_str"))] String),
  Null,
}

#[cfg(feature = "wasm")]
fn serialize_json_str<S: serde::Serializer>(s: &str, serializer: S) -> Result<S::Ok, S::Error> {
  let value: serde_json::Value = serde_json::from_str(s).map_err(serde::ser::Error::custom)?;
  serde::Serialize::serialize(&value, serializer)
}

impl PartialEq for EngineValue {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (EngineValue::Null, EngineValue::Null) => true,
      (EngineValue::Integer(left), EngineValue::Integer(right)) => left == right,
      (EngineValue::Float(left), EngineValue::Float(right)) => {
        if left == right {
          true
        } else {
          left.is_nan() && right.is_nan()
        }
      }
      (EngineValue::Text(left), EngineValue::Text(right)) => left == right,
      (EngineValue::Uuid(left), EngineValue::Uuid(right)) => left == right,
      (EngineValue::Blob(left), EngineValue::Blob(right)) => left == right,
      (EngineValue::Json(left), EngineValue::Json(right)) => left == right,
      _ => false,
    }
  }
}

impl Eq for EngineValue {}

impl Hash for EngineValue {
  fn hash<H: Hasher>(&self, state: &mut H) {
    state.write_u8(self.discriminant_byte());
    match self {
      EngineValue::Null => {}
      EngineValue::Integer(value) => state.write_i64(*value),
      EngineValue::Float(value) => state.write_u64(canonical_float_bits(*value)),
      EngineValue::Text(value) => value.hash(state),
      EngineValue::Uuid(value) => value.hash(state),
      EngineValue::Blob(value) => value.hash(state),
      EngineValue::Json(value) => value.hash(state),
    }
  }
}

impl EngineValue {
  fn discriminant_byte(&self) -> u8 {
    match self {
      EngineValue::Null => 0,
      EngineValue::Integer(_) => 1,
      EngineValue::Float(_) => 2,
      EngineValue::Text(_) => 3,
      EngineValue::Uuid(_) => 4,
      EngineValue::Blob(_) => 5,
      EngineValue::Json(_) => 6,
    }
  }

  fn type_precedence(&self) -> u8 {
    self.discriminant_byte()
  }

  fn cmp_same_type(&self, other: &Self) -> Ordering {
    match (self, other) {
      (EngineValue::Null, EngineValue::Null) => Ordering::Equal,
      (EngineValue::Integer(left), EngineValue::Integer(right)) => left.cmp(right),
      (EngineValue::Float(left), EngineValue::Float(right)) => compare_floats(*left, *right),
      (EngineValue::Text(left), EngineValue::Text(right)) => left.cmp(right),
      (EngineValue::Uuid(left), EngineValue::Uuid(right)) => left.cmp(right),
      (EngineValue::Blob(left), EngineValue::Blob(right)) => left.cmp(right),
      (EngineValue::Json(left), EngineValue::Json(right)) => left.cmp(right),
      _ => self.type_precedence().cmp(&other.type_precedence()),
    }
  }

  fn fmt_uuid_bytes(bytes: &[u8], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (i, byte) in bytes.iter().enumerate() {
      if matches!(i, 4 | 6 | 8 | 10) {
        write!(f, "-")?;
      }
      write!(f, "{:02x}", byte)?;
    }
    Ok(())
  }

  fn fmt_blob_bytes(bytes: &[u8], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "0x")?;
    for byte in bytes {
      write!(f, "{:02x}", byte)?;
    }
    Ok(())
  }

  fn fmt_uuid(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let EngineValue::Uuid(value) = self {
      Self::fmt_uuid_bytes(value, f)
    } else {
      Err(fmt::Error)
    }
  }

  fn fmt_blob(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let EngineValue::Blob(value) = self {
      Self::fmt_blob_bytes(value, f)
    } else {
      Err(fmt::Error)
    }
  }
}

fn canonical_float_bits(value: f64) -> u64 {
  if value == 0.0 {
    0_u64
  } else if value.is_nan() {
    f64::NAN.to_bits()
  } else {
    value.to_bits()
  }
}

impl PartialOrd for EngineValue {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

fn compare_floats(left: f64, right: f64) -> Ordering {
  if left == right {
    Ordering::Equal
  } else if left.is_nan() {
    if right.is_nan() {
      Ordering::Equal
    } else {
      Ordering::Greater
    }
  } else if right.is_nan() {
    Ordering::Less
  } else {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
  }
}

impl Ord for EngineValue {
  fn cmp(&self, other: &Self) -> Ordering {
    match (self, other) {
      (EngineValue::Integer(left), EngineValue::Float(right)) => {
        compare_floats(*left as f64, *right)
      }
      (EngineValue::Float(left), EngineValue::Integer(right)) => {
        compare_floats(*left, *right as f64)
      }
      _ => self.cmp_same_type(other),
    }
  }
}

impl fmt::Display for EngineValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      EngineValue::Integer(value) => write!(f, "{}", value),
      EngineValue::Float(value) => write!(f, "{}", value),
      EngineValue::Text(value) => write!(f, "{}", value),
      EngineValue::Uuid(_) => self.fmt_uuid(f),
      EngineValue::Blob(_) => self.fmt_blob(f),
      EngineValue::Json(value) => write!(f, "{}", value),
      EngineValue::Null => write!(f, "NULL"),
    }
  }
}

macro_rules! impl_engine_value_from {
  ($source:ty, $variant:ident) => {
    impl From<$source> for EngineValue {
      fn from(value: $source) -> Self {
        EngineValue::$variant(value)
      }
    }
  };
}

impl_engine_value_from!(i64, Integer);
impl_engine_value_from!(f64, Float);
impl_engine_value_from!(String, Text);
impl_engine_value_from!(Vec<u8>, Blob);

impl From<&str> for EngineValue {
  fn from(value: &str) -> Self {
    EngineValue::Text(value.to_string())
  }
}

impl From<&[u8]> for EngineValue {
  fn from(value: &[u8]) -> Self {
    EngineValue::Blob(value.to_vec())
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(
  feature = "wasm",
  tsify(into_wasm_abi, from_wasm_abi),
  serde(transparent)
)]
pub struct PrimaryKey(pub [u8; 16]);

impl PrimaryKey {
  pub fn new(bytes: [u8; 16]) -> Self {
    Self(bytes)
  }

  pub fn as_bytes(&self) -> &[u8; 16] {
    &self.0
  }
}

impl From<[u8; 16]> for PrimaryKey {
  fn from(value: [u8; 16]) -> Self {
    Self(value)
  }
}

impl From<PrimaryKey> for [u8; 16] {
  fn from(value: PrimaryKey) -> Self {
    value.0
  }
}

impl PrimaryKey {
  /// Encode a PrimaryKey to an EngineKey (bytes).
  pub fn to_engine_key(self) -> EngineKey {
    use crate::key_encoding::{DefaultEncoding, KeyEncoding};
    <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Uuid(self.0)])
  }
}

/// Encoded storage key (orderable bytes where byte-order matches semantic order).
pub type EngineKey = Vec<u8>;

/// Semantic row data (vector of typed values). Will be encoded to bytes at storage boundary.
pub type EngineRow = Vec<EngineValue>;
