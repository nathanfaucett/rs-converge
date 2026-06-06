#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  vec::Vec,
};

use core::{
  cmp::Ordering,
  f64,
  hash::{Hash, Hasher},
};

use uuid::Uuid;

use crate::JsonValue;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "automerge",
  derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
#[derive(Default)]
pub enum Value {
  #[default]
  Null,
  Type(ValueType),
  Uuid(Uuid),
  Bool(bool),
  Integer(i64),
  Float(f64),
  Text(String),
  Json(JsonValue),
  Blob(Vec<u8>),
}

impl From<()> for Value {
  fn from(_: ()) -> Self {
    Value::Null
  }
}

impl From<ValueType> for Value {
  fn from(value_type: ValueType) -> Self {
    Value::Type(value_type)
  }
}

impl From<Uuid> for Value {
  fn from(uuid: Uuid) -> Self {
    Value::Uuid(uuid)
  }
}

impl From<bool> for Value {
  fn from(b: bool) -> Self {
    Value::Bool(b)
  }
}

impl From<i64> for Value {
  fn from(i: i64) -> Self {
    Value::Integer(i)
  }
}

impl From<f64> for Value {
  fn from(f: f64) -> Self {
    Value::Float(f)
  }
}

impl From<String> for Value {
  fn from(s: String) -> Self {
    Value::Text(s)
  }
}

impl From<&str> for Value {
  fn from(s: &str) -> Self {
    Value::Text(s.to_string())
  }
}

impl From<Vec<u8>> for Value {
  fn from(bytes: Vec<u8>) -> Self {
    Value::Blob(bytes)
  }
}

impl From<JsonValue> for Value {
  fn from(json: JsonValue) -> Self {
    Value::Json(json)
  }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Value> for Value {
  fn from(json: serde_json::Value) -> Self {
    Value::Json(JsonValue::from(json))
  }
}

impl Value {
  pub fn r#type(&self) -> ValueType {
    match self {
      Value::Null => ValueType::Null,
      Value::Type(_) => ValueType::Type,
      Value::Uuid(_) => ValueType::Uuid,
      Value::Bool(_) => ValueType::Bool,
      Value::Integer(_) => ValueType::Integer,
      Value::Float(_) => ValueType::Float,
      Value::Text(_) => ValueType::Text,
      Value::Blob(_) => ValueType::Blob,
      Value::Json(_) => ValueType::Json,
    }
  }
}

impl PartialEq for Value {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Value::Null, Value::Null) => true,
      (Value::Type(a), Value::Type(b)) => a == b,
      (Value::Uuid(a), Value::Uuid(b)) => a == b,
      (Value::Bool(a), Value::Bool(b)) => a == b,
      (Value::Integer(a), Value::Integer(b)) => a == b,
      (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
      (Value::Text(a), Value::Text(b)) => a == b,
      (Value::Blob(a), Value::Blob(b)) => a == b,
      (Value::Json(a), Value::Json(b)) => a == b,
      _ => false,
    }
  }
}

impl Eq for Value {}

impl Hash for Value {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.r#type().hash(state);

    match self {
      Value::Null => {}
      Value::Type(value_type) => value_type.hash(state),
      Value::Uuid(bytes) => bytes.hash(state),
      Value::Bool(value) => value.hash(state),
      Value::Integer(value) => value.hash(state),
      Value::Float(value) => value.to_bits().hash(state),
      Value::Text(text) => text.hash(state),
      Value::Json(value) => value.hash(state),
      Value::Blob(bytes) => bytes.hash(state),
    }
  }
}

impl PartialOrd for Value {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Value {
  fn cmp(&self, other: &Self) -> Ordering {
    match (self, other) {
      (Value::Null, Value::Null) => Ordering::Equal,
      (Value::Type(a), Value::Type(b)) => a.rank().cmp(&b.rank()),
      (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
      (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
      (Value::Text(a), Value::Text(b)) => a.cmp(b),
      (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),
      (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
      (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
      (Value::Float(a), Value::Float(b)) => a.to_bits().cmp(&b.to_bits()),
      // Allow comparison between integers and floats for convenience
      (Value::Float(a), Value::Integer(b)) => a.to_bits().cmp(&(*b as f64).to_bits()),
      (Value::Integer(a), Value::Float(b)) => (*a as f64).to_bits().cmp(&b.to_bits()),
      // Different variants — deterministic ordering by variant rank
      _ => self.r#type().rank().cmp(&other.r#type().rank()),
    }
  }
}

impl Value {
  pub fn as_null(&self) -> Option<()> {
    match self {
      Value::Null => Some(()),
      _ => None,
    }
  }

  pub fn as_type(&self) -> Option<&ValueType> {
    match self {
      Value::Type(value_type) => Some(value_type),
      _ => None,
    }
  }

  pub fn as_uuid(&self) -> Option<&Uuid> {
    match self {
      Value::Uuid(uuid) => Some(uuid),
      _ => None,
    }
  }

  pub fn as_bool(&self) -> Option<bool> {
    match self {
      Value::Bool(b) => Some(*b),
      _ => None,
    }
  }

  pub fn as_integer(&self) -> Option<i64> {
    match self {
      Value::Integer(i) => Some(*i),
      Value::Float(f) => {
        if f.is_finite() && *f >= (i64::MIN as f64) && *f <= (i64::MAX as f64) {
          Some(*f as i64)
        } else {
          None
        }
      }
      _ => None,
    }
  }

  pub fn as_float(&self) -> Option<f64> {
    match self {
      Value::Float(f) => Some(*f),
      Value::Integer(i) => Some(*i as f64),
      _ => None,
    }
  }

  pub fn as_text(&self) -> Option<&str> {
    match self {
      Value::Text(s) => Some(s.as_str()),
      _ => None,
    }
  }

  pub fn as_blob(&self) -> Option<&[u8]> {
    match self {
      Value::Blob(b) => Some(b.as_slice()),
      _ => None,
    }
  }

  pub fn as_json(&self) -> Option<&JsonValue> {
    match self {
      Value::Json(j) => Some(j),
      _ => None,
    }
  }
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "automerge",
  derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
pub enum ValueType {
  Null,
  Type,
  Uuid,
  Bool,
  Integer,
  Float,
  Text,
  Json,
  Blob,
}

impl ValueType {
  pub fn rank(&self) -> u8 {
    match self {
      ValueType::Null => 0,
      ValueType::Type => 1,
      ValueType::Uuid => 2,
      ValueType::Bool => 3,
      ValueType::Integer => 4,
      ValueType::Float => 5,
      ValueType::Text => 6,
      ValueType::Json => 7,
      ValueType::Blob => 8,
    }
  }
}

impl Ord for ValueType {
  fn cmp(&self, other: &Self) -> Ordering {
    self.rank().cmp(&other.rank())
  }
}

impl PartialOrd for ValueType {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}
