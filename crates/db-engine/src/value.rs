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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Value {
  Null,
  Type(ValueType),
  Uuid(Uuid),
  Bool(bool),
  Integer(i64),
  Float(f64),
  Text(String),
  Blob(Vec<u8>),
  Json(serde_json::Value),
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
      // Allow comparison between integers and floats for convenience
      (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
      (Value::Float(a), Value::Float(b)) => a.to_bits().cmp(&b.to_bits()),
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

  pub fn as_json(&self) -> Option<&serde_json::Value> {
    match self {
      Value::Json(j) => Some(j),
      _ => None,
    }
  }
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ValueType {
  Null,
  Type,
  Uuid,
  Bool,
  Integer,
  Float,
  Text,
  Blob,
  Json,
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
      ValueType::Blob => 7,
      ValueType::Json => 8,
    }
  }
}
