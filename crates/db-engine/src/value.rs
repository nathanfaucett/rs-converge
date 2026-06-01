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
  Uuid(Uuid),
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
      Value::Uuid(_) => ValueType::Uuid,
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
      (Value::Uuid(a), Value::Uuid(b)) => a == b,
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
      Value::Uuid(bytes) => bytes.hash(state),
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
      (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
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

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ValueType {
  Null,
  Uuid,
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
      ValueType::Uuid => 1,
      ValueType::Integer => 2,
      ValueType::Float => 3,
      ValueType::Text => 4,
      ValueType::Blob => 5,
      ValueType::Json => 6,
    }
  }
}
