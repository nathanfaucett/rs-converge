#[cfg(not(feature = "std"))]
use alloc::{
  collections::BTreeMap,
  format,
  string::{String, ToString},
  vec::Vec,
};
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use core::{fmt, hash::Hash};

use crate::JsonNumber;

#[derive(
  Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(
  feature = "automerge",
  derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum JsonValue {
  Null,
  Bool(bool),
  Number(JsonNumber),
  String(String),
  Array(Vec<JsonValue>),
  Object(BTreeMap<String, JsonValue>),
}

impl From<()> for JsonValue {
  fn from(_: ()) -> Self {
    JsonValue::Null
  }
}

impl From<bool> for JsonValue {
  fn from(b: bool) -> Self {
    JsonValue::Bool(b)
  }
}

impl<T> From<T> for JsonValue
where
  T: Into<JsonNumber>,
{
  fn from(number: T) -> Self {
    JsonValue::Number(number.into())
  }
}

impl From<String> for JsonValue {
  fn from(s: String) -> Self {
    JsonValue::String(s)
  }
}

impl From<&str> for JsonValue {
  fn from(s: &str) -> Self {
    JsonValue::String(s.to_string())
  }
}

impl<K, V> FromIterator<(K, V)> for JsonValue
where
  K: Into<String>,
  V: Into<JsonValue>,
{
  fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
    JsonValue::Object(
      iter
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect(),
    )
  }
}

impl<T> FromIterator<T> for JsonValue
where
  T: Into<JsonValue>,
{
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    JsonValue::Array(iter.into_iter().map(Into::into).collect())
  }
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Value> for JsonValue {
  fn from(value: serde_json::Value) -> Self {
    match value {
      serde_json::Value::Null => JsonValue::Null,
      serde_json::Value::Bool(b) => JsonValue::Bool(b),
      serde_json::Value::Number(n) => JsonValue::Number(n.into()),
      serde_json::Value::String(s) => JsonValue::String(s),
      serde_json::Value::Array(arr) => {
        JsonValue::Array(arr.into_iter().map(JsonValue::from).collect())
      }
      serde_json::Value::Object(obj) => JsonValue::Object(
        obj
          .into_iter()
          .map(|(k, v)| (k, JsonValue::from(v)))
          .collect(),
      ),
    }
  }
}

impl fmt::Display for JsonValue {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      JsonValue::Null => write!(f, "null"),
      JsonValue::Bool(b) => write!(f, "{}", b),
      JsonValue::Number(n) => write!(f, "{}", n),
      JsonValue::String(s) => write!(f, "\"{}\"", s),
      JsonValue::Array(arr) => {
        let items = arr
          .iter()
          .map(|v| v.to_string())
          .collect::<Vec<_>>()
          .join(", ");
        write!(f, "[{}]", items)
      }
      JsonValue::Object(obj) => {
        let items = obj
          .iter()
          .map(|(k, v)| format!("\"{}\": {}", k, v))
          .collect::<Vec<_>>()
          .join(", ");
        write!(f, "{{{}}}", items)
      }
    }
  }
}
