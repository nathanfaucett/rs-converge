#[cfg(not(feature = "std"))]
use alloc::string::String;

#[cfg(not(feature = "std"))]
use alloc::string::ToString;

use core::fmt;
use serde::de;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RowDeserializeError {
  #[error("Invalid schema: {0}")]
  SchemaError(String),
}

impl de::Error for RowDeserializeError {
  fn custom<T: fmt::Display>(msg: T) -> Self {
    RowDeserializeError::SchemaError(msg.to_string())
  }
}

pub type FromRowResult<T> = Result<T, RowDeserializeError>;
