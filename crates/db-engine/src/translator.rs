#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

use async_trait::async_trait;
use core::error;
use hashbrown::HashMap;
use thiserror::Error;

use crate::{DescribeSchema, Statement, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryParams {
  Positional(Vec<Value>),
  Named(HashMap<String, Value>),
}

#[derive(Error, Debug)]
pub enum TranslateError {
  #[error("Custom error: {0}")]
  Custom(String),

  #[error("Missing named parameter: {0}")]
  MissingNamedParameter(String),

  #[error("cannot mix named and positional/indexed placeholders in one query")]
  MixedPlaceholderStyles,

  #[error("Other error: {0}")]
  Other(#[from] Box<dyn error::Error + Send + Sync>),
}

#[async_trait]
pub trait Translator {
  async fn translate_with_params<S>(
    &self,
    query: &str,
    params: Option<&QueryParams>,
    resolver: &S,
  ) -> Result<Statement, TranslateError>
  where
    S: DescribeSchema + Send + Sync;

  async fn translate<S>(&self, query: &str, resolver: &S) -> Result<Statement, TranslateError>
  where
    S: DescribeSchema + Send + Sync,
  {
    self.translate_with_params(query, None, resolver).await
  }
}
