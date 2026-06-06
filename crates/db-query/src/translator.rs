#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, collections::BTreeMap, string::String, vec::Vec};
use db_core::{MaybeSend, MaybeSendFuture, MaybeSync};
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use thiserror::Error;

use db_schema::DescribeSchema;
use db_value::Value;

use crate::Statement;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(
  feature = "wasm",
  derive(tsify::Tsify),
  tsify(into_wasm_abi, from_wasm_abi)
)]
pub enum QueryParams {
  Positional(Vec<Value>),
  Named(BTreeMap<String, Value>),
}

#[derive(Error, Debug)]
pub enum TranslateError {
  #[error("Missing named parameter: {0}")]
  MissingNamedParameter(String),

  #[error("cannot mix named and positional/indexed placeholders in one query")]
  MixedPlaceholderStyles,

  #[error("Error: {0}")]
  Custom(String),
}

impl TranslateError {
  pub fn custom<T>(error: T) -> Self
  where
    T: ToString,
  {
    Self::Custom(error.to_string())
  }
}

pub trait Translator: MaybeSend + MaybeSync {
  fn translate_with_params<S>(
    &self,
    query: &str,
    params: Option<&QueryParams>,
    resolver: &S,
  ) -> impl MaybeSendFuture<Output = Result<Statement, TranslateError>>
  where
    S: DescribeSchema;

  fn translate<S>(
    &self,
    query: &str,
    resolver: &S,
  ) -> impl MaybeSendFuture<Output = Result<Statement, TranslateError>>
  where
    S: DescribeSchema,
  {
    self.translate_with_params(query, None, resolver)
  }
}
