#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String};

use async_trait::async_trait;
use core::error;
use thiserror::Error;

use crate::{DescribeSchema, MaybeSendFuture, Query, Value};

#[derive(Error, Debug)]
pub enum TranslateError {
  #[error("Custom error: {0}")]
  Custom(String),

  #[error("Other error: {0}")]
  Other(#[from] Box<dyn error::Error + Send + Sync>),
}

#[async_trait]
pub trait Translator {
  async fn translate_with_params<S>(
    &self,
    query: &str,
    params: Option<&[Value]>,
    resolver: &S,
  ) -> Result<Query, TranslateError>
  where
    S: DescribeSchema + Send + Sync;

  async fn translate<S>(&self, query: &str, resolver: &S) -> Result<Query, TranslateError>
  where
    S: DescribeSchema + Send + Sync,
  {
    self.translate_with_params(query, None, resolver).await
  }
}
