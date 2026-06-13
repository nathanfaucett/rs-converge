#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  sync::Arc,
  vec::Vec,
};
#[cfg(feature = "std")]
use std::sync::Arc;

use thiserror::Error;

use db_btree::BTreeError;
use db_query::{QueryParams, QueryResult, Statement, TranslateError, Translator};

use crate::kernel::EngineKernel;

#[derive(Error, Debug)]
pub enum EngineError {
  #[error("Translate error: {0}")]
  TranslateError(#[from] TranslateError),

  #[error("BTree error: {0}")]
  BTreeError(#[from] BTreeError),

  #[error("Unsupported query shape for MVP executor: {0}")]
  Unsupported(&'static str),

  #[error("Invalid query: {0}")]
  InvalidQuery(&'static str),
}

pub type EngineResult<T> = Result<T, EngineError>;

pub struct Engine<K> {
  pub(crate) kernel: Arc<K>,
}

impl<K> From<K> for Engine<K> {
  fn from(kernel: K) -> Self {
    Self {
      kernel: Arc::new(kernel),
    }
  }
}

impl<K> Engine<K> {
  pub fn new(kernel: K) -> Self {
    Self::from(kernel)
  }
}

impl<K> Engine<K>
where
  K: EngineKernel,
{
  pub async fn translate_and_execute_with_params<T>(
    &self,
    query: &str,
    params: Option<&QueryParams>,
    translator: &T,
  ) -> EngineResult<Vec<QueryResult>>
  where
    T: Translator,
  {
    let statements = translator.translate_with_params(query, params).await?;
    self.execute(statements).await
  }

  pub async fn translate_and_execute<T>(
    &self,
    query: &str,
    translator: &T,
  ) -> EngineResult<Vec<QueryResult>>
  where
    T: Translator,
  {
    let statements = translator.translate(query).await?;
    self.execute(statements).await
  }

  pub async fn execute(&self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
    crate::executor::execute_statement(self, statements).await
  }
}
