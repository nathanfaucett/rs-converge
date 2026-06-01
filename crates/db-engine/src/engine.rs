#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  sync::Arc,
  vec::Vec,
};
#[cfg(feature = "std")]
use std::sync::Arc;

use thiserror::Error;

use crate::{
  BTreeDefinition, BTreeError, BTreeManager, DescribeSchema, IndexSchema, Query, QueryParams, Row,
  TableSchema, TranslateError, Translator, Value,
};

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

pub const ENGINE_TABLES: &str = "tables";
pub const ENGINE_INDICES: &str = "indices";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EngineBTreeDefinition {
  pub table_name: String,
}

impl From<String> for EngineBTreeDefinition {
  fn from(table_name: String) -> Self {
    Self { table_name }
  }
}

impl From<&str> for EngineBTreeDefinition {
  fn from(table_name: &str) -> Self {
    Self {
      table_name: table_name.to_string(),
    }
  }
}

impl BTreeDefinition for EngineBTreeDefinition {
  type Key = Vec<Value>;
  type Value = Row;

  fn id(&self) -> &str {
    &self.table_name
  }
}

pub struct Engine<M> {
  pub manager: Arc<M>,
}

impl<M> From<M> for Engine<M> {
  fn from(manager: M) -> Self {
    Self {
      manager: Arc::new(manager),
    }
  }
}

impl<M> Engine<M> {
  pub fn new(manager: M) -> Self {
    Self::from(manager)
  }
}

impl<M> DescribeSchema for Engine<M>
where
  M: BTreeManager,
{
  async fn describe_table(&self, _table_name: &str) -> Option<TableSchema> {
    let definition = EngineBTreeDefinition::from(ENGINE_TABLES);
    let _btree = self.manager.get(&definition).await.ok()?;
    unimplemented!("use the executor to query for the table name and convert rows to TableSchema")
  }
}

impl<M> Engine<M>
where
  M: BTreeManager,
{
  pub async fn describe_index(&self, _index_name: &str) -> Option<IndexSchema> {
    let definition = EngineBTreeDefinition::from(ENGINE_INDICES);
    let _btree = self.manager.get(&definition).await.ok()?;
    unimplemented!("use the executor to query for the index name and convert rows to IndexSchema")
  }

  pub async fn translate_and_execute_with_params<T>(
    &self,
    query: &str,
    params: Option<&QueryParams>,
    translator: T,
  ) -> EngineResult<Vec<Row>>
  where
    T: Translator,
  {
    let q = translator
      .translate_with_params(query, params, self)
      .await?;
    self.execute(q).await
  }

  pub async fn translate_and_execute<T>(&self, query: &str, translator: T) -> EngineResult<Vec<Row>>
  where
    T: Translator,
  {
    self
      .translate_and_execute_with_params(query, None, translator)
      .await
  }

  pub async fn execute(&self, query: Query) -> EngineResult<Vec<Row>> {
    crate::planner::validate(&query)?;
    crate::executor::execute(self, query).await
  }
}
