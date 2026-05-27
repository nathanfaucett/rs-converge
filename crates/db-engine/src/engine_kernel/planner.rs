use crate::store_backend::EngineStoreBackend;
use alloc::{
  string::{String, ToString},
  sync::Arc,
  vec::Vec,
};

use crate::{
  EngineError, IndexSchema, TableSchema, query::Aggregate, query::QualifiedColumn,
  query::ResultColumn, query::SelectOptions,
};

use super::catalog::EngineCatalog;
use super::executor::EngineWriteTxn;
use super::transaction_lifecycle::TransactionLifecycle;
use crate::ChangeListenerRegistry;

#[derive(Debug, Clone)]
pub(crate) struct EngineKernel<S> {
  store: S,
  catalog: EngineCatalog,
  change_listener_registry: Arc<ChangeListenerRegistry>,
}

impl<S> EngineKernel<S>
where
  S: EngineStoreBackend,
{
  pub(super) fn dedupe_result_column_names(columns: &mut [ResultColumn]) {
    use hashbrown::HashMap;

    let mut counts: HashMap<String, usize> = HashMap::new();
    for column in columns.iter() {
      *counts.entry(column.name.clone()).or_insert(0) += 1;
    }

    for column in columns.iter_mut() {
      if counts.get(&column.name).copied().unwrap_or(0) > 1
        && let Some(table) = &column.source_table
      {
        column.name = format!("{}.{}", table, column.name);
      }
    }

    let mut seen: HashMap<String, usize> = HashMap::new();
    for column in columns.iter_mut() {
      let entry = seen.entry(column.name.clone()).or_insert(0);
      if *entry > 0 {
        column.name = format!("{}_{}", column.name, *entry + 1);
      }
      *entry += 1;
    }
  }

  fn projection_columns_for_qualified(
    &self,
    projection: &[QualifiedColumn],
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let mut columns = Vec::with_capacity(projection.len());

    for column_ref in projection {
      let schema = self.table(&column_ref.table)?;
      let column = schema.columns.get(column_ref.column_index).ok_or_else(|| {
        EngineError::SchemaMismatch(format!(
          "projection index {} is out of bounds for table {}",
          column_ref.column_index, column_ref.table
        ))
      })?;
      columns.push(ResultColumn::new(
        column.name.clone(),
        Some(column_ref.table.clone()),
        Some(column_ref.column_index),
      ));
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

  fn aggregate_label(aggregate: &Aggregate) -> String {
    match aggregate {
      Aggregate::Count(None) => "count_all".to_string(),
      Aggregate::Count(Some(column)) => {
        format!("count_{}_{}", column.table, column.column_index)
      }
      Aggregate::Sum(column) => format!("sum_{}_{}", column.table, column.column_index),
      Aggregate::Min(column) => format!("min_{}_{}", column.table, column.column_index),
      Aggregate::Max(column) => format!("max_{}_{}", column.table, column.column_index),
      Aggregate::Avg(column) => format!("avg_{}_{}", column.table, column.column_index),
    }
  }

  pub(super) fn output_columns_for_select(
    &self,
    projection: &[QualifiedColumn],
    options: &SelectOptions,
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();
    if !needs_grouping {
      return self.projection_columns_for_qualified(projection);
    }

    let mut columns = Vec::with_capacity(options.group_by.len() + options.aggregates.len());
    for group_column in &options.group_by {
      let schema = self.table(&group_column.table)?;
      let column = schema
        .columns
        .get(group_column.column_index)
        .ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "GROUP BY index {} is out of bounds for table {}",
            group_column.column_index, group_column.table
          ))
        })?;
      columns.push(ResultColumn::new(
        column.name.clone(),
        Some(group_column.table.clone()),
        Some(group_column.column_index),
      ));
    }

    for aggregate in &options.aggregates {
      columns.push(ResultColumn::new(
        Self::aggregate_label(aggregate),
        None,
        None,
      ));
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

  pub(crate) fn new(store: S, change_listener_registry: Arc<ChangeListenerRegistry>) -> Self {
    Self {
      store,
      catalog: EngineCatalog::new(),
      change_listener_registry,
    }
  }

  pub(crate) async fn open(
    store: S,
    change_listener_registry: Arc<ChangeListenerRegistry>,
  ) -> Result<Self, EngineError> {
    let mut kernel = Self::new(store, change_listener_registry);
    kernel.load_schema().await?;
    Ok(kernel)
  }

  pub(crate) async fn load_schema(&mut self) -> Result<(), EngineError> {
    self.catalog.load_from_store(&self.store).await
  }

  pub(crate) fn store(&self) -> &S {
    &self.store
  }

  pub(crate) fn table(&self, table_name: &str) -> Result<&TableSchema, EngineError> {
    self.catalog.table(table_name)
  }

  pub(crate) async fn register_table(
    &mut self,
    schema: TableSchema,
    if_not_exists: bool,
  ) -> Result<(), EngineError> {
    self
      .catalog
      .register_table(&self.store, schema, if_not_exists)
      .await
  }

  pub(crate) async fn drop_table(
    &mut self,
    table_name: &str,
    if_exists: bool,
  ) -> Result<(), EngineError> {
    self
      .catalog
      .drop_table(&self.store, table_name, if_exists)
      .await
  }

  pub(crate) async fn register_index(&mut self, schema: IndexSchema) -> Result<(), EngineError> {
    self.catalog.register_index(&self.store, schema).await
  }

  pub(crate) async fn drop_index(&mut self, index_name: &str) -> Result<(), EngineError> {
    self.catalog.drop_index(&self.store, index_name).await
  }

  pub(crate) fn writer(&self) -> EngineWriteTxn<'_, S> {
    EngineWriteTxn {
      store: &self.store,
      catalog: &self.catalog,
      lifecycle: TransactionLifecycle::new(),
      change_listener_registry: self.change_listener_registry.clone(),
      pending_events: Vec::new(),
    }
  }
}
