use alloc::{string::String, vec::Vec};
use futures::{StreamExt, pin_mut};
use hashbrown::HashMap;

use crate::store_adapter::{EngineStore, EngineStoreTransaction};
use crate::{EngineError, IndexSchema, TableSchema};

#[derive(Debug, Clone, Default)]
pub(crate) struct EngineCatalog {
  tables: HashMap<String, TableSchema>,
  indexes: HashMap<String, IndexSchema>,
}

impl EngineCatalog {
  pub(crate) fn new() -> Self {
    Self::default()
  }

  pub(crate) fn table(&self, table_name: &str) -> Result<&TableSchema, EngineError> {
    self
      .tables
      .get(table_name)
      .ok_or_else(|| EngineError::TableNotFound(table_name.into()))
  }

  pub(crate) fn contains_table(&self, table_name: &str) -> bool {
    self.tables.contains_key(table_name)
  }

  pub(crate) fn contains_index(&self, index_name: &str) -> bool {
    self.indexes.contains_key(index_name)
  }

  pub(crate) fn insert_table(&mut self, schema: TableSchema) {
    self.tables.insert(schema.name.clone(), schema);
  }

  pub(crate) fn insert_index(&mut self, schema: IndexSchema) {
    self.indexes.insert(schema.name.clone(), schema);
  }

  pub(crate) fn indexes_for_table(&self, table_name: &str) -> Vec<IndexSchema> {
    self
      .indexes
      .values()
      .filter(|index| index.table_name == table_name)
      .cloned()
      .collect()
  }

  pub(crate) async fn load_from_store<S>(&mut self, store: &S) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    let mut tx = store.engine_transaction().await?;
    let (tables, indexes) = tx.load_catalog().await?;

    self.tables.clear();
    self.indexes.clear();

    for table in tables {
      self.tables.insert(table.name.clone(), table);
    }
    for index in indexes {
      self.indexes.insert(index.name.clone(), index);
    }

    Ok(())
  }

  pub(crate) async fn register_table<S>(
    &mut self,
    store: &S,
    schema: TableSchema,
    if_not_exists: bool,
  ) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    self.load_from_store(store).await?;

    schema.validate_primary_key_definition()?;

    if self.contains_table(&schema.name) {
      return if if_not_exists {
        Ok(())
      } else {
        Err(EngineError::DuplicateTable(schema.name))
      };
    }

    let mut tx = store.engine_transaction().await?;
    tx.insert_table_schema(schema.clone()).await?;
    tx.commit().await?;

    self.insert_table(schema);
    Ok(())
  }

  pub(crate) async fn register_index<S>(
    &mut self,
    store: &S,
    schema: IndexSchema,
  ) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    self.load_from_store(store).await?;

    if self.contains_index(&schema.name) {
      return Err(EngineError::DuplicateIndex(schema.name));
    }

    let table = self.table(&schema.table_name)?;
    schema.validate_for_table(table)?;

    let mut tx = store.engine_transaction().await?;
    tx.insert_index_schema(schema.clone()).await?;
    tx.commit().await?;

    self.insert_index(schema);
    Ok(())
  }

  pub(crate) async fn drop_table<S>(
    &mut self,
    store: &S,
    table_name: &str,
    if_exists: bool,
  ) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    self.load_from_store(store).await?;

    if !self.contains_table(table_name) {
      return if if_exists {
        Ok(())
      } else {
        Err(EngineError::TableNotFound(table_name.into()))
      };
    }

    let indexes = self.indexes_for_table(table_name);
    let mut tx = store.engine_transaction().await?;

    for index in &indexes {
      remove_index_entries(&mut tx, index).await?;
      tx.remove_index_schema(&index.name).await?;
    }

    remove_table_rows(&mut tx, table_name).await?;
    tx.remove_table_schema(table_name).await?;
    tx.commit().await?;

    self.tables.remove(table_name);
    for index in indexes {
      self.indexes.remove(&index.name);
    }

    Ok(())
  }

  pub(crate) async fn drop_index<S>(
    &mut self,
    store: &S,
    index_name: &str,
  ) -> Result<(), EngineError>
  where
    S: EngineStore,
  {
    self.load_from_store(store).await?;

    if !self.contains_index(index_name) {
      return Err(EngineError::IndexNotFound(index_name.into()));
    }

    let index = self.indexes.get(index_name).cloned().unwrap();
    let mut tx = store.engine_transaction().await?;
    remove_index_entries(&mut tx, &index).await?;
    tx.remove_index_schema(index_name).await?;
    tx.commit().await?;

    self.indexes.remove(index_name);
    Ok(())
  }
}

async fn remove_index_entries<TX>(tx: &mut TX, index: &IndexSchema) -> Result<(), EngineError>
where
  TX: EngineStoreTransaction,
{
  let mut entries = Vec::new();
  {
    let stream = tx.scan_index_entries(&index.name);
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (entry_key, pk) = item?;
      entries.push((entry_key, pk));
    }
  }

  for (entry_key, pk) in entries {
    let (index_key, _) = index
      .split_entry_key(&entry_key)
      .map_err(|err| EngineError::SchemaMismatch(format!("invalid index entry key: {err:?}")))?;
    tx.remove_index_entry(index, &index_key, &pk).await?;
  }

  Ok(())
}

async fn remove_table_rows<TX>(tx: &mut TX, table_name: &str) -> Result<(), EngineError>
where
  TX: EngineStoreTransaction,
{
  let mut pks = Vec::new();
  {
    let stream = tx.scan_table_rows(table_name);
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (pk, _) = item?;
      pks.push(pk);
    }
  }

  for pk in pks {
    tx.remove_table_row(table_name, &pk).await?;
  }

  Ok(())
}
