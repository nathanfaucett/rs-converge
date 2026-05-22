use crate::{EngineError, EngineKey, EngineRow, IndexSchema, PrimaryKey, TableSchema};
use async_stream::stream;
use core::future::Future;
use db_core::NamedTreeTransaction;
use db_types::persistence::{
  INDEX_SCHEMA_TREE, TABLE_SCHEMA_TREE, decode_index_schema_rows, decode_table_schema_rows,
  encode_index_schema, encode_table_schema, index_schema_entry_key, index_tree, row_tree,
  table_schema_entry_key,
};
use futures::{Stream, StreamExt, pin_mut};

use super::{
  collect_tree_rows, decode_row_bytes, encode_row_bytes, primary_key_from_engine_key,
  schema_decode_error,
};

pub struct NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>>,
{
  inner: T,
}

impl<T> NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>>,
{
  pub fn new(inner: T) -> Self {
    Self { inner }
  }
}

impl<T> super::RowStore for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      self
        .inner
        .get(&row_tree(table_name), &storage_key)
        .await
        .map_err(EngineError::from)
        .and_then(|row| row.map(|bytes| decode_row_bytes(&bytes)).transpose())
    }
  }

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      let row_bytes = encode_row_bytes(&row);
      self
        .inner
        .insert(&row_tree(table_name), storage_key, row_bytes)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      self
        .inner
        .remove(&row_tree(table_name), &storage_key)
        .await
        .map_err(EngineError::from)
        .and_then(|row| row.map(|bytes| decode_row_bytes(&bytes)).transpose())
    }
  }

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a {
    let tree = row_tree(table_name);
    let inner = &self.inner;
    stream! {
      let s = inner.range(&tree, ..);
      pin_mut!(s);
      while let Some(item) = s.next().await {
        yield item
          .map_err(EngineError::from)
          .and_then(|(key, row_bytes)| {
            let row = decode_row_bytes(&row_bytes)?;
            primary_key_from_engine_key(&key).map(|pk| (pk, row))
          });
      }
    }
  }
}

impl<T> super::SchemaStore for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = table_schema_entry_key(schema.name.clone());
      let value = encode_row_bytes(&encode_table_schema(&schema));
      self
        .inner
        .insert(TABLE_SCHEMA_TREE, key, value)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = table_schema_entry_key(table_name);
      self
        .inner
        .remove(TABLE_SCHEMA_TREE, &key)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = index_schema_entry_key(schema.name.clone());
      let value = encode_row_bytes(&encode_index_schema(&schema));
      self
        .inner
        .insert(INDEX_SCHEMA_TREE, key, value)
        .await
        .map_err(EngineError::from)
    }
  }

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = index_schema_entry_key(index_name);
      self
        .inner
        .remove(INDEX_SCHEMA_TREE, &key)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a {
    async move {
      let table_rows = collect_tree_rows(&self.inner, TABLE_SCHEMA_TREE).await?;
      let index_rows = collect_tree_rows(&self.inner, INDEX_SCHEMA_TREE).await?;
      let tables = decode_table_schema_rows(table_rows).map_err(schema_decode_error)?;
      let indexes = decode_index_schema_rows(index_rows).map_err(schema_decode_error)?;
      Ok((tables, indexes))
    }
  }
}

impl<T> super::IndexStore for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let row_pk_key = row_pk.to_engine_key();
      let composite = index.make_entry_key(index_key, &row_pk_key);
      self
        .inner
        .insert(&index_tree(&index.name), composite, Vec::new())
        .await
        .map_err(EngineError::from)
    }
  }

  fn delete_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let row_pk_key = row_pk.to_engine_key();
      let composite = index.make_entry_key(index_key, &row_pk_key);
      self
        .inner
        .remove(&index_tree(&index.name), &composite)
        .await
        .map_err(EngineError::from)?;
      Ok(())
    }
  }

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a {
    let tree = index_tree(&index.name);
    let inner = &self.inner;
    stream! {
      let s = inner.range(&tree, ..);
      pin_mut!(s);
      while let Some(item) = s.next().await {
        yield item.map_err(EngineError::from).and_then(|(composite, _)| {
          index.split_entry_key(&composite)
            .map_err(|_e| EngineError::SchemaMismatch("failed to split entry key".into()))
            .and_then(|(index_key, row_pk_key)| {
              primary_key_from_engine_key(&row_pk_key).map(|row_pk| (index_key, row_pk))
            })
        });
      }
    }
  }
}

impl<T> super::TransactionControl for NamedTreeEngineTransaction<T>
where
  T: NamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn commit(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.commit().await.map_err(EngineError::from) }
  }

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.rollback().await.map_err(EngineError::from) }
  }
}
