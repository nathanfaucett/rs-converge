use super::EngineNamedTreeTransaction;
use crate::persistence::{
  INDEX_SCHEMA_TREE, TABLE_SCHEMA_TREE, decode_index_schema_rows, decode_table_schema_rows,
  encode_index_schema, encode_table_schema, index_schema_entry_key, index_tree, row_tree,
  table_schema_entry_key,
};
use crate::{EngineError, EngineKey, EngineRow, IndexSchema, PrimaryKey, TableSchema};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use async_stream::stream;
use core::future::Future;
use futures::{Stream, StreamExt, pin_mut};

use super::named_tree_backend::{
  collect_tree_rows, decode_row_bytes, encode_row_bytes, get_bytes, insert_bytes,
  primary_key_from_engine_key, range_bytes, remove_bytes,
};

pub struct NamedTreeEngineTransaction<T>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  inner: T,
}

impl<T> NamedTreeEngineTransaction<T>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  pub fn new(inner: T) -> Self {
    Self { inner }
  }

  async fn get_bytes<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<Vec<u8>>, EngineError> {
    get_bytes(&mut self.inner, tree, key).await
  }

  async fn insert_bytes<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: Vec<u8>,
  ) -> Result<(), EngineError> {
    insert_bytes(&mut self.inner, tree, key, value).await
  }

  async fn remove_bytes<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<Vec<u8>>, EngineError> {
    remove_bytes(&mut self.inner, tree, key).await
  }

  fn range_bytes(
    &self,
    tree: String,
  ) -> impl Stream<Item = Result<(EngineKey, Vec<u8>), EngineError>> + '_ {
    range_bytes(&self.inner, tree)
  }
}

impl<T> super::EngineStoreReadTransaction for NamedTreeEngineTransaction<T>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl Future<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let storage_key = primary_key.to_engine_key();
      self
        .get_bytes(&row_tree(table_name), &storage_key)
        .await
        .and_then(|row| row.map(|bytes| decode_row_bytes(&bytes)).transpose())
    }
  }

  fn range_table_rows<'a>(
    &'a self,
    table_name: &'a str,
  ) -> impl Stream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a {
    let tree = row_tree(table_name).to_string();
    let stream = self.range_bytes(tree);
    stream! {
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        yield item
          .and_then(|(key, row_bytes)| {
            let row = decode_row_bytes(&row_bytes)?;
            primary_key_from_engine_key(&key).map(|pk| (pk, row))
          });
      }
    }
  }

  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl Future<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a {
    async move {
      let table_rows = collect_tree_rows(&self.inner, TABLE_SCHEMA_TREE).await?;
      let index_rows = collect_tree_rows(&self.inner, INDEX_SCHEMA_TREE).await?;
      let tables = decode_table_schema_rows(table_rows).map_err(EngineError::from)?;
      let indexes = decode_index_schema_rows(index_rows).map_err(EngineError::from)?;
      Ok((tables, indexes))
    }
  }

  fn range_index_entries<'a>(
    &'a self,
    index: &'a IndexSchema,
  ) -> impl Stream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a {
    let tree = index_tree(&index.name).to_string();
    let stream = self.range_bytes(tree);
    stream! {
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        yield item.and_then(|(composite, _)| {
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

impl<T> super::EngineStoreTransaction for NamedTreeEngineTransaction<T>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>> + 'static,
{
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
        .insert_bytes(&row_tree(table_name), storage_key, row_bytes)
        .await
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
        .remove_bytes(&row_tree(table_name), &storage_key)
        .await
        .and_then(|row| row.map(|bytes| decode_row_bytes(&bytes)).transpose())
    }
  }

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = table_schema_entry_key(schema.name.clone());
      let value = encode_row_bytes(&encode_table_schema(&schema));
      self.insert_bytes(TABLE_SCHEMA_TREE, key, value).await
    }
  }

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = table_schema_entry_key(table_name);
      self.remove_bytes(TABLE_SCHEMA_TREE, &key).await?;
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
      self.insert_bytes(INDEX_SCHEMA_TREE, key, value).await
    }
  }

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl Future<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = index_schema_entry_key(index_name);
      self.remove_bytes(INDEX_SCHEMA_TREE, &key).await?;
      Ok(())
    }
  }

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
        .insert_bytes(&index_tree(&index.name), composite, Vec::new())
        .await
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
        .remove_bytes(&index_tree(&index.name), &composite)
        .await?;
      Ok(())
    }
  }

  fn commit(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.commit().await.map_err(EngineError::from) }
  }

  fn rollback(self) -> impl Future<Output = Result<(), EngineError>> {
    async move { self.inner.rollback().await.map_err(EngineError::from) }
  }
}
