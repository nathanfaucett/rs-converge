#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::{
  format,
  string::{String, ToString},
  vec::Vec,
};
#[cfg(feature = "std")]
use std::{
  format,
  string::{String, ToString},
  vec::Vec,
};

use async_stream::stream;
use core::ops::RangeBounds;
use db_core::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
  MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync, NamedBTreeMap, ValueCodec,
};
#[cfg(feature = "in-memory")]
use db_in_memory::{InMemoryNamedBTree, InMemoryNamedTreeTransaction};
use futures::{StreamExt, pin_mut};
use hashbrown::HashMap;

use crate::key_encoding::{DefaultEncoding, EngineRowCodec};
use crate::persistence::{
  INDEX_SCHEMA_TREE, TABLE_SCHEMA_TREE, decode_index_schema_rows, decode_table_schema_rows,
  encode_index_schema, encode_table_schema, index_schema_entry_key, index_tree, row_tree,
  table_schema_entry_key,
};
use crate::predicate::{EvalContext, PredicateEvaluator};
use crate::query::QualifiedPredicate;
use crate::{EngineError, EngineKey, EngineRow, EngineValue, IndexSchema, PrimaryKey, TableSchema};

pub trait EngineStoreTransaction<K = EngineKey, V = Vec<u8>>: MaybeSend + MaybeSync {
  fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a K,
  ) -> impl MaybeSendFuture<Output = Result<Option<V>, BTreeError>> + 'a
  where
    K: Ord;

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl MaybeSendStream<Item = Result<(K, V), BTreeError>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a;

  fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: K,
    value: V,
  ) -> impl MaybeSendFuture<Output = Result<(), BTreeError>> + 'a
  where
    K: Ord;

  fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a K,
  ) -> impl MaybeSendFuture<Output = Result<Option<V>, BTreeError>> + 'a
  where
    K: Ord;

  fn commit(self) -> impl MaybeSendFuture<Output = Result<(), BTreeError>>
  where
    Self: Sized;

  fn rollback(self) -> impl MaybeSendFuture<Output = Result<(), BTreeError>>
  where
    Self: Sized;
}

pub trait EngineStoreBackend<K = EngineKey, V = Vec<u8>>:
  NamedBTreeMap<K, V> + Clone + MaybeSend + MaybeSync + 'static
where
  Self::Tree: BTree<K, V> + MaybeSend + MaybeSync + 'static,
{
  type Transaction: EngineStoreTransaction<K, V> + MaybeSend + MaybeSync;

  fn begin_transaction<'a>(
    &'a self,
    tree_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, BTreeError>> + 'a;

  fn begin_read_transaction<'a>(
    &'a self,
    tree_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, BTreeError>> + 'a;
}

#[cfg(feature = "in-memory")]
impl<K, V> EngineStoreTransaction<K, V> for InMemoryNamedTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a>(&'a mut self, _tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    BTreeReadExecutor::get(self, key).await
  }

  async fn insert<'a>(&'a mut self, _tree: &'a str, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    BTreeWriteExecutor::insert(self, key, value).await
  }

  async fn remove<'a>(&'a mut self, _tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    BTreeWriteExecutor::remove(self, key).await
  }

  fn range<'a, R>(
    &'a self,
    _tree: &'a str,
    range: R,
  ) -> impl MaybeSendStream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    BTreeReadExecutor::range(self, range)
  }

  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    BTreeTransaction::commit(self).await
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    BTreeTransaction::rollback(self).await
  }
}

#[cfg(feature = "in-memory")]
impl<K, V> EngineStoreBackend<K, V> for InMemoryNamedBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Transaction = InMemoryNamedTreeTransaction<K, V>;

  fn begin_transaction<'a>(
    &'a self,
    tree_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, BTreeError>> + 'a {
    async move {
      let tree = self.get_tree(tree_name).await?;
      tree.transaction().await
    }
  }

  fn begin_read_transaction<'a>(
    &'a self,
    tree_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, BTreeError>> + 'a {
    self.begin_transaction(tree_name)
  }
}

pub struct NamedTreeEngineTransaction<S>
where
  S: EngineStoreBackend,
{
  backend: S,
  transactions: HashMap<String, <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction>,
}

impl<S> NamedTreeEngineTransaction<S>
where
  S: EngineStoreBackend,
{
  pub fn new(backend: S) -> Self {
    Self {
      backend,
      transactions: HashMap::new(),
    }
  }

  async fn open_tree_transaction(
    &mut self,
    tree_name: &str,
  ) -> Result<&mut <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction, EngineError> {
    if self.transactions.contains_key(tree_name) {
      return Ok(self.transactions.get_mut(tree_name).unwrap());
    }

    let tree = self
      .backend
      .get_tree(tree_name)
      .await
      .map_err(EngineError::StoreError)?;
    let tx = tree.transaction().await.map_err(EngineError::StoreError)?;
    self.transactions.insert(tree_name.to_string(), tx);
    Ok(self.transactions.get_mut(tree_name).unwrap())
  }

  pub async fn load_catalog(
    &mut self,
  ) -> Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError> {
    let table_rows = {
      let mut rows = Vec::new();
      let tables_tx = self.open_tree_transaction(TABLE_SCHEMA_TREE).await?;
      let table_stream = BTreeReadExecutor::range(tables_tx, ..);
      pin_mut!(table_stream);
      while let Some(item) = table_stream.next().await {
        let (_key, row_bytes) = item.map_err(EngineError::StoreError)?;
        let row = EngineRowCodec::decode_checked(&row_bytes).map_err(|_| {
          EngineError::StoreError(BTreeError::Custom(
            "invalid table schema row encoding".into(),
          ))
        })?;
        rows.push(row);
      }
      rows
    };

    let index_rows = {
      let mut rows = Vec::new();
      let indexes_tx = self.open_tree_transaction(INDEX_SCHEMA_TREE).await?;
      let index_stream = BTreeReadExecutor::range(indexes_tx, ..);
      pin_mut!(index_stream);
      while let Some(item) = index_stream.next().await {
        let (_key, row_bytes) = item.map_err(EngineError::StoreError)?;
        let row = EngineRowCodec::decode_checked(&row_bytes).map_err(|_| {
          EngineError::StoreError(BTreeError::Custom(
            "invalid index schema row encoding".into(),
          ))
        })?;
        rows.push(row);
      }
      rows
    };

    let tables = decode_table_schema_rows(table_rows).map_err(|err| {
      EngineError::StoreError(BTreeError::Custom(format!(
        "invalid table schema row: {err:?}"
      )))
    })?;
    let indexes = decode_index_schema_rows(index_rows).map_err(|err| {
      EngineError::StoreError(BTreeError::Custom(format!(
        "invalid index schema row: {err:?}"
      )))
    })?;
    Ok((tables, indexes))
  }

  pub async fn insert_table_schema(&mut self, schema: TableSchema) -> Result<(), EngineError> {
    let tx = self.open_tree_transaction(TABLE_SCHEMA_TREE).await?;
    BTreeWriteExecutor::insert(
      tx,
      table_schema_entry_key(&schema.name),
      EngineRowCodec::encode(&encode_table_schema(&schema)),
    )
    .await
    .map_err(EngineError::StoreError)
  }

  pub async fn insert_index_schema(&mut self, schema: IndexSchema) -> Result<(), EngineError> {
    let tx = self.open_tree_transaction(INDEX_SCHEMA_TREE).await?;
    BTreeWriteExecutor::insert(
      tx,
      index_schema_entry_key(&schema.name),
      EngineRowCodec::encode(&encode_index_schema(&schema)),
    )
    .await
    .map_err(EngineError::StoreError)
  }

  pub async fn remove_table_schema(
    &mut self,
    table_name: &str,
  ) -> Result<Option<TableSchema>, EngineError> {
    let tx = self.open_tree_transaction(TABLE_SCHEMA_TREE).await?;
    let row = tx
      .remove(&table_schema_entry_key(table_name))
      .await
      .map_err(EngineError::StoreError)?;
    if let Some(row_bytes) = row {
      let row = EngineRowCodec::decode_checked(&row_bytes).map_err(|_| {
        EngineError::StoreError(BTreeError::Custom(
          "invalid table schema row encoding".into(),
        ))
      })?;
      Ok(Some(
        decode_table_schema_rows(vec![row]).map_err(|err| {
          EngineError::StoreError(BTreeError::Custom(format!(
            "invalid table schema row: {err:?}"
          )))
        })?[0]
          .clone(),
      ))
    } else {
      Ok(None)
    }
  }

  pub async fn remove_index_schema(
    &mut self,
    index_name: &str,
  ) -> Result<Option<IndexSchema>, EngineError> {
    let tx = self.open_tree_transaction(INDEX_SCHEMA_TREE).await?;
    let row = tx
      .remove(&index_schema_entry_key(index_name))
      .await
      .map_err(EngineError::StoreError)?;
    if let Some(row_bytes) = row {
      let row = EngineRowCodec::decode_checked(&row_bytes).map_err(|_| {
        EngineError::StoreError(BTreeError::Custom(
          "invalid index schema row encoding".into(),
        ))
      })?;
      Ok(Some(
        decode_index_schema_rows(vec![row]).map_err(|err| {
          EngineError::StoreError(BTreeError::Custom(format!(
            "invalid index schema row: {err:?}"
          )))
        })?[0]
          .clone(),
      ))
    } else {
      Ok(None)
    }
  }

  pub async fn get_table_row(
    &mut self,
    table_name: &str,
    primary_key: &PrimaryKey,
  ) -> Result<Option<EngineRow>, EngineError> {
    let key = primary_key.to_engine_key();
    let tx = self.open_tree_transaction(&row_tree(table_name)).await?;
    let row_bytes = BTreeReadExecutor::get(tx, &key)
      .await
      .map_err(EngineError::StoreError)?;
    match row_bytes {
      Some(bytes) => Ok(Some(EngineRowCodec::decode_checked(&bytes).map_err(
        |_| EngineError::StoreError(BTreeError::Custom("invalid table row encoding".into())),
      )?)),
      None => Ok(None),
    }
  }

  pub async fn insert_table_row(
    &mut self,
    table_name: &str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> Result<(), EngineError> {
    let key = primary_key.to_engine_key();
    let tx = self.open_tree_transaction(&row_tree(table_name)).await?;
    BTreeWriteExecutor::insert(tx, key, EngineRowCodec::encode(&row))
      .await
      .map_err(EngineError::StoreError)
  }

  pub async fn remove_table_row(
    &mut self,
    table_name: &str,
    primary_key: &PrimaryKey,
  ) -> Result<Option<EngineRow>, EngineError> {
    let key = primary_key.to_engine_key();
    let tx = self.open_tree_transaction(&row_tree(table_name)).await?;
    let row_bytes = BTreeWriteExecutor::remove(tx, &key)
      .await
      .map_err(EngineError::StoreError)?;
    match row_bytes {
      Some(bytes) => Ok(Some(EngineRowCodec::decode_checked(&bytes).map_err(
        |_| EngineError::StoreError(BTreeError::Custom("invalid table row encoding".into())),
      )?)),
      None => Ok(None),
    }
  }

  pub fn scan_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl MaybeSendStream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a {
    let table_name = row_tree(table_name);
    stream! {
      let tx = self.open_tree_transaction(&table_name).await?;
      let range = BTreeReadExecutor::range(tx, ..);
      pin_mut!(range);
      while let Some(item) = range.next().await {
        let (key, row_bytes) = item.map_err(EngineError::StoreError)?;
        let primary_key = primary_key_from_bytes(&key)?;
        let row = EngineRowCodec::decode_checked(&row_bytes).map_err(|_| EngineError::StoreError(BTreeError::Custom("invalid row encoding".into())))?;
        yield Ok((primary_key, row));
      }
    }
  }

  pub fn scan_index_entries<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl MaybeSendStream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a {
    let tree_name = index_tree(index_name);
    stream! {
      let tx = self.open_tree_transaction(&tree_name).await?;
      let range = BTreeReadExecutor::range(tx, ..);
      pin_mut!(range);
      while let Some(item) = range.next().await {
        let (key, pk_bytes) = item.map_err(EngineError::StoreError)?;
        let primary_key = primary_key_from_bytes(&pk_bytes)?;
        yield Ok((key, primary_key));
      }
    }
  }

  pub async fn insert_index_entry(
    &mut self,
    index: &IndexSchema,
    index_key: &EngineKey,
    row_pk: &PrimaryKey,
  ) -> Result<(), EngineError> {
    let key = row_pk.to_engine_key();
    let tx = self.open_tree_transaction(&index_tree(&index.name)).await?;
    let entry_key = index.make_entry_key(index_key, &key);
    BTreeWriteExecutor::insert(tx, entry_key, key)
      .await
      .map_err(EngineError::StoreError)
  }

  pub async fn remove_index_entry(
    &mut self,
    index: &IndexSchema,
    index_key: &EngineKey,
    row_pk: &PrimaryKey,
  ) -> Result<Option<PrimaryKey>, EngineError> {
    let key = row_pk.to_engine_key();
    let tx = self.open_tree_transaction(&index_tree(&index.name)).await?;
    let entry_key = index.make_entry_key(index_key, &key);
    let removed = BTreeWriteExecutor::remove(tx, &entry_key)
      .await
      .map_err(EngineError::StoreError)?;
    match removed {
      Some(bytes) => Ok(Some(primary_key_from_bytes(&bytes)?)),
      None => Ok(None),
    }
  }

  pub async fn commit(self) -> Result<(), EngineError> {
    for (_name, tx) in self.transactions {
      tx.commit().await.map_err(EngineError::StoreError)?;
    }
    Ok(())
  }

  pub async fn rollback(self) -> Result<(), EngineError> {
    for (_name, tx) in self.transactions {
      tx.rollback().await.map_err(EngineError::StoreError)?;
    }
    Ok(())
  }
}

fn primary_key_from_bytes(key: &[u8]) -> Result<PrimaryKey, EngineError> {
  let values =
    <DefaultEncoding as crate::key_encoding::KeyEncoding>::decode_values(key).map_err(|_| {
      EngineError::StoreError(BTreeError::Custom("invalid primary key encoding".into()))
    })?;
  if values.len() != 1 {
    return Err(EngineError::StoreError(BTreeError::Custom(
      "invalid primary key encoding".into(),
    )));
  }
  match &values[0] {
    EngineValue::Uuid(bytes) => Ok(PrimaryKey::new(*bytes)),
    _ => Err(EngineError::StoreError(BTreeError::Custom(
      "invalid primary key encoding".into(),
    ))),
  }
}

pub async fn collect_table_rows<S>(
  tx: &mut NamedTreeEngineTransaction<S>,
  table_name: &str,
  predicate: Option<QualifiedPredicate>,
) -> Result<Vec<(PrimaryKey, EngineRow)>, EngineError>
where
  S: EngineStoreBackend,
{
  let mut rows = Vec::new();
  let eval_ctx = EvalContext::empty();
  let stream = tx.scan_table_rows(table_name);
  pin_mut!(stream);
  let evaluator = PredicateEvaluator::new(&eval_ctx);
  while let Some(item) = stream.next().await {
    let (pk, row) = item?;
    if let Some(pred) = predicate.as_ref() {
      if !evaluator.matches_row(pred, table_name, &row) {
        continue;
      }
    }
    rows.push((pk, row));
  }
  Ok(rows)
}
