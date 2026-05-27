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
  BTree, BTreeError, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor, MaybeSend,
  MaybeSendFuture, MaybeSendStream, MaybeSync, NamedBTreeMap, ValueCodec,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCapability {
  SingleTreeAtomicity,
  MultiTreeAtomicity,
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionContract {
  pub atomicity: BackendCapability,
  pub multi_tree_write_atomicity: bool,
  pub schema_mutation_atomicity: bool,
}

impl Default for TransactionContract {
  fn default() -> Self {
    Self {
      atomicity: BackendCapability::SingleTreeAtomicity,
      multi_tree_write_atomicity: false,
      schema_mutation_atomicity: false,
    }
  }
}

impl TransactionContract {
  pub fn validate(&self) -> Result<(), EngineError> {
    if self.atomicity == BackendCapability::MultiTreeAtomicity && !self.multi_tree_write_atomicity {
      return Err(EngineError::StoreError(BTreeError::Custom(
        "backend reports inconsistent multi-tree atomicity".into(),
      )));
    }
    Ok(())
  }
}

pub trait EngineStore: Clone + MaybeSend + MaybeSync + 'static {
  type Transaction: EngineStoreTransaction;

  fn engine_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, EngineError>> + '_;
  fn engine_read_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, EngineError>> + '_;
  fn transaction_contract(&self) -> TransactionContract;
}

pub trait EngineStoreTransaction: MaybeSend + MaybeSync {
  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl MaybeSendFuture<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a;

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a;

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a;

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Option<TableSchema>, EngineError>> + 'a;

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Option<IndexSchema>, EngineError>> + 'a;

  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a;

  fn update_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a;

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<Option<EngineRow>, EngineError>> + 'a;

  fn scan_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl MaybeSendStream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a;

  fn scan_index_entries<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl MaybeSendStream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a;

  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a;

  fn remove_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<Option<PrimaryKey>, EngineError>> + 'a;

  fn commit(self) -> impl MaybeSendFuture<Output = Result<(), EngineError>>
  where
    Self: Sized;

  fn rollback(self) -> impl MaybeSendFuture<Output = Result<(), EngineError>>
  where
    Self: Sized;
}

pub trait EngineNamedTreeTransaction<K, V>: MaybeSend + MaybeSync {
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

pub trait EngineNamedTreeBackend<K, V>: MaybeSend + MaybeSync {
  type Transaction: EngineNamedTreeTransaction<K, V>;

  fn begin_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, BTreeError>> + '_;

  fn begin_read_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, BTreeError>> + '_;
}

pub struct NamedTreeEngineStore<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
{
  store: S,
}

impl<S> NamedTreeEngineStore<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
{
  pub fn new(store: S) -> Self {
    Self { store }
  }
}

pub struct NamedTreeEngineStoreTransaction<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
  S::Tree: BTree<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
  <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction:
    BTreeTransaction<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  backend: S,
  transactions: HashMap<String, <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction>,
}

impl<S> Clone for NamedTreeEngineStore<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
{
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
    }
  }
}

impl<S> EngineStore for NamedTreeEngineStore<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
  S::Tree: BTree<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
  <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction:
    BTreeTransaction<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  type Transaction = NamedTreeEngineStoreTransaction<S>;

  fn engine_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, EngineError>> + '_ {
    async move {
      Ok(NamedTreeEngineStoreTransaction {
        backend: self.store.clone(),
        transactions: HashMap::new(),
      })
    }
  }

  fn engine_read_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = Result<Self::Transaction, EngineError>> + '_ {
    self.engine_transaction()
  }

  fn transaction_contract(&self) -> TransactionContract {
    TransactionContract::default()
  }
}

impl<S> NamedTreeEngineStoreTransaction<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
  S::Tree: BTree<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
  <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction:
    BTreeTransaction<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
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
}

impl<S> EngineStoreTransaction for NamedTreeEngineStoreTransaction<S>
where
  S: NamedBTreeMap<EngineKey, Vec<u8>> + Clone + MaybeSend + MaybeSync + 'static,
  S::Tree: BTree<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
  <S::Tree as BTree<EngineKey, Vec<u8>>>::Transaction:
    BTreeTransaction<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  fn load_catalog<'a>(
    &'a mut self,
  ) -> impl MaybeSendFuture<Output = Result<(Vec<TableSchema>, Vec<IndexSchema>), EngineError>> + 'a
  {
    async move {
      let table_rows = {
        let mut rows = Vec::new();
        let tables_tx = self.open_tree_transaction(TABLE_SCHEMA_TREE).await?;
        let table_stream = tables_tx.range(..);
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
        let index_stream = indexes_tx.range(..);
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
  }

  fn insert_table_schema<'a>(
    &'a mut self,
    schema: TableSchema,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a {
    async move {
      let tx = self.open_tree_transaction(TABLE_SCHEMA_TREE).await?;
      tx.insert(
        table_schema_entry_key(&schema.name),
        EngineRowCodec::encode(&encode_table_schema(&schema)),
      )
      .await
      .map_err(EngineError::StoreError)
    }
  }

  fn insert_index_schema<'a>(
    &'a mut self,
    schema: IndexSchema,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a {
    async move {
      let tx = self.open_tree_transaction(INDEX_SCHEMA_TREE).await?;
      tx.insert(
        index_schema_entry_key(&schema.name),
        EngineRowCodec::encode(&encode_index_schema(&schema)),
      )
      .await
      .map_err(EngineError::StoreError)
    }
  }

  fn remove_table_schema<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Option<TableSchema>, EngineError>> + 'a {
    async move {
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
  }

  fn remove_index_schema<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl MaybeSendFuture<Output = Result<Option<IndexSchema>, EngineError>> + 'a {
    async move {
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
  }

  fn get_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let key = primary_key.to_engine_key();
      let tx = self.open_tree_transaction(&row_tree(table_name)).await?;
      let row_bytes = tx.get(&key).await.map_err(EngineError::StoreError)?;
      match row_bytes {
        Some(bytes) => Ok(Some(EngineRowCodec::decode_checked(&bytes).map_err(
          |_| EngineError::StoreError(BTreeError::Custom("invalid table row encoding".into())),
        )?)),
        None => Ok(None),
      }
    }
  }

  fn insert_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = primary_key.to_engine_key();
      let tx = self.open_tree_transaction(&row_tree(table_name)).await?;
      tx.insert(key, EngineRowCodec::encode(&row))
        .await
        .map_err(EngineError::StoreError)
    }
  }

  fn update_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: PrimaryKey,
    row: EngineRow,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a {
    async move { self.insert_table_row(table_name, primary_key, row).await }
  }

  fn remove_table_row<'a>(
    &'a mut self,
    table_name: &'a str,
    primary_key: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<Option<EngineRow>, EngineError>> + 'a {
    async move {
      let key = primary_key.to_engine_key();
      let tx = self.open_tree_transaction(&row_tree(table_name)).await?;
      let row_bytes = tx.remove(&key).await.map_err(EngineError::StoreError)?;
      match row_bytes {
        Some(bytes) => Ok(Some(EngineRowCodec::decode_checked(&bytes).map_err(
          |_| EngineError::StoreError(BTreeError::Custom("invalid table row encoding".into())),
        )?)),
        None => Ok(None),
      }
    }
  }

  fn scan_table_rows<'a>(
    &'a mut self,
    table_name: &'a str,
  ) -> impl MaybeSendStream<Item = Result<(PrimaryKey, EngineRow), EngineError>> + 'a {
    let table_name = row_tree(table_name);
    stream! {
      let tx = self.open_tree_transaction(&table_name).await?;
      let range = tx.range(..);
      pin_mut!(range);
      while let Some(item) = range.next().await {
        let (key, row_bytes) = item.map_err(EngineError::StoreError)?;
        let primary_key = primary_key_from_bytes(&key)?;
        let row = EngineRowCodec::decode_checked(&row_bytes).map_err(|_| EngineError::StoreError(BTreeError::Custom("invalid row encoding".into())))?;
        yield Ok((primary_key, row));
      }
    }
  }

  fn scan_index_entries<'a>(
    &'a mut self,
    index_name: &'a str,
  ) -> impl MaybeSendStream<Item = Result<(EngineKey, PrimaryKey), EngineError>> + 'a {
    let tree_name = index_tree(index_name);
    stream! {
      let tx = self.open_tree_transaction(&tree_name).await?;
      let range = tx.range(..);
      pin_mut!(range);
      while let Some(item) = range.next().await {
        let (key, pk_bytes) = item.map_err(EngineError::StoreError)?;
        let primary_key = primary_key_from_bytes(&pk_bytes)?;
        yield Ok((key, primary_key));
      }
    }
  }

  fn insert_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<(), EngineError>> + 'a {
    async move {
      let key = row_pk.to_engine_key();
      let tx = self.open_tree_transaction(&index_tree(&index.name)).await?;
      let entry_key = index.make_entry_key(index_key, &key);
      tx.insert(entry_key, key)
        .await
        .map_err(EngineError::StoreError)
    }
  }

  fn remove_index_entry<'a>(
    &'a mut self,
    index: &'a IndexSchema,
    index_key: &'a EngineKey,
    row_pk: &'a PrimaryKey,
  ) -> impl MaybeSendFuture<Output = Result<Option<PrimaryKey>, EngineError>> + 'a {
    async move {
      let key = row_pk.to_engine_key();
      let tx = self.open_tree_transaction(&index_tree(&index.name)).await?;
      let entry_key = index.make_entry_key(index_key, &key);
      let removed = tx
        .remove(&entry_key)
        .await
        .map_err(EngineError::StoreError)?;
      match removed {
        Some(bytes) => Ok(Some(primary_key_from_bytes(&bytes)?)),
        None => Ok(None),
      }
    }
  }

  fn commit(self) -> impl MaybeSendFuture<Output = Result<(), EngineError>> {
    async move {
      for (_name, tx) in self.transactions {
        tx.commit().await.map_err(EngineError::StoreError)?;
      }
      Ok(())
    }
  }

  fn rollback(self) -> impl MaybeSendFuture<Output = Result<(), EngineError>> {
    async move {
      for (_name, tx) in self.transactions {
        tx.rollback().await.map_err(EngineError::StoreError)?;
      }
      Ok(())
    }
  }
}

pub async fn collect_table_rows<TX>(
  tx: &mut TX,
  table_name: &str,
  predicate: Option<QualifiedPredicate>,
) -> Result<Vec<(PrimaryKey, EngineRow)>, EngineError>
where
  TX: EngineStoreTransaction,
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
