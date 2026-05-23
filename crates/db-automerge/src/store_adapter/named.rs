use std::borrow::Borrow;

use async_stream::stream;
use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::Value;
use automerge::transaction::Transactable;
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, NamedTreeProvider, NamedTreeTransaction,
};
use db_types::{
  EngineKey, EngineValue, StoreKey,
  key_encoding::{DefaultEncoding, RowEncoding},
};

use super::AutomergeEngineStore;
use super::doc_payload::{
  clear_doc_fields, read_row_columns, read_store_key_metadata, set_row_columns,
  set_store_key_metadata,
};
use super::key_in_range;
use super::named_routing::{doc_id_for_tree_key, is_row_tree, tree_uuid_range};

const NAMED_KEY_FIELD: &str = "named_key";
const NAMED_VALUE_FIELD: &str = "value";
const NAMED_TOMBSTONE_FIELD: &str = "deleted";

fn scalar_bytes(value: Value<'_>) -> Result<Vec<u8>, BTreeError> {
  match value {
    Value::Scalar(scalar) => match scalar.as_ref() {
      ScalarValue::Bytes(bytes) => Ok(bytes.to_vec()),
      _ => Err(BTreeError::UnsupportedOperation),
    },
    _ => Err(BTreeError::UnsupportedOperation),
  }
}

fn set_named_key_metadata(doc: &mut AutoCommit, key: &EngineKey) -> Result<(), BTreeError> {
  if let Ok(Some(_)) = doc.get(&automerge::ROOT, NAMED_KEY_FIELD) {
    doc
      .delete(&automerge::ROOT, NAMED_KEY_FIELD)
      .map_err(BTreeError::other)?;
  }
  doc
    .put(&automerge::ROOT, NAMED_KEY_FIELD, key.clone())
    .map_err(BTreeError::other)?;
  Ok(())
}

fn set_named_value(doc: &mut AutoCommit, value: &[u8]) -> Result<(), BTreeError> {
  if let Ok(Some(_)) = doc.get(&automerge::ROOT, NAMED_VALUE_FIELD) {
    doc
      .delete(&automerge::ROOT, NAMED_VALUE_FIELD)
      .map_err(BTreeError::other)?;
  }
  doc
    .put(&automerge::ROOT, NAMED_VALUE_FIELD, value.to_vec())
    .map_err(BTreeError::other)?;
  Ok(())
}

fn set_named_tombstone(doc: &mut AutoCommit) -> Result<(), BTreeError> {
  if let Ok(Some(_)) = doc.get(&automerge::ROOT, NAMED_TOMBSTONE_FIELD) {
    doc
      .delete(&automerge::ROOT, NAMED_TOMBSTONE_FIELD)
      .map_err(BTreeError::other)?;
  }
  doc
    .put(&automerge::ROOT, NAMED_TOMBSTONE_FIELD, true)
    .map_err(BTreeError::other)?;
  Ok(())
}

pub(super) fn is_named_tombstone(doc: &AutoCommit) -> Result<bool, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, NAMED_TOMBSTONE_FIELD) {
    return match value {
      Value::Scalar(scalar) => match scalar.as_ref() {
        ScalarValue::Boolean(b) => Ok(*b),
        _ => Err(BTreeError::UnsupportedOperation),
      },
      _ => Err(BTreeError::UnsupportedOperation),
    };
  }
  Ok(false)
}

pub(super) fn read_named_value_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, NAMED_VALUE_FIELD) {
    return Ok(Some(scalar_bytes(value)?));
  }
  Ok(None)
}

fn read_named_key_metadata(doc: &AutoCommit) -> Result<Option<EngineKey>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, NAMED_KEY_FIELD) {
    return Ok(Some(scalar_bytes(value)?));
  }
  Ok(None)
}

fn build_named_doc(
  existing: Option<AutoCommit>,
  key: &EngineKey,
  value: &[u8],
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing.unwrap_or_default();
  clear_doc_fields(&mut doc)?;
  set_named_key_metadata(&mut doc, key)?;
  set_named_value(&mut doc, value)?;
  Ok(doc)
}

pub(super) fn build_named_doc_with_store_key(
  existing: Option<AutoCommit>,
  store_key: &StoreKey,
  key: &EngineKey,
  value: &[u8],
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing.unwrap_or_default();
  clear_doc_fields(&mut doc)?;
  set_named_key_metadata(&mut doc, key)?;
  set_store_key_metadata(&mut doc, store_key)?;
  set_named_value(&mut doc, value)?;
  Ok(doc)
}

pub(super) fn build_named_tombstone_with_store_key(
  existing: AutoCommit,
  store_key: &StoreKey,
  key: &EngineKey,
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing;
  clear_doc_fields(&mut doc)?;
  set_named_key_metadata(&mut doc, key)?;
  set_store_key_metadata(&mut doc, store_key)?;
  set_named_tombstone(&mut doc)?;
  Ok(doc)
}

fn build_named_tombstone(existing: AutoCommit, key: &EngineKey) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing;
  clear_doc_fields(&mut doc)?;
  set_named_key_metadata(&mut doc, key)?;
  set_named_tombstone(&mut doc)?;
  Ok(doc)
}

pub(super) fn build_named_tree_document(
  tree: &str,
  key: &EngineKey,
  value: Vec<u8>,
  existing: Option<AutoCommit>,
) -> Result<AutoCommit, BTreeError> {
  if is_row_tree(tree) {
    let row = decode_row_bytes(&value)?;
    let mut doc = existing.unwrap_or_default();
    clear_doc_fields(&mut doc)?;
    set_store_key_metadata(
      &mut doc,
      &StoreKey::TableRow {
        table_name: tree.strip_prefix("t:").unwrap_or(tree).to_string(),
        primary_key: key.clone(),
      },
    )?;
    set_row_columns(&mut doc, &row)?;
    set_doc_tree(doc, tree)
  } else {
    build_named_doc(existing, key, &value)
  }
}

fn decode_row_bytes(row: &[u8]) -> Result<Vec<EngineValue>, BTreeError> {
  <DefaultEncoding as RowEncoding>::decode_values(row).map_err(BTreeError::other)
}

fn encode_row_bytes(row: &[EngineValue]) -> Vec<u8> {
  <DefaultEncoding as RowEncoding>::encode_values(row)
}

fn doc_tree(doc: &AutoCommit) -> Option<String> {
  match doc.get(&automerge::ROOT, "tree") {
    Ok(Some((value, _))) => {
      let text = value.to_string();
      let cleaned = text
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(&text);
      Some(cleaned.to_string())
    }
    _ => None,
  }
}

fn row_key_from_doc(doc: &AutoCommit) -> Result<EngineKey, BTreeError> {
  match read_store_key_metadata(doc)? {
    Some(StoreKey::TableRow { primary_key, .. }) => Ok(primary_key),
    _ => Err(BTreeError::UnsupportedOperation),
  }
}

fn set_doc_tree(mut doc: AutoCommit, tree: &str) -> Result<AutoCommit, BTreeError> {
  doc
    .put(&automerge::ROOT, "tree", tree)
    .map_err(BTreeError::other)?;
  Ok(doc)
}

#[derive(Clone)]
pub struct AutomergeNamedTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  store: AutomergeEngineStore<B>,
  name: String,
}

pub struct AutomergeNamedTreeTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: AutomergeNamedTransaction<B>,
  name: String,
}

pub struct AutomergeNamedTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: <AutomergeBTree<B> as BTree<Uuid, AutoCommit>>::Transaction,
}

impl<B> AutomergeNamedTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get_named(&self, tree: &str, key: &EngineKey) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = doc_id_for_tree_key(tree, key)?;
    let Some(doc) = self.inner.get(&doc_id).await? else {
      return Ok(None);
    };
    if is_row_tree(tree) {
      Ok(read_row_columns(&doc)?.and_then(|row| {
        if row.is_empty() {
          None
        } else {
          Some(encode_row_bytes(&row))
        }
      }))
    } else {
      if is_named_tombstone(&doc)? {
        return Ok(None);
      }
      read_named_value_bytes(&doc)
    }
  }
}

impl<B> AutomergeNamedTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub(super) fn build_named_tree_document(
    tree: &str,
    key: &EngineKey,
    value: Vec<u8>,
    existing: Option<AutoCommit>,
  ) -> Result<AutoCommit, BTreeError>
  where
    EngineKey: Ord,
  {
    if is_row_tree(tree) {
      let row = decode_row_bytes(&value)?;
      let mut doc = existing.unwrap_or_default();
      clear_doc_fields(&mut doc)?;
      set_store_key_metadata(
        &mut doc,
        &StoreKey::TableRow {
          table_name: tree.strip_prefix("t:").unwrap_or(tree).to_string(),
          primary_key: key.clone(),
        },
      )?;
      set_row_columns(&mut doc, &row)?;
      set_doc_tree(doc, tree)
    } else {
      build_named_doc(existing, key, &value)
    }
  }

  fn build_removed_named_row_doc(
    mut doc: AutoCommit,
    tree: &str,
  ) -> Result<(AutoCommit, Option<Vec<u8>>), BTreeError> {
    let removed = read_row_columns(&doc)?.and_then(|row| {
      if row.is_empty() {
        None
      } else {
        Some(encode_row_bytes(&row))
      }
    });

    if removed.is_some() {
      set_row_columns(&mut doc, &[])?;
      let next_doc = set_doc_tree(doc, tree)?;
      Ok((next_doc, removed))
    } else {
      Ok((doc, None))
    }
  }

  fn build_removed_named_doc(
    doc: AutoCommit,
    key: &EngineKey,
  ) -> Result<(AutoCommit, Option<Vec<u8>>), BTreeError> {
    if is_named_tombstone(&doc)? {
      return Ok((doc, None));
    }

    let removed = read_named_value_bytes(&doc)?;
    if removed.is_none() {
      return Ok((doc, None));
    }

    let next_doc = build_named_tombstone(doc, key)?;
    Ok((next_doc, removed))
  }
}

impl<B> NamedTreeTransaction<EngineKey, Vec<u8>> for AutomergeNamedTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
  {
    self.get_named(tree, key).await
  }

  async fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: Vec<u8>,
  ) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = doc_id_for_tree_key(tree, &key)?;
    let existing = self.inner.get(&doc_id).await?;
    let new_doc = Self::build_named_tree_document(tree, &key, value, existing)?;
    self.inner.insert(doc_id, new_doc).await
  }

  async fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
  {
    let doc_id = doc_id_for_tree_key(tree, key)?;
    let Some(existing) = self.inner.get(&doc_id).await? else {
      return Ok(None);
    };

    let (next_doc, removed) = if is_row_tree(tree) {
      Self::build_removed_named_row_doc(existing, tree)?
    } else {
      Self::build_removed_named_doc(existing, key)?
    };

    if removed.is_some() {
      self.inner.insert(doc_id, next_doc).await?;
    }

    Ok(removed)
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl futures::Stream<Item = Result<(EngineKey, Vec<u8>), BTreeError>> + Send + 'a
  where
    EngineKey: Ord,
    R: core::ops::RangeBounds<EngineKey> + Send + 'a,
  {
    let row_tree = is_row_tree(tree);
    let (tree_start, tree_end) = tree_uuid_range(tree);
    let tree_name = tree.to_string();
    let inner = &self.inner;
    stream! {
      let doc_stream = inner.range(tree_start..=tree_end);
      pin_mut!(doc_stream);

      let mut entries: alloc::vec::Vec<(EngineKey, Vec<u8>)> = alloc::vec::Vec::new();
      while let Some(item) = doc_stream.next().await {
        let (_, doc) = item?;
        if row_tree && doc_tree(&doc).as_deref() != Some(tree_name.as_str()) {
          continue;
        }

        if row_tree {
          let row = match read_row_columns(&doc) {
            Ok(Some(row)) => row,
            Ok(None) => continue,
            Err(e) => { yield Err(e); return; }
          };
          if row.is_empty() {
            continue;
          }
          entries.push((row_key_from_doc(&doc)?, encode_row_bytes(&row)));
        } else {
          if let Ok(true) = is_named_tombstone(&doc) {
            continue;
          }
          let key = match read_named_key_metadata(&doc) {
            Ok(Some(key)) => key,
            Ok(None) => continue,
            Err(e) => { yield Err(e); return; }
          };
          let value = match read_named_value_bytes(&doc) {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(e) => { yield Err(e); return; }
          };
          entries.push((key, value));
        }
      }
      entries.sort_by(|(a, _), (b, _)| a.cmp(b));
      for (key, row) in entries {
        if key_in_range(&key, &range) {
          yield Ok((key, row));
        }
      }
    }
  }

  async fn commit(self) -> Result<(), BTreeError> {
    self.inner.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner.rollback().await
  }
}

impl<B> BTreeExecutor<EngineKey, Vec<u8>> for AutomergeNamedTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let tx = self.transaction().await?;
    tx.get(key.borrow()).await
  }

  async fn insert(&mut self, key: EngineKey, value: Vec<u8>) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    let mut tx = self.transaction().await?;
    tx.insert(key, value).await?;
    tx.commit().await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let mut tx = self.transaction().await?;
    let removed = tx.remove(key.borrow()).await?;
    tx.commit().await?;
    Ok(removed)
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(EngineKey, Vec<u8>), BTreeError>> + Send + 'a
  where
    EngineKey: Ord + Clone,
    R: core::ops::RangeBounds<EngineKey> + Send + 'a,
  {
    stream! {
      let tx = match self.transaction().await {
        Ok(tx) => tx,
        Err(e) => { yield Err(e); return; }
      };
      let range_stream = tx.range(range);
      pin_mut!(range_stream);
      while let Some(item) = range_stream.next().await {
        yield item;
      }
    }
  }
}

impl<B> BTreeTransaction<EngineKey, Vec<u8>> for AutomergeNamedTreeTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    self.inner.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner.rollback().await
  }
}

impl<B> BTreeExecutor<EngineKey, Vec<u8>> for AutomergeNamedTreeTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    self.inner.get_named(&self.name, key.borrow()).await
  }

  async fn insert(&mut self, key: EngineKey, value: Vec<u8>) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    self.inner.insert(&self.name, key, value).await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord + Clone,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    self.inner.remove(&self.name, key.borrow()).await
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(EngineKey, Vec<u8>), BTreeError>> + Send + 'a
  where
    EngineKey: Ord + Clone,
    R: core::ops::RangeBounds<EngineKey> + Send + 'a,
  {
    self.inner.range(&self.name, range)
  }
}

impl<B> BTree<EngineKey, Vec<u8>> for AutomergeNamedTree<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeNamedTreeTransaction<B>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    Ok(AutomergeNamedTreeTransaction {
      inner: self.store.begin_transaction().await?,
      name: self.name.clone(),
    })
  }
}

impl<B> NamedTreeProvider<EngineKey, Vec<u8>> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Tree = AutomergeNamedTree<B>;
  type Transaction = AutomergeNamedTransaction<B>;

  fn get_tree<'a>(
    &'a self,
    name: &str,
  ) -> impl core::future::Future<Output = Result<Self::Tree, BTreeError>> + Send + 'a {
    let store = self.clone();
    let name = name.to_string();
    async move { Ok(AutomergeNamedTree { store, name }) }
  }

  fn begin_transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = Result<Self::Transaction, BTreeError>> + Send + 'a {
    let automerge = self.automerge.clone();
    async move {
      let guard = automerge.read().await;
      let inner = guard.transaction().await?;
      Ok(AutomergeNamedTransaction { inner })
    }
  }
}
