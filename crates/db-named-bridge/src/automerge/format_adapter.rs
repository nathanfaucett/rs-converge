//! Automerge **format** adapter over a named-tree **layout backend**.
//!
//! - **Layout backend**: owns named B-tree storage (`REDBNamedBTree`, `InMemoryNamedBTree`).
//! - **Format adapter**: wires layout trees to `db_automerge` for document merge and sync.

use std::borrow::Borrow;

use async_stream::stream;
use db_automerge::{AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction, NamedBTreeMap};
use db_engine::EngineKey;
use db_engine::{EngineNamedTreeBackend, EngineNamedTreeTransaction};
use futures::{StreamExt, pin_mut};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::automerge::catalog::{ensure_tree_initialized, register_tree_name};
use crate::automerge::format_docs::{
  build_named_tree_document, build_removed_named_doc, build_removed_named_row_doc, doc_id_for_key,
  encode_row_bytes, is_named_tombstone, is_row_tree, read_named_key_metadata,
  read_named_value_bytes, read_row_columns, row_key_from_doc,
};
use crate::glue::LayoutFormatBridge;

#[derive(Clone)]
pub struct AutomergeFormatAdapter<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  backend: P,
}

impl<P> AutomergeFormatAdapter<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new(backend: P) -> Self {
    Self { backend }
  }

  async fn format_store_for_tree(
    &self,
    tree: &str,
  ) -> Result<AutomergeEngineStore<P::Tree>, BTreeError> {
    ensure_tree_initialized(&self.backend, tree).await?;
    register_tree_name(&self.backend, tree).await?;
    let tree_backend = self.backend.get_tree(tree).await?;
    Ok(AutomergeEngineStore::new_with_backend(tree_backend))
  }

  pub fn layout_backend(&self) -> &P {
    &self.backend
  }
}

impl<P> LayoutFormatBridge for AutomergeFormatAdapter<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type LayoutBackend = P;

  fn layout_backend(&self) -> &P {
    AutomergeFormatAdapter::layout_backend(self)
  }
}

fn key_in_range<K, R>(key: &K, range: &R) -> bool
where
  K: Ord,
  R: core::ops::RangeBounds<K>,
{
  use core::ops::Bound;

  let start = match range.start_bound() {
    Bound::Included(lower) => key >= lower,
    Bound::Excluded(lower) => key > lower,
    Bound::Unbounded => true,
  };
  let end = match range.end_bound() {
    Bound::Included(upper) => key <= upper,
    Bound::Excluded(upper) => key < upper,
    Bound::Unbounded => true,
  };
  start && end
}

#[derive(Clone)]
pub struct AutomergeFormatTree<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  store: AutomergeFormatAdapter<P>,
  name: String,
}

pub struct AutomergeFormatTreeTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: AutomergeFormatTransaction<P>,
  name: String,
}

pub struct AutomergeFormatTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  store: AutomergeFormatAdapter<P>,
  patches: BTreeMap<String, BTreeMap<EngineKey, Option<Vec<u8>>>>,
}

impl<P> Clone for AutomergeFormatTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      patches: self.patches.clone(),
    }
  }
}

impl<P> AutomergeFormatTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get_named_base(&self, tree: &str, key: &EngineKey) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
  {
    let store = self.store.format_store_for_tree(tree).await?;
    let guard = store.automerge.read().await;
    let doc_id = doc_id_for_key(key);
    let Some(doc) = guard.get(&doc_id).await? else {
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

  async fn range_base(&self, tree: &str) -> Result<Vec<(EngineKey, Vec<u8>)>, BTreeError>
  where
    EngineKey: Ord,
  {
    let store = self.store.format_store_for_tree(tree).await?;
    let guard = store.automerge.read().await;
    let doc_stream = guard.range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
    pin_mut!(doc_stream);

    let mut entries = Vec::new();
    while let Some(item) = doc_stream.next().await {
      let (_, doc) = item?;
      if is_row_tree(tree) {
        let row = match read_row_columns(&doc) {
          Ok(Some(row)) => row,
          Ok(None) => continue,
          Err(e) => return Err(e),
        };
        if row.is_empty() {
          continue;
        }
        entries.push((row_key_from_doc(&doc)?, encode_row_bytes(&row)));
      } else {
        if is_named_tombstone(&doc)? {
          continue;
        }
        let key = match read_named_key_metadata(&doc)? {
          Some(key) => key,
          None => continue,
        };
        let value = match read_named_value_bytes(&doc)? {
          Some(value) => value,
          None => continue,
        };
        entries.push((key, value));
      }
    }
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(entries)
  }

  fn tree_patch_mut(&mut self, tree: &str) -> &mut BTreeMap<EngineKey, Option<Vec<u8>>> {
    self.patches.entry(tree.to_string()).or_default()
  }
}

impl<P> EngineNamedTreeTransaction<EngineKey, Vec<u8>> for AutomergeFormatTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
  {
    if let Some(tree_patch) = self.patches.get(tree)
      && let Some(changed) = tree_patch.get(key)
    {
      return Ok(changed.clone());
    }

    self.get_named_base(tree, key).await
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
    self.tree_patch_mut(tree).insert(key, Some(value));
    Ok(())
  }

  async fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
  {
    if let Some(existing) = self
      .tree_patch_mut(tree)
      .insert(key.clone(), None)
      .flatten()
    {
      return Ok(Some(existing));
    }

    self.get_named_base(tree, key).await
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
    let tree_name = tree.to_string();
    let tree_patch = self.patches.get(tree).cloned().unwrap_or_default();
    stream! {
      let mut merged: BTreeMap<EngineKey, Vec<u8>> = BTreeMap::new();

      let base = match self.range_base(&tree_name).await {
        Ok(entries) => entries,
        Err(e) => { yield Err(e); return; }
      };
      for (key, value) in base {
        merged.insert(key, value);
      }

      for (key, value) in tree_patch {
        match value {
          Some(next) => {
            merged.insert(key, next);
          }
          None => {
            merged.remove(&key);
          }
        }
      }

      for (key, value) in merged {
        if key_in_range(&key, &range) {
          yield Ok((key, value));
        }
      }
    }
  }

  async fn commit(self) -> Result<(), BTreeError> {
    for (tree, patch) in self.patches {
      let store = self.store.format_store_for_tree(&tree).await?;
      let guard = store.automerge.read().await;
      let mut tx = guard.transaction().await?;

      for (key, value) in patch {
        let doc_id = doc_id_for_key(&key);
        match value {
          Some(next_value) => {
            let existing = tx.get(&doc_id).await?;
            let next_doc = build_named_tree_document(&tree, &key, next_value, existing)?;
            tx.insert(doc_id, next_doc).await?;
          }
          None => {
            let Some(existing) = tx.get(&doc_id).await? else {
              continue;
            };

            let (next_doc, removed) = if is_row_tree(&tree) {
              build_removed_named_row_doc(existing)?
            } else {
              build_removed_named_doc(existing, &key)?
            };

            if removed.is_some() {
              tx.insert(doc_id, next_doc).await?;
            }
          }
        }
      }

      tx.commit().await?;
    }

    Ok(())
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    Ok(())
  }
}

impl<P> BTreeExecutor<EngineKey, Vec<u8>> for AutomergeFormatTree<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let mut tx = self.store.begin_transaction().await?;
    tx.get(&self.name, key.borrow()).await
  }

  async fn insert(&mut self, key: EngineKey, value: Vec<u8>) -> Result<(), BTreeError>
  where
    EngineKey: Ord,
  {
    let mut tx = self.store.begin_transaction().await?;
    tx.insert(&self.name, key, value).await?;
    tx.commit().await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let mut tx = self.store.begin_transaction().await?;
    let removed = tx.remove(&self.name, key.borrow()).await?;
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
      let tx = match self.store.begin_transaction().await {
        Ok(tx) => tx,
        Err(e) => { yield Err(e); return; }
      };
      let range_stream = tx.range(&self.name, range);
      pin_mut!(range_stream);
      while let Some(item) = range_stream.next().await {
        yield item;
      }
    }
  }
}

impl<P> BTreeTransaction<EngineKey, Vec<u8>> for AutomergeFormatTreeTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    self.inner.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner.rollback().await
  }
}

impl<P> BTreeExecutor<EngineKey, Vec<u8>> for AutomergeFormatTreeTransaction<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<Vec<u8>>, BTreeError>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + Send + 'a,
  {
    let mut inner = self.inner.clone();
    inner.get(&self.name, key.borrow()).await
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

impl<P> BTree<EngineKey, Vec<u8>> for AutomergeFormatTree<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeFormatTreeTransaction<P>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    Ok(AutomergeFormatTreeTransaction {
      inner: self.store.begin_transaction().await?,
      name: self.name.clone(),
    })
  }
}

impl<P> NamedBTreeMap<EngineKey, Vec<u8>> for AutomergeFormatAdapter<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Tree = AutomergeFormatTree<P>;

  fn get_tree<'a>(
    &'a self,
    name: &str,
  ) -> impl core::future::Future<Output = Result<Self::Tree, BTreeError>> + Send + 'a {
    let store = self.clone();
    let name = name.to_string();
    async move { Ok(AutomergeFormatTree { store, name }) }
  }

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send + '_ {
    let name = name.to_string();
    async move {
      let mut tx = tree.store.begin_transaction().await?;
      let tree_name = tree.name;
      let mut entries = Vec::new();
      {
        let range_stream = tx.range(&tree_name, ..);
        pin_mut!(range_stream);
        while let Some(item) = range_stream.next().await {
          entries.push(item?);
        }
      }
      for (key, value) in entries {
        tx.insert(&name, key, value).await?;
      }
      tx.commit().await
    }
  }

  fn delete_tree(
    &self,
    name: &str,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send + '_ {
    let store = self.clone();
    let name = name.to_string();
    async move {
      let mut tx = store.begin_transaction().await?;
      let mut keys = Vec::new();
      {
        let range_stream = tx.range(&name, ..);
        pin_mut!(range_stream);
        while let Some(item) = range_stream.next().await {
          let (key, _) = item?;
          keys.push(key);
        }
      }
      for key in keys {
        tx.remove(&name, &key).await?;
      }
      tx.commit().await
    }
  }

  fn list_names(&self) -> impl core::future::Future<Output = Vec<String>> + Send + '_ {
    let store = self.clone();
    async move {
      let tx = match store.begin_transaction().await {
        Ok(tx) => tx,
        Err(_) => return Vec::new(),
      };
      let mut names = Vec::new();
      let range_stream = tx.range("sys:automerge_trees", ..);
      pin_mut!(range_stream);
      while let Some(item) = range_stream.next().await {
        let Ok((_key, value)) = item else {
          continue;
        };
        let Ok(name) = String::from_utf8(value) else {
          continue;
        };
        if !name.is_empty() {
          names.push(name);
        }
      }
      names.sort();
      names.dedup();
      names
    }
  }
}

impl<P> EngineNamedTreeBackend<EngineKey, Vec<u8>> for AutomergeFormatAdapter<P>
where
  P: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static,
  P::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeFormatTransaction<P>;

  fn begin_transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = Result<Self::Transaction, BTreeError>> + Send + 'a {
    let store = self.clone();
    async move {
      Ok(AutomergeFormatTransaction {
        store,
        patches: BTreeMap::new(),
      })
    }
  }
}
