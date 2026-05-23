use std::borrow::Borrow;

use async_stream::stream;
use automerge::AutoCommit;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, NamedTreeProvider, NamedTreeTransaction,
};
use db_types::{
  EngineKey, EngineValue,
  key_encoding::{DefaultEncoding, RowEncoding},
};

use super::AutomergeEngineStore;
use super::doc_payload::{
  encode_snapshot_base64, read_row_columns, set_row_columns, snapshot_bytes, snapshot_doc,
};
use super::named_routing::{
  doc_id_for_tree_key, is_row_tree, row_key_from_doc_id, tree_uuid_range,
};
use super::snapshot::{EngineSnapshotAdapter, find_entry, key_in_range, parse_entries, set_entry};

fn parse_named_snapshot(buf: &[u8]) -> Result<Vec<(EngineKey, Vec<u8>)>, BTreeError> {
  parse_entries::<EngineSnapshotAdapter>(buf)
}

fn find_in_named_snapshot(buf: &[u8], needle: &EngineKey) -> Result<Option<Vec<u8>>, BTreeError> {
  find_entry::<EngineSnapshotAdapter>(buf, needle)
}

fn set_in_named_snapshot(
  buf: Option<&[u8]>,
  key: EngineKey,
  row: Vec<u8>,
) -> Result<Vec<u8>, BTreeError> {
  set_entry::<EngineSnapshotAdapter>(buf, &key, &row)
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
      let Some(bytes) = snapshot_bytes(&doc)? else {
        return Ok(None);
      };
      find_in_named_snapshot(&bytes, key)
    }
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
    let row_values = if is_row_tree(tree) {
      Some(decode_row_bytes(&value)?)
    } else {
      None
    };
    let new_snapshot = if row_values.is_some() {
      Vec::new()
    } else {
      let existing_bytes = match &existing {
        Some(doc) => snapshot_bytes(doc)?,
        None => None,
      };
      set_in_named_snapshot(existing_bytes.as_deref(), key, value)?
    };

    let new_doc = if let Some(doc) = existing {
      let mut updated = doc;
      if let Some(row) = row_values.as_ref() {
        set_row_columns(&mut updated, row)?;
      } else {
        updated
          .put(
            &automerge::ROOT,
            "snapshot",
            encode_snapshot_base64(&new_snapshot),
          )
          .map_err(BTreeError::other)?;
      }
      if is_row_tree(tree) {
        set_doc_tree(updated, tree)?
      } else {
        updated
      }
    } else {
      let mut created = if row_values.is_some() {
        AutoCommit::new()
      } else {
        snapshot_doc(&new_snapshot)?
      };
      if let Some(row) = row_values.as_ref() {
        set_row_columns(&mut created, row)?;
      }
      if is_row_tree(tree) {
        set_doc_tree(created, tree)?
      } else {
        created
      }
    };
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
    let removed = if is_row_tree(tree) {
      read_row_columns(&existing)?.and_then(|row| {
        if row.is_empty() {
          None
        } else {
          Some(encode_row_bytes(&row))
        }
      })
    } else {
      let bytes = snapshot_bytes(&existing)?;
      if let Some(ref b) = bytes {
        find_in_named_snapshot(b, key)?
      } else {
        None
      }
    };
    if removed.is_some() {
      let mut tombstone = existing;
      if is_row_tree(tree) {
        set_row_columns(&mut tombstone, &[])?;
      } else {
        // Tombstone: update the snapshot to empty so the delete is recorded as
        // an Automerge operation and propagates causally to peers on sync.
        tombstone
          .put(&automerge::ROOT, "snapshot", encode_snapshot_base64(&[]))
          .map_err(BTreeError::other)?;
      }
      if is_row_tree(tree) {
        tombstone = set_doc_tree(tombstone, tree)?;
      }
      self.inner.insert(doc_id, tombstone).await?;
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
    let (tree_start, tree_end) = if row_tree {
      (Uuid::from_u128(0), Uuid::from_u128(u128::MAX))
    } else {
      tree_uuid_range(tree)
    };
    let tree_name = tree.to_string();
    let inner = &self.inner;
    stream! {
      let doc_stream = inner.range(tree_start..=tree_end);
      pin_mut!(doc_stream);

      let mut entries: alloc::vec::Vec<(EngineKey, Vec<u8>)> = alloc::vec::Vec::new();
      while let Some(item) = doc_stream.next().await {
        let (doc_id, doc) = item?;
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
          entries.push((row_key_from_doc_id(doc_id), encode_row_bytes(&row)));
        } else {
          let bytes = match snapshot_bytes(&doc) {
            Ok(Some(b)) => b,
            Ok(None) => continue,
            Err(e) => { yield Err(e); return; }
          };
          match parse_named_snapshot(&bytes) {
            Ok(pairs) => entries.extend(pairs),
            Err(e) => { yield Err(e); return; }
          }
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
