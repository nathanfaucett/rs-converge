use std::{borrow::Borrow, collections::BTreeMap, sync::Arc};

use async_lock::RwLock;
use async_stream::stream;
use automerge::AutoCommit;
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};
use db_types::{StoreKey, StoreValue};

mod doc_payload;
mod named;
mod named_routing;

use doc_payload::{
  is_direct_document, is_tombstone, read_direct_document_entry, read_row_columns,
  read_store_key_metadata, set_doc_value, set_row_columns, set_store_key_metadata, set_tombstone,
};
pub use named::{AutomergeNamedTransaction, AutomergeNamedTree, AutomergeNamedTreeTransaction};

/// Automerge-backed engine store: each logical collection (table/index/schema)
/// is represented by an Automerge `AutoCommit` document stored in the
/// `AutomergeBTree<B>` backend. Engine-level keys/values are encoded as direct
/// Automerge document fields and decoded on read.
#[derive(Clone)]
pub struct AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub automerge: Arc<RwLock<AutomergeBTree<B>>>,
}

impl<B> AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new_with_backend(backend: B) -> Self {
    let automerge = Arc::new(RwLock::new(AutomergeBTree::new(backend)));
    Self { automerge }
  }
}

pub async fn collect_documents<B>(
  store: &AutomergeEngineStore<B>,
) -> Result<BTreeMap<Uuid, AutoCommit>, BTreeError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let guard = store.automerge.read().await;
  let stream = guard.range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
  futures::pin_mut!(stream);

  let mut docs = BTreeMap::new();
  while let Some(item) = stream.next().await {
    let (doc_id, doc) = match item {
      Ok(pair) => pair,
      Err(_) if docs.is_empty() => return Ok(docs),
      Err(err) => return Err(err),
    };
    docs.insert(doc_id, doc);
  }

  Ok(docs)
}

pub async fn apply_documents<B>(
  store: &AutomergeEngineStore<B>,
  docs: &BTreeMap<Uuid, AutoCommit>,
) -> Result<(), BTreeError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let guard = store.automerge.read().await;
  let mut tx = guard.transaction().await?;

  for (doc_id, doc) in docs {
    tx.insert(*doc_id, doc.clone()).await?;
  }

  tx.commit().await?;
  Ok(())
}

pub async fn sync_automerge_stores<B>(
  left: &AutomergeEngineStore<B>,
  right: &AutomergeEngineStore<B>,
) -> Result<(), BTreeError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let left_docs = collect_documents(left).await?;
  let right_docs = collect_documents(right).await?;

  let mut merged_docs = left_docs;
  for (doc_id, mut right_doc) in right_docs {
    if let Some(left_doc) = merged_docs.get_mut(&doc_id) {
      left_doc.merge(&mut right_doc).map_err(BTreeError::other)?;
    } else {
      merged_docs.insert(doc_id, right_doc);
    }
  }

  apply_documents(left, &merged_docs).await?;
  apply_documents(right, &merged_docs).await?;

  Ok(())
}

pub fn automerge_metrics(docs: &BTreeMap<Uuid, AutoCommit>) -> (usize, usize) {
  let document_count = docs.len();
  let total_document_bytes = docs
    .values()
    .map(|doc| {
      let mut copy = doc.clone();
      copy.save().len()
    })
    .sum();
  (document_count, total_document_bytes)
}

pub(crate) fn make_doc_id(prefix: &str, name: &str) -> Uuid {
  let mut hasher = Sha256::new();
  hasher.update(prefix.as_bytes());
  hasher.update(name.as_bytes());
  let digest = hasher.finalize();
  Uuid::from_slice(&digest[..16]).unwrap_or(Uuid::nil())
}

fn doc_id_for_key(key: &StoreKey) -> Uuid {
  match key {
    StoreKey::TableRow {
      table_name,
      primary_key,
    } => {
      let mut hasher = Sha256::new();
      hasher.update(b"table:row:");
      hasher.update(table_name.as_bytes());
      hasher.update(primary_key); // EngineKey is already bytes
      let digest = hasher.finalize();
      Uuid::from_slice(&digest[..16]).unwrap_or(Uuid::nil())
    }
    StoreKey::IndexEntry {
      index_name,
      index_key,
      row_pk,
    } => {
      let mut hasher = Sha256::new();
      hasher.update(b"index:entry:");
      hasher.update(index_name.as_bytes());
      hasher.update(index_key); // EngineKey is already bytes
      hasher.update(row_pk); // EngineKey is already bytes
      let digest = hasher.finalize();
      Uuid::from_slice(&digest[..16]).unwrap_or(Uuid::nil())
    }
    StoreKey::TableSchema { table_name } => make_doc_id("table:schema:", table_name),
    StoreKey::IndexSchema { index_name } => make_doc_id("index:schema:", index_name),
  }
}

fn is_table_row_key(key: &StoreKey) -> bool {
  matches!(key, StoreKey::TableRow { .. })
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

fn doc_with_row(
  existing: Option<AutoCommit>,
  key: &StoreKey,
  row: &[db_types::EngineValue],
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing.unwrap_or_default();
  set_store_key_metadata(&mut doc, key)?;
  set_row_columns(&mut doc, row)?;
  Ok(doc)
}

fn doc_with_value(
  existing: Option<AutoCommit>,
  key: &StoreKey,
  value: &StoreValue,
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing.unwrap_or_default();
  set_store_key_metadata(&mut doc, key)?;
  set_doc_value(&mut doc, value)?;
  Ok(doc)
}

fn doc_with_tombstone(
  existing: Option<AutoCommit>,
  key: &StoreKey,
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing.unwrap_or_default();
  set_store_key_metadata(&mut doc, key)?;
  set_tombstone(&mut doc)?;
  Ok(doc)
}

pub struct AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  inner: <AutomergeBTree<B> as BTree<Uuid, AutoCommit>>::Transaction,
}

impl<B> AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn build_removed_row_doc(
    mut doc: AutoCommit,
    key: &StoreKey,
  ) -> Result<(AutoCommit, Option<StoreValue>), BTreeError> {
    let prev = read_row_columns(&doc)?;
    if prev.as_ref().is_none_or(|row| row.is_empty()) {
      return Ok((doc, None));
    }

    set_store_key_metadata(&mut doc, key)?;
    set_row_columns(&mut doc, &[])?;
    Ok((doc, prev.map(StoreValue::Row)))
  }

  fn build_removed_direct_doc(
    doc: AutoCommit,
    key: &StoreKey,
  ) -> Result<(AutoCommit, Option<StoreValue>), BTreeError> {
    if is_tombstone(&doc)? {
      return Ok((doc, None));
    }

    let prev = match read_direct_document_entry(&doc)? {
      Some(value) => value,
      None => return Ok((doc, None)),
    };

    let next_doc = doc_with_tombstone(Some(doc), key)?;
    Ok((next_doc, Some(prev)))
  }
}

impl<B> BTreeExecutor<StoreKey, StoreValue> for AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  #[allow(clippy::manual_async_fn)]
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let key = key.borrow().clone();
    let inner = &self.inner;
    async move {
      let doc_id = doc_id_for_key(&key);
      match inner.get(&doc_id).await? {
        None => Ok(None),
        Some(doc) => {
          if is_table_row_key(&key) {
            return Ok(read_row_columns(&doc)?.and_then(|row| {
              if row.is_empty() {
                None
              } else {
                Some(StoreValue::Row(row))
              }
            }));
          }

          if is_tombstone(&doc)? {
            return Ok(None);
          }

          if let Some(value) = read_direct_document_entry(&doc)? {
            return Ok(Some(value));
          }

          Ok(None)
        }
      }
    }
  }

  #[allow(clippy::manual_async_fn)]
  fn insert<'a>(
    &'a mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
  {
    let inner = &mut self.inner;
    async move {
      let doc_id = doc_id_for_key(&key);
      let existing = inner.get(&doc_id).await?;
      let next_doc = if is_table_row_key(&key) {
        match &value {
          StoreValue::Row(row) => doc_with_row(existing, &key, row)?,
          _ => return Err(BTreeError::UnsupportedOperation),
        }
      } else {
        doc_with_value(existing, &key, &value)?
      };
      inner.insert(doc_id, next_doc).await
    }
  }

  #[allow(clippy::manual_async_fn)]
  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let key = key.borrow().clone();
    let inner = &mut self.inner;
    async move {
      let doc_id = doc_id_for_key(&key);
      let existing = inner.get(&doc_id).await?;
      let Some(doc) = existing else {
        return Ok(None);
      };

      let (next_doc, result) = if is_table_row_key(&key) {
        Self::build_removed_row_doc(doc, &key)?
      } else {
        Self::build_removed_direct_doc(doc, &key)?
      };

      inner.insert(doc_id, next_doc).await?;
      Ok(result)
    }
  }

  #[allow(clippy::collapsible_if)]
  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), BTreeError>> + Send + 'a
  where
    StoreKey: Ord + Clone,
    R: core::ops::RangeBounds<StoreKey> + Send + 'a,
  {
    let doc_stream = self
      .inner
      .range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
    stream! {
      pin_mut!(doc_stream);
      while let Some(item) = doc_stream.next().await {
        let (_doc_id, doc) = item?;
        if is_direct_document(&doc)? {
          if let Some(value) = read_direct_document_entry(&doc)? {
            if let Some(key) = read_store_key_metadata(&doc)? {
              if key_in_range(&key, &range) {
                yield Ok((key, value));
              }
            }
          }
        }
      }
    }
  }
}

impl<B> BTreeTransaction<StoreKey, StoreValue> for AutomergeEngineStoreTransaction<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  #[allow(clippy::manual_async_fn)]
  fn commit(self) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send {
    async move { self.inner.commit().await }
  }

  #[allow(clippy::manual_async_fn)]
  fn rollback(self) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send {
    async move { self.inner.rollback().await }
  }
}

impl<B> BTreeExecutor<StoreKey, StoreValue> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn get<'a, Q>(
    &'a self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let key = key.borrow().clone();
    async move {
      let tx = self.transaction().await?;
      tx.get(key).await
    }
  }

  #[allow(clippy::manual_async_fn)]
  fn insert<'a>(
    &'a mut self,
    key: StoreKey,
    value: StoreValue,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
  {
    async move {
      let mut tx = self.transaction().await?;
      tx.insert(key, value).await?;
      tx.commit().await
    }
  }

  #[allow(clippy::manual_async_fn)]
  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl core::future::Future<Output = Result<Option<StoreValue>, BTreeError>> + Send + 'a
  where
    StoreKey: Ord,
    Q: Borrow<StoreKey> + Send + 'a,
  {
    let key = key.borrow().clone();
    async move {
      let mut tx = self.transaction().await?;
      let result = tx.remove(&key).await?;
      tx.commit().await?;
      Ok(result)
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl futures::Stream<Item = Result<(StoreKey, StoreValue), BTreeError>> + Send + 'a
  where
    StoreKey: Ord + Clone,
    R: core::ops::RangeBounds<StoreKey> + Send + 'a,
  {
    stream! {
      let tx = self.transaction().await?;
      let rows = tx.range(range);
      pin_mut!(rows);
      while let Some(item) = rows.next().await {
        yield item;
      }
    }
  }
}

impl<B> BTree<StoreKey, StoreValue> for AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeEngineStoreTransaction<B>;

  fn transaction<'a>(
    &'a self,
  ) -> impl core::future::Future<Output = Result<Self::Transaction, BTreeError>> + Send + 'a {
    let automerge = self.automerge.clone();
    async move {
      let guard = automerge.read().await;
      let inner_tx = guard.transaction().await?;
      Ok(AutomergeEngineStoreTransaction { inner: inner_tx })
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use automerge::{ReadDoc, ScalarValue, Value};
  use db_core::{NamedTreeProvider, NamedTreeTransaction, block_on};
  use db_in_memory::InMemoryBTree;
  use db_types::key_encoding::{DefaultEncoding, KeyEncoding, RowEncoding};
  use db_types::{
    EngineKey, EngineValue, StoreKey, StoreValue,
    schema::{IndexSchema, TableSchema},
  };

  fn store() -> AutomergeEngineStore<InMemoryBTree<DocumentChangeKey, AutomergeEntry>> {
    AutomergeEngineStore::new_with_backend(InMemoryBTree::new())
  }

  fn key(values: Vec<EngineValue>) -> EngineKey {
    <DefaultEncoding as KeyEncoding>::encode_values(&values)
  }

  fn row(values: Vec<EngineValue>) -> Vec<u8> {
    <DefaultEncoding as RowEncoding>::encode_values(&values)
  }

  #[test]
  fn named_transaction_commits_isolated_trees() {
    block_on(async {
      let store = store();
      let mut tx = store.begin_transaction().await.expect("begin");
      let key = key(vec![EngineValue::Integer(1)]);
      let row_bytes = row(vec![EngineValue::Text("first".into())]);

      tx.insert("first", key.clone(), row_bytes.clone())
        .await
        .expect("insert first");
      tx.insert(
        "second",
        key.clone(),
        row(vec![EngineValue::Text("second".into())]),
      )
      .await
      .expect("insert second");
      tx.commit().await.expect("commit");

      let mut read = store.begin_transaction().await.expect("read");
      assert_eq!(
        read.get("first", &key).await.expect("get first"),
        Some(row_bytes)
      );
      assert_eq!(
        read.get("second", &key).await.expect("get second"),
        Some(row(vec![EngineValue::Text("second".into())]))
      );
    });
  }

  #[test]
  fn row_trees_with_same_uuid_key_do_not_collide() {
    block_on(async {
      let store = store();
      let key = key(vec![EngineValue::Uuid(
        *uuid::Uuid::from_u128(1).as_bytes(),
      )]);
      let alice = row(vec![EngineValue::Text("alice".into())]);
      let bob = row(vec![EngineValue::Text("bob".into())]);

      {
        let mut tx = store.begin_transaction().await.expect("begin users");
        tx.insert("t:users", key.clone(), alice.clone())
          .await
          .expect("insert users");
        tx.commit().await.expect("commit users");
      }

      {
        let mut tx = store.begin_transaction().await.expect("begin orders");
        tx.insert("t:orders", key.clone(), bob.clone())
          .await
          .expect("insert orders");
        tx.commit().await.expect("commit orders");
      }

      let mut read = store.begin_transaction().await.expect("read");
      assert_eq!(
        read.get("t:users", &key).await.expect("get user"),
        Some(alice)
      );
      assert_eq!(
        read.get("t:orders", &key).await.expect("get order"),
        Some(bob)
      );
    });
  }

  #[test]
  fn named_transaction_range_returns_all_rows() {
    block_on(async {
      let store = store();
      let mut tx = store.begin_transaction().await.expect("begin");
      let key1 = key(vec![EngineValue::Integer(1)]);
      let row1 = row(vec![EngineValue::Text("alice".into())]);
      let key2 = key(vec![EngineValue::Integer(2)]);
      let row2 = row(vec![EngineValue::Text("bob".into())]);

      tx.insert("users", key1.clone(), row1.clone())
        .await
        .expect("insert alice");
      tx.insert("users", key2.clone(), row2.clone())
        .await
        .expect("insert bob");
      tx.commit().await.expect("commit");

      let read = store.begin_transaction().await.expect("read");
      let mut rows = Vec::new();
      let stream = read.range("users", ..);
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (key, value) = item.expect("range failed");
        rows.push((key, value));
      }

      assert_eq!(rows.len(), 2);
      assert_eq!(rows[0].0, key1);
      assert_eq!(rows[0].1, row1);
      assert_eq!(rows[1].0, key2);
      assert_eq!(rows[1].1, row2);
    });
  }

  #[test]
  fn named_range_after_two_separate_committed_transactions() {
    block_on(async {
      let store = store();

      let key1 = key(vec![EngineValue::Integer(1)]);
      let row1 = row(vec![EngineValue::Text("alice".into())]);
      let key2 = key(vec![EngineValue::Integer(2)]);
      let row2 = row(vec![EngineValue::Text("bob".into())]);

      {
        let mut tx = store.begin_transaction().await.expect("begin tx1");
        tx.insert("schemas", key1.clone(), row1.clone())
          .await
          .expect("insert 1");
        tx.commit().await.expect("commit tx1");
      }
      {
        let mut tx = store.begin_transaction().await.expect("begin tx2");
        tx.insert("schemas", key2.clone(), row2.clone())
          .await
          .expect("insert 2");
        tx.commit().await.expect("commit tx2");
      }

      let read = store.begin_transaction().await.expect("begin read");
      let mut rows = Vec::new();
      let stream = read.range("schemas", ..);
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        rows.push(item.expect("range item"));
      }

      assert_eq!(rows.len(), 2, "expected 2 rows, got: {:?}", rows);
    });
  }

  #[test]
  fn two_separate_tables_load_catalog() {
    block_on(async {
      let store = store();

      // First schema
      let key1 = key(vec![EngineValue::Text("users".into())]);
      let val1 = row(vec![EngineValue::Blob(b"users_schema_bytes".to_vec())]);

      {
        let mut tx = store.begin_transaction().await.expect("tx1");
        tx.insert("sys:table_schemas", key1.clone(), val1.clone())
          .await
          .expect("insert schema 1");
        tx.commit().await.expect("commit tx1");
      }

      // Range should return 1 row
      {
        let read = store.begin_transaction().await.expect("read tx");
        let mut rows = Vec::new();
        let stream = read.range("sys:table_schemas", ..);
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
          rows.push(item.expect("range item"));
        }
        assert_eq!(rows.len(), 1, "expected 1 schema row, got: {:?}", rows);
        assert_eq!(rows[0].0, key1);
        assert_eq!(rows[0].1, val1);
      }
    });
  }

  #[test]
  fn direct_schema_and_index_documents_roundtrip() {
    block_on(async {
      let store = store();

      let table_schema_key = StoreKey::TableSchema {
        table_name: "users".to_string(),
      };
      let table_schema_value = StoreValue::TableSchema(TableSchema {
        name: "users".to_string(),
        columns: Vec::new(),
        primary_key: Vec::new(),
      });

      let index_schema_key = StoreKey::IndexSchema {
        index_name: "users_by_name".to_string(),
      };
      let index_schema_value = StoreValue::IndexSchema(IndexSchema {
        name: "users_by_name".to_string(),
        table_name: "users".to_string(),
        column_indices: vec![0],
        unique: true,
      });

      let index_entry_key = StoreKey::IndexEntry {
        index_name: "users_by_name".to_string(),
        index_key: key(vec![EngineValue::Text("alice".to_string())]),
        row_pk: key(vec![EngineValue::Integer(1)]),
      };
      let index_entry_value = StoreValue::IndexEntry;

      let mut tx = store.transaction().await.expect("begin tx");
      tx.insert(table_schema_key.clone(), table_schema_value.clone())
        .await
        .expect("insert table schema");
      tx.insert(index_schema_key.clone(), index_schema_value.clone())
        .await
        .expect("insert index schema");
      tx.insert(index_entry_key.clone(), index_entry_value.clone())
        .await
        .expect("insert index entry");
      tx.commit().await.expect("commit tx");

      let read = store.transaction().await.expect("read tx");
      assert_eq!(
        read.get(&table_schema_key).await.expect("get table schema"),
        Some(table_schema_value)
      );
      assert_eq!(
        read.get(&index_schema_key).await.expect("get index schema"),
        Some(index_schema_value)
      );
      assert_eq!(
        read.get(&index_entry_key).await.expect("get index entry"),
        Some(index_entry_value)
      );
    });
  }

  #[test]
  fn non_row_named_tree_stores_direct_automerge_document() {
    block_on(async {
      let store = store();
      let key1 = key(vec![EngineValue::Integer(1)]);
      let row1 = row(vec![EngineValue::Text("alice".into())]);

      let mut tx = store.begin_transaction().await.expect("begin");
      tx.insert("users", key1.clone(), row1.clone())
        .await
        .expect("insert");
      tx.commit().await.expect("commit");

      let docs = collect_documents(&store).await.expect("collect docs");
      let doc_id = super::named_routing::doc_id_for_tree_key("users", &key1).expect("doc id");
      let doc = docs.get(&doc_id).expect("doc exists");

      assert!(doc.get(&automerge::ROOT, "snapshot").unwrap().is_none());
      assert!(doc.get(&automerge::ROOT, "named_key").unwrap().is_some());
      assert!(doc.get(&automerge::ROOT, "value").unwrap().is_some());
      assert!(doc.get(&automerge::ROOT, "deleted").unwrap().is_none());
    });
  }

  #[test]
  fn non_row_named_tree_tombstone_records_deleted_document() {
    block_on(async {
      let store = store();
      let key1 = key(vec![EngineValue::Integer(1)]);
      let row1 = row(vec![EngineValue::Text("alice".into())]);

      {
        let mut tx = store.begin_transaction().await.expect("begin");
        tx.insert("users", key1.clone(), row1.clone())
          .await
          .expect("insert");
        tx.commit().await.expect("commit");
      }

      {
        let mut tx = store.begin_transaction().await.expect("begin remove");
        let removed = tx.remove("users", &key1).await.expect("remove");
        assert_eq!(removed, Some(row1));
        tx.commit().await.expect("commit remove");
      }

      let mut read = store.begin_transaction().await.expect("read");
      assert_eq!(
        read.get("users", &key1).await.expect("get after remove"),
        None
      );

      let docs = collect_documents(&store).await.expect("collect docs");
      let doc_id = super::named_routing::doc_id_for_tree_key("users", &key1).expect("doc id");
      let doc = docs.get(&doc_id).expect("doc exists");

      assert!(doc.get(&automerge::ROOT, "snapshot").unwrap().is_none());
      assert!(doc.get(&automerge::ROOT, "named_key").unwrap().is_some());
      assert_eq!(doc.get(&automerge::ROOT, "value").unwrap(), None);
      assert!(
        matches!(doc.get(&automerge::ROOT, "deleted").unwrap(), Some((Value::Scalar(scalar), _)) if scalar.as_ref() == &ScalarValue::Boolean(true))
      );
    });
  }
}
