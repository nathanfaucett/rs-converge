use std::{fmt::Debug, marker::PhantomData, path::Path, sync::Arc};

use async_stream::stream;
use dashmap::DashMap;
use db_core::{BTreeError, BTreeResult, KeyCodec, MaybeSend, NamedBTreeMap, ValueCodec};
use db_engine::{EngineStoreBackend, EngineStoreTransaction};
use futures::Stream;
use redb::{
  Database, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};

use crate::redb_btree::{EncodedKey, EncodedValue, REDBBTree, RedbKeyCodec, RedbValueCodec};

type RedbNamedTableDefinition<'a, K, V, KC, VC> =
  TableDefinition<'a, EncodedKey<K, KC>, EncodedValue<V, VC>>;

struct NameStore {
  names: DashMap<String, Arc<str>>,
}

impl NameStore {
  fn new() -> Self {
    Self {
      names: DashMap::new(),
    }
  }

  fn get(&self, name: &str) -> Arc<str> {
    if let Some(existing) = self.names.get(name) {
      return Arc::clone(existing.value());
    }

    let key = name.to_string();
    let arc_name = Arc::<str>::from(key.clone());
    self.names.insert(key, Arc::clone(&arc_name));
    arc_name
  }

  fn list(&self) -> Vec<String> {
    self
      .names
      .iter()
      .map(|entry| entry.key().to_owned())
      .collect()
  }

  fn remove(&self, name: &str) {
    self.names.remove(name);
  }
}

fn arc_str_to_static(name: &Arc<str>) -> &'static str {
  unsafe { std::mem::transmute::<&str, &'static str>(&**name) }
}

/// An atomic transaction spanning multiple named REDB tables.
///
/// REDB's `WriteTransaction` already provides multi-table atomicity natively:
/// committing commits all open tables together.
enum REDBNamedTransactionKind {
  Read(ReadTransaction),
  Write(WriteTransaction),
}

pub struct REDBNamedTransaction<K, V, KC = RedbKeyCodec, VC = RedbValueCodec>
where
  K: Debug + 'static,
  V: Debug + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  txn: REDBNamedTransactionKind,
  names: Arc<NameStore>,
  _phantom: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> REDBNamedTransaction<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K> + Default + Send + Sync + 'static,
  VC: ValueCodec<V> + Default + Send + Sync + 'static,
{
  pub fn named_commit(self) -> BTreeResult<()> {
    match self.txn {
      REDBNamedTransactionKind::Read(_) => Ok(()),
      REDBNamedTransactionKind::Write(write_tx) => write_tx.commit().map_err(BTreeError::other),
    }
  }

  pub fn named_rollback(self) -> BTreeResult<()> {
    match self.txn {
      REDBNamedTransactionKind::Read(_) => Ok(()),
      REDBNamedTransactionKind::Write(write_tx) => write_tx.abort().map_err(BTreeError::other),
    }
  }
}

impl<K, V, KC, VC> EngineStoreTransaction<K, V> for REDBNamedTransaction<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K> + Default + Send + Sync + 'static,
  VC: ValueCodec<V> + Default + Send + Sync + 'static,
{
  async fn get<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let table_name = self.names.get(tree);
    let name = arc_str_to_static(&table_name);
    let def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(name);
    match &mut self.txn {
      REDBNamedTransactionKind::Read(read_tx) => {
        let table = read_tx.open_table(def).map_err(BTreeError::other)?;
        let guard = table.get(key).map_err(BTreeError::other)?;
        Ok(guard.map(|g| g.value()))
      }
      REDBNamedTransactionKind::Write(write_tx) => {
        let table = write_tx.open_table(def).map_err(BTreeError::other)?;
        let guard = table.get(key).map_err(BTreeError::other)?;
        Ok(guard.map(|g| g.value()))
      }
    }
  }

  async fn insert<'a>(&'a mut self, tree: &'a str, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let table_name = self.names.get(tree);
    let name = arc_str_to_static(&table_name);
    let def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(name);
    match &mut self.txn {
      REDBNamedTransactionKind::Read(_) => Err(BTreeError::UnsupportedOperation),
      REDBNamedTransactionKind::Write(write_tx) => {
        let mut table = write_tx.open_table(def).map_err(BTreeError::other)?;
        table.insert(key, value).map_err(BTreeError::other)?;
        Ok(())
      }
    }
  }

  async fn remove<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let table_name = self.names.get(tree);
    let name = arc_str_to_static(&table_name);
    let def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(name);
    match &mut self.txn {
      REDBNamedTransactionKind::Read(_) => Err(BTreeError::UnsupportedOperation),
      REDBNamedTransactionKind::Write(write_tx) => {
        let mut table = write_tx.open_table(def).map_err(BTreeError::other)?;
        let guard = table.remove(key).map_err(BTreeError::other)?;
        Ok(guard.map(|g| g.value()))
      }
    }
  }

  fn range<'a, R>(&'a self, tree: &'a str, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: core::ops::RangeBounds<K> + MaybeSend + 'a,
  {
    let table_name = self.names.get(tree);
    let name = arc_str_to_static(&table_name);
    let def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(name);
    stream! {
      match &self.txn {
        REDBNamedTransactionKind::Read(read_tx) => {
          let table = match read_tx.open_table(def) {
            Ok(table) => table,
            Err(e) => { yield Err(BTreeError::other(e)); return; }
          };
          let range_iter = match table.range(range) {
            Ok(r) => r,
            Err(e) => { yield Err(BTreeError::other(e)); return; }
          };
          for entry in range_iter {
            match entry {
              Ok((k, v)) => yield Ok((k.value(), v.value())),
              Err(e) => { yield Err(BTreeError::other(e)); return; }
            }
          }
        }
        REDBNamedTransactionKind::Write(write_tx) => {
          let table = match write_tx.open_table(def) {
            Ok(table) => table,
            Err(e) => { yield Err(BTreeError::other(e)); return; }
          };
          let range_iter = match table.range(range) {
            Ok(r) => r,
            Err(e) => { yield Err(BTreeError::other(e)); return; }
          };
          for entry in range_iter {
            match entry {
              Ok((k, v)) => yield Ok((k.value(), v.value())),
              Err(e) => { yield Err(BTreeError::other(e)); return; }
            }
          }
        }
      }
    }
  }

  async fn commit(self) -> BTreeResult<()> {
    self.named_commit()
  }

  async fn rollback(self) -> BTreeResult<()> {
    self.named_rollback()
  }
}

/// A named-tree provider backed by a single REDB database.
///
/// Each distinct name maps to a native REDB table, allowing efficient use of
/// the database's B-tree structure per logical tree.
#[derive(Clone)]
pub struct REDBNamedBTree<K, V, KC = RedbKeyCodec, VC = RedbValueCodec>
where
  K: Debug + 'static,
  V: Debug + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  db: Arc<Database>,
  names: Arc<NameStore>,
  _phantom: PhantomData<(K, V, KC, VC)>,
}

impl<K, V> REDBNamedBTree<K, V>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  RedbKeyCodec: KeyCodec<K>,
  RedbValueCodec: ValueCodec<V>,
{
  pub fn open(path: impl AsRef<Path>) -> Result<Self, BTreeError> {
    let db = Database::create(path).map_err(BTreeError::other)?;
    Ok(Self {
      db: Arc::new(db),
      names: Arc::new(NameStore::new()),
      _phantom: PhantomData,
    })
  }

  pub fn from_database(db: Database) -> Self {
    Self {
      db: Arc::new(db),
      names: Arc::new(NameStore::new()),
      _phantom: PhantomData,
    }
  }
}

impl<K, V, KC, VC> REDBNamedBTree<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  pub fn open_with_codecs(path: impl AsRef<Path>) -> Result<Self, BTreeError> {
    let db = Database::create(path).map_err(BTreeError::other)?;
    Ok(Self {
      db: Arc::new(db),
      names: Arc::new(NameStore::new()),
      _phantom: PhantomData,
    })
  }

  pub fn from_database_with_codecs(db: Database) -> Self {
    Self {
      db: Arc::new(db),
      names: Arc::new(NameStore::new()),
      _phantom: PhantomData,
    }
  }
}

impl<K, V, KC, VC> NamedBTreeMap<K, V> for REDBNamedBTree<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K> + Default + Clone + Send + Sync + 'static,
  VC: ValueCodec<V> + Default + Clone + Send + Sync + 'static,
{
  type Tree = REDBBTree<K, V, KC, VC>;

  fn get_tree<'a>(
    &'a self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<REDBBTree<K, V, KC, VC>>> + 'a {
    let table_name = self.names.get(name);
    let db = Arc::clone(&self.db);
    async move { Ok(REDBBTree::from_arc_with_codecs(db, table_name)) }
  }

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let dest_table_name = self.names.get(name);
    let db = Arc::clone(&self.db);
    async move {
      let write_tx = db.begin_write().map_err(BTreeError::other)?;
      let source_def = tree.table_definition();
      let dest_def: RedbNamedTableDefinition<K, V, KC, VC> =
        TableDefinition::new(arc_str_to_static(&dest_table_name));

      {
        let source = write_tx.open_table(source_def).map_err(BTreeError::other)?;
        let mut dest_table = write_tx.open_table(dest_def).map_err(BTreeError::other)?;

        let range_iter = source.iter().map_err(BTreeError::other)?;
        for entry in range_iter {
          let (key, value) = entry.map_err(BTreeError::other)?;
          dest_table
            .insert(key.value(), value.value())
            .map_err(BTreeError::other)?;
        }
      }

      write_tx.commit().map_err(BTreeError::other)?;
      Ok(())
    }
  }

  fn delete_tree(&self, name: &str) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let table_name_string = name.to_string();
    let table_name = self.names.get(&table_name_string);
    let db = Arc::clone(&self.db);
    let names = Arc::clone(&self.names);
    async move {
      let write_tx = db.begin_write().map_err(BTreeError::other)?;
      let def: RedbNamedTableDefinition<K, V, KC, VC> =
        TableDefinition::new(arc_str_to_static(&table_name));
      write_tx.delete_table(def).map_err(BTreeError::other)?;
      write_tx.commit().map_err(BTreeError::other)?;
      names.remove(&table_name_string);
      Ok(())
    }
  }

  async fn list_names(&self) -> Vec<String> {
    self.names.list()
  }
}

impl<K, V, KC, VC> EngineStoreBackend<K, V> for REDBNamedBTree<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K> + Default + Clone + Send + Sync + 'static,
  VC: ValueCodec<V> + Default + Clone + Send + Sync + 'static,
{
  type Transaction = REDBNamedTransaction<K, V, KC, VC>;

  fn begin_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + '_ {
    let db = Arc::clone(&self.db);
    let names = Arc::clone(&self.names);
    async move {
      let write_tx = db.begin_write().map_err(BTreeError::other)?;
      Ok(REDBNamedTransaction {
        txn: REDBNamedTransactionKind::Write(write_tx),
        names,
        _phantom: PhantomData,
      })
    }
  }

  fn begin_read_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + '_ {
    let db = Arc::clone(&self.db);
    let names = Arc::clone(&self.names);
    async move {
      let read_tx = db.begin_read().map_err(BTreeError::other)?;
      Ok(REDBNamedTransaction {
        txn: REDBNamedTransactionKind::Read(read_tx),
        names,
        _phantom: PhantomData,
      })
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::{BTreeError, block_on};
  use futures::StreamExt;
  use std::path::PathBuf;
  use std::time::{SystemTime, UNIX_EPOCH};

  fn temp_redb_path() -> PathBuf {
    let suffix = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("time went backwards")
      .as_nanos();
    std::env::temp_dir().join(format!("aicacia_redb_test_{suffix}.db"))
  }

  #[test]
  fn read_transaction_is_read_only() {
    let path = temp_redb_path();
    let backend = REDBNamedBTree::<u64, u64>::open(&path).expect("open redb");

    block_on(async {
      let mut tx = backend.begin_transaction().await.expect("begin tx");
      tx.insert("tree", 1, 100).await.expect("insert");
      tx.commit().await.expect("commit");
    });

    block_on(async {
      let mut tx = backend
        .begin_read_transaction()
        .await
        .expect("begin read tx");
      assert_eq!(tx.get("tree", &1).await.expect("get"), Some(100));

      let err = tx
        .insert("tree", 1, 200)
        .await
        .expect_err("write should fail");
      assert!(matches!(err, BTreeError::UnsupportedOperation));

      let stream = tx.range("tree", 0..=1);
      futures::pin_mut!(stream);
      let first = stream
        .next()
        .await
        .expect("stream item")
        .expect("range entry");
      assert_eq!(first, (1, 100));
      assert!(stream.next().await.is_none());
    });

    let _ = std::fs::remove_file(path);
  }

  #[test]
  fn list_names_is_scoped_to_provider() {
    let path1 = temp_redb_path();
    let path2 = temp_redb_path();
    let provider1 = REDBNamedBTree::<u64, u64>::open(&path1).expect("open provider1");
    let provider2 = REDBNamedBTree::<u64, u64>::open(&path2).expect("open provider2");

    block_on(async {
      provider1.get_tree("tree1").await.expect("get tree1");
      provider2.get_tree("tree2").await.expect("get tree2");

      assert_eq!(provider1.list_names().await, vec!["tree1".to_string()]);
      assert_eq!(provider2.list_names().await, vec!["tree2".to_string()]);
    });

    let _ = std::fs::remove_file(path1);
    let _ = std::fs::remove_file(path2);
  }

  #[test]
  fn delete_tree_removes_name_from_list() {
    let path = temp_redb_path();
    let provider = REDBNamedBTree::<u64, u64>::open(&path).expect("open redb");

    block_on(async {
      provider.get_tree("tree").await.expect("get tree");
      assert_eq!(provider.list_names().await, vec!["tree".to_string()]);
      provider.delete_tree("tree").await.expect("delete tree");
      assert_eq!(provider.list_names().await, Vec::<String>::new());
    });

    let _ = std::fs::remove_file(path);
  }
}
