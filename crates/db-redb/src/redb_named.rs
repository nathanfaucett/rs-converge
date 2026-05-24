use std::{
  fmt::Debug,
  marker::PhantomData,
  path::Path,
  sync::{Arc, Mutex},
};

use async_stream::stream;
use db_core::{BTreeError, BTreeResult, KeyCodec, MaybeSend, NamedBTreeMap, ValueCodec};
use db_engine::{EngineNamedTreeBackend, EngineNamedTreeTransaction};
use futures::Stream;
use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};

use crate::redb_btree::{EncodedKey, EncodedValue, REDBBTree, RedbKeyCodec, RedbValueCodec};

type RedbNamedTable<'a, K, V, KC, VC> = redb::Table<'a, EncodedKey<K, KC>, EncodedValue<V, VC>>;

type RedbNamedTableDefinition<'a, K, V, KC, VC> =
  TableDefinition<'a, EncodedKey<K, KC>, EncodedValue<V, VC>>;

/// Interns a string as a `'static` reference.
///
/// The set of distinct names is bounded by the number of tables, indexes, and
/// schema namespaces: small and stable over the process lifetime.
pub fn intern(name: &str) -> &'static str {
  use std::collections::BTreeSet;
  static INTERNED: Mutex<Option<BTreeSet<&'static str>>> = Mutex::new(None);
  let mut guard = INTERNED.lock().unwrap();
  let set = guard.get_or_insert_with(BTreeSet::new);
  if let Some(&existing) = set.get(name) {
    return existing;
  }
  let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
  set.insert(leaked);
  leaked
}

fn interned_names() -> Vec<String> {
  use std::collections::BTreeSet;
  static INTERNED: Mutex<Option<BTreeSet<&'static str>>> = Mutex::new(None);
  let guard = INTERNED.lock().unwrap();
  guard
    .as_ref()
    .map(|set| set.iter().map(|name| (*name).to_string()).collect())
    .unwrap_or_default()
}

/// An atomic transaction spanning multiple named REDB tables.
///
/// REDB's `WriteTransaction` already provides multi-table atomicity natively:
/// committing commits all open tables together.
pub struct REDBNamedTransaction<K, V, KC = RedbKeyCodec, VC = RedbValueCodec>
where
  K: Debug + 'static,
  V: Debug + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  write_tx: WriteTransaction,
  _phantom: PhantomData<(K, V, KC, VC)>,
}

impl<K, V, KC, VC> REDBNamedTransaction<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K> + Default + Send + Sync + 'static,
  VC: ValueCodec<V> + Default + Send + Sync + 'static,
{
  pub fn open_table<'a>(
    &'a self,
    tree: &'a str,
  ) -> Result<RedbNamedTable<'a, K, V, KC, VC>, BTreeError> {
    let name = intern(tree);
    let def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(name);
    self.write_tx.open_table(def).map_err(BTreeError::other)
  }

  pub fn named_commit(self) -> BTreeResult<()> {
    self.write_tx.commit().map_err(BTreeError::other)
  }

  pub fn named_rollback(self) -> BTreeResult<()> {
    self.write_tx.abort().map_err(BTreeError::other)
  }
}

impl<K, V, KC, VC> EngineNamedTreeTransaction<K, V> for REDBNamedTransaction<K, V, KC, VC>
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
    let table = self.open_table(tree)?;
    let guard = table.get(key).map_err(BTreeError::other)?;
    Ok(guard.map(|g| g.value()))
  }

  async fn insert<'a>(&'a mut self, tree: &'a str, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let mut table = self.open_table(tree)?;
    table.insert(key, value).map_err(BTreeError::other)?;
    Ok(())
  }

  async fn remove<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let mut table = self.open_table(tree)?;
    let guard = table.remove(key).map_err(BTreeError::other)?;
    Ok(guard.map(|g| g.value()))
  }

  fn range<'a, R>(&'a self, tree: &'a str, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: core::ops::RangeBounds<K> + MaybeSend + 'a,
  {
    stream! {
      let table = match self.open_table(tree) {
        Ok(t) => t,
        Err(e) => { yield Err(e); return; }
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
      _phantom: PhantomData,
    })
  }

  pub fn from_database(db: Database) -> Self {
    Self {
      db: Arc::new(db),
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
      _phantom: PhantomData,
    })
  }

  pub fn from_database_with_codecs(db: Database) -> Self {
    Self {
      db: Arc::new(db),
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
    let static_name = intern(name);
    let db = Arc::clone(&self.db);
    async move { Ok(REDBBTree::from_arc_with_codecs(db, static_name)) }
  }

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let dest = intern(name);
    let db = Arc::clone(&self.db);
    async move {
      let write_tx = db.begin_write().map_err(BTreeError::other)?;
      let source_def = tree.table_definition();
      let dest_def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(dest);

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
    let table_name = intern(name);
    let db = Arc::clone(&self.db);
    async move {
      let write_tx = db.begin_write().map_err(BTreeError::other)?;
      let def: RedbNamedTableDefinition<K, V, KC, VC> = TableDefinition::new(table_name);
      write_tx.delete_table(def).map_err(BTreeError::other)?;
      write_tx.commit().map_err(BTreeError::other)?;
      Ok(())
    }
  }

  async fn list_names(&self) -> Vec<String> {
    interned_names()
  }
}

impl<K, V, KC, VC> EngineNamedTreeBackend<K, V> for REDBNamedBTree<K, V, KC, VC>
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
    async move {
      let write_tx = db.begin_write().map_err(BTreeError::other)?;
      Ok(REDBNamedTransaction {
        write_tx,
        _phantom: PhantomData,
      })
    }
  }
}
