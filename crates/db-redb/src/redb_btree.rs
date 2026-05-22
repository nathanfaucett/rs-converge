use std::{
  borrow::Borrow, fmt::Debug, marker::PhantomData, ops::RangeBounds, path::Path, sync::Arc,
};

use async_stream::stream;
use db_core::{
  BTree, BTreeError, BTreeExecutor, BTreeTransaction, FastKeyCodec, KeyCodec, KeyScratch,
  MaybeSend, ValueCodec,
};
use futures::Stream;
use redb::{
  Database, Key, ReadableDatabase, ReadableTable, TableDefinition, TypeName, Value,
  WriteTransaction,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct RedbKeyCodec;

#[derive(Debug, Clone, Copy, Default)]
pub struct RedbValueCodec;

#[derive(Clone, Copy)]
pub(crate) struct EncodedKey<K, C>(PhantomData<(K, C)>);

#[derive(Clone, Copy)]
pub(crate) struct EncodedValue<V, C>(PhantomData<(V, C)>);

macro_rules! impl_encoded_debug {
  ($encoded:ident<$value:ident>, $name:literal) => {
    impl<$value, C> Debug for $encoded<$value, C> {
      fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str($name)
      }
    }
  };
}

impl_encoded_debug!(EncodedKey<K>, "EncodedKey");
impl_encoded_debug!(EncodedValue<V>, "EncodedValue");

impl<T> ValueCodec<T> for RedbKeyCodec
where
  T: Debug + Key + 'static,
  for<'a> T: Value<SelfType<'a> = T>,
{
  type Bytes<'a>
    = <T as Value>::AsBytes<'a>
  where
    Self: 'a,
    T: 'a;

  fn fixed_width() -> Option<usize> {
    T::fixed_width()
  }

  fn encode<'a>(value: &'a T) -> Self::Bytes<'a> {
    T::as_bytes(value)
  }

  fn decode(data: &[u8]) -> T {
    T::from_bytes(data)
  }
}

impl<T> KeyCodec<T> for RedbKeyCodec
where
  T: Debug + Key + 'static,
  for<'a> T: Value<SelfType<'a> = T>,
{
  fn compare(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    T::compare(left, right)
  }
}

impl<T> FastKeyCodec<T> for RedbKeyCodec
where
  T: Debug + Key + 'static,
  for<'a> T: Value<SelfType<'a> = T>,
{
  fn encode_into(&self, value: &T, scratch: &mut KeyScratch) {
    let bytes = <RedbKeyCodec as ValueCodec<T>>::encode(value);
    scratch.buf.extend_from_slice(bytes.as_ref());
  }

  fn compare_encoded(&self, left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    <RedbKeyCodec as KeyCodec<T>>::compare(left, right)
  }
}

impl<T> ValueCodec<T> for RedbValueCodec
where
  T: Debug + Value + 'static,
  for<'a> T: Value<SelfType<'a> = T>,
{
  type Bytes<'a>
    = <T as Value>::AsBytes<'a>
  where
    Self: 'a,
    T: 'a;

  fn fixed_width() -> Option<usize> {
    T::fixed_width()
  }

  fn encode<'a>(value: &'a T) -> Self::Bytes<'a> {
    T::as_bytes(value)
  }

  fn decode(data: &[u8]) -> T {
    T::from_bytes(data)
  }
}

macro_rules! impl_encoded_value {
  ($encoded:ident<$value:ident>, $codec:ident) => {
    impl<$value, C> Value for $encoded<$value, C>
    where
      $value: Debug + 'static,
      C: $codec<$value>,
    {
      type SelfType<'a>
        = $value
      where
        Self: 'a;
      type AsBytes<'a>
        = C::Bytes<'a>
      where
        Self: 'a;

      fn fixed_width() -> Option<usize> {
        C::fixed_width()
      }

      fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
      where
        Self: 'a,
      {
        C::decode_checked(data).unwrap_or_else(|e| panic!("decode failed: {}", e))
      }

      fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
      where
        Self: 'b,
      {
        C::encode(value)
      }

      fn type_name() -> TypeName {
        TypeName::new(core::any::type_name::<Self>())
      }
    }
  };
}

impl_encoded_value!(EncodedKey<K>, KeyCodec);
impl_encoded_value!(EncodedValue<V>, ValueCodec);

impl<K, C> Key for EncodedKey<K, C>
where
  K: Debug + 'static,
  C: KeyCodec<K>,
{
  fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
    C::compare(data1, data2)
  }
}

#[derive(Clone)]
pub struct REDBBTree<K, V, KC = RedbKeyCodec, VC = RedbValueCodec>
where
  K: Debug + 'static,
  V: Debug + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  db: Arc<Database>,
  table_definition: TableDefinition<'static, EncodedKey<K, KC>, EncodedValue<V, VC>>,
}

pub struct REDBBTreeTransaction<K, V, KC = RedbKeyCodec, VC = RedbValueCodec>
where
  K: Debug + 'static,
  V: Debug + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  write_tx: WriteTransaction,
  table_definition: TableDefinition<'static, EncodedKey<K, KC>, EncodedValue<V, VC>>,
}

impl<K, V> REDBBTree<K, V>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  RedbKeyCodec: KeyCodec<K>,
  RedbValueCodec: ValueCodec<V>,
{
  pub fn open(path: impl AsRef<Path>, table_name: &'static str) -> Result<Self, BTreeError> {
    let db = Database::create(path).map_err(BTreeError::other)?;
    Ok(Self {
      db: Arc::new(db),
      table_definition: TableDefinition::new(table_name),
    })
  }

  pub fn from_database(db: Database, table_name: &'static str) -> Self {
    Self {
      db: Arc::new(db),
      table_definition: TableDefinition::new(table_name),
    }
  }
}

impl<K, V, KC, VC> REDBBTree<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  pub fn open_with_codecs(
    path: impl AsRef<Path>,
    table_name: &'static str,
  ) -> Result<Self, BTreeError> {
    let db = Database::create(path).map_err(BTreeError::other)?;
    Ok(Self {
      db: Arc::new(db),
      table_definition: TableDefinition::new(table_name),
    })
  }

  pub fn from_database_with_codecs(db: Database, table_name: &'static str) -> Self {
    Self {
      db: Arc::new(db),
      table_definition: TableDefinition::new(table_name),
    }
  }

  pub(crate) fn from_arc_with_codecs(db: Arc<Database>, table_name: &'static str) -> Self {
    Self {
      db,
      table_definition: TableDefinition::new(table_name),
    }
  }
}

impl<K, V, KC, VC> BTreeExecutor<K, V> for REDBBTree<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let read_tx = self.db.begin_read().map_err(BTreeError::other)?;
    let table = read_tx
      .open_table(self.table_definition)
      .map_err(BTreeError::other)?;
    let guard = table.get(key).map_err(BTreeError::other)?;
    Ok(guard.map(|g| g.value()))
  }

  async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    let write_tx = self.db.begin_write().map_err(BTreeError::other)?;
    {
      let mut table = write_tx
        .open_table(self.table_definition)
        .map_err(BTreeError::other)?;

      table.insert(key, value).map_err(BTreeError::other)?;
    }

    write_tx.commit().map_err(BTreeError::other)?;

    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let write_tx = self.db.begin_write().map_err(BTreeError::other)?;
    let removed = {
      let mut table = write_tx
        .open_table(self.table_definition)
        .map_err(BTreeError::other)?;
      let guard = table.remove(key).map_err(BTreeError::other)?;
      guard.map(|g| g.value())
    };

    if let Err(e) = write_tx.commit() {
      return Err(BTreeError::other(e));
    }

    Ok(removed)
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = Result<(K, V), BTreeError>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let table_definition = self.table_definition;
    let db = Arc::clone(&self.db);

    stream! {
      let read_tx = match db.begin_read() {
        Ok(tx) => tx,
        Err(e) => { yield Err(BTreeError::other(e)); return; },
      };

      let table = match read_tx.open_table(table_definition) {
        Ok(table) => table,
        Err(e) => { yield Err(BTreeError::other(e)); return; },
      };

      let range_iter = match table.range(range) {
        Ok(range_iter) => range_iter,
        Err(e) => { yield Err(BTreeError::other(e)); return; },
      };

      for entry in range_iter {
        match entry {
          Ok((key, value)) => yield Ok((key.value(), value.value())),
          Err(e) => { yield Err(BTreeError::other(e)); return; },
        }
      }
    }
  }
}

impl<K, V, KC, VC> BTree<K, V> for REDBBTree<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  type Transaction = REDBBTreeTransaction<K, V, KC, VC>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let write_tx = self.db.begin_write().map_err(BTreeError::other)?;
    Ok(REDBBTreeTransaction {
      write_tx,
      table_definition: self.table_definition,
    })
  }
}

// Begin adapter rewrite: explicit StoragePort impl so this adapter is the
// declared port implementation for the engine. The impl is empty since the
// required methods are provided by the existing `BTree` implementation.

impl<K, V, KC, VC> REDBBTreeTransaction<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  fn open_table(
    &self,
  ) -> Result<redb::Table<'_, EncodedKey<K, KC>, EncodedValue<V, VC>>, BTreeError> {
    self
      .write_tx
      .open_table(self.table_definition)
      .map_err(BTreeError::other)
  }
}

impl<K, V, KC, VC> BTreeExecutor<K, V> for REDBBTreeTransaction<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let table = self.open_table()?;
    let guard = table.get(key).ok().flatten();
    Ok(guard.map(|g| g.value()))
  }

  async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    let mut table = self.open_table()?;
    table.insert(key, value).map_err(BTreeError::other)?;
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let mut table = self.open_table()?;
    let guard = table.remove(key).ok().flatten();
    Ok(guard.map(|g| g.value()))
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = Result<(K, V), BTreeError>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    stream! {
      let table = match self.open_table() {
        Ok(table) => table,
        Err(e) => { yield Err(BTreeError::other(e)); return; },
      };

      let range_iter = match table.range(range) {
        Ok(range_iter) => range_iter,
        Err(e) => { yield Err(BTreeError::other(e)); return; },
      };

      for entry in range_iter {
        match entry {
          Ok((key, value)) => yield Ok((key.value(), value.value())),
          Err(e) => { yield Err(BTreeError::other(e)); return; },
        }
      }
    }
  }
}

impl<K, V, KC, VC> BTreeTransaction<K, V> for REDBBTreeTransaction<K, V, KC, VC>
where
  K: Debug + Clone + Ord + Send + Sync + 'static,
  V: Debug + Clone + Send + Sync + 'static,
  KC: KeyCodec<K>,
  VC: ValueCodec<V>,
{
  async fn commit(self) -> Result<(), BTreeError> {
    self.write_tx.commit().map_err(BTreeError::other)
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.write_tx.abort().map_err(BTreeError::other)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::block_on;
  use futures::{StreamExt, pin_mut};
  use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
  };

  fn test_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
      "aicacia_db_redb_{}_{}.db",
      name,
      SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
    ));
    path
  }

  #[test]
  fn round_trip_insert_get_remove() {
    block_on(async {
      let path = test_path("round_trip");
      let _ = fs::remove_file(&path);

      let mut store = REDBBTree::<u64, u64>::open(&path, "test_table").expect("open redb");
      store.insert(1, 100).await.expect("insert");
      assert_eq!(store.get(&1).await.expect("get failed"), Some(100));
      assert_eq!(store.remove(&1).await.expect("remove failed"), Some(100));
      assert_eq!(store.get(&1).await.expect("get failed"), None);

      let _ = fs::remove_file(&path);
    });
  }

  #[test]
  fn transaction_commit_and_rollback() {
    block_on(async {
      let path = test_path("transaction");
      let _ = fs::remove_file(&path);

      let store = REDBBTree::<u64, u64>::open(&path, "test_table").expect("open redb");
      let mut tx = store.transaction().await.expect("begin transaction");
      tx.insert(1, 100).await.expect("insert");
      tx.commit().await.expect("commit");

      assert_eq!(store.get(&1).await.expect("get failed"), Some(100));

      let mut tx = store.transaction().await.expect("begin transaction");
      tx.insert(2, 200).await.expect("insert");
      tx.rollback().await.expect("rollback");

      assert_eq!(store.get(&2).await.expect("get failed"), None);

      let _ = fs::remove_file(&path);
    });
  }

  #[test]
  fn range_returns_ordered_pairs() {
    block_on(async {
      let path = test_path("range");
      let _ = fs::remove_file(&path);

      let mut store = REDBBTree::<u64, u64>::open(&path, "test_table").expect("open redb");
      store.insert(1, 100).await.expect("insert");
      store.insert(2, 200).await.expect("insert");
      store.insert(3, 300).await.expect("insert");

      let mut items = Vec::new();
      let stream = store.range(1..3);
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (key, value) = item.expect("range failed");
        items.push((key, value));
      }

      assert_eq!(items, vec![(1, 100), (2, 200)]);
      let _ = fs::remove_file(&path);
    });
  }
}
