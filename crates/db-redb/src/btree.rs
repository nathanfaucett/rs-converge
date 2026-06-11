use std::{borrow::Borrow, marker::PhantomData, ops::RangeBounds, sync::Arc};

use async_stream::stream;
use futures::Stream;
use redb::{Database, ReadableDatabase};

use db_btree::{BTree, BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeValue};
use db_core::MaybeSend;

use crate::{
  Codec, RedbBTreeTransaction,
  util::{rx_range, table_definition, tx_read},
};

#[derive(Clone)]
pub struct RedbBTree<K, V> {
  db: Arc<Database>,
  name: String,
  _phantom_marker: PhantomData<(K, V)>,
}

impl<K, V> RedbBTree<K, V> {
  pub fn new(db: Arc<Database>, name: impl Into<String>) -> Self {
    Self {
      db,
      name: name.into(),
      _phantom_marker: PhantomData,
    }
  }
}

impl<K, V> BTreeReadExecutor<K, V> for RedbBTree<K, V>
where
  K: BTreeKey + Codec,
  V: BTreeValue + Codec,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let db = self.db.begin_read().map_err(BTreeError::custom)?;
    let table = db
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_read::<K, V, _>(&table, key.borrow())
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    stream! {
        let db = self.db.begin_read().map_err(BTreeError::custom)?;
        let table = db
          .open_table(table_definition(&self.name))
          .map_err(BTreeError::custom)?;

        let results = rx_range(&table, range)?;

        for result in results {
            yield Ok(result);
        }
    }
  }
}

impl<K, V> BTree<K, V> for RedbBTree<K, V>
where
  K: BTreeKey + Codec,
  V: BTreeValue + Codec,
{
  type Transaction = RedbBTreeTransaction<K, V>;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let tx = self.db.begin_write().map_err(BTreeError::custom)?;

    Ok(RedbBTreeTransaction::new(tx, &self.name))
  }
}

#[cfg(test)]
mod test {
  use futures::executor::block_on;

  use db_btree::{BTree, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor};

  use super::*;

  fn tmp_path() -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let name = format!("test-{}", uuid::Uuid::now_v7());
    path.push(format!("db-redb-{}.db", name));
    path
  }

  #[test]
  fn it_works() {
    block_on(async {
      let db = Database::create(tmp_path()).expect("failed to create database");
      let tree = RedbBTree::<String, String>::new(Arc::new(db), "test_tree");

      let mut tx = tree
        .transaction()
        .await
        .expect("failed to create transaction");
      tx.insert("key1".to_string(), "value1".to_string())
        .await
        .expect("failed to insert");
      tx.commit().await.expect("failed to commit");

      let got = tree
        .get("key1".to_string())
        .await
        .expect("failed to get")
        .expect("missing key");

      assert_eq!(got, "value1".to_string());
    });
  }
}
