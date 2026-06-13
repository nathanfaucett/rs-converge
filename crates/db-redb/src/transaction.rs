use std::{marker::PhantomData, ops::RangeBounds};

use async_stream::stream;
use futures::Stream;
use redb::{Key, Value, WriteTransaction};

use db_btree::{
  BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeValue,
  BTreeWriteExecutor,
};

use crate::util::{table_definition, tx_insert, tx_range, tx_read, tx_remove, tx_update};

pub struct RedbBTreeTransaction<K, V> {
  tx: WriteTransaction,
  name: String,
  _phantom_marker: PhantomData<(K, V)>,
}

impl<K, V> RedbBTreeTransaction<K, V> {
  pub fn new(tx: WriteTransaction, name: impl Into<String>) -> Self {
    Self {
      tx,
      name: name.into(),
      _phantom_marker: PhantomData,
    }
  }
}

impl<K, V> BTreeReadExecutor<K, V> for RedbBTreeTransaction<K, V>
where
  K: BTreeKey + Key,
  V: BTreeValue + Value,
{
  async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
    let table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_read::<K, V, _, _>(&table, key)
  }

  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
  where
    R: RangeBounds<K>,
  {
    stream! {
        let table = self
          .tx
          .open_table(table_definition(&self.name))
          .map_err(BTreeError::custom)?;

        let results = tx_range(&table, range)?;

        for result in results {
            yield Ok(result);
        }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for RedbBTreeTransaction<K, V>
where
  K: BTreeKey + Key,
  V: BTreeValue + Value,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_insert(&mut table, key, value)
  }

  async fn update<F>(&mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut V) -> BTreeResult<()>,
  {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_update::<K, V, _>(&mut table, key, |current| {
      if let Some(mut value) = current {
        update_fn(&mut value)?;
        Ok(Some(value))
      } else {
        Ok(None)
      }
    })
  }
  async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_remove::<K, V, _>(&mut table, key)
  }
}

impl<K, V> BTreeTransaction<K, V> for RedbBTreeTransaction<K, V>
where
  K: BTreeKey + Key,
  V: BTreeValue + Value,
{
  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    let RedbBTreeTransaction { tx, .. } = self;

    tx.commit().map_err(BTreeError::custom)?;

    Ok(())
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    Ok(())
  }
}
