use std::{borrow::Borrow, marker::PhantomData, ops::RangeBounds};

use async_stream::stream;
use futures::Stream;
use redb::WriteTransaction;

use db_btree::{
  BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeValue,
  BTreeWriteExecutor,
};
use db_core::MaybeSend;

use crate::{
  Codec,
  util::{rx_range, table_definition, tx_insert, tx_read, tx_remove, tx_update},
};

pub struct RedbBTreeTransaction<K, V> {
  tx: WriteTransaction,
  name: String,
  _phantom_marker: PhantomData<(K, V)>,
}

impl<K, V> RedbBTreeTransaction<K, V> {
  pub fn new(tx: WriteTransaction, name: impl Into<String>) -> Self {
    Self {
      tx: tx,
      name: name.into(),
      _phantom_marker: PhantomData,
    }
  }
}

impl<K, V> BTreeReadExecutor<K, V> for RedbBTreeTransaction<K, V>
where
  K: BTreeKey + Codec,
  V: BTreeValue + Codec,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_read::<K, V, _>(&table, key.borrow())
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    stream! {
        let table = self
          .tx
          .open_table(table_definition(&self.name))
          .map_err(BTreeError::custom)?;

        let results = rx_range(&table, range)?;

        for result in results {
            yield Ok(result);
        }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for RedbBTreeTransaction<K, V>
where
  K: BTreeKey + Codec,
  V: BTreeValue + Codec,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_insert(&mut table, key, value)
  }

  async fn update<'a, F>(&'a mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
  where
    K: Ord,
    F: FnOnce(&mut V) -> BTreeResult<()> + MaybeSend + 'a,
  {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_update(&mut table, key, |current| {
      if let Some(mut value) = current {
        update_fn(&mut value)?;
        Ok(Some(value))
      } else {
        Ok(None)
      }
    })
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    tx_remove::<K, V>(&mut table, key.borrow())
  }
}

impl<K, V> BTreeTransaction<K, V> for RedbBTreeTransaction<K, V>
where
  K: BTreeKey + Codec,
  V: BTreeValue + Codec,
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
