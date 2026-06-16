use std::{borrow::Borrow, marker::PhantomData, ops::RangeBounds};

use async_stream::stream;
use futures::Stream;
use redb::{ReadableTable, WriteTransaction};

use db_btree::{
  BTreeError, BTreeQuery, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};

use crate::util::{RedbKey, RedbValue, map_range, range_as_ref, table_definition};

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
  K: RedbKey,
  V: RedbValue,
{
  async fn get<Q>(&self, query: &Q) -> BTreeResult<Option<V>>
  where
    Q: BTreeQuery<K> + ?Sized,
    K: Borrow<Q>,
  {
    let table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    let key: K = query.to_key();
    let key_bytes = key.encode().map_err(BTreeError::custom)?;

    let guard = match table
      .get(key_bytes.as_slice())
      .map_err(BTreeError::custom)?
    {
      Some(value) => value,
      None => return Ok(None),
    };

    let value = V::decode(guard.value()).map_err(BTreeError::custom)?;

    Ok(Some(value))
  }

  fn range<Q, R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
  where
    Q: BTreeQuery<K> + ?Sized,
    K: Borrow<Q>,
    R: RangeBounds<Q>,
  {
    stream! {
        let table = self
          .tx
          .open_table(table_definition(&self.name))
          .map_err(BTreeError::custom)?;

        let mapped_range = map_range(range).map_err(BTreeError::custom)?;
        let mapped_range_bytes = range_as_ref(&mapped_range);
        let results = table.range(mapped_range_bytes).map_err(BTreeError::custom)?;

        for result in results {
            let (guard_key, guard_value) = result.map_err(BTreeError::custom)?;
            let key: K = K::decode(guard_key.value()).map_err(BTreeError::custom)?;
            let value: V = V::decode(guard_value.value()).map_err(BTreeError::custom)?;
            yield Ok((key, value));
        }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for RedbBTreeTransaction<K, V>
where
  K: RedbKey,
  V: RedbValue,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    let key_bytes = key.encode().map_err(BTreeError::custom)?;
    let value_bytes = value.encode().map_err(BTreeError::custom)?;

    table
      .insert(key_bytes.as_slice(), value_bytes.as_slice())
      .map_err(BTreeError::custom)?;

    Ok(())
  }

  async fn update<F>(&mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut V) -> BTreeResult<()>,
  {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    let key_bytes = key.encode().map_err(BTreeError::custom)?;

    if let Some(entry) = table
      .get_mut(key_bytes.as_slice())
      .map_err(BTreeError::custom)?
    {
      let mut value = V::decode(entry.value()).map_err(BTreeError::custom)?;
      update_fn(&mut value)?;
      Ok(Some(()))
    } else {
      Ok(None)
    }
  }
  async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
    let mut table = self
      .tx
      .open_table(table_definition(&self.name))
      .map_err(BTreeError::custom)?;

    let key_bytes = key.encode().map_err(BTreeError::custom)?;

    let value = if let Some(entry) = table
      .get(key_bytes.as_slice())
      .map_err(BTreeError::custom)?
    {
      let value = V::decode(entry.value()).map_err(BTreeError::custom)?;
      Some(value)
    } else {
      None
    };

    if value.is_some() {
      table
        .remove(key_bytes.as_slice())
        .map_err(BTreeError::custom)?;
    }

    Ok(value)
  }
}

impl<K, V> BTreeTransaction<K, V> for RedbBTreeTransaction<K, V>
where
  K: RedbKey,
  V: RedbValue,
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
