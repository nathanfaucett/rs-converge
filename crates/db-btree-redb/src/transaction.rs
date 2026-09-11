use std::{marker::PhantomData, ops::RangeBounds};

use async_stream::stream;
use futures::Stream;
use redb::{ReadableTable, WriteTransaction};

use db_btree::{BTreeError, BTreeRead, BTreeResult, BTreeTransaction};

use crate::{
    key::Key,
    redb::{RedbKey, RedbValue, table_definition},
    value::Value,
};

pub struct RedbBTreeTransaction<K, V> {
    tx: WriteTransaction,
    name: String,
    _phantom_marker: PhantomData<(K, V)>,
}

unsafe impl<K, V> Send for RedbBTreeTransaction<K, V> {}

impl<K, V> RedbBTreeTransaction<K, V> {
    pub fn new(tx: WriteTransaction, name: impl Into<String>) -> Self {
        Self {
            tx,
            name: name.into(),
            _phantom_marker: PhantomData,
        }
    }
}

impl<K, V> BTreeRead<K, V> for RedbBTreeTransaction<K, V>
where
    K: RedbKey,
    V: RedbValue,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let table = self
            .tx
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;

        let guard = match table
            .get(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
        {
            Some(value) => value,
            None => return Ok(None),
        };

        let value: V = guard.value().into_inner();

        Ok(Some(value))
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
    where
        R: RangeBounds<K>,
    {
        stream!({
            let table = self
                .tx
                .open_table(table_definition::<K, V>(&self.name))
                .map_err(BTreeError::custom)?;

            let mapped_range = Key::range(range);
            let results = table.range(mapped_range).map_err(BTreeError::custom)?;

            for result in results {
                let (guard_key, guard_value) = result.map_err(BTreeError::custom)?;
                let key: K = guard_key.value().into_inner();
                let value: V = guard_value.value().into_inner();
                yield Ok((key, value));
            }
        })
    }
}

impl<K, V> BTreeTransaction<K, V> for RedbBTreeTransaction<K, V>
where
    K: RedbKey,
    V: RedbValue,
{
    async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
        let mut table = self
            .tx
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;

        table
            .insert(Key::new(key), Value::new(value))
            .map_err(BTreeError::custom)?;

        Ok(())
    }

    async fn update<F>(&mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut V) -> BTreeResult<()>,
    {
        let mut table = self
            .tx
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;

        if let Some(entry) = table
            .get_mut(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
        {
            let mut value = entry.value().into_inner();
            update_fn(&mut value)?;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
    async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
        let mut table = self
            .tx
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;

        let key = Key::new(key.clone());

        let value = if let Some(entry) = table.get(&key).map_err(BTreeError::custom)? {
            let value = entry.value().into_inner();
            Some(value)
        } else {
            None
        };

        if value.is_some() {
            table.remove(&key).map_err(BTreeError::custom)?;
        }

        Ok(value)
    }

    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
    where
        R: RangeBounds<K>,
    {
        stream!({
            let mut table = self
                .tx
                .open_table(table_definition::<K, V>(&self.name))
                .map_err(BTreeError::custom)?;

            let mapped_range = Key::range(range);
            let results = table.range(mapped_range).map_err(BTreeError::custom)?;

            let mut keys_to_remove = Vec::new();

            for result in results {
                let (guard_key, guard_value) = result.map_err(BTreeError::custom)?;
                let key: K = guard_key.value().into_inner();
                let value: V = guard_value.value().into_inner();
                keys_to_remove.push(Key::new(key.clone()));
                yield Ok((key, value));
            }

            for key in keys_to_remove {
                table.remove(&key).map_err(BTreeError::custom)?;
            }
        })
    }

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
