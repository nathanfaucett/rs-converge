use std::{marker::PhantomData, ops::RangeBounds};

use async_stream::stream;
use futures::Stream;
use redb::{ReadableTable, WriteTransaction};

use btree::{BTreeError, BTreeRead, BTreeResult, BTreeTransaction};

use crate::{
    key::Key,
    redb::{RedbKey, RedbValue, table_definition},
    value::Value,
};

trait WriteTransactionHandle {
    fn transaction(&self) -> &WriteTransaction;
    fn commit(self) -> BTreeResult<()>;
    fn rollback(self) -> BTreeResult<()>;
}

impl WriteTransactionHandle for WriteTransaction {
    fn transaction(&self) -> &WriteTransaction {
        self
    }

    fn commit(self) -> BTreeResult<()> {
        WriteTransaction::commit(self).map_err(BTreeError::custom)
    }

    fn rollback(self) -> BTreeResult<()> {
        WriteTransaction::abort(self).map_err(BTreeError::custom)
    }
}

impl WriteTransactionHandle for &WriteTransaction {
    fn transaction(&self) -> &WriteTransaction {
        self
    }

    fn commit(self) -> BTreeResult<()> {
        Ok(())
    }

    fn rollback(self) -> BTreeResult<()> {
        Ok(())
    }
}

pub struct RedbBTreeTransactionCore<T, K, V> {
    tx: T,
    name: String,
    _phantom_marker: PhantomData<fn(K, V)>,
}

pub type RedbBTreeTransaction<K, V> = RedbBTreeTransactionCore<WriteTransaction, K, V>;
pub type RedbBTreeScopedTransaction<'a, K, V> =
    RedbBTreeTransactionCore<&'a WriteTransaction, K, V>;

impl<T, K, V> RedbBTreeTransactionCore<T, K, V> {
    pub fn new(tx: T, name: impl Into<String>) -> Self {
        Self {
            tx,
            name: name.into(),
            _phantom_marker: PhantomData,
        }
    }
}

impl<T, K, V> BTreeRead<K, V> for RedbBTreeTransactionCore<T, K, V>
where
    T: WriteTransactionHandle + Send + Sync,
    K: RedbKey,
    V: RedbValue,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let table = self
            .tx
            .transaction()
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;

        let guard = match table
            .get(Key::new(key.clone()))
            .map_err(BTreeError::custom)?
        {
            Some(value) => value,
            None => return Ok(None),
        };

        Ok(Some(guard.value().into_inner()))
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream!({
            let table = self
                .tx
                .transaction()
                .open_table(table_definition::<K, V>(&self.name))
                .map_err(BTreeError::custom)?;
            let results = table.range(Key::range(range)).map_err(BTreeError::custom)?;

            for result in results {
                let (key, value) = result.map_err(BTreeError::custom)?;
                yield Ok((key.value().into_inner(), value.value().into_inner()));
            }
        })
    }
}

impl<T, K, V> BTreeTransaction<K, V> for RedbBTreeTransactionCore<T, K, V>
where
    T: WriteTransactionHandle + Send + Sync,
    K: RedbKey,
    V: RedbValue,
{
    async fn insert(&mut self, key: K, value: V) -> BTreeResult<()> {
        let mut table = self
            .tx
            .transaction()
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;
        table
            .insert(Key::new(key), Value::new(value))
            .map_err(BTreeError::custom)?;
        Ok(())
    }

    async fn update<F>(&mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut V) -> BTreeResult<()> + Send,
    {
        let mut table = self
            .tx
            .transaction()
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;
        let Some(entry) = table.get_mut(Key::new(key)).map_err(BTreeError::custom)? else {
            return Ok(None);
        };
        let mut value = entry.value().into_inner();
        update_fn(&mut value)?;
        Ok(Some(()))
    }

    async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
        let mut table = self
            .tx
            .transaction()
            .open_table(table_definition::<K, V>(&self.name))
            .map_err(BTreeError::custom)?;
        let key = Key::new(key.clone());
        let value = table
            .get(&key)
            .map_err(BTreeError::custom)?
            .map(|entry| entry.value().into_inner());
        if value.is_some() {
            table.remove(&key).map_err(BTreeError::custom)?;
        }
        Ok(value)
    }

    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream!({
            let mut table = self
                .tx
                .transaction()
                .open_table(table_definition::<K, V>(&self.name))
                .map_err(BTreeError::custom)?;
            let results = table.range(Key::range(range)).map_err(BTreeError::custom)?;
            let mut keys = Vec::new();

            for result in results {
                let (key, value) = result.map_err(BTreeError::custom)?;
                let key: K = key.value().into_inner();
                let value: V = value.value().into_inner();
                keys.push(Key::new(key.clone()));
                yield Ok((key, value));
            }
            for key in keys {
                table.remove(&key).map_err(BTreeError::custom)?;
            }
        })
    }

    async fn commit(self) -> BTreeResult<()> {
        let Self { tx, .. } = self;
        tx.commit()
    }

    async fn rollback(self) -> BTreeResult<()> {
        let Self { tx, .. } = self;
        tx.rollback()
    }
}
