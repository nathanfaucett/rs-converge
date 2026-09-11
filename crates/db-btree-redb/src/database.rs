use std::sync::Arc;

use redb::{Database, WriteTransaction};

use db_btree::BTreeResult;

use crate::{
    RedbBTreeScopedTransaction,
    redb::{RedbKey, RedbValue, table_definition},
};

#[derive(Clone)]
pub struct RedbDatabase {
    inner: Arc<Database>,
}

impl RedbDatabase {
    pub fn new(inner: Arc<Database>) -> Self {
        Self { inner }
    }

    pub fn transaction(&self) -> BTreeResult<RedbDatabaseTransaction> {
        Ok(RedbDatabaseTransaction {
            inner: self
                .inner
                .begin_write()
                .map_err(db_btree::BTreeError::custom)?,
        })
    }
}

pub struct RedbDatabaseTransaction {
    inner: WriteTransaction,
}

impl RedbDatabaseTransaction {
    pub fn table<K, V>(&self, name: impl Into<String>) -> RedbBTreeScopedTransaction<'_, K, V> {
        RedbBTreeScopedTransaction::new(&self.inner, name)
    }

    pub fn create_table<K, V>(&self, name: &str) -> BTreeResult<()>
    where
        K: RedbKey,
        V: RedbValue,
    {
        self.inner
            .open_table(table_definition::<K, V>(name))
            .map_err(db_btree::BTreeError::custom)?;
        Ok(())
    }

    pub fn drop_table<K, V>(&self, name: &str) -> BTreeResult<bool>
    where
        K: RedbKey,
        V: RedbValue,
    {
        self.inner
            .delete_table(table_definition::<K, V>(name))
            .map_err(db_btree::BTreeError::custom)
    }

    pub fn commit(self) -> BTreeResult<()> {
        self.inner.commit().map_err(db_btree::BTreeError::custom)
    }

    pub fn rollback(self) -> BTreeResult<()> {
        self.inner.abort().map_err(db_btree::BTreeError::custom)
    }
}
