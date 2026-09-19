use std::sync::Arc;

use async_stream::stream;
use db_btree::{BTreeRead, BTreeTransaction};
use db_btree_redb::{Bytes, RedbDatabase, RedbDatabaseTransaction};
use db_engine::{EngineError, EngineResult, Kernel, KernelTransaction};

use futures::Stream;

#[derive(Clone)]
pub struct RedbKernel {
    database: RedbDatabase,
}

impl RedbKernel {
    pub fn new(database: Arc<redb::Database>) -> Self {
        Self {
            database: RedbDatabase::new(database),
        }
    }

    pub fn from_database(database: RedbDatabase) -> Self {
        Self { database }
    }
}

pub struct RedbKernelTransaction {
    pub(crate) database: RedbDatabaseTransaction,
}

impl Kernel for RedbKernel {
    type Transaction = RedbKernelTransaction;

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        self.database
            .transaction()
            .map(|database| RedbKernelTransaction { database })
            .map_err(EngineError::custom)
    }
}

impl RedbKernelTransaction {
    pub(crate) fn entries(
        &self,
        table: &str,
    ) -> db_btree_redb::RedbBTreeScopedTransaction<'_, Bytes, Bytes> {
        self.database.table(table)
    }
}

impl KernelTransaction for RedbKernelTransaction {
    async fn ensure_table(&mut self, name: &str) -> EngineResult<()> {
        self.database
            .create_table::<Bytes, Bytes>(name)
            .map_err(EngineError::custom)
    }

    async fn drop_table(&mut self, name: &str) -> EngineResult<()> {
        self.database
            .drop_table::<Bytes, Bytes>(name)
            .map(|_| ())
            .map_err(EngineError::custom)
    }

    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.entries(table)
            .get(&Bytes(key.to_vec()))
            .await
            .map(|value| value.map(|value| value.0))
            .map_err(EngineError::custom)
    }

    fn scan_bytes(&self, table: &str) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> {
        stream! {
            let entries = self.entries(table);
            for await entry in entries.range(..) {
                yield entry.map(|(key, value)| (key.0, value.0)).map_err(EngineError::custom);
            }
        }
    }

    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.entries(table)
            .insert(Bytes(key), Bytes(value))
            .await
            .map_err(EngineError::custom)
    }

    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.entries(table)
            .remove(&Bytes(key.to_vec()))
            .await
            .map(|value| value.map(|value| value.0))
            .map_err(EngineError::custom)
    }

    async fn commit(self) -> EngineResult<()> {
        self.database.commit().map_err(EngineError::custom)
    }

    async fn rollback(self) -> EngineResult<()> {
        self.database.rollback().map_err(EngineError::custom)
    }
}
