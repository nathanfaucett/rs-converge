use std::sync::Arc;

use async_stream::stream;
use db_btree::{BTreeRead, BTreeTransaction};
use db_btree_redb::{RedbDatabase, RedbDatabaseTransaction};
use db_engine::{EngineError, EngineResult, Kernel, KernelTransaction};
use db_value::Row;
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
    ) -> db_btree_redb::RedbBTreeScopedTransaction<'_, Row, Row> {
        self.database.table(table)
    }
}

impl KernelTransaction for RedbKernelTransaction {
    async fn ensure_table(&mut self, name: &str) -> EngineResult<()> {
        self.database
            .create_table::<Row, Row>(name)
            .map_err(EngineError::custom)
    }

    async fn drop_table(&mut self, name: &str) -> EngineResult<()> {
        self.database
            .drop_table::<Row, Row>(name)
            .map(|_| ())
            .map_err(EngineError::custom)
    }

    async fn get_entry(&self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        self.entries(table)
            .get(key)
            .await
            .map_err(EngineError::custom)
    }

    fn scan_entries(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>> {
        stream! {
            let entries = self.entries(table);
            for await entry in entries.range(..) {
                yield entry.map_err(EngineError::custom);
            }
        }
    }

    async fn put_entry(&mut self, table: &str, key: Row, value: Row) -> EngineResult<()> {
        self.entries(table)
            .insert(key, value)
            .await
            .map_err(EngineError::custom)
    }

    async fn remove_entry(&mut self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        self.entries(table)
            .remove(key)
            .await
            .map_err(EngineError::custom)
    }

    async fn commit(self) -> EngineResult<()> {
        self.database.commit().map_err(EngineError::custom)
    }

    async fn rollback(self) -> EngineResult<()> {
        self.database.rollback().map_err(EngineError::custom)
    }
}
