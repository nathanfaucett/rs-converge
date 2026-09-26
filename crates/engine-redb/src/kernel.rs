use std::sync::Arc;

use async_stream::stream;
use btree::{BTreeRead, BTreeTransaction};
use btree_redb::{Bytes, RedbDatabase, RedbDatabaseTransaction};
use engine::{EngineError, EngineResult, Kernel, KernelTransaction};

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
    ) -> btree_redb::RedbBTreeScopedTransaction<'_, Bytes, Bytes> {
        self.database.table(table)
    }
}

impl KernelTransaction for RedbKernelTransaction {
    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        self.database
            .create_table::<Bytes, Bytes>(table)
            .map_err(EngineError::custom)
    }

    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.database
            .drop_table::<Bytes, Bytes>(table)
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
            let name = table.to_string();
            let entries = self.entries(&name);
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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use futures::executor::block_on;
    use uuid::Uuid;

    use super::RedbKernel;
    use engine::{Kernel, KernelTransaction};

    #[test]
    fn logical_names_are_isolated_and_persisted() {
        let path = PathBuf::from(format!(
            "{}-engine-redb-names.redb",
            std::env::temp_dir()
                .join(Uuid::now_v7().to_string())
                .display()
        ));
        let database = Arc::new(redb::Database::create(&path).unwrap());
        let kernel = RedbKernel::new(database.clone());
        block_on(async {
            let mut transaction = kernel.transaction().await.unwrap();
            transaction.ensure_table("alpha").await.unwrap();
            transaction.ensure_table("beta").await.unwrap();
            transaction
                .put_bytes("alpha", b"key".to_vec(), b"alpha".to_vec())
                .await
                .unwrap();
            transaction
                .put_bytes("beta", b"key".to_vec(), b"beta".to_vec())
                .await
                .unwrap();
            transaction.commit().await.unwrap();

            let transaction = kernel.transaction().await.unwrap();
            assert_eq!(
                transaction.get_bytes("alpha", b"key").await.unwrap(),
                Some(b"alpha".to_vec())
            );
            assert_eq!(
                transaction.get_bytes("beta", b"key").await.unwrap(),
                Some(b"beta".to_vec())
            );
            transaction.rollback().await.unwrap();
        });
        drop(kernel);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }
}
