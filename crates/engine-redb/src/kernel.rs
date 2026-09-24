use std::sync::Arc;

use async_stream::stream;
use btree::{BTreeRead, BTreeTransaction};
use btree_redb::{Bytes, RedbDatabase, RedbDatabaseTransaction};
use engine::{
    ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
    ENGINE_TABLES_STORAGE, EngineError, EngineResult, Kernel, KernelTransaction,
};

use futures::Stream;
use uuid::Uuid;

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

fn physical_name(table: Uuid) -> String {
    if table == ENGINE_TABLES_STORAGE {
        String::from("tables")
    } else if table == ENGINE_TABLE_FIELDS_STORAGE {
        String::from("table_fields")
    } else if table == ENGINE_INDICES_STORAGE {
        String::from("indices")
    } else if table == ENGINE_INDEX_FIELDS_STORAGE {
        String::from("index_fields")
    } else {
        format!("__storage_{}", table)
    }
}

impl KernelTransaction for RedbKernelTransaction {
    async fn ensure_table(&mut self, table: Uuid) -> EngineResult<()> {
        self.database
            .create_table::<Bytes, Bytes>(&physical_name(table))
            .map_err(EngineError::custom)
    }

    async fn drop_table(&mut self, table: Uuid) -> EngineResult<()> {
        self.database
            .drop_table::<Bytes, Bytes>(&physical_name(table))
            .map(|_| ())
            .map_err(EngineError::custom)
    }

    async fn get_bytes(&self, table: Uuid, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.entries(&physical_name(table))
            .get(&Bytes(key.to_vec()))
            .await
            .map(|value| value.map(|value| value.0))
            .map_err(EngineError::custom)
    }

    fn scan_bytes(&self, table: Uuid) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> {
        stream! {
            let name = physical_name(table);
            let entries = self.entries(&name);
            for await entry in entries.range(..) {
                yield entry.map(|(key, value)| (key.0, value.0)).map_err(EngineError::custom);
            }
        }
    }

    async fn put_bytes(&mut self, table: Uuid, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.entries(&physical_name(table))
            .insert(Bytes(key), Bytes(value))
            .await
            .map_err(EngineError::custom)
    }

    async fn remove_bytes(&mut self, table: Uuid, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.entries(&physical_name(table))
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
    fn permanent_catalog_names_are_stable() {
        assert_eq!(
            super::physical_name(engine::ENGINE_TABLES_STORAGE),
            "tables"
        );
        assert_eq!(
            super::physical_name(engine::ENGINE_TABLE_FIELDS_STORAGE),
            "table_fields"
        );
        assert_eq!(
            super::physical_name(engine::ENGINE_INDICES_STORAGE),
            "indices"
        );
        assert_eq!(
            super::physical_name(engine::ENGINE_INDEX_FIELDS_STORAGE),
            "index_fields"
        );
    }

    #[test]
    fn permanent_catalog_names_persist_values() {
        let path = PathBuf::from(format!(
            "{}-engine-redb-catalog.redb",
            std::env::temp_dir()
                .join(Uuid::now_v7().to_string())
                .display()
        ));
        let database = Arc::new(redb::Database::create(&path).unwrap());
        let kernel = RedbKernel::new(database.clone());
        let catalogs = [
            (engine::ENGINE_TABLES_STORAGE, b"tables".to_vec()),
            (
                engine::ENGINE_TABLE_FIELDS_STORAGE,
                b"table_fields".to_vec(),
            ),
            (engine::ENGINE_INDICES_STORAGE, b"indices".to_vec()),
            (
                engine::ENGINE_INDEX_FIELDS_STORAGE,
                b"index_fields".to_vec(),
            ),
        ];
        block_on(async {
            let mut transaction = kernel.transaction().await.unwrap();
            for (table, value) in &catalogs {
                transaction.ensure_table(*table).await.unwrap();
                transaction
                    .put_bytes(*table, b"key".to_vec(), value.clone())
                    .await
                    .unwrap();
            }
            transaction.commit().await.unwrap();
            let transaction = kernel.transaction().await.unwrap();
            for (table, value) in &catalogs {
                assert_eq!(
                    transaction.get_bytes(*table, b"key").await.unwrap(),
                    Some(value.clone())
                );
            }
            transaction.rollback().await.unwrap();
        });
        drop(kernel);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn backing_uuids_are_isolated() {
        let path = PathBuf::from(format!(
            "{}-engine-redb-isolation.redb",
            std::env::temp_dir()
                .join(Uuid::now_v7().to_string())
                .display()
        ));
        let database = Arc::new(redb::Database::create(&path).unwrap());
        let kernel = RedbKernel::new(database.clone());
        block_on(async {
            let first = Uuid::from_u128(1);
            let second = Uuid::from_u128(2);
            let mut transaction = kernel.transaction().await.unwrap();
            transaction.ensure_table(first).await.unwrap();
            transaction.ensure_table(second).await.unwrap();
            transaction
                .put_bytes(first, b"key".to_vec(), b"first".to_vec())
                .await
                .unwrap();
            transaction.commit().await.unwrap();

            let transaction = kernel.transaction().await.unwrap();
            assert_eq!(
                transaction.get_bytes(first, b"key").await.unwrap(),
                Some(b"first".to_vec())
            );
            assert_eq!(transaction.get_bytes(second, b"key").await.unwrap(), None);
            transaction.rollback().await.unwrap();
        });
        drop(kernel);
        drop(database);
        std::fs::remove_file(path).unwrap();
    }
}
