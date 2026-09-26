use alloc::{collections::BTreeMap, sync::Arc, vec, vec::Vec};

use async_lock::RwLock;

use futures::{Stream, stream};

use crate::{
    EngineError, EngineResult,
    kernel::{Kernel, KernelTransaction},
};

type Tables = BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>;

struct State {
    revision: u64,
    tables: Tables,
}

#[derive(Clone)]
pub struct InMemoryKernel {
    state: Arc<RwLock<State>>,
}

impl InMemoryKernel {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State {
                revision: 0,
                tables: Tables::new(),
            })),
        }
    }
}

impl Default for InMemoryKernel {
    fn default() -> Self {
        Self::new()
    }
}

pub struct InMemoryKernelTransaction {
    state: Arc<RwLock<State>>,
    revision: u64,
    tables: Tables,
}

impl Kernel for InMemoryKernel {
    type Transaction = InMemoryKernelTransaction;

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        let state = self.state.read().await;
        Ok(InMemoryKernelTransaction {
            state: self.state.clone(),
            revision: state.revision,
            tables: state.tables.clone(),
        })
    }
}

impl InMemoryKernelTransaction {
    fn table(&self, table: &str) -> EngineResult<&BTreeMap<Vec<u8>, Vec<u8>>> {
        self.tables
            .get(table)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn table_mut(&mut self, table: &str) -> EngineResult<&mut BTreeMap<Vec<u8>, Vec<u8>>> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn scan(&self, table: &str) -> Vec<EngineResult<(Vec<u8>, Vec<u8>)>> {
        match self.table(table) {
            Ok(table) => table
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect(),
            Err(error) => vec![Err(error)],
        }
    }
}

impl KernelTransaction for InMemoryKernelTransaction {
    async fn ensure_table(&mut self, table: &str) -> EngineResult<()> {
        self.tables.entry(table.to_string()).or_default();
        Ok(())
    }

    async fn drop_table(&mut self, table: &str) -> EngineResult<()> {
        self.tables
            .remove(table)
            .map(|_| ())
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    async fn get_bytes(&self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        Ok(self.table(table)?.get(key).cloned())
    }

    fn scan_bytes(&self, table: &str) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> {
        stream::iter(self.scan(table))
    }

    async fn put_bytes(&mut self, table: &str, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        self.table_mut(table)?.insert(key, value);
        Ok(())
    }

    async fn remove_bytes(&mut self, table: &str, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        Ok(self.table_mut(table)?.remove(key))
    }

    async fn commit(self) -> EngineResult<()> {
        let InMemoryKernelTransaction {
            state,
            revision,
            tables,
        } = self;
        let mut state = state.write().await;
        if state.revision != revision {
            return Err(EngineError::custom("Transaction conflict"));
        }
        state.tables = tables;
        state.revision += 1;
        Ok(())
    }

    async fn rollback(self) -> EngineResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::InMemoryKernel;
    use crate::{Kernel, KernelTransaction};

    #[test]
    fn logical_names_are_isolated() {
        block_on(async {
            let kernel = InMemoryKernel::new();
            let first = "first";
            let second = "second";
            let key = b"key";

            let mut transaction = kernel.transaction().await.unwrap();
            transaction.ensure_table(first).await.unwrap();
            transaction.ensure_table(second).await.unwrap();
            transaction
                .put_bytes(first, key.to_vec(), b"first".to_vec())
                .await
                .unwrap();
            transaction.commit().await.unwrap();

            let transaction = kernel.transaction().await.unwrap();
            assert_eq!(
                transaction.get_bytes(first, key).await.unwrap(),
                Some(b"first".to_vec())
            );
            assert_eq!(transaction.get_bytes(second, key).await.unwrap(), None);
            transaction.rollback().await.unwrap();
        });
    }
}
