use alloc::{collections::BTreeMap, string::String, sync::Arc, vec, vec::Vec};

use async_lock::RwLock;

use futures::{Stream, stream};

use crate::{
    EngineError, EngineResult,
    catalog::{ENGINE_INDICES, ENGINE_TABLE_FIELDS, ENGINE_TABLES},
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
        let mut tables = Tables::new();
        for name in [ENGINE_TABLES, ENGINE_TABLE_FIELDS, ENGINE_INDICES] {
            tables.insert(String::from(name), BTreeMap::new());
        }
        Self {
            state: Arc::new(RwLock::new(State {
                revision: 0,
                tables,
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
    fn table(&self, name: &str) -> EngineResult<&BTreeMap<Vec<u8>, Vec<u8>>> {
        self.tables
            .get(name)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn table_mut(&mut self, name: &str) -> EngineResult<&mut BTreeMap<Vec<u8>, Vec<u8>>> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn scan(&self, name: &str) -> Vec<EngineResult<(Vec<u8>, Vec<u8>)>> {
        match self.table(name) {
            Ok(table) => table
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect(),
            Err(error) => vec![Err(error)],
        }
    }
}

impl KernelTransaction for InMemoryKernelTransaction {
    async fn ensure_table(&mut self, name: &str) -> EngineResult<()> {
        self.tables.entry(String::from(name)).or_default();
        Ok(())
    }

    async fn drop_table(&mut self, name: &str) -> EngineResult<()> {
        self.tables
            .remove(name)
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
