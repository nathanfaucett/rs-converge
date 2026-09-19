use alloc::{collections::BTreeMap, string::String, sync::Arc, vec, vec::Vec};

use async_lock::RwLock;
use db_value::Row;
use futures::{Stream, stream};

use crate::{
    EngineError, EngineResult,
    catalog::{ENGINE_INDICES, ENGINE_TABLE_FIELDS, ENGINE_TABLES},
    kernel::{Kernel, KernelTransaction},
};

type Tables = BTreeMap<String, BTreeMap<Row, Row>>;

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
    fn table(&self, name: &str) -> EngineResult<&BTreeMap<Row, Row>> {
        self.tables
            .get(name)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn table_mut(&mut self, name: &str) -> EngineResult<&mut BTreeMap<Row, Row>> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| EngineError::custom("Table not found"))
    }

    fn scan(&self, name: &str) -> Vec<EngineResult<(Row, Row)>> {
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

    async fn get_entry(&self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        Ok(self.table(table)?.get(key).cloned())
    }

    fn scan_entries(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>> {
        stream::iter(self.scan(table))
    }

    async fn put_entry(&mut self, table: &str, key: Row, value: Row) -> EngineResult<()> {
        self.table_mut(table)?.insert(key, value);
        Ok(())
    }

    async fn remove_entry(&mut self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
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
