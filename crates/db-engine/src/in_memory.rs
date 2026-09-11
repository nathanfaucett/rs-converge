use alloc::{collections::BTreeMap, string::String, sync::Arc};

use async_lock::RwLock;
use db_btree::InMemoryBTree;
use db_value::Row;

use crate::{
    EngineError, EngineResult,
    catalog::{ENGINE_INDEX_FIELDS, ENGINE_INDICES, ENGINE_TABLE_FIELDS, ENGINE_TABLES},
    kernel::{Kernel, KernelRead, KernelTransaction},
};

type Tables = BTreeMap<String, InMemoryBTree<Row, Row>>;

#[derive(Clone)]
pub struct InMemoryKernel {
    tables: Arc<RwLock<Tables>>,
}

impl InMemoryKernel {
    pub fn new() -> Self {
        let mut tables = Tables::new();
        for name in [
            ENGINE_TABLES,
            ENGINE_TABLE_FIELDS,
            ENGINE_INDICES,
            ENGINE_INDEX_FIELDS,
        ] {
            tables.insert(String::from(name), InMemoryBTree::new());
        }
        Self {
            tables: Arc::new(RwLock::new(tables)),
        }
    }
}

impl Default for InMemoryKernel {
    fn default() -> Self {
        Self::new()
    }
}

pub struct InMemoryKernelTransaction {
    tables: Arc<RwLock<Tables>>,
}

impl KernelRead for InMemoryKernel {
    type ReadTable = InMemoryBTree<Row, Row>;

    async fn read_table(&self, name: &str) -> EngineResult<Self::ReadTable> {
        self.tables
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::custom("Table not found"))
    }
}

impl Kernel for InMemoryKernel {
    type Transaction = InMemoryKernelTransaction;

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        Ok(InMemoryKernelTransaction {
            tables: self.tables.clone(),
        })
    }
}

impl KernelRead for InMemoryKernelTransaction {
    type ReadTable = InMemoryBTree<Row, Row>;

    async fn read_table(&self, name: &str) -> EngineResult<Self::ReadTable> {
        self.tables
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::custom("Table not found"))
    }
}

impl KernelTransaction for InMemoryKernelTransaction {
    type WriteTable = InMemoryBTree<Row, Row>;

    async fn write_table(&self, name: &str) -> EngineResult<Self::WriteTable> {
        Ok(self
            .tables
            .write()
            .await
            .entry(String::from(name))
            .or_insert_with(InMemoryBTree::new)
            .clone())
    }

    async fn commit(self) -> EngineResult<()> {
        Ok(())
    }

    async fn rollback(self) -> EngineResult<()> {
        Ok(())
    }
}
