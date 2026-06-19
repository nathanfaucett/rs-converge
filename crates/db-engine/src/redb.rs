use std::sync::Arc;

use db_redb::{RedbBTree, table_definition};
use db_value::Row;
use redb::Database;

use crate::{
    EngineError, EngineKernel, EngineResult,
    kernel::{EngineKernelRead, EngineKernelTransaction},
};

pub struct RedbEngineKernel {
    db: Arc<Database>,
}

impl EngineKernelRead for RedbEngineKernel {
    type ReadTable = RedbBTree<Row, Row>;

    async fn read_table(&self, name: &str) -> EngineResult<Self::ReadTable> {
        Ok(RedbBTree::new(self.db.clone(), name))
    }
}

impl EngineKernel for RedbEngineKernel {
    type Transaction = RedbEngineKernelTransaction;

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        Ok(RedbEngineKernelTransaction {
            db: self.db.clone(),
        })
    }
}

pub struct RedbEngineKernelTransaction {
    db: Arc<Database>,
}

impl EngineKernelRead for RedbEngineKernelTransaction {
    type ReadTable = RedbBTree<Row, Row>;

    async fn read_table(&self, name: &str) -> EngineResult<Self::ReadTable> {
        Ok(RedbBTree::new(self.db.clone(), name))
    }
}

impl EngineKernelTransaction for RedbEngineKernelTransaction {
    type WriteTable = RedbBTree<Row, Row>;

    async fn write_table(&self, name: &str) -> EngineResult<Self::WriteTable> {
        let db = self.db.clone();
        let name = name.to_string();

        let tx = db.begin_write().map_err(EngineError::custom)?;

        tx.open_table(table_definition::<Row, Row>(&name))
            .map_err(EngineError::custom)?;

        tx.commit().map_err(EngineError::custom)?;

        Ok(RedbBTree::new(db.clone(), name))
    }

    async fn commit(self) -> EngineResult<()> {
        Ok(())
    }

    async fn rollback(self) -> EngineResult<()> {
        Ok(())
    }
}
