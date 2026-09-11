use db_btree::{BTree, BTreeRead};
use db_value::Row;

use crate::EngineResult;

pub trait KernelRead {
    type ReadTable: BTreeRead<Row, Row>;

    fn read_table(&self, name: &str) -> impl Future<Output = EngineResult<Self::ReadTable>>;
}

pub trait KernelTransaction: KernelRead {
    type WriteTable: BTree<Row, Row>;

    fn write_table(&self, name: &str) -> impl Future<Output = EngineResult<Self::WriteTable>>;

    fn commit(self) -> impl Future<Output = EngineResult<()>>;
    fn rollback(self) -> impl Future<Output = EngineResult<()>>;
}

pub trait Kernel: KernelRead {
    type Transaction: KernelTransaction;

    fn transaction(&self) -> impl Future<Output = EngineResult<Self::Transaction>>;
}
