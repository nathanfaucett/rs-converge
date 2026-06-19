use db_btree::BTree;
use db_value::Row;

use crate::EngineResult;

pub trait EngineKernelExecutor {
  type TableBTree: BTree<Row, Row>;

  fn table(&self, name: &str) -> impl Future<Output = EngineResult<Self::TableBTree>>;
}

pub trait EngineKernelTransaction: EngineKernelExecutor {
  fn commit(self) -> impl Future<Output = EngineResult<()>>;
  fn rollback(self) -> impl Future<Output = EngineResult<()>>;
}

pub trait EngineKernel: EngineKernelExecutor {
  type Transaction: EngineKernelTransaction;

  fn transaction(&self) -> impl Future<Output = EngineResult<Self::Transaction>>;
}
