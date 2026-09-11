use db_value::Row;
use futures::Stream;

use crate::EngineResult;

pub trait KernelTransaction {
    fn ensure_table(&mut self, name: &str) -> impl Future<Output = EngineResult<()>>;
    fn drop_table(&mut self, name: &str) -> impl Future<Output = EngineResult<()>>;

    fn get_entry(&self, table: &str, key: &Row) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn scan_entries(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>>;
    fn put_entry(
        &mut self,
        table: &str,
        key: Row,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>>;
    fn remove_entry(
        &mut self,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;

    fn commit(self) -> impl Future<Output = EngineResult<()>>;
    fn rollback(self) -> impl Future<Output = EngineResult<()>>;
}

pub trait Kernel {
    type Transaction: KernelTransaction;

    fn transaction(&self) -> impl Future<Output = EngineResult<Self::Transaction>>;
}
