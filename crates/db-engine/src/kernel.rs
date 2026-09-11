use db_value::Row;
use futures::Stream;

use crate::EngineResult;

pub trait KernelTransaction {
    fn create_table(&mut self, name: &str) -> impl Future<Output = EngineResult<()>>;
    fn drop_table(&mut self, name: &str) -> impl Future<Output = EngineResult<()>>;

    fn get_record(&self, table: &str, key: &Row)
    -> impl Future<Output = EngineResult<Option<Row>>>;
    fn scan_records(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>>;
    fn put_record(
        &mut self,
        table: &str,
        key: Row,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>>;
    fn remove_record(
        &mut self,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;

    fn get_row(&self, table: &str, key: &Row) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn scan_rows(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>>;
    fn put_row(
        &mut self,
        table: &str,
        key: Row,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>>;
    fn remove_row(
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
