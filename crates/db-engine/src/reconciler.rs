use db_value::Row;
use futures::Stream;

use crate::{EngineResult, KernelTransaction};

pub trait RowReconciler<T>
where
    T: KernelTransaction,
{
    fn ensure_table(
        &self,
        transaction: &mut T,
        table: &str,
    ) -> impl Future<Output = EngineResult<()>>;
    fn drop_table(
        &self,
        transaction: &mut T,
        table: &str,
    ) -> impl Future<Output = EngineResult<()>>;

    fn get_row(
        &self,
        transaction: &T,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn scan_rows(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(Row, Row)>>;
    fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        key: Row,
        row: Row,
    ) -> impl Future<Output = EngineResult<()>>;
    fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        key: &Row,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DirectRowReconciler;

impl<T> RowReconciler<T> for DirectRowReconciler
where
    T: KernelTransaction,
{
    async fn ensure_table(&self, transaction: &mut T, table: &str) -> EngineResult<()> {
        transaction.ensure_table(table).await
    }

    async fn drop_table(&self, transaction: &mut T, table: &str) -> EngineResult<()> {
        transaction.drop_table(table).await
    }

    async fn get_row(&self, transaction: &T, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        transaction.get_entry(table, key).await
    }

    fn scan_rows(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(Row, Row)>> {
        transaction.scan_entries(table)
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: &str,
        key: Row,
        row: Row,
    ) -> EngineResult<()> {
        transaction.put_entry(table, key, row).await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: &str,
        key: &Row,
    ) -> EngineResult<Option<Row>> {
        transaction.remove_entry(table, key).await
    }
}
