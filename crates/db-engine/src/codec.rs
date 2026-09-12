use alloc::vec::Vec;

use db_value::Row;
use futures::Stream;

use crate::{EngineResult, KernelTransaction};

pub trait RowCodec<T>
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

    fn encode_row(
        &self,
        transaction: &T,
        table: &str,
        key: &Row,
        row: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>>;
    fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        key: Row,
        value: &[u8],
    ) -> impl Future<Output = EngineResult<Row>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DirectRowCodec;

impl<T> RowCodec<T> for DirectRowCodec
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

    async fn encode_row(
        &self,
        _: &T,
        _: &str,
        _: &Row,
        row: &Row,
        _: &[usize],
    ) -> EngineResult<Vec<u8>> {
        postcard::to_allocvec(row).map_err(crate::EngineError::custom)
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &str,
        key: Row,
        value: &[u8],
    ) -> EngineResult<Row> {
        let row: Row = postcard::from_bytes(value).map_err(crate::EngineError::custom)?;
        self.put_row(transaction, table, key, row.clone()).await?;
        Ok(row)
    }
}
