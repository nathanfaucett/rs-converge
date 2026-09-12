use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

use db_value::{Row, Value};
use futures::Stream;

use crate::{EngineResult, KernelTransaction, RowGenerationId, TableGenerationId};

const ROWS: &str = "__db_rows";
const TOMBSTONES: &str = "__db_row_tombstones";

pub trait RowCodec<T>
where
    T: KernelTransaction,
{
    fn ensure_table(
        &self,
        transaction: &mut T,
        table: TableGenerationId,
    ) -> impl Future<Output = EngineResult<()>>;
    fn drop_table(
        &self,
        transaction: &mut T,
        table: TableGenerationId,
    ) -> impl Future<Output = EngineResult<()>>;
    fn get_row(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn scan_rows(
        &self,
        transaction: &T,
        table: &TableGenerationId,
    ) -> impl Stream<Item = EngineResult<(RowGenerationId, Row)>>;
    fn put_row(
        &self,
        transaction: &mut T,
        table: TableGenerationId,
        row: RowGenerationId,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>>;
    fn remove_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn encode_row(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>>;
    fn conflicted_columns(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> impl Future<Output = EngineResult<alloc::vec::Vec<usize>>>;
    fn encode_resolution(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>>;
    fn merge_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: RowGenerationId,
        value: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn export_row_state(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>>;
    fn merge_row_state(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: RowGenerationId,
        state: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn tombstone_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> impl Future<Output = EngineResult<Option<Row>>>;
    fn row_tombstones(
        &self,
        transaction: &T,
        table: &TableGenerationId,
    ) -> impl Stream<Item = EngineResult<RowGenerationId>>;
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct DirectRowCodec;

impl DirectRowCodec {
    fn key(table: &TableGenerationId, row: &RowGenerationId) -> Row {
        Row::new(alloc::vec![Value::Uuid(table.0), Value::Uuid(row.0)])
    }

    async fn tombstoned<T>(
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<bool>
    where
        T: KernelTransaction,
    {
        Ok(transaction
            .get_entry(TOMBSTONES, &Self::key(table, row))
            .await?
            .is_some())
    }
}

impl<T> RowCodec<T> for DirectRowCodec
where
    T: KernelTransaction,
{
    async fn ensure_table(&self, transaction: &mut T, _: TableGenerationId) -> EngineResult<()> {
        transaction.ensure_table(ROWS).await?;
        transaction.ensure_table(TOMBSTONES).await
    }

    async fn drop_table(&self, _: &mut T, _: TableGenerationId) -> EngineResult<()> {
        Ok(())
    }

    async fn get_row(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Row>> {
        let key = Self::key(table, row);
        if transaction.get_entry(TOMBSTONES, &key).await?.is_some() {
            return Ok(None);
        }
        transaction.get_entry(ROWS, &key).await
    }

    fn scan_rows(
        &self,
        transaction: &T,
        table: &TableGenerationId,
    ) -> impl Stream<Item = EngineResult<(RowGenerationId, Row)>> {
        let table = table.0;
        futures::StreamExt::filter_map(transaction.scan_entries(ROWS), move |entry| async move {
            match entry {
                Ok((key, value)) if key.values.first().and_then(Value::as_uuid) == Some(&table) => {
                    key.values
                        .get(1)
                        .and_then(Value::as_uuid)
                        .copied()
                        .map(RowGenerationId)
                        .map(|row| Ok((row, value)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: TableGenerationId,
        row: RowGenerationId,
        value: Row,
    ) -> EngineResult<()> {
        transaction
            .put_entry(ROWS, Self::key(&table, &row), value)
            .await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Row>> {
        let key = Self::key(table, row);
        let value = transaction.remove_entry(ROWS, &key).await?;
        transaction
            .put_entry(TOMBSTONES, key, Row::default())
            .await?;
        Ok(value)
    }

    async fn encode_row(
        &self,
        _: &T,
        _: &TableGenerationId,
        _: &RowGenerationId,
        row: &Row,
        _: &[usize],
    ) -> EngineResult<Vec<u8>> {
        postcard::to_allocvec(row).map_err(crate::EngineError::custom)
    }

    async fn conflicted_columns(
        &self,
        _: &T,
        _: &TableGenerationId,
        _: &RowGenerationId,
    ) -> EngineResult<alloc::vec::Vec<usize>> {
        Ok(alloc::vec::Vec::new())
    }

    async fn encode_resolution(
        &self,
        _: &T,
        _: &TableGenerationId,
        _: &RowGenerationId,
        row: &Row,
        _: &[usize],
    ) -> EngineResult<Vec<u8>> {
        postcard::to_allocvec(row).map_err(crate::EngineError::custom)
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: RowGenerationId,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        self.merge_row_state(transaction, table, row, value).await
    }

    async fn export_row_state(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.get_row(transaction, table, row)
            .await?
            .map(|value| postcard::to_allocvec(&value).map_err(crate::EngineError::custom))
            .transpose()
    }

    async fn merge_row_state(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: RowGenerationId,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        if Self::tombstoned(transaction, table, &row).await? {
            return Ok(None);
        }
        let value: Row = postcard::from_bytes(value).map_err(crate::EngineError::custom)?;
        self.put_row(transaction, *table, row, value.clone())
            .await?;
        Ok(Some(value))
    }

    async fn tombstone_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Row>> {
        self.remove_row(transaction, table, row).await
    }

    fn row_tombstones(
        &self,
        transaction: &T,
        table: &TableGenerationId,
    ) -> impl Stream<Item = EngineResult<RowGenerationId>> {
        let table = table.0;
        futures::StreamExt::filter_map(
            transaction.scan_entries(TOMBSTONES),
            move |entry| async move {
                match entry {
                    Ok((key, _)) if key.values.first().and_then(Value::as_uuid) == Some(&table) => {
                        key.values
                            .get(1)
                            .and_then(Value::as_uuid)
                            .copied()
                            .map(RowGenerationId)
                            .map(Ok)
                    }
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            },
        )
    }
}
