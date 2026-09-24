use alloc::vec::Vec;

use futures::Stream;
use uuid::Uuid;
use value::Row;

use crate::{DocumentChangeKey, EngineResult, KernelTransaction};

pub trait RowCodec<T>: Send + Sync
where
    T: KernelTransaction,
{
    fn ensure_table(
        &self,
        transaction: &mut T,
        table: Uuid,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn drop_table(
        &self,
        transaction: &mut T,
        table: Uuid,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn get_row(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn scan_rows(
        &self,
        transaction: &T,
        table: Uuid,
    ) -> impl Stream<Item = EngineResult<(Uuid, Row)>> + Send;
    fn put_row(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: Uuid,
        value: Row,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn remove_row(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn encode_row(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn conflicted_columns(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Vec<usize>>> + Send;
    fn encode_resolution(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
        value: &Row,
        changed_columns: &[usize],
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn merge_row(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: Uuid,
        value: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn export_row_state(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn merge_row_state(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: Uuid,
        state: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn sync_change_inventory(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Vec<DocumentChangeKey>>> + Send;
    fn export_incremental_change(
        &self,
        transaction: &T,
        table: Uuid,
        key: &DocumentChangeKey,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn apply_incremental_change(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: Uuid,
        key: &DocumentChangeKey,
        payload: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn delete_row(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn row_is_deleted(
        &self,
        transaction: &T,
        table: Uuid,
        row: &Uuid,
    ) -> impl Future<Output = EngineResult<bool>> + Send;
    fn export_row_metadata(
        &self,
        transaction: &T,
        table: Uuid,
    ) -> impl Stream<Item = EngineResult<(Uuid, Vec<u8>)>> + Send;
    fn merge_row_metadata(
        &self,
        transaction: &mut T,
        table: Uuid,
        row: Uuid,
        metadata: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + Send;
}
