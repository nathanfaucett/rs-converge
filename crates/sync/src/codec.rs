use alloc::vec::Vec;

use engine::{EngineResult, KernelTransaction, RowCodec};
use uuid::Uuid;
use value::Row;

use crate::SyncChangeId;

pub trait SyncRowCodec<T>: RowCodec<T>
where
    T: KernelTransaction,
{
    fn row_ids(
        &self,
        transaction: &T,
        table: &str,
    ) -> impl Future<Output = EngineResult<Vec<Uuid>>> + Send;
    fn export_state(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn merge_state(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        state: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
    fn export_metadata(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
    ) -> impl Future<Output = EngineResult<Vec<u8>>> + Send;
    fn merge_metadata(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        metadata: &[u8],
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn change_inventory(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
    ) -> impl Future<Output = EngineResult<Vec<SyncChangeId>>> + Send;
    fn export_change(
        &self,
        transaction: &T,
        table: &str,
        row: Uuid,
        id: &SyncChangeId,
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn apply_change(
        &self,
        transaction: &mut T,
        table: &str,
        row: Uuid,
        id: &SyncChangeId,
        payload: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Row>>> + Send;
}
