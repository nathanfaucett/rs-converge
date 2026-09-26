use alloc::vec::Vec;

use futures::Stream;

use crate::EngineResult;

pub trait KernelTransaction: Send + Sync {
    fn ensure_table(&mut self, table: &str) -> impl Future<Output = EngineResult<()>> + Send;
    fn drop_table(&mut self, table: &str) -> impl Future<Output = EngineResult<()>> + Send;

    fn get_bytes(
        &self,
        table: &str,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;
    fn scan_bytes(
        &self,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>> + Send;
    fn put_bytes(
        &mut self,
        table: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> impl Future<Output = EngineResult<()>> + Send;
    fn remove_bytes(
        &mut self,
        table: &str,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>> + Send;

    fn commit(self) -> impl Future<Output = EngineResult<()>> + Send;
    fn rollback(self) -> impl Future<Output = EngineResult<()>> + Send;
}

pub trait Kernel: Send + Sync {
    type Transaction: KernelTransaction;

    fn transaction(&self) -> impl Future<Output = EngineResult<Self::Transaction>> + Send;
}
