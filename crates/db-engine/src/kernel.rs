use alloc::vec::Vec;

use futures::Stream;

use crate::EngineResult;

pub trait KernelTransaction {
    fn ensure_table(&mut self, name: &str) -> impl Future<Output = EngineResult<()>>;
    fn drop_table(&mut self, name: &str) -> impl Future<Output = EngineResult<()>>;

    fn get_bytes(
        &self,
        table: &str,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>>;
    fn scan_bytes(&self, table: &str) -> impl Stream<Item = EngineResult<(Vec<u8>, Vec<u8>)>>;
    fn put_bytes(
        &mut self,
        table: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> impl Future<Output = EngineResult<()>>;
    fn remove_bytes(
        &mut self,
        table: &str,
        key: &[u8],
    ) -> impl Future<Output = EngineResult<Option<Vec<u8>>>>;

    fn commit(self) -> impl Future<Output = EngineResult<()>>;
    fn rollback(self) -> impl Future<Output = EngineResult<()>>;
}

pub trait Kernel {
    type Transaction: KernelTransaction;

    fn transaction(&self) -> impl Future<Output = EngineResult<Self::Transaction>>;
}
