#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};

use futures::Stream;

use core::ops::RangeBounds;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BTreeError {
    #[error("Invalid document")]
    InvalidDocument,

    #[error("Type mismatch")]
    TypeMismatch,

    #[error("Conflict")]
    Conflict,

    #[error("Commit failed")]
    CommitFailed,

    #[error("Rollback failed")]
    RollbackFailed,

    #[error("Unsupported operation")]
    UnsupportedOperation,

    #[error("Error: {0}")]
    Custom(String),
}

impl BTreeError {
    pub fn custom<T>(error: T) -> Self
    where
        T: ToString,
    {
        Self::Custom(error.to_string())
    }
}

pub type BTreeResult<T> = Result<T, BTreeError>;

pub trait BTreeValue {}

impl<T> BTreeValue for T where T: {}

pub trait BTreeKey: BTreeValue + Ord {}

impl<T> BTreeKey for T where T: BTreeValue + Ord {}

pub trait BTreeRead<K, V>: Send + Sync
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
{
    fn get(&self, key: &K) -> impl Future<Output = BTreeResult<Option<V>>> + Send;
    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send;
}

pub trait BTreeTransaction<K, V>: BTreeRead<K, V> + Send + Sync
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
{
    fn insert(&mut self, key: K, value: V) -> impl Future<Output = BTreeResult<()>> + Send;
    fn update<F>(
        &mut self,
        key: K,
        update_fn: F,
    ) -> impl Future<Output = BTreeResult<Option<()>>> + Send
    where
        F: FnOnce(&mut V) -> BTreeResult<()> + Send;

    fn remove(&mut self, key: &K) -> impl Future<Output = BTreeResult<Option<V>>> + Send;
    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send;

    fn commit(self) -> impl Future<Output = BTreeResult<()>> + Send;
    fn rollback(self) -> impl Future<Output = BTreeResult<()>> + Send;
}

pub trait BTree<K, V>: BTreeRead<K, V>
where
    K: BTreeKey + Send + Sync,
    V: BTreeValue + Send + Sync,
{
    type Transaction: BTreeTransaction<K, V>;

    fn transaction(&self) -> impl Future<Output = BTreeResult<Self::Transaction>> + Send;
}
