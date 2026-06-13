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

pub trait BTreeReadExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn get(&self, key: &K) -> impl Future<Output = BTreeResult<Option<V>>>;
  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
  where
    R: RangeBounds<K>;
}

pub trait BTreeWriteExecutor<K, V>: BTreeReadExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn insert(&mut self, key: K, value: V) -> impl Future<Output = BTreeResult<()>>;

  fn update<F>(&mut self, key: K, update_fn: F) -> impl Future<Output = BTreeResult<Option<()>>>
  where
    F: FnOnce(&mut V) -> BTreeResult<()>;

  fn remove(&mut self, key: &K) -> impl Future<Output = BTreeResult<Option<V>>>;
}

pub trait BTreeTransaction<K, V>: BTreeWriteExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn commit(self) -> impl Future<Output = BTreeResult<()>>;
  fn rollback(self) -> impl Future<Output = BTreeResult<()>>;
}

pub trait BTree<K, V>: BTreeReadExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  type Transaction: BTreeTransaction<K, V>;

  fn transaction(&self) -> impl Future<Output = BTreeResult<Self::Transaction>>;
}
