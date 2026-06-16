#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};

use futures::Stream;

use core::{borrow::Borrow, ops::RangeBounds};

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

pub trait BTreeQuery<K>: Ord
where
  K: BTreeKey + Borrow<Self>,
{
  fn to_key(&self) -> K;
}

impl<T> BTreeQuery<T> for T
where
  T: BTreeKey + Borrow<T> + Clone,
{
  fn to_key(&self) -> T {
    self.borrow().clone()
  }
}

impl BTreeQuery<String> for str {
  fn to_key(&self) -> String {
    self.to_string()
  }
}

pub trait BTreeReadExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn get<Q>(&self, key: &Q) -> impl Future<Output = BTreeResult<Option<V>>>
  where
    Q: BTreeQuery<K> + ?Sized,
    K: Borrow<Q>;
  fn range<Q, R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>>
  where
    Q: BTreeQuery<K> + ?Sized,
    K: Borrow<Q>,
    R: RangeBounds<Q>;
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

  fn remove<Q>(&mut self, key: &Q) -> impl Future<Output = BTreeResult<Option<V>>>
  where
    Q: BTreeQuery<K> + ?Sized,
    K: Borrow<Q>;
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
