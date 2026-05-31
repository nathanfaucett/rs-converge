#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String};
use core::{borrow::Borrow, error::Error, ops::RangeBounds};

use thiserror::Error;

use crate::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};

#[derive(Error, Debug)]
pub enum BTreeError {
  #[error("Not found")]
  NotFound,

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

  #[error("Custom error: {0}")]
  Custom(String),

  #[error("Other error: {0}")]
  Other(#[from] Box<dyn Error + Send + Sync>),
}

pub type BTreeResult<T> = Result<T, BTreeError>;

impl BTreeError {
  pub fn other<E>(error: E) -> Self
  where
    E: Error + Send + Sync + 'static,
  {
    BTreeError::Other(Box::new(error))
  }
}

pub trait BTreeKey: Ord + MaybeSend + MaybeSync + Clone + 'static {}
impl<T> BTreeKey for T where T: Ord + MaybeSend + MaybeSync + Clone + 'static {}

pub trait BTreeValue: MaybeSend + MaybeSync + Clone + 'static {}
impl<T> BTreeValue for T where T: MaybeSend + MaybeSync + Clone + 'static {}

pub trait BTreeReadExecutor<K, V>: MaybeSend + MaybeSync
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn get<'a, Q>(&'a self, key: Q) -> impl MaybeSendFuture<Output = BTreeResult<Option<V>>> + 'a
  where
    Q: Borrow<K> + MaybeSend + 'a;

  fn range<'a, R>(&'a self, range: R) -> impl MaybeSendStream<Item = BTreeResult<(K, V)>> + 'a
  where
    R: RangeBounds<K> + MaybeSend + 'a;
}

pub trait BTreeWriteExecutor<K, V>: BTreeReadExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn insert<'a>(
    &'a mut self,
    key: K,
    value: V,
  ) -> impl MaybeSendFuture<Output = BTreeResult<()>> + 'a;

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Option<V>>> + 'a
  where
    Q: Borrow<K> + MaybeSend + 'a;
}

pub trait BTreeTransaction<K, V>: BTreeWriteExecutor<K, V>
where
  K: BTreeKey,
  V: BTreeValue,
{
  fn commit(self) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    Self: Sized;

  fn rollback(self) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    Self: Sized;
}

pub trait BTree<K, V>: Clone + BTreeReadExecutor<K, V> + 'static
where
  K: BTreeKey,
  V: BTreeValue,
{
  type Transaction: BTreeTransaction<K, V>;

  fn create<D>(definition: &D) -> impl MaybeSendFuture<Output = BTreeResult<Self>>
  where
    Self: Sized,
    D: BTreeDefinition<Key = K, Value = V>;

  fn transaction<'a>(
    &'a self,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + 'a;
}

pub trait BTreeDefinition: Clone + MaybeSend + MaybeSync + 'static {
  type Key: BTreeKey;
  type Value: BTreeValue;

  fn id(&self) -> &str;
}

pub trait BTreeManager: MaybeSend + MaybeSync {
  type BTree<K, V>: BTree<K, V>
  where
    K: BTreeKey,
    V: BTreeValue;

  fn get<D>(
    &self,
    definition: &D,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::BTree<D::Key, D::Value>>>
  where
    D: BTreeDefinition;

  fn insert<D>(
    &self,
    definition: &D,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::BTree<D::Key, D::Value>>>
  where
    D: BTreeDefinition;

  fn remove<D>(&self, definition: &D) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    D: BTreeDefinition;
}
