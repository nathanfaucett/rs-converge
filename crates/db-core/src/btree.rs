#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use core::{borrow::Borrow, error::Error, ops::RangeBounds};
#[cfg(feature = "std")]
use std::boxed::Box;
#[cfg(feature = "std")]
use std::{borrow::Borrow, error::Error, ops::RangeBounds};

use thiserror::Error;

use crate::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};

#[derive(Error, Debug)]
pub enum BTreeError {
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

pub trait BTreeReadExecutor<K, V>: MaybeSend + MaybeSync {
  fn get<'a, Q>(&'a self, key: Q) -> impl MaybeSendFuture<Output = BTreeResult<Option<V>>> + 'a
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a;

  fn range<'a, R>(&'a self, range: R) -> impl MaybeSendStream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: RangeBounds<K> + MaybeSend + 'a;
}

pub trait BTreeWriteExecutor<K, V>: BTreeReadExecutor<K, V> {
  fn insert<'a>(
    &'a mut self,
    key: K,
    value: V,
  ) -> impl MaybeSendFuture<Output = BTreeResult<()>> + 'a
  where
    K: Ord;

  fn remove<'a, Q>(
    &'a mut self,
    key: Q,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Option<V>>> + 'a
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a;
}

pub trait BTreeTransaction<K, V>: BTreeWriteExecutor<K, V> {
  fn commit(self) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    Self: Sized;

  fn rollback(self) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    Self: Sized;
}

pub trait BTree<K, V>: BTreeReadExecutor<K, V> {
  type Transaction: BTreeTransaction<K, V>;

  fn transaction<'a>(
    &'a self,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + 'a;
}
