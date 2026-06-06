#[cfg(not(feature = "std"))]
use alloc::{
  boxed::Box,
  string::{String, ToString},
  vec::Vec,
};

use core::{any::Any, borrow::Borrow, error::Error, ops::RangeBounds};

use postcard::{from_bytes, to_stdvec};
use thiserror::Error;

use crate::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
use serde::{Serialize, de::DeserializeOwned};

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
  pub fn custom<T>(error: T) -> Self
  where
    T: ToString,
  {
    BTreeError::Custom(error.to_string())
  }

  pub fn other<E>(error: E) -> Self
  where
    E: Error + Send + Sync + 'static,
  {
    BTreeError::Other(Box::new(error))
  }
}

pub trait BTreeValue:
  MaybeSend + MaybeSync + Clone + Serialize + DeserializeOwned + 'static
{
  fn encode(&self) -> BTreeResult<Vec<u8>>;

  fn decode(bytes: &[u8]) -> BTreeResult<Self>
  where
    Self: Sized;
}

impl<T> BTreeValue for T
where
  T: MaybeSend + MaybeSync + Clone + Serialize + DeserializeOwned + 'static,
{
  fn encode(&self) -> BTreeResult<Vec<u8>> {
    to_stdvec(self).map_err(|e| BTreeError::Custom(format!("postcard value ser error: {}", e)))
  }

  fn decode(bytes: &[u8]) -> BTreeResult<Self>
  where
    Self: Sized,
  {
    from_bytes(bytes).map_err(|e| BTreeError::Custom(format!("postcard value de error: {}", e)))
  }
}

pub trait BTreeKey:
  BTreeValue + Ord + MaybeSend + MaybeSync + Clone + Serialize + DeserializeOwned + 'static
{
}
impl<T> BTreeKey for T where
  T: BTreeValue + Ord + MaybeSend + MaybeSync + Clone + Serialize + DeserializeOwned + 'static
{
}

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

  fn transaction<'a>(
    &'a self,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + 'a;
}

pub trait BTreeDefinition: Clone + MaybeSend + MaybeSync + 'static {
  type Key: BTreeKey;
  type Value: BTreeValue;

  fn id(&self) -> &str;
}

pub trait BTreeFactory: MaybeSend + MaybeSync {
  type BTree<K, V>: BTree<K, V>
  where
    K: BTreeKey,
    V: BTreeValue;

  fn create<D>(
    &self,
    definition: &D,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::BTree<D::Key, D::Value>>>
  where
    D: BTreeDefinition;
}

pub trait BTreeManagerAnyBTree: Any + MaybeSend + MaybeSync + 'static {}

impl<T> BTreeManagerAnyBTree for T where T: Any + MaybeSend + MaybeSync + 'static {}

pub trait BTreeManager<F>: MaybeSend + MaybeSync
where
  F: BTreeFactory,
{
  fn entry<D>(
    &self,
    definition: &D,
  ) -> impl MaybeSendFuture<Output = BTreeResult<F::BTree<D::Key, D::Value>>>
  where
    D: BTreeDefinition;

  fn remove<D>(&self, definition: &D) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    D: BTreeDefinition;
}
