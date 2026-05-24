#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::future::Future;
use db_core::{MaybeSend, MaybeSync};

use super::EngineNamedTreeBackend;

use super::backend_contract::TransactionContract;
use super::named_tree::NamedTreeEngineTransaction;
use crate::{EngineError, EngineKey};

/// Engine-level backend contract.
///
/// This trait isolates the engine from the raw named-tree backend implementation.
pub trait EngineStore: Clone + MaybeSend + MaybeSync + 'static {
  type Transaction: super::EngineStoreTransaction + MaybeSend + 'static;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>>;

  fn engine_read_transaction(
    &self,
  ) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    self.engine_transaction()
  }

  /// Return the transactional contract this backend honors.
  /// Must be consistent across all calls and implementations.
  fn transaction_contract(&self) -> TransactionContract {
    TransactionContract::coupled_multi_tree()
  }
}

/// Explicit adapter for named-tree backends.
///
/// This is the engine-level wrapper that maps raw named-tree storage into the
/// engine store contract.
pub struct NamedTreeEngineStore<T>
where
  T: Clone + EngineNamedTreeBackend<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  inner: T,
}

impl<T> NamedTreeEngineStore<T>
where
  T: Clone + EngineNamedTreeBackend<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  pub fn new(inner: T) -> Self {
    Self { inner }
  }

  pub fn inner(&self) -> &T {
    &self.inner
  }
}

impl<T> Clone for NamedTreeEngineStore<T>
where
  T: Clone + EngineNamedTreeBackend<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<T> From<T> for NamedTreeEngineStore<T>
where
  T: Clone + EngineNamedTreeBackend<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  fn from(inner: T) -> Self {
    NamedTreeEngineStore::new(inner)
  }
}

impl<T> EngineStore for NamedTreeEngineStore<T>
where
  T: Clone + EngineNamedTreeBackend<EngineKey, Vec<u8>> + MaybeSend + MaybeSync + 'static,
{
  type Transaction = NamedTreeEngineTransaction<T::Transaction>;

  fn engine_transaction(&self) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    let inner = &self.inner;
    async move {
      inner
        .begin_transaction()
        .await
        .map(NamedTreeEngineTransaction::new)
        .map_err(EngineError::from)
    }
  }

  fn engine_read_transaction(
    &self,
  ) -> impl Future<Output = Result<Self::Transaction, EngineError>> {
    let inner = &self.inner;
    async move {
      inner
        .begin_read_transaction()
        .await
        .map(NamedTreeEngineTransaction::new)
        .map_err(EngineError::from)
    }
  }
}
