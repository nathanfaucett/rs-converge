use core::ops::RangeBounds;

use db_core::{BTreeResult, MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};

pub trait EngineNamedTreeTransaction<K, V>: MaybeSend + 'static {
  fn get<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a K,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Option<V>>> + 'a
  where
    K: Ord;

  fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: K,
    value: V,
  ) -> impl MaybeSendFuture<Output = BTreeResult<()>> + 'a
  where
    K: Ord;

  fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a K,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Option<V>>> + 'a
  where
    K: Ord;

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl MaybeSendStream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: RangeBounds<K> + MaybeSend + 'a;

  fn commit(self) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    Self: Sized;

  fn rollback(self) -> impl MaybeSendFuture<Output = BTreeResult<()>>
  where
    Self: Sized;
}

pub trait EngineNamedTreeBackend<K, V>: Clone + MaybeSend + MaybeSync {
  type Transaction: EngineNamedTreeTransaction<K, V>;

  fn begin_transaction(&self)
  -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + '_;

  fn begin_read_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + '_ {
    self.begin_transaction()
  }
}
