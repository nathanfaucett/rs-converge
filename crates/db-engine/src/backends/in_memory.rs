use core::ops::RangeBounds;

use db_core::{BTreeResult, MaybeSend};
use db_in_memory::{InMemoryNamedBTree, InMemoryNamedTransaction};
use futures::Stream;

use crate::store_adapter::{EngineNamedTreeBackend, EngineNamedTreeTransaction};

impl<K, V> EngineNamedTreeTransaction<K, V> for InMemoryNamedTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    self.named_get(tree, key).await
  }

  async fn insert<'a>(&'a mut self, tree: &'a str, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    self.named_insert(tree, key, value).await
  }

  async fn remove<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    self.named_remove(tree, key).await
  }

  fn range<'a, R>(&'a self, tree: &'a str, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    self.named_range(tree, range)
  }

  async fn commit(self) -> BTreeResult<()> {
    self.named_commit().await
  }

  async fn rollback(self) -> BTreeResult<()> {
    self.named_rollback().await
  }
}

impl<K, V> EngineNamedTreeBackend<K, V> for InMemoryNamedBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Transaction = InMemoryNamedTransaction<K, V>;

  fn begin_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + '_ {
    self.begin_named_transaction()
  }
}
