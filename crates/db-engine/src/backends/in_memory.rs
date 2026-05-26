use async_stream::stream;
use core::ops::RangeBounds;

use db_core::{BTreeReadExecutor, BTreeResult, BTreeWriteExecutor, MaybeSend, NamedBTreeMap};
use db_in_memory::InMemoryNamedBTree;
use futures::{Stream, StreamExt, pin_mut};

use crate::store_adapter::{EngineNamedTreeBackend, EngineNamedTreeTransaction};

#[derive(Clone)]
pub struct InMemoryNamedTreeEngineTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  store: InMemoryNamedBTree<K, V>,
}

impl<K, V> EngineNamedTreeTransaction<K, V> for InMemoryNamedTreeEngineTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let named_tree = self.store.get_tree(tree).await?;
    named_tree.get(key).await
  }

  async fn insert<'a>(&'a mut self, tree: &'a str, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let mut named_tree = self.store.get_tree(tree).await?;
    named_tree.insert(key, value).await
  }

  async fn remove<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let mut named_tree = self.store.get_tree(tree).await?;
    named_tree.remove(key).await
  }

  fn range<'a, R>(&'a self, tree: &'a str, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let store = self.store.clone();
    let name = tree.to_string();

    stream! {
      let named_tree = match store.get_tree(&name).await {
        Ok(tree) => tree,
        Err(error) => { yield Err(error); return; }
      };

      let rows = named_tree.range(range);
      pin_mut!(rows);
      while let Some(item) = rows.next().await {
        yield item;
      }
    }
  }

  async fn commit(self) -> BTreeResult<()> {
    Ok(())
  }

  async fn rollback(self) -> BTreeResult<()> {
    Ok(())
  }
}

impl<K, V> EngineNamedTreeBackend<K, V> for InMemoryNamedBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Transaction = InMemoryNamedTreeEngineTransaction<K, V>;

  fn begin_transaction(
    &self,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Transaction>> + '_ {
    let store = self.clone();
    async move { Ok(InMemoryNamedTreeEngineTransaction { store }) }
  }
}
