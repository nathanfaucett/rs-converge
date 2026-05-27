use async_stream::stream;
use core::borrow::Borrow;
use core::ops::RangeBounds;
use db_core::{BTree, BTreeError, BTreeResult, MaybeSend, NamedBTreeMap};
use db_engine::{EngineKey, EngineStoreBackend, EngineStoreTransaction};
use db_in_memory::InMemoryNamedBTree;
use futures::{Stream, StreamExt, pin_mut};

use crate::store_adapter::{StoreAdapterCallbacks, StoreAdapterTransaction, StoreAdapterTree};

#[derive(Clone)]
pub enum PluggableBackendStore {
  InMemory(InMemoryNamedBTree<EngineKey, Vec<u8>>),
  External(StoreAdapterCallbacks),
}

pub enum PluggableBackendTransaction {
  InMemory(InMemoryNamedTreeBackendTransaction<EngineKey, Vec<u8>>),
  External(StoreAdapterTransaction),
}

#[derive(Clone)]
pub struct InMemoryNamedTreeBackendTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  store: InMemoryNamedBTree<K, V>,
}

impl<K, V> EngineStoreTransaction<K, V> for InMemoryNamedTreeBackendTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a>(&'a mut self, tree: &'a str, key: &'a K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let mut named_tree = self.store.get_tree(tree).await?;
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

      let mut rows = named_tree.range(range);
      pin_mut!(rows);
      while let Some(item) = rows.next().await {
        yield item;
      }
    }
  }

  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    Ok(())
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    Ok(())
  }
}

pub enum PluggableBackendTree {
  External(StoreAdapterTree),
}

impl NamedBTreeMap<EngineKey, Vec<u8>> for PluggableBackendStore {
  type Tree = PluggableBackendTree;

  fn get_tree(
    &self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<Self::Tree>> + '_ {
    let name = name.to_string();
    async move {
      match self {
        PluggableBackendStore::InMemory(_) => Err(BTreeError::UnsupportedOperation),
        PluggableBackendStore::External(adapter) => {
          let tree = adapter.get_tree(&name).await?;
          Ok(PluggableBackendTree::External(tree))
        }
      }
    }
  }

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    async move {
      match (self, tree) {
        (PluggableBackendStore::External(adapter), PluggableBackendTree::External(tree)) => {
          adapter.insert_tree(name, tree).await
        }
        _ => Err(BTreeError::UnsupportedOperation),
      }
    }
  }

  fn delete_tree(&self, name: &str) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    async move {
      match self {
        PluggableBackendStore::InMemory(store) => store.delete_tree(name).await,
        PluggableBackendStore::External(adapter) => adapter.delete_tree(name).await,
      }
    }
  }

  fn list_names(&self) -> impl core::future::Future<Output = Vec<String>> + '_ {
    async move {
      match self {
        PluggableBackendStore::InMemory(store) => store.list_names().await,
        PluggableBackendStore::External(adapter) => adapter.list_names().await,
      }
    }
  }
}

impl EngineStoreBackend<EngineKey, Vec<u8>> for PluggableBackendStore {
  type Transaction = PluggableBackendTransaction;

  async fn begin_transaction(&self) -> BTreeResult<Self::Transaction> {
    match self {
      PluggableBackendStore::InMemory(store) => Ok(PluggableBackendTransaction::InMemory(
        InMemoryNamedTreeBackendTransaction {
          store: store.clone(),
        },
      )),
      PluggableBackendStore::External(adapter) => {
        let tx = adapter.begin_transaction().await?;
        Ok(PluggableBackendTransaction::External(tx))
      }
    }
  }
}

impl EngineStoreTransaction<EngineKey, Vec<u8>> for PluggableBackendTransaction {
  async fn get<'a>(&'a mut self, tree: &'a str, key: &'a EngineKey) -> BTreeResult<Option<Vec<u8>>>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.get(tree, key).await,
      PluggableBackendTransaction::External(tx) => tx.get(tree, key).await,
    }
  }

  async fn insert<'a>(
    &'a mut self,
    tree: &'a str,
    key: EngineKey,
    value: Vec<u8>,
  ) -> BTreeResult<()>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.insert(tree, key, value).await,
      PluggableBackendTransaction::External(tx) => tx.insert(tree, key, value).await,
    }
  }

  async fn remove<'a>(
    &'a mut self,
    tree: &'a str,
    key: &'a EngineKey,
  ) -> BTreeResult<Option<Vec<u8>>>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.remove(tree, key).await,
      PluggableBackendTransaction::External(tx) => tx.remove(tree, key).await,
    }
  }

  fn range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + MaybeSend + 'a,
  {
    stream! {
      match self {
        PluggableBackendTransaction::InMemory(tx) => {
          let rows = tx.range(tree, range);
          pin_mut!(rows);
          while let Some(item) = rows.next().await {
            yield item;
          }
        }
        PluggableBackendTransaction::External(tx) => {
          let rows = tx.range(tree, range);
          pin_mut!(rows);
          while let Some(item) = rows.next().await {
            yield item;
          }
        }
      }
    }
  }

  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.commit().await,
      PluggableBackendTransaction::External(tx) => EngineStoreTransaction::commit(tx).await,
    }
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    match self {
      PluggableBackendTransaction::InMemory(tx) => tx.rollback().await,
      PluggableBackendTransaction::External(tx) => EngineStoreTransaction::rollback(tx).await,
    }
  }
}

impl db_core::BTreeWriteExecutor<EngineKey, Vec<u8>> for PluggableBackendTree {
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<Vec<u8>>>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + MaybeSend + 'a,
  {
    match self {
      PluggableBackendTree::External(tree) => tree.get(key).await,
    }
  }

  async fn insert(&mut self, key: EngineKey, value: Vec<u8>) -> BTreeResult<()>
  where
    EngineKey: Ord,
  {
    match self {
      PluggableBackendTree::External(tree) => tree.insert(key, value).await,
    }
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<Vec<u8>>>
  where
    EngineKey: Ord,
    Q: Borrow<EngineKey> + MaybeSend + 'a,
  {
    match self {
      PluggableBackendTree::External(tree) => tree.remove(key).await,
    }
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(EngineKey, Vec<u8>)>> + 'a
  where
    EngineKey: Ord,
    R: RangeBounds<EngineKey> + MaybeSend + 'a,
  {
    stream! {
      match self {
        PluggableBackendTree::External(tree) => {
          let rows = tree.range(range);
          pin_mut!(rows);
          while let Some(item) = rows.next().await {
            yield item;
          }
        }
      }
    }
  }
}

impl db_core::BTree<EngineKey, Vec<u8>> for PluggableBackendTree {
  type Transaction = StoreAdapterTransaction;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    match self {
      PluggableBackendTree::External(tree) => tree.transaction().await,
    }
  }
}
