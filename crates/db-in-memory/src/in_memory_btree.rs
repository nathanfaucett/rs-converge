use async_lock::RwLock;
use async_stream::stream;
use core::{borrow::Borrow, mem::take, ops::RangeBounds};
use db_core::{
  BTree, BTreeError, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor, MaybeSend,
  TransactionPatch,
};
use futures::Stream;

#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeMap, sync::Arc};
#[cfg(feature = "std")]
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct InMemoryBTree<K, V> {
  inner: Arc<RwLock<BTreeMap<K, V>>>,
}

impl<K, V> InMemoryBTree<K, V> {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(BTreeMap::new())),
    }
  }

  pub fn with_map(map: BTreeMap<K, V>) -> Self {
    Self {
      inner: Arc::new(RwLock::new(map)),
    }
  }
}

impl<K, V> Default for InMemoryBTree<K, V>
where
  K: Ord,
{
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> BTreeReadExecutor<K, V> for InMemoryBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let inner = self.inner.clone();
    let guard = inner.read().await;
    Ok(guard.get(key.borrow()).cloned())
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = Result<(K, V), BTreeError>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let inner = self.inner.clone();
    stream! {
        let guard = inner.read().await;
        for (key, value) in guard.range(range) {
            yield Ok((key.clone(), value.clone()));
        }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for InMemoryBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    let inner = self.inner.clone();
    let mut guard = inner.write().await;
    guard.insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let inner = self.inner.clone();
    let mut guard = inner.write().await;
    Ok(guard.remove(key.borrow()))
  }
}

#[derive(Debug)]
pub struct InMemoryBTreeTransaction<K, V> {
  inner: Arc<RwLock<BTreeMap<K, V>>>,
  patch: Arc<RwLock<TransactionPatch<K, V>>>,
}

impl<K, V> BTreeTransaction<K, V> for InMemoryBTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    let patch = take(&mut *self.patch.write().await);
    patch.commit(&mut *self.inner.write().await);
    Ok(())
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    let _ = take(&mut *self.patch.write().await);
    Ok(())
  }
}

impl<K, V> BTreeReadExecutor<K, V> for InMemoryBTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let value = self.patch.read().await.get(&*self.inner.read().await, &key);
    Ok(value)
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = Result<(K, V), BTreeError>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let inner = self.inner.clone();
    let patch = self.patch.clone();

    stream! {
      let inner_guard = inner.read().await;
      let patch_guard = patch.read().await;

      let merged = patch_guard.range(&*inner_guard, range);

      for (key, value) in merged {
        yield Ok((key, value));
      }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for InMemoryBTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError>
  where
    K: Ord,
  {
    self.patch.write().await.insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<V>, BTreeError>
  where
    K: Ord + Clone,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let value = self
      .patch
      .write()
      .await
      .remove(&*self.inner.read().await, key);
    Ok(value)
  }
}

impl<K, V> BTree<K, V> for InMemoryBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Transaction = InMemoryBTreeTransaction<K, V>;

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let inner = self.inner.clone();
    Ok(InMemoryBTreeTransaction {
      inner,
      patch: Arc::new(RwLock::new(TransactionPatch::default())),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_core::block_on;
  use futures::{StreamExt, pin_mut};

  #[cfg(not(feature = "std"))]
  use alloc::vec::Vec;

  #[test]
  fn transaction_commit_and_rollback() {
    block_on(async {
      let mut store = InMemoryBTree::new();
      store
        .insert(1, 100)
        .await
        .expect("insert initial value into store");
      let mut tx = store.transaction().await.expect("start transaction");
      tx.insert(2, 200)
        .await
        .expect("insert value in transaction");
      tx.remove(&1).await.expect("remove failed");
      tx.commit().await.expect("commit transaction");

      assert_eq!(store.get(&1).await.expect("get failed"), None);
      assert_eq!(store.get(&2).await.expect("get failed"), Some(200));
    });
  }

  #[test]
  fn transaction_range_merges_pending_changes() {
    block_on(async {
      let mut store = InMemoryBTree::new();
      store
        .insert(1, 100)
        .await
        .expect("insert initial value into store");
      store
        .insert(3, 300)
        .await
        .expect("insert second value into store");

      let mut tx = store.transaction().await.expect("start transaction");
      tx.insert(2, 200)
        .await
        .expect("insert value in transaction");
      tx.remove(&3).await.expect("remove failed");

      let mut values = Vec::new();
      let stream = tx.range(0..10);
      pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (key, value) = item.expect("range item failed");
        values.push((key, value));
      }

      assert_eq!(values, Vec::from([(1, 100), (2, 200)]));
    });
  }

  #[test]
  fn transaction_get_honors_pending_delete() {
    block_on(async {
      let mut store = InMemoryBTree::new();
      store
        .insert(1, 100)
        .await
        .expect("insert initial value into store");

      let mut tx = store.transaction().await.expect("start transaction");
      tx.remove(&1).await.expect("remove failed");

      assert_eq!(tx.get(&1).await.expect("get failed"), None);
    });
  }
}
