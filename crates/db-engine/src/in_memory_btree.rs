#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeMap, sync::Arc};
#[cfg(feature = "std")]
use std::{collections::BTreeMap, sync::Arc};

use async_lock::RwLock;
use async_stream::stream;
use core::{any::Any, borrow::Borrow, mem::take, ops::RangeBounds};
use futures::Stream;

use crate::{
  BTree, BTreeDefinition, BTreeError, BTreeManager, BTreeReadExecutor, BTreeResult,
  BTreeTransaction, BTreeWriteExecutor, MaybeSend, MaybeSync,
};

#[derive(Debug)]
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

impl<K, V> Clone for InMemoryBTree<K, V> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
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
  K: Clone + Ord + MaybeSend + MaybeSync + 'static,
  V: Clone + MaybeSend + MaybeSync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let guard = self.inner.read().await;
    Ok(guard.get(key.borrow()).cloned())
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
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
  K: Clone + Ord + MaybeSend + MaybeSync + 'static,
  V: Clone + MaybeSend + MaybeSync + 'static,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let mut guard = self.inner.write().await;
    guard.insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let mut guard = self.inner.write().await;
    Ok(guard.remove(key.borrow()))
  }
}

#[derive(Debug, Clone)]
pub enum InMemoryTransactionPatchEntry<V> {
  Present(V),
  Deleted,
}

impl<V> InMemoryTransactionPatchEntry<V> {
  pub fn as_option(&self) -> Option<&V> {
    match self {
      InMemoryTransactionPatchEntry::Present(value) => Some(value),
      InMemoryTransactionPatchEntry::Deleted => None,
    }
  }
}

#[derive(Debug, Clone)]
pub struct InMemoryTransactionPatch<K, V>(BTreeMap<K, InMemoryTransactionPatchEntry<V>>);

impl<K, V> Default for InMemoryTransactionPatch<K, V> {
  fn default() -> Self {
    Self(BTreeMap::new())
  }
}

impl<K, V> InMemoryTransactionPatch<K, V> {
  pub fn get<Q>(&self, base: &BTreeMap<K, V>, key: &Q) -> Option<V>
  where
    K: Ord,
    V: Clone,
    Q: Borrow<K>,
  {
    match self.0.get(key.borrow()) {
      Some(entry) => entry.as_option().cloned(),
      None => base.get(key.borrow()).cloned(),
    }
  }

  pub fn insert(&mut self, key: K, value: V)
  where
    K: Ord,
  {
    self
      .0
      .insert(key, InMemoryTransactionPatchEntry::Present(value));
  }

  pub fn remove<Q>(&mut self, base: &BTreeMap<K, V>, key: Q) -> Option<V>
  where
    K: Ord + Clone,
    V: Clone,
    Q: Borrow<K>,
  {
    match self.0.get(key.borrow()) {
      Some(InMemoryTransactionPatchEntry::Present(value)) => {
        let removed = value.clone();
        self
          .0
          .insert(key.borrow().clone(), InMemoryTransactionPatchEntry::Deleted);
        Some(removed)
      }
      Some(InMemoryTransactionPatchEntry::Deleted) => None,
      None => {
        if let Some(existing) = base.get(key.borrow()) {
          self
            .0
            .insert(key.borrow().clone(), InMemoryTransactionPatchEntry::Deleted);
          Some(existing.clone())
        } else {
          None
        }
      }
    }
  }

  pub fn commit(self, base: &mut BTreeMap<K, V>)
  where
    K: Ord,
  {
    for (key, entry) in self.0 {
      match entry {
        InMemoryTransactionPatchEntry::Present(value) => {
          base.insert(key, value);
        }
        InMemoryTransactionPatchEntry::Deleted => {
          base.remove(&key);
        }
      }
    }
  }

  pub fn range<R>(&self, base: &BTreeMap<K, V>, range: R) -> BTreeMap<K, V>
  where
    K: Ord + Clone,
    V: Clone,
    R: RangeBounds<K>,
  {
    merge_range_maps(
      base,
      &self.0,
      range,
      |k| !matches!(self.0.get(k), Some(InMemoryTransactionPatchEntry::Deleted)),
      |k, entry, merged| match entry {
        InMemoryTransactionPatchEntry::Present(value) => {
          merged.insert(k.clone(), value.clone());
        }
        InMemoryTransactionPatchEntry::Deleted => {
          merged.remove(k);
        }
      },
    )
  }
}

fn merge_range_maps<K, V, P, R, FInclude, FApply>(
  base: &BTreeMap<K, V>,
  patch: &BTreeMap<K, P>,
  range: R,
  mut include_base: FInclude,
  mut apply_patch: FApply,
) -> BTreeMap<K, V>
where
  K: Ord + Clone,
  V: Clone,
  R: RangeBounds<K>,
  FInclude: FnMut(&K) -> bool,
  FApply: FnMut(&K, &P, &mut BTreeMap<K, V>),
{
  struct RangeBoundsRef<'a, R>(&'a R);

  impl<'a, R> Copy for RangeBoundsRef<'a, R> {}

  impl<'a, R> Clone for RangeBoundsRef<'a, R> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, T: ?Sized, R> RangeBounds<T> for RangeBoundsRef<'a, R>
  where
    R: RangeBounds<T>,
  {
    fn start_bound(&self) -> core::ops::Bound<&T> {
      self.0.start_bound()
    }

    fn end_bound(&self) -> core::ops::Bound<&T> {
      self.0.end_bound()
    }
  }

  let range_ref = RangeBoundsRef(&range);

  let mut merged = BTreeMap::new();

  for (k, v) in base.range(range_ref) {
    if include_base(k) {
      merged.insert(k.clone(), v.clone());
    }
  }

  for (k, p) in patch.range(range_ref) {
    apply_patch(k, p, &mut merged);
  }

  merged
}

#[derive(Debug)]
pub struct InMemoryBTreeTransaction<K, V> {
  inner: Arc<RwLock<BTreeMap<K, V>>>,
  patch: Arc<RwLock<InMemoryTransactionPatch<K, V>>>,
}

impl<K, V> BTreeTransaction<K, V> for InMemoryBTreeTransaction<K, V>
where
  K: Clone + Ord + MaybeSend + MaybeSync + 'static,
  V: Clone + MaybeSend + MaybeSync + 'static,
{
  async fn commit(self) -> BTreeResult<()> {
    let patch = take(&mut *self.patch.write().await);
    patch.commit(&mut *self.inner.write().await);
    Ok(())
  }

  async fn rollback(self) -> BTreeResult<()> {
    let _ = take(&mut *self.patch.write().await);
    Ok(())
  }
}

impl<K, V> BTreeReadExecutor<K, V> for InMemoryBTreeTransaction<K, V>
where
  K: Clone + Ord + MaybeSend + MaybeSync + 'static,
  V: Clone + MaybeSend + MaybeSync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let guard = self.inner.read().await;
    let patch_guard = self.patch.read().await;
    Ok(patch_guard.get(&*guard, &key))
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let inner = self.inner.clone();
    let patch = self.patch.clone();

    stream! {
      let guard = inner.read().await;
      let patch_guard = patch.read().await;
      let merged = patch_guard.range(&*guard, range);

      for (key, value) in merged {
        yield Ok((key, value));
      }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for InMemoryBTreeTransaction<K, V>
where
  K: Clone + Ord + MaybeSend + MaybeSync + 'static,
  V: Clone + MaybeSend + MaybeSync + 'static,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let patch = self.patch.clone();
    patch.write().await.insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord + Clone,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let inner = self.inner.clone();
    let patch = self.patch.clone();
    let key_owned = key.borrow().clone();

    let mut guard = patch.write().await;
    Ok(guard.remove(&*inner.read().await, key_owned))
  }
}

impl<K, V> BTree<K, V> for InMemoryBTree<K, V>
where
  K: Clone + Ord + MaybeSend + MaybeSync + 'static,
  V: Clone + MaybeSend + MaybeSync + 'static,
{
  type Transaction = InMemoryBTreeTransaction<K, V>;

  async fn create<D>(_: &D) -> BTreeResult<Self>
  where
    Self: Sized,
    D: BTreeDefinition<Key = K, Value = V>,
  {
    Ok(Self::new())
  }

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let inner = self.inner.clone();
    Ok(InMemoryBTreeTransaction {
      inner,
      patch: Arc::new(RwLock::new(InMemoryTransactionPatch::default())),
    })
  }
}

trait InMemoryBTreeManagerValue: Any + MaybeSend + MaybeSync + 'static {}

impl<T> InMemoryBTreeManagerValue for T where T: Any + MaybeSend + MaybeSync + 'static {}

pub struct InMemoryBTreeManager {
  inner: Arc<RwLock<BTreeMap<String, Box<dyn InMemoryBTreeManagerValue>>>>,
}

impl BTreeManager for InMemoryBTreeManager {
  type BTree<K, V> = InMemoryBTree<K, V>;

  async fn get<D>(&self, definition: &D) -> BTreeResult<Self::BTree<D::Key, D::Value>>
  where
    D: BTreeDefinition,
    <D as BTreeDefinition>::Key: Clone + Ord + MaybeSend + MaybeSync + 'static,
    <D as BTreeDefinition>::Value: Clone + MaybeSend + MaybeSync + 'static,
  {
    if let Some(btree_box) = self.inner.read().await.get(definition.id()) {
      let btree_any = btree_box.as_ref() as &dyn Any;

      if let Some(typed) = btree_any.downcast_ref::<InMemoryBTree<D::Key, D::Value>>() {
        return Ok(typed.clone());
      } else {
        return Err(BTreeError::TypeMismatch);
      }
    } else {
      let btree = InMemoryBTree::<D::Key, D::Value>::new();
      let btree_any: Box<dyn InMemoryBTreeManagerValue> = Box::new(btree.clone());

      self
        .inner
        .write()
        .await
        .insert(definition.id().to_string(), btree_any);

      Ok(btree)
    }
  }

  async fn insert<D>(&self, definition: &D) -> BTreeResult<Self::BTree<D::Key, D::Value>>
  where
    D: BTreeDefinition,
    <D as BTreeDefinition>::Key: Clone + Ord + MaybeSend + MaybeSync + 'static,
    <D as BTreeDefinition>::Value: Clone + MaybeSend + MaybeSync + 'static,
  {
    self.get(definition).await
  }

  async fn remove<D>(&self, definition: &D) -> BTreeResult<()>
  where
    D: BTreeDefinition,
    <D as BTreeDefinition>::Key: Clone + Ord + MaybeSend + MaybeSync + 'static,
    <D as BTreeDefinition>::Value: Clone + MaybeSend + MaybeSync + 'static,
  {
    self.inner.write().await.remove(definition.id());
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use futures::{StreamExt, executor::block_on, pin_mut};

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

  #[test]
  fn transaction_rollback_discards_changes() {
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
      tx.rollback().await.expect("rollback transaction");

      assert_eq!(store.get(&1).await.expect("get failed"), Some(100));
      assert_eq!(store.get(&2).await.expect("get failed"), None);
    });
  }

  #[test]
  fn transaction_remove_pending_insert_returns_old_value() {
    block_on(async {
      let store = InMemoryBTree::new();
      let mut tx = store.transaction().await.expect("start transaction");
      tx.insert(1, 100)
        .await
        .expect("insert value in transaction");

      assert_eq!(tx.remove(&1).await.expect("remove failed"), Some(100));
      assert_eq!(tx.get(&1).await.expect("get failed"), None);

      tx.commit().await.expect("commit transaction");
      assert_eq!(store.get(&1).await.expect("get failed"), None);
    });
  }
}
