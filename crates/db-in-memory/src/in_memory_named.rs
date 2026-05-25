use async_lock::RwLock;
use async_stream::stream;
use core::{borrow::Borrow, ops::RangeBounds};
use db_core::{
  BTree, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor, MaybeSend,
  NamedBTreeMap, TransactionEntry, TransactionPatch,
};
use futures::Stream;

use crate::patch_map::{
  commit_patch_into_map, get_from_patch_then_map, merge_patch_range, remove_from_patch_then_map,
};

#[cfg(not(feature = "std"))]
use alloc::{
  collections::BTreeMap,
  string::{String, ToString},
  sync::Arc,
  vec::Vec,
};
#[cfg(feature = "std")]
use std::{collections::BTreeMap, string::String, sync::Arc};

type Inner<K, V> = Arc<RwLock<BTreeMap<String, BTreeMap<K, V>>>>;

macro_rules! tree_or_empty {
  ($result:ident, $guard:expr, $name:expr) => {
    let empty = BTreeMap::new();
    let $result = $guard.get($name).unwrap_or(&empty);
  };
}

/// A named-tree provider backed by in-memory storage.
///
/// Each distinct name maps to an independent sub-tree. Trees are created
/// lazily on first access. All clones share the same underlying storage.
#[derive(Clone)]
pub struct InMemoryNamedBTree<K, V> {
  inner: Inner<K, V>,
}

impl<K, V> InMemoryNamedBTree<K, V> {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(BTreeMap::new())),
    }
  }
}

impl<K, V> Default for InMemoryNamedBTree<K, V>
where
  K: Ord,
{
  fn default() -> Self {
    Self::new()
  }
}

/// A multi-tree transaction that buffers changes per named tree and applies
/// them all atomically on commit by acquiring a single write lock.
pub struct InMemoryNamedTransaction<K, V> {
  pub(crate) inner: Inner<K, V>,
  pub(crate) patches: BTreeMap<String, TransactionPatch<K, V>>,
}

#[derive(Clone)]
pub struct InMemoryNamedTree<K, V> {
  inner: Inner<K, V>,
  name: String,
}

pub struct InMemoryNamedTreeTransaction<K, V> {
  inner: Inner<K, V>,
  name: String,
  patch: TransactionPatch<K, V>,
}

impl<K, V> BTreeReadExecutor<K, V> for InMemoryNamedTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let guard = self.inner.read().await;
    Ok(
      guard
        .get(&self.name)
        .and_then(|m| m.get(key.borrow()))
        .cloned(),
    )
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let inner = Arc::clone(&self.inner);
    let name = self.name.clone();

    stream! {
      let guard = inner.read().await;
      if let Some(tree) = guard.get(&name) {
        for (key, value) in tree.range(range) {
          yield Ok((key.clone(), value.clone()));
        }
      }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for InMemoryNamedTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let mut guard = self.inner.write().await;
    guard
      .entry(self.name.clone())
      .or_default()
      .insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let mut guard = self.inner.write().await;
    Ok(
      guard
        .get_mut(&self.name)
        .and_then(|m| m.remove(key.borrow())),
    )
  }
}

impl<K, V> BTreeTransaction<K, V> for InMemoryNamedTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn commit(self) -> BTreeResult<()> {
    let mut guard = self.inner.write().await;
    let tree = guard.entry(self.name).or_default();
    commit_patch_into_map(self.patch, tree);
    Ok(())
  }

  async fn rollback(self) -> BTreeResult<()> {
    Ok(())
  }
}

impl<K, V> BTreeReadExecutor<K, V> for InMemoryNamedTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let guard = self.inner.read().await;
    tree_or_empty!(tree, guard, &self.name);
    Ok(get_from_patch_then_map(&self.patch, tree, key.borrow()))
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord + Clone,
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    let inner = Arc::clone(&self.inner);
    let name = self.name.clone();
    let patch = self.patch.clone();

    stream! {
      let guard = inner.read().await;
      tree_or_empty!(tree, guard, &name);
      let merged = merge_patch_range(&patch, tree, range);
      for (key, value) in merged {
        yield Ok((key, value));
      }
    }
  }
}

impl<K, V> BTreeWriteExecutor<K, V> for InMemoryNamedTreeTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  async fn insert(&mut self, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    self.patch.insert(key, value);
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    K: Ord + Clone,
    Q: Borrow<K> + MaybeSend + 'a,
  {
    let guard = self.inner.read().await;
    tree_or_empty!(tree, guard, &self.name);
    Ok(remove_from_patch_then_map(
      &mut self.patch,
      tree,
      key.borrow(),
    ))
  }
}

impl<K, V> BTree<K, V> for InMemoryNamedTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Transaction = InMemoryNamedTreeTransaction<K, V>;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    Ok(InMemoryNamedTreeTransaction {
      inner: Arc::clone(&self.inner),
      name: self.name.clone(),
      patch: TransactionPatch::default(),
    })
  }
}

impl<K, V> InMemoryNamedTransaction<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  pub async fn named_get(&mut self, tree: &str, key: &K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    if let Some(patch) = self.patches.get(tree) {
      match patch.get(key) {
        Some(TransactionEntry::Present(v)) => return Ok(Some(v.clone())),
        Some(TransactionEntry::Deleted) => return Ok(None),
        None => {}
      }
    }
    let guard = self.inner.read().await;
    Ok(guard.get(tree).and_then(|m| m.get(key)).cloned())
  }

  pub async fn named_insert(&mut self, tree: &str, key: K, value: V) -> BTreeResult<()>
  where
    K: Ord,
  {
    let patch = self.patches.entry(tree.to_string()).or_default();
    patch.insert(key, value);
    Ok(())
  }

  pub async fn named_remove(&mut self, tree: &str, key: &K) -> BTreeResult<Option<V>>
  where
    K: Ord,
  {
    let tree_owned = tree.to_string();
    {
      let patch = self.patches.entry(tree_owned.clone()).or_default();
      if let Some(existing) = patch.remove(key.clone()) {
        return Ok(Some(existing));
      }
    }

    let guard = self.inner.read().await;
    tree_or_empty!(sub_map, guard, tree);
    let patch = self.patches.entry(tree_owned).or_default();
    Ok(remove_from_patch_then_map(patch, sub_map, key))
  }

  pub fn named_range<'a, R>(
    &'a self,
    tree: &'a str,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(K, V)>> + 'a
  where
    K: Ord,
    R: core::ops::RangeBounds<K> + MaybeSend + 'a,
  {
    let inner = Arc::clone(&self.inner);
    let patch = self.patches.get(tree).cloned().unwrap_or_default();
    let tree = tree.to_string();

    stream! {
      let guard = inner.read().await;
      tree_or_empty!(sub_map, guard, &tree);
      let merged = merge_patch_range(&patch, sub_map, range);
      for (k, v) in merged {
        yield Ok((k, v));
      }
    }
  }

  pub async fn named_commit(self) -> BTreeResult<()> {
    let mut guard = self.inner.write().await;
    for (name, patch) in self.patches {
      let sub = guard.entry(name).or_default();
      commit_patch_into_map(patch, sub);
    }
    Ok(())
  }

  pub async fn named_rollback(self) -> BTreeResult<()> {
    Ok(())
  }
}

impl<K, V> InMemoryNamedBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  pub async fn begin_named_transaction(&self) -> BTreeResult<InMemoryNamedTransaction<K, V>> {
    Ok(InMemoryNamedTransaction {
      inner: Arc::clone(&self.inner),
      patches: BTreeMap::new(),
    })
  }
}

impl<K, V> NamedBTreeMap<K, V> for InMemoryNamedBTree<K, V>
where
  K: Clone + Ord + Send + Sync + 'static,
  V: Clone + Send + Sync + 'static,
{
  type Tree = InMemoryNamedTree<K, V>;

  fn get_tree<'a>(
    &'a self,
    name: &str,
  ) -> impl core::future::Future<Output = BTreeResult<InMemoryNamedTree<K, V>>> + 'a {
    let owned = name.to_string();
    let inner = Arc::clone(&self.inner);
    async move {
      let mut guard = inner.write().await;
      guard.entry(owned.clone()).or_default();
      drop(guard);
      Ok(InMemoryNamedTree { inner, name: owned })
    }
  }

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let target = name.to_string();
    let inner = Arc::clone(&self.inner);
    async move {
      let guard = tree.inner.read().await;
      let data = guard.get(&tree.name).cloned().unwrap_or_default();
      drop(guard);
      let mut guard = inner.write().await;
      guard.insert(target, data);
      Ok(())
    }
  }

  fn delete_tree(&self, name: &str) -> impl core::future::Future<Output = BTreeResult<()>> + '_ {
    let target = name.to_string();
    let inner = Arc::clone(&self.inner);
    async move {
      let mut guard = inner.write().await;
      guard.remove(&target);
      Ok(())
    }
  }

  fn list_names<'a>(&'a self) -> impl core::future::Future<Output = Vec<String>> + 'a {
    let inner = Arc::clone(&self.inner);
    async move {
      let guard = inner.read().await;
      guard.keys().cloned().collect()
    }
  }
}

#[cfg(test)]
mod tests {
  use db_core::block_on;

  use super::*;

  #[test]
  fn get_tree_returns_shared_isolated_trees() {
    block_on(async {
      let provider = InMemoryNamedBTree::<u64, u64>::new();
      let mut first = provider.get_tree("first").await.expect("first tree");
      let first_again = provider.get_tree("first").await.expect("first again");
      let second = provider.get_tree("second").await.expect("second tree");

      first.insert(1, 10).await.expect("insert first");

      assert_eq!(
        first_again.get(&1).await.expect("get first again"),
        Some(10)
      );
      assert_eq!(second.get(&1).await.expect("get second"), None);
    });
  }

  #[test]
  fn named_transaction_commits_across_trees() {
    block_on(async {
      let provider = InMemoryNamedBTree::<u64, u64>::new();
      let mut tx = provider.begin_named_transaction().await.expect("begin");

      tx.named_insert("first", 1, 10).await.expect("insert first");
      tx.named_insert("second", 1, 20)
        .await
        .expect("insert second");
      tx.named_commit().await.expect("commit");

      let first = provider.get_tree("first").await.expect("first tree");
      let second = provider.get_tree("second").await.expect("second tree");

      assert_eq!(first.get(&1).await.expect("get first"), Some(10));
      assert_eq!(second.get(&1).await.expect("get second"), Some(20));
    });
  }

  #[test]
  fn named_tree_transaction_is_scoped_to_one_tree() {
    block_on(async {
      let provider = InMemoryNamedBTree::<u64, u64>::new();
      let first = provider.get_tree("first").await.expect("first tree");
      let second = provider.get_tree("second").await.expect("second tree");
      let mut tx = first.transaction().await.expect("begin tree tx");

      tx.insert(1, 10).await.expect("insert");
      tx.commit().await.expect("commit");

      assert_eq!(first.get(&1).await.expect("get first"), Some(10));
      assert_eq!(second.get(&1).await.expect("get second"), None);
    });
  }
}
