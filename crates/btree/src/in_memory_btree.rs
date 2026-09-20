#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeMap, sync::Arc, vec::Vec};
#[cfg(feature = "std")]
use std::{collections::BTreeMap, sync::Arc};

use async_lock::RwLock;
use async_stream::stream;
use core::{
    borrow::Borrow,
    ops::{Bound, RangeBounds},
};
use futures::Stream;

use crate::{BTree, BTreeKey, BTreeRead, BTreeResult, BTreeTransaction, BTreeValue};

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

impl<K, V> Default for InMemoryBTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> BTreeRead<K, V> for InMemoryBTree<K, V>
where
    K: BTreeKey + Clone + Send + Sync,
    V: BTreeValue + Clone + Send + Sync,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        let guard = self.inner.read().await;
        Ok(guard.get(key).cloned())
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream!({
            for (key, value) in self.inner.read().await.range(range) {
                yield Ok((key.clone(), value.clone()));
            }
        })
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

#[derive(Debug)]
pub struct InMemoryTransactionPatch<K, V> {
    inner: BTreeMap<K, InMemoryTransactionPatchEntry<V>>,
}

impl<K, V> Default for InMemoryTransactionPatch<K, V> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K, V> InMemoryTransactionPatch<K, V>
where
    K: BTreeKey + Clone,
    V: BTreeValue + Clone,
{
    pub fn get(&self, base: &BTreeMap<K, V>, key: &K) -> Option<V> {
        match self.inner.get(key) {
            Some(entry) => entry.as_option().cloned(),
            None => base.get(key).cloned(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.inner
            .insert(key, InMemoryTransactionPatchEntry::Present(value));
    }

    pub fn remove(&mut self, base: &BTreeMap<K, V>, key: &K) -> Option<V> {
        match self.inner.get_key_value(key) {
            Some((key, InMemoryTransactionPatchEntry::Present(value))) => {
                let removed = value.clone();
                self.inner
                    .insert(key.clone(), InMemoryTransactionPatchEntry::Deleted);
                Some(removed)
            }
            Some((_key, InMemoryTransactionPatchEntry::Deleted)) => None,
            None => {
                if let Some((key, existing)) = base.get_key_value(key.borrow()) {
                    self.inner
                        .insert(key.clone(), InMemoryTransactionPatchEntry::Deleted);
                    Some(existing.clone())
                } else {
                    None
                }
            }
        }
    }

    pub fn commit(self, base: &mut BTreeMap<K, V>) {
        for (key, entry) in self.inner {
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

    pub fn range<Q, R>(&self, base: &BTreeMap<K, V>, range: R) -> BTreeMap<K, V>
    where
        Q: Ord + ?Sized,
        K: Borrow<Q>,
        R: RangeBounds<Q>,
    {
        merge_range_maps(
            base,
            &self.inner,
            range,
            |k| {
                !matches!(
                    self.inner.get(k.borrow()),
                    Some(InMemoryTransactionPatchEntry::Deleted)
                )
            },
            |k, entry, merged| match entry {
                InMemoryTransactionPatchEntry::Present(value) => {
                    merged.insert(k.clone(), value.clone());
                }
                InMemoryTransactionPatchEntry::Deleted => {
                    merged.remove(k.borrow());
                }
            },
        )
    }
}

fn merge_range_maps<K, V, Q, P, R, FInclude, FApply>(
    base: &BTreeMap<K, V>,
    patch: &BTreeMap<K, P>,
    range: R,
    mut include_base: FInclude,
    mut apply_patch: FApply,
) -> BTreeMap<K, V>
where
    K: BTreeKey + Borrow<Q> + Clone,
    V: Clone,
    Q: Ord + ?Sized,
    R: RangeBounds<Q>,
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
        fn start_bound(&self) -> Bound<&T> {
            self.0.start_bound()
        }

        fn end_bound(&self) -> Bound<&T> {
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
    patch: InMemoryTransactionPatch<K, V>,
}

unsafe impl<K, V> Send for InMemoryBTreeTransaction<K, V> {}

unsafe impl<K, V> Sync for InMemoryBTreeTransaction<K, V> {}

impl<K, V> BTreeRead<K, V> for InMemoryBTreeTransaction<K, V>
where
    K: BTreeKey + Clone + Send + Sync,
    V: BTreeValue + Clone + Send + Sync,
{
    async fn get(&self, key: &K) -> BTreeResult<Option<V>> {
        Ok(self.patch.get(&*self.inner.read().await, key))
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream!({
            let merged = self.patch.range(&*self.inner.read().await, range);

            for (key, value) in merged {
                yield Ok((key, value));
            }
        })
    }
}

impl<K, V> BTreeTransaction<K, V> for InMemoryBTreeTransaction<K, V>
where
    K: BTreeKey + Clone + Send + Sync,
    V: BTreeValue + Clone + Send + Sync,
{
    async fn insert(&mut self, key: K, value: V) -> BTreeResult<()>
    where
        K: Ord,
    {
        self.patch.insert(key, value);
        Ok(())
    }

    async fn update<F>(&mut self, key: K, update_fn: F) -> BTreeResult<Option<()>>
    where
        K: Ord,
        F: FnOnce(&mut V) -> BTreeResult<()>,
    {
        if let Some(value) = self.patch.get(&*self.inner.read().await, &key) {
            let mut value_clone = value.clone();
            update_fn(&mut value_clone)?;
            self.patch.insert(key.clone(), value_clone);
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    async fn remove(&mut self, key: &K) -> BTreeResult<Option<V>> {
        Ok(self.patch.remove(&*self.inner.read().await, key))
    }

    fn remove_range<R>(&mut self, range: R) -> impl Stream<Item = BTreeResult<(K, V)>> + Send
    where
        R: RangeBounds<K> + Send,
    {
        stream!({
            let merged = self.patch.range(&*self.inner.read().await, range);

            for (key, value) in merged {
                self.patch.remove(&*self.inner.read().await, &key);
                yield Ok((key, value));
            }
        })
    }

    async fn commit(self) -> BTreeResult<()> {
        let InMemoryBTreeTransaction { inner, patch } = self;
        patch.commit(&mut *inner.write().await);
        Ok(())
    }

    async fn rollback(self) -> BTreeResult<()> {
        Ok(())
    }
}

impl<K, V> BTree<K, V> for InMemoryBTree<K, V>
where
    K: BTreeKey + Clone + Send + Sync,
    V: BTreeValue + Clone + Send + Sync,
{
    type Transaction = InMemoryBTreeTransaction<K, V>;

    async fn transaction(&self) -> BTreeResult<Self::Transaction> {
        Ok(InMemoryBTreeTransaction {
            inner: self.inner.clone(),
            patch: InMemoryTransactionPatch::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "std"))]
    use alloc::vec::Vec;

    use futures::{StreamExt, executor::block_on, pin_mut};

    use super::*;

    #[test]
    fn transaction_commit_and_rollback() {
        block_on(async {
            let store = InMemoryBTree::new();
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.insert(1, 100)
                    .await
                    .expect("insert initial value into store");
                tx.commit().await.expect("commit transaction");
            }
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.insert(2, 200)
                    .await
                    .expect("insert value in transaction");
                tx.remove(&1).await.expect("remove failed");
                tx.rollback().await.expect("rollback transaction");
            }
            assert_eq!(store.get(&1).await.expect("get failed"), Some(100));
            assert_eq!(store.get(&2).await.expect("get failed"), None);
        });
    }

    #[test]
    fn transaction_range_merges_pending_changes() {
        block_on(async {
            let store = InMemoryBTree::new();
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.insert(1, 100)
                    .await
                    .expect("insert initial value into store");
                tx.insert(3, 300)
                    .await
                    .expect("insert second value into store");
                tx.commit().await.expect("commit transaction");
            }
            {
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
            }
        });
    }

    #[test]
    fn transaction_get_honors_pending_delete() {
        block_on(async {
            let store = InMemoryBTree::new();
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.insert(1, 100)
                    .await
                    .expect("insert initial value into store");
                tx.commit().await.expect("commit transaction");
            }
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.remove(&1).await.expect("remove failed");

                assert_eq!(tx.get(&1).await.expect("get failed"), None);
            }
        });
    }

    #[test]
    fn transaction_rollback_discards_changes() {
        block_on(async {
            let store = InMemoryBTree::new();
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.insert(1, 100)
                    .await
                    .expect("insert initial value into store");
                tx.commit().await.expect("commit transaction");
            }
            {
                let mut tx = store.transaction().await.expect("start transaction");
                tx.insert(2, 200)
                    .await
                    .expect("insert value in transaction");
                tx.remove(&1).await.expect("remove failed");
                tx.rollback().await.expect("rollback transaction");

                assert_eq!(store.get(&1).await.expect("get failed"), Some(100));
                assert_eq!(store.get(&2).await.expect("get failed"), None);
            }
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
