use core::ops::RangeBounds;

use crate::btree::{BTree, BTreeResult};
use crate::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};

/// An atomic transaction that spans multiple named trees within a single backend.
///
/// All mutations are buffered and applied together on [`commit`]. A [`rollback`]
/// discards all pending changes.
pub trait NamedTreeTransaction<K, V>: MaybeSend + 'static {
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

/// A factory that opens or creates a named logical tree from the backend.
///
/// Backends implement this trait to expose multiple isolated key-value trees
/// identified by a string name. This allows the engine to treat each table,
/// index, and schema namespace as a separate tree without encoding routing
/// information inside the key type.
///
/// # Backend contract
///
/// - Two calls to `get_tree` with the same `name` MUST return handles that
///   share the same physical storage.
/// - Two calls with different names MUST be isolated from each other.
/// - Ordering within a tree is determined solely by the tree's own key codec;
///   no cross-tree ordering is required.
/// - Mutations through [`begin_transaction`] MUST be atomic: either all
///   changes across all trees commit together, or none do.
pub trait NamedTreeProvider<K, V>: Clone + MaybeSend + MaybeSync {
  type Tree: BTree<K, V>;
  type Transaction: NamedTreeTransaction<K, V>;

  fn get_tree(&self, name: &str) -> impl MaybeSendFuture<Output = BTreeResult<Self::Tree>> + '_;

  fn begin_transaction(&self)
  -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + '_;

  fn begin_read_transaction(
    &self,
  ) -> impl MaybeSendFuture<Output = BTreeResult<Self::Transaction>> + '_ {
    self.begin_transaction()
  }
}
