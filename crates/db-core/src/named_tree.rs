#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(feature = "std")]
use std::string::String;

use crate::btree::{BTree, BTreeResult};
use crate::{MaybeSend, MaybeSendFuture, MaybeSync};

/// A map of named BTree instances.
///
/// The trait only exposes operations that create, read, or delete a whole
/// BTree. Entry-level operations are performed on the returned `BTree`
/// directly.
pub trait NamedBTreeMap<K, V>: Clone + MaybeSend + MaybeSync {
  type Tree: BTree<K, V>;

  fn get_tree(&self, name: &str) -> impl MaybeSendFuture<Output = BTreeResult<Self::Tree>> + '_;

  fn insert_tree(
    &self,
    name: &str,
    tree: Self::Tree,
  ) -> impl MaybeSendFuture<Output = BTreeResult<()>> + '_;

  fn delete_tree(&self, name: &str) -> impl MaybeSendFuture<Output = BTreeResult<()>> + '_;

  fn list_names(&self) -> impl MaybeSendFuture<Output = Vec<String>> + '_;
}
