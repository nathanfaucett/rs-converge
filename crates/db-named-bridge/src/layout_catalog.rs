#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use db_core::BTreeError;

/// Catalog operations on a layout backend (tree registry and discovery).
pub trait TreeLayoutCatalog<L> {
  fn prepare_tree(
    layout: &L,
    tree: &str,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send;

  fn record_tree(
    layout: &L,
    tree: &str,
  ) -> impl core::future::Future<Output = Result<(), BTreeError>> + Send;

  fn list_trees(layout: &L) -> impl core::future::Future<Output = Vec<String>> + Send;
}

pub async fn merged_tree_names<L, C>(left: &L, right: &L) -> Vec<String>
where
  C: TreeLayoutCatalog<L>,
  L: Send + Sync,
{
  let mut names = C::list_trees(left).await;
  names.extend(C::list_trees(right).await);
  names.sort();
  names.dedup();
  names
}
