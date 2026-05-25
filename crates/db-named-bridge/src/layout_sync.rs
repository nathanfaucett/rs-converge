use db_core::BTreeError;

use crate::layout_catalog::{TreeLayoutCatalog, merged_tree_names};

/// Sync one logical tree between two layout backends using a format-specific handler.
pub trait PerTreeFormatSync<L> {
  fn sync_tree<'a>(
    left: &'a L,
    right: &'a L,
    tree: &'a str,
  ) -> std::pin::Pin<Box<dyn core::future::Future<Output = Result<(), BTreeError>> + 'a>>;
}

/// Discover trees via the layout catalog, then sync each tree through the format handler.
pub async fn sync_cataloged_layouts<L, C, S>(left: &L, right: &L) -> Result<(), BTreeError>
where
  C: TreeLayoutCatalog<L>,
  S: PerTreeFormatSync<L>,
  L: Send + Sync,
{
  let tree_names = merged_tree_names::<L, C>(left, right).await;
  for tree in tree_names {
    S::sync_tree(left, right, &tree).await?;
  }
  Ok(())
}
