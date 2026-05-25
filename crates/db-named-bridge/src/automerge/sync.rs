use db_automerge::{AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError};

use crate::AutomergeSyncMetrics;
use crate::automerge::catalog::AutomergeTreeCatalog;
use crate::layout_sync::{PerTreeFormatSync, sync_cataloged_layouts};

use super::catalog::{
  AutomergeLayout, ensure_tree_initialized, known_tree_names, register_tree_name,
};

pub struct AutomergePerTreeSync;

impl<L> PerTreeFormatSync<L> for AutomergePerTreeSync
where
  L: AutomergeLayout + Sync,
  L::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  fn sync_tree<'a>(
    left: &'a L,
    right: &'a L,
    tree: &'a str,
  ) -> std::pin::Pin<Box<dyn core::future::Future<Output = Result<(), BTreeError>> + 'a>> {
    Box::pin(async move {
      for layout in [left, right] {
        ensure_tree_initialized(layout, tree).await?;
        register_tree_name(layout, tree).await?;
      }
      let left_store = AutomergeEngineStore::new_with_backend(left.get_tree(tree).await?);
      let right_store = AutomergeEngineStore::new_with_backend(right.get_tree(tree).await?);
      db_automerge::sync_automerge_stores(&left_store, &right_store).await
    })
  }
}

/// Sync all cataloged trees between two Automerge layout backends (layout concern).
pub async fn sync_automerge_layouts<L>(left: &L, right: &L) -> Result<(), BTreeError>
where
  L: AutomergeLayout,
  L::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  sync_cataloged_layouts::<L, AutomergeTreeCatalog, AutomergePerTreeSync>(left, right).await
}

/// Document metrics across all cataloged trees on one layout backend.
pub async fn automerge_layout_metrics<L>(layout: &L) -> Result<AutomergeSyncMetrics, BTreeError>
where
  L: AutomergeLayout,
  L::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let mut total_document_count = 0usize;
  let mut total_document_bytes = 0usize;

  for tree in known_tree_names(layout).await {
    ensure_tree_initialized(layout, &tree).await?;
    register_tree_name(layout, &tree).await?;
    let store = AutomergeEngineStore::new_with_backend(layout.get_tree(&tree).await?);
    let docs = db_automerge::collect_documents(&store).await?;
    let (document_count, document_bytes) = db_automerge::automerge_metrics(&docs);
    total_document_count += document_count;
    total_document_bytes += document_bytes;
  }

  Ok(AutomergeSyncMetrics {
    document_count: total_document_count,
    total_document_bytes,
  })
}
