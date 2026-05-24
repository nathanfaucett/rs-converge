//! Automerge layout tree catalog (registry of logical tree names).

use db_automerge::{AutomergeEntry, DocumentChangeKey, DocumentType};
use db_core::{BTree, BTreeError, NamedBTreeMap};
use db_engine::{EngineNamedTreeBackend, EngineNamedTreeTransaction};
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::layout_catalog::TreeLayoutCatalog;

pub const TREE_CATALOG_NAME: &str = "sys:automerge_trees";

pub struct AutomergeTreeCatalog;

pub fn init_sentinel_key() -> DocumentChangeKey {
  DocumentChangeKey {
    doc_id: Uuid::nil(),
    doc_type: DocumentType::Snapshot,
    change_hash: [0u8; 32],
  }
}

pub fn tree_catalog_key(tree: &str) -> DocumentChangeKey {
  let mut hasher = Sha256::new();
  hasher.update(b"tree-catalog:");
  hasher.update(tree.as_bytes());
  let digest = hasher.finalize();

  let mut doc_bytes = [0u8; 16];
  doc_bytes.copy_from_slice(&digest[..16]);

  let mut change_hash = [0u8; 32];
  change_hash.copy_from_slice(&digest[..32]);

  DocumentChangeKey {
    doc_id: Uuid::from_bytes(doc_bytes),
    doc_type: DocumentType::Snapshot,
    change_hash,
  }
}

pub async fn ensure_tree_initialized<L>(layout: &L, tree: &str) -> Result<(), BTreeError>
where
  L: AutomergeLayout,
{
  let mut tx = layout.begin_transaction().await?;
  let _ = tx.remove(tree, &init_sentinel_key()).await?;
  tx.commit().await
}

pub async fn register_tree_name<L>(layout: &L, tree: &str) -> Result<(), BTreeError>
where
  L: AutomergeLayout,
{
  let mut tx = layout.begin_transaction().await?;
  tx.insert(
    TREE_CATALOG_NAME,
    tree_catalog_key(tree),
    tree.as_bytes().to_vec(),
  )
  .await?;
  tx.commit().await
}

pub async fn known_tree_names<L>(layout: &L) -> Vec<String>
where
  L: AutomergeLayout,
{
  let mut names = layout.list_names().await;
  if !names.is_empty() {
    names.sort();
    names.dedup();
    return names;
  }

  let Ok(tx) = layout.begin_transaction().await else {
    return names;
  };

  let tree_stream = tx.range(TREE_CATALOG_NAME, ..);
  pin_mut!(tree_stream);

  while let Some(item) = tree_stream.next().await {
    let Ok((_key, value)) = item else {
      continue;
    };
    let Ok(name) = String::from_utf8(value) else {
      continue;
    };
    if !name.is_empty() {
      names.push(name);
    }
  }

  names.sort();
  names.dedup();
  names
}

pub trait AutomergeLayout:
  NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
  + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
  + Clone
  + Send
  + Sync
  + 'static
{
}

impl<T> AutomergeLayout for T where
  T: NamedBTreeMap<DocumentChangeKey, AutomergeEntry>
    + EngineNamedTreeBackend<DocumentChangeKey, AutomergeEntry>
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<L> TreeLayoutCatalog<L> for AutomergeTreeCatalog
where
  L: AutomergeLayout,
  L::Tree: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn prepare_tree(layout: &L, tree: &str) -> Result<(), BTreeError> {
    ensure_tree_initialized(layout, tree).await
  }

  async fn record_tree(layout: &L, tree: &str) -> Result<(), BTreeError> {
    register_tree_name(layout, tree).await
  }

  async fn list_trees(layout: &L) -> Vec<String> {
    known_tree_names(layout).await
  }
}
