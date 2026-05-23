use std::{collections::BTreeMap, sync::Arc};

use async_lock::RwLock;
use automerge::AutoCommit;
use futures::StreamExt;
use uuid::Uuid;

use crate::automerge_btree::{AutomergeBTree, AutomergeEntry, DocumentChangeKey};
use db_core::{BTree, BTreeError, BTreeExecutor, BTreeTransaction};

mod doc_payload;
mod named;
mod named_routing;

/// Automerge-backed engine store: each logical collection (table/index/schema)
/// is represented by an Automerge `AutoCommit` document stored in the
/// `AutomergeBTree<B>` backend. Engine-level keys/values are encoded as direct
/// Automerge document fields and decoded on read.
#[derive(Clone)]
pub struct AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub automerge: Arc<RwLock<AutomergeBTree<B>>>,
}

impl<B> AutomergeEngineStore<B>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new_with_backend(backend: B) -> Self {
    let automerge = Arc::new(RwLock::new(AutomergeBTree::new(backend)));
    Self { automerge }
  }
}

pub async fn collect_documents<B>(
  store: &AutomergeEngineStore<B>,
) -> Result<BTreeMap<Uuid, AutoCommit>, BTreeError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let guard = store.automerge.read().await;
  let stream = guard.range(Uuid::from_u128(0)..=Uuid::from_u128(u128::MAX));
  futures::pin_mut!(stream);

  let mut docs = BTreeMap::new();
  while let Some(item) = stream.next().await {
    let (doc_id, doc) = match item {
      Ok(pair) => pair,
      Err(_) if docs.is_empty() => return Ok(docs),
      Err(err) => return Err(err),
    };
    docs.insert(doc_id, doc);
  }

  Ok(docs)
}

pub async fn apply_documents<B>(
  store: &AutomergeEngineStore<B>,
  docs: &BTreeMap<Uuid, AutoCommit>,
) -> Result<(), BTreeError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let guard = store.automerge.read().await;
  let mut tx = guard.transaction().await?;

  for (doc_id, doc) in docs {
    tx.insert(*doc_id, doc.clone()).await?;
  }

  tx.commit().await?;
  Ok(())
}

pub async fn sync_automerge_stores<B>(
  left: &AutomergeEngineStore<B>,
  right: &AutomergeEngineStore<B>,
) -> Result<(), BTreeError>
where
  B: BTree<DocumentChangeKey, AutomergeEntry> + Clone + Send + Sync + 'static,
{
  let left_docs = collect_documents(left).await?;
  let right_docs = collect_documents(right).await?;

  let mut merged_docs = left_docs;
  for (doc_id, mut right_doc) in right_docs {
    if let Some(left_doc) = merged_docs.get_mut(&doc_id) {
      left_doc.merge(&mut right_doc).map_err(BTreeError::other)?;
    } else {
      merged_docs.insert(doc_id, right_doc);
    }
  }

  apply_documents(left, &merged_docs).await?;
  apply_documents(right, &merged_docs).await?;

  Ok(())
}

pub fn automerge_metrics(docs: &BTreeMap<Uuid, AutoCommit>) -> (usize, usize) {
  let document_count = docs.len();
  let total_document_bytes = docs
    .values()
    .map(|doc| {
      let mut copy = doc.clone();
      copy.save().len()
    })
    .sum();
  (document_count, total_document_bytes)
}

fn key_in_range<K, R>(key: &K, range: &R) -> bool
where
  K: Ord,
  R: core::ops::RangeBounds<K>,
{
  use core::ops::Bound;

  let start = match range.start_bound() {
    Bound::Included(lower) => key >= lower,
    Bound::Excluded(lower) => key > lower,
    Bound::Unbounded => true,
  };
  let end = match range.end_bound() {
    Bound::Included(upper) => key <= upper,
    Bound::Excluded(upper) => key < upper,
    Bound::Unbounded => true,
  };
  start && end
}
