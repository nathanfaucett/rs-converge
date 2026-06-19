use core::ops::RangeBounds;
use std::sync::Arc;

use async_lock::RwLock;
use async_stream::stream;
use automerge::{ActorId, AutoCommit};
use futures::{Stream, StreamExt, pin_mut};

use db_btree::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};

use crate::{
  AutomergeBTreeTransaction, CompactionPolicy, DocumentChangeKey, ThresholdPolicy,
  document_change_key::DocumentId,
  hash_heads,
  reconstruction::{ReconstructedDocument, reconstruct_document},
  run_compaction,
  transaction::AutomergeBTreeTransactionInner,
};

#[derive(Clone)]
pub struct AutomergeBTreeInner<B> {
  inner: Arc<RwLock<B>>,
  policy: ThresholdPolicy,
}

impl<B> AutomergeBTreeInner<B> {
  pub fn new(inner: B) -> Self {
    Self {
      inner: Arc::new(RwLock::new(inner)),
      policy: ThresholdPolicy::default(),
    }
  }
  pub fn with_compaction(inner: B, threshold_count: usize, threshold_bytes: usize) -> Self {
    Self {
      inner: Arc::new(RwLock::new(inner)),
      policy: ThresholdPolicy {
        threshold_count,
        threshold_bytes,
      },
    }
  }
}

async fn tx_get_document<T>(
  tx: &mut T,
  doc_id: &DocumentId,
  allow_compaction: bool,
  policy: &ThresholdPolicy,
) -> BTreeResult<Option<(AutoCommit, bool)>>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
  let reconstructed_document = reconstruct_document(tx, doc_id).await?;

  let mut doc = match reconstructed_document.doc {
    Some(doc) => doc,
    None => return Ok(None),
  };

  let compacted = if allow_compaction {
    if policy.should_compact(
      reconstructed_document.deltas,
      reconstructed_document.bytes_size,
    ) {
      run_compaction(tx, &reconstructed_document.id, &mut doc).await?;
      true
    } else {
      false
    }
  } else {
    false
  };

  Ok(Some((doc, compacted)))
}

async fn tx_remove_document<T>(tx: &mut T, doc_id: &DocumentId) -> BTreeResult<Option<AutoCommit>>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
  let (keys_to_remove, reconstructed_document) = {
    let range = DocumentChangeKey::range_for(doc_id);
    let stream = tx.range(range);
    pin_mut!(stream);

    let mut reconstructed_document_option = None;

    let mut results = Vec::new();
    while let Some(item) = stream.next().await {
      let (key, data) = item?;

      let reconstructed_document =
        reconstructed_document_option.get_or_insert_with(|| ReconstructedDocument::new(key.id()));

      reconstructed_document.apply(&key, &data)?;
      results.push(key);
    }

    (
      results,
      reconstructed_document_option
        .ok_or_else(|| BTreeError::custom("Document not found".to_string()))?,
    )
  };

  for key in keys_to_remove {
    tx.remove(&key).await?;
  }

  Ok(reconstructed_document.doc)
}

async fn tx_insert_snapshot<B>(
  tx: &mut B::Transaction,
  key: DocumentId,
  mut value: AutoCommit,
) -> BTreeResult<()>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  let key = DocumentChangeKey::new_snapshot(key, hash_heads(value.get_heads()));
  tx.insert(key, value.save()).await?;
  Ok(())
}

async fn tx_update<T, F>(
  tx: &mut T,
  doc_id: DocumentId,
  update_fn: F,
  policy: &ThresholdPolicy,
) -> BTreeResult<()>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
  F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
{
  let mut current_doc = tx_get_document(tx, &doc_id, false, policy)
    .await?
    .map(|(doc, _)| doc)
    .unwrap_or_else(|| {
      AutoCommit::new().with_actor(ActorId::from(
        DocumentChangeKey::id_to_uuid(doc_id.as_slice()).as_bytes(),
      ))
    });

  update_fn(&mut current_doc)?;

  let key = DocumentChangeKey::new_incremental(doc_id, hash_heads(current_doc.get_heads()));
  let delta = current_doc.save_incremental();

  tx.insert(key, delta).await?;

  Ok(())
}

impl<B> BTreeReadExecutor<DocumentId, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  async fn get(&self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    let inner_guard = self.inner.read().await;
    let mut tx = inner_guard.transaction().await?;
    if let Some((result, compacted)) = tx_get_document(&mut tx, key, true, &self.policy).await? {
      if compacted {
        tx.commit().await?;
      }
      Ok(Some(result))
    } else {
      Ok(None)
    }
  }

  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    stream! {
      let inner_range = DocumentChangeKey::map_document_id_range(range);

      let inner_guard = self.inner.read().await;
      let inner_stream = inner_guard.range(inner_range);
      pin_mut!(inner_stream);

      let mut reconstructed_document_option: Option<ReconstructedDocument> = None;

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;

        if let Some(mut doc) = reconstructed_document_option.take() {
            if doc.same_id(&k) {
                reconstructed_document_option = Some(doc);
            } else {
                if let Some(completed_doc) = doc.doc.take() {
                    yield Ok((doc.id, completed_doc));
                }
            }
        }

        let reconstructed_document = reconstructed_document_option
            .get_or_insert_with(|| ReconstructedDocument::new(k.id()));

        if let Err(e) = reconstructed_document.apply(&k, &v) {
            yield Err(BTreeError::Custom(e.to_string()));
            continue;
        }
      }

      if let Some(mut doc) = reconstructed_document_option
          && let Some(completed_doc) = doc.doc.take() {
              yield Ok((doc.id, completed_doc));
          }
    }
  }
}

impl<B> BTreeWriteExecutor<DocumentId, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  async fn insert(&mut self, key: DocumentId, value: AutoCommit) -> BTreeResult<()> {
    let inner_guard = self.inner.read().await;
    let mut tx = inner_guard.transaction().await?;
    tx_insert_snapshot::<B>(&mut tx, key, value).await?;
    tx.commit().await
  }

  async fn update<F>(&mut self, key: DocumentId, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
  {
    let inner_guard = self.inner.read().await;
    let mut tx = inner_guard.transaction().await?;
    tx_update(&mut tx, key, update_fn, &self.policy).await?;
    tx.commit().await?;
    Ok(Some(()))
  }

  async fn remove(&mut self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    let inner_guard = self.inner.read().await;
    let mut tx = inner_guard.transaction().await?;
    let result = tx_remove_document(&mut tx, key).await?;
    tx.commit().await?;
    Ok(result)
  }
}

impl<B> BTree<DocumentId, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  type Transaction = AutomergeBTreeTransactionInner<B::Transaction>;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let inner_guard = self.inner.read().await;
    let inner_tx = inner_guard.transaction().await?;
    Ok(AutomergeBTreeTransactionInner::new(inner_tx))
  }
}

#[derive(Clone)]
pub struct AutomergeBTree<B> {
  pub inner: B,
}

impl<B> AutomergeBTree<B> {
  pub fn new(inner: B) -> Self {
    Self { inner }
  }
}

impl<B> AutomergeBTree<AutomergeBTreeInner<B>> {
  pub fn new_automerge(inner: B) -> Self {
    Self {
      inner: AutomergeBTreeInner::new(inner),
    }
  }

  pub fn with_compaction_automerge(
    inner: B,
    threshold_count: usize,
    threshold_bytes: usize,
  ) -> Self {
    Self {
      inner: AutomergeBTreeInner::with_compaction(inner, threshold_count, threshold_bytes),
    }
  }
}

impl<B> BTreeReadExecutor<DocumentId, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentId, AutoCommit>,
{
  async fn get(&self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    self.inner.get(key).await
  }

  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    self.inner.range(range)
  }
}

impl<B> BTreeWriteExecutor<DocumentId, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentId, AutoCommit>,
{
  async fn insert(&mut self, key: DocumentId, value: AutoCommit) -> BTreeResult<()> {
    let mut tx = self.inner.transaction().await?;
    tx.insert(key, value).await?;
    tx.commit().await
  }

  async fn update<F>(&mut self, key: DocumentId, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
  {
    let mut tx = self.inner.transaction().await?;

    if let Some(mut current_value) = tx.get(&key).await? {
      update_fn(&mut current_value)?;
      tx.insert(key, current_value).await?;
      tx.commit().await?;
      Ok(Some(()))
    } else {
      Ok(None)
    }
  }

  async fn remove(&mut self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    let mut tx = self.inner.transaction().await?;
    let value = tx.remove(key).await?;
    tx.commit().await?;
    Ok(value)
  }
}

impl<B> BTree<DocumentId, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentId, AutoCommit>,
{
  type Transaction = AutomergeBTreeTransaction<B::Transaction>;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let tx = self.inner.transaction().await?;
    Ok(AutomergeBTreeTransaction::new(tx))
  }
}
