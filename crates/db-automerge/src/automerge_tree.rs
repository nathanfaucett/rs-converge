use core::ops::RangeBounds;
use std::sync::Arc;

use async_lock::RwLock;
use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt, pin_mut};

use db_btree::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};

use crate::{
  AutomergeBTreeTransaction, DocumentChangeKey, ThresholdPolicy,
  document_change_key::DocumentId,
  reconstruction::ReconstructedDocument,
  transaction::AutomergeBTreeTransactionInner,
  util::{tx_get_document, tx_insert_snapshot, tx_remove_document, tx_update},
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
    stream!({
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
          .get_or_insert_with(|| ReconstructedDocument::new(k.id().clone()));

        if let Err(e) = reconstructed_document.apply(&k, &v) {
          yield Err(BTreeError::Custom(e.to_string()));
          continue;
        }
      }

      if let Some(mut doc) = reconstructed_document_option
        && let Some(completed_doc) = doc.doc.take()
      {
        yield Ok((doc.id, completed_doc));
      }
    })
  }
}

impl<B> BTreeWriteExecutor<DocumentId, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  async fn insert(&mut self, key: DocumentId, value: AutoCommit) -> BTreeResult<()> {
    let inner_guard = self.inner.read().await;
    let mut tx = inner_guard.transaction().await?;
    tx_insert_snapshot(&mut tx, key, value).await?;
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

  fn remove_range<R>(
    &mut self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    stream!({
      let inner_range = DocumentChangeKey::map_document_id_range(range);

      let inner_guard = self.inner.write().await;
      let inner_stream = inner_guard.range(inner_range);
      pin_mut!(inner_stream);

      let mut reconstructed_document_option: Option<ReconstructedDocument> = None;
      let mut keys_to_remove = Vec::new();

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;

        keys_to_remove.push(k.clone());

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
          .get_or_insert_with(|| ReconstructedDocument::new(k.id().clone()));

        if let Err(e) = reconstructed_document.apply(&k, &v) {
          yield Err(BTreeError::Custom(e.to_string()));
          continue;
        }
      }

      if let Some(mut doc) = reconstructed_document_option
        && let Some(completed_doc) = doc.doc.take()
      {
        yield Ok((doc.id, completed_doc));
      }

      let mut tx = inner_guard.transaction().await?;
      for doc_id in keys_to_remove {
        tx.remove(&doc_id).await?;
      }
      tx.commit().await?;
    })
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
    Ok(AutomergeBTreeTransactionInner::new(
      inner_tx,
      self.policy.clone(),
    ))
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

  fn remove_range<R>(
    &mut self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    stream!({
      let mut tx = self.inner.transaction().await?;
      {
        let inner_stream = tx.remove_range(range);
        pin_mut!(inner_stream);

        while let Some(item) = inner_stream.next().await {
          yield item;
        }
      }
      tx.commit().await?;
    })
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
