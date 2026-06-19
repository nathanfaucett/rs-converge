use core::ops::RangeBounds;

use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt, pin_mut};

use db_btree::{BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor};

use crate::{
  DocumentChangeKey, ThresholdPolicy,
  document_change_key::DocumentId,
  reconstruction::{ReconstructedDocument, reconstruct_document},
  util::{tx_insert_snapshot, tx_remove_document, tx_update},
};

pub struct AutomergeBTreeTransactionInner<T> {
  inner_tx: T,
  policy: ThresholdPolicy,
}

unsafe impl<T> Send for AutomergeBTreeTransactionInner<T> where T: Send {}

impl<T> AutomergeBTreeTransactionInner<T> {
  pub(crate) fn new(inner_tx: T, policy: ThresholdPolicy) -> Self {
    Self { inner_tx, policy }
  }
}

impl<T> BTreeTransaction<DocumentId, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn commit(self) -> BTreeResult<()> {
    let AutomergeBTreeTransactionInner { inner_tx, .. } = self;
    inner_tx.commit().await
  }

  async fn rollback(self) -> BTreeResult<()> {
    self.inner_tx.rollback().await
  }
}

impl<T> BTreeReadExecutor<DocumentId, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn get(&self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    let reconstructed = reconstruct_document(&self.inner_tx, key).await?;
    Ok(reconstructed.doc)
  }

  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    stream!({
      let inner_range = DocumentChangeKey::map_document_id_range(range);
      let inner_stream = self.inner_tx.range(inner_range);
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

impl<T> BTreeWriteExecutor<DocumentId, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn insert(&mut self, key: DocumentId, value: AutoCommit) -> BTreeResult<()>
  where
    DocumentId: Ord,
  {
    tx_insert_snapshot(&mut self.inner_tx, key, value).await
  }

  async fn update<F>(&mut self, key: DocumentId, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
  {
    tx_update(&mut self.inner_tx, key, update_fn, &self.policy).await?;
    Ok(Some(()))
  }

  async fn remove(&mut self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    tx_remove_document(&mut self.inner_tx, key).await
  }

  fn remove_range<R>(
    &mut self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    stream!({
      let keys_to_remove = {
        let inner_range = DocumentChangeKey::map_document_id_range(range);

        let inner_stream = self.inner_tx.range(inner_range);
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

        keys_to_remove
      };

      for doc_id in keys_to_remove {
        self.inner_tx.remove(&doc_id).await?;
      }
    })
  }
}

pub struct AutomergeBTreeTransaction<T>(T);

impl<T> AutomergeBTreeTransaction<T> {
  pub fn new(inner: T) -> Self {
    Self(inner)
  }
}

impl<T> BTreeReadExecutor<DocumentId, AutoCommit> for AutomergeBTreeTransaction<T>
where
  T: BTreeTransaction<DocumentId, AutoCommit>,
{
  async fn get(&self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    self.0.get(key).await
  }

  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    self.0.range(range)
  }
}

impl<T> BTreeWriteExecutor<DocumentId, AutoCommit> for AutomergeBTreeTransaction<T>
where
  T: BTreeTransaction<DocumentId, AutoCommit>,
{
  async fn insert(&mut self, key: DocumentId, value: AutoCommit) -> BTreeResult<()> {
    self.0.insert(key, value).await
  }

  async fn update<F>(&mut self, key: DocumentId, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
  {
    self.0.update(key, update_fn).await
  }

  async fn remove(&mut self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    self.0.remove(key).await
  }

  fn remove_range<R>(
    &mut self,
    range: R,
  ) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    self.0.remove_range(range)
  }
}

impl<T> BTreeTransaction<DocumentId, AutoCommit> for AutomergeBTreeTransaction<T>
where
  T: BTreeTransaction<DocumentId, AutoCommit>,
{
  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    self.0.commit().await
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    self.0.rollback().await
  }
}
