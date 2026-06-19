use core::ops::RangeBounds;
use std::collections::BTreeMap;

use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt, pin_mut};

use db_btree::{BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor};

use crate::{
  DocumentChangeKey,
  document_change_key::DocumentId,
  hash_heads,
  reconstruction::{ReconstructedDocument, reconstruct_document},
};

pub struct AutomergeBTreeTransactionInner<T> {
  inner_tx: T,
  pending: BTreeMap<DocumentId, Option<AutoCommit>>,
}

unsafe impl<T> Send for AutomergeBTreeTransactionInner<T> where T: Send {}

impl<T> AutomergeBTreeTransactionInner<T> {
  pub(crate) fn new(inner_tx: T) -> Self {
    Self {
      inner_tx,
      pending: BTreeMap::new(),
    }
  }
}

impl<T> AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
  async fn commit_pending_changes(
    inner_tx: &mut T,
    pending: BTreeMap<DocumentId, Option<AutoCommit>>,
  ) -> BTreeResult<()> {
    for (doc_id, op) in pending {
      Self::commit_pending_change(inner_tx, doc_id, op).await?;
    }
    Ok(())
  }

  async fn commit_pending_change(
    inner_tx: &mut T,
    doc_id: DocumentId,
    op: Option<AutoCommit>,
  ) -> BTreeResult<()> {
    match op {
      Some(mut snapshot_doc) => {
        let key = DocumentChangeKey::new_snapshot(doc_id, hash_heads(snapshot_doc.get_heads()));
        inner_tx.insert(key, snapshot_doc.save()).await?;
      }
      None => {
        Self::remove_doc_entries(inner_tx, doc_id).await?;
      }
    }
    Ok(())
  }

  async fn remove_doc_entries(inner_tx: &mut T, doc_id: DocumentId) -> BTreeResult<()> {
    let keys_to_remove = {
      let mut collected: Vec<DocumentChangeKey> = Vec::new();

      let range = DocumentChangeKey::range_for(&doc_id);
      let stream = inner_tx.range(range);
      pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (k, _v) = item?;
        collected.push(k);
      }
      collected
    };

    for k in keys_to_remove {
      inner_tx.remove(&k).await?;
    }

    Ok(())
  }
}

impl<T> BTreeTransaction<DocumentId, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn commit(self) -> BTreeResult<()> {
    let AutomergeBTreeTransactionInner {
      mut inner_tx,
      pending,
    } = self;
    Self::commit_pending_changes(&mut inner_tx, pending).await?;
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
    if let Some(pending) = self.pending.get(key) {
      return Ok(pending.clone());
    }
    Ok(reconstruct_document(&self.inner_tx, key).await?.doc)
  }

  fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
  where
    R: RangeBounds<DocumentId>,
  {
    stream! {
      let mut merged: BTreeMap<DocumentId, AutoCommit> = BTreeMap::new();

      let range = DocumentChangeKey::map_document_id_range(range);
      let inner_stream = self.inner_tx.range(range);
      pin_mut!(inner_stream);

      let mut reconstructed_document_option: Option<ReconstructedDocument> = None;

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;

        if let Some(mut doc) = reconstructed_document_option.take() {
          if doc.same_id(&k) {
            reconstructed_document_option = Some(doc);
          } else if let Some(completed_doc) = doc.doc.take() {
            merged.insert(doc.id, completed_doc);
          }
        }

        let reconstructed_document = reconstructed_document_option
          .get_or_insert_with(|| ReconstructedDocument::new(k.id()));

        reconstructed_document.apply(&k, &v)?;
      }

      if let Some(mut doc) = reconstructed_document_option
        && let Some(completed_doc) = doc.doc.take() {
          merged.insert(doc.id, completed_doc);
        }

      for (doc_id, op) in &self.pending {
        match op {
          Some(doc) => {
            merged.insert(doc_id.clone(), doc.clone());
          }
          None => {
            merged.remove(doc_id);
          }
        }
      }

      for (doc_id, doc) in merged {
        yield Ok((doc_id, doc));
      }
    }
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
    self.pending.insert(key, Some(value));
    Ok(())
  }

  async fn update<F>(&mut self, key: DocumentId, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
  {
    let pending_doc_option = self.pending.get(&key).cloned().flatten();

    let mut doc = if let Some(pending_doc) = pending_doc_option {
      pending_doc
    } else {
      reconstruct_document(&self.inner_tx, &key)
        .await?
        .doc
        .ok_or_else(|| {
          BTreeError::Custom(format!("Document with id {:x?} not found for update", key))
        })?
    };

    update_fn(&mut doc)?;

    self.pending.insert(key, Some(doc));

    Ok(Some(()))
  }

  async fn remove(&mut self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
    if let Some((doc_id, existing)) = self.pending.remove_entry(key) {
      self.pending.insert(doc_id, None);
      return Ok(existing);
    }

    let existing = reconstruct_document(&self.inner_tx, key).await?;
    if existing.doc.is_some() {
      self.pending.insert(existing.id, None);
    }
    Ok(existing.doc)
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
