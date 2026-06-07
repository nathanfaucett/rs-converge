#[cfg(not(feature = "std"))]
use alloc::{collections::BTreeMap, vec::Vec};
#[cfg(feature = "std")]
use std::collections::BTreeMap;

use core::{borrow::Borrow, ops::RangeBounds};

use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt, pin_mut};
use uuid::Uuid;

use db_btree::{BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor};
use db_core::{MaybeSend, MaybeSendStream};

use crate::{
  DocumentChangeKey, DocumentType, hash_heads,
  reconstruction::{ReconstructedDocument, reconstruct_document},
};

pub struct AutomergeBTreeTransactionInner<T> {
  inner_tx: T,
  pending: BTreeMap<Uuid, Option<AutoCommit>>,
}

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
    pending: BTreeMap<Uuid, Option<AutoCommit>>,
  ) -> BTreeResult<()> {
    for (doc_id, op) in pending {
      Self::commit_pending_change(inner_tx, doc_id, op).await?;
    }
    Ok(())
  }

  async fn commit_pending_change(
    inner_tx: &mut T,
    doc_id: Uuid,
    op: Option<AutoCommit>,
  ) -> BTreeResult<()> {
    match op {
      Some(mut snapshot_doc) => {
        let key = DocumentChangeKey {
          doc_id,
          doc_type: DocumentType::Snapshot,
          change_hash: hash_heads(snapshot_doc.get_heads()),
        };
        inner_tx.insert(key, snapshot_doc.save()).await?;
      }
      None => {
        Self::remove_doc_entries(inner_tx, doc_id).await?;
      }
    }
    Ok(())
  }

  async fn remove_doc_entries(inner_tx: &mut T, doc_id: Uuid) -> BTreeResult<()> {
    let keys_to_remove: Vec<DocumentChangeKey> = {
      let mut collected: Vec<DocumentChangeKey> = Vec::new();
      let stream = inner_tx.range(DocumentChangeKey::range_for(doc_id));
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

impl<T> BTreeTransaction<Uuid, AutoCommit> for AutomergeBTreeTransactionInner<T>
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

impl<T> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(pending) = self.pending.get(&doc_id) {
      return Ok(pending.clone());
    }
    Ok(reconstruct_document(&self.inner_tx, doc_id).await?.doc)
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    Uuid: Ord,
    R: core::ops::RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let mut merged: BTreeMap<Uuid, AutoCommit> = BTreeMap::new();

      let inner_stream = self.inner_tx.range(DocumentChangeKey::map_uuid_range(range));
      pin_mut!(inner_stream);

      let mut reconstructed_document_option: Option<ReconstructedDocument> = None;

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;

        if let Some(mut doc) = reconstructed_document_option.take() {
          if doc.matches(&k) {
            reconstructed_document_option = Some(doc);
          } else if let Some(completed_doc) = doc.doc.take() {
            merged.insert(doc.id, completed_doc);
          }
        }

        let reconstructed_document = reconstructed_document_option
          .get_or_insert_with(|| ReconstructedDocument::new(k.doc_id));

        reconstructed_document.apply(&k, &v)?;
      }

      if let Some(mut doc) = reconstructed_document_option {
        if let Some(completed_doc) = doc.doc.take() {
          merged.insert(doc.id, completed_doc);
        }
      }

      for (doc_id, op) in &self.pending {
        match op {
          Some(doc) => {
            merged.insert(*doc_id, doc.clone());
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

impl<T> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn insert(&mut self, key: Uuid, value: AutoCommit) -> BTreeResult<()>
  where
    Uuid: Ord,
  {
    self.pending.insert(key, Some(value));
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Uuid: Ord,
    Q: core::borrow::Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();

    if let Some(existing) = self.pending.remove(&doc_id) {
      self.pending.insert(doc_id, None);
      return Ok(existing);
    }

    let existing = reconstruct_document(&self.inner_tx, doc_id).await?;
    if existing.doc.is_some() {
      self.pending.insert(doc_id, None);
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

impl<T> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeBTreeTransaction<T>
where
  T: BTreeTransaction<Uuid, AutoCommit>,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    self.0.get(key).await
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl MaybeSendStream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    self.0.range(range)
  }
}

impl<T> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeBTreeTransaction<T>
where
  T: BTreeTransaction<Uuid, AutoCommit>,
{
  async fn insert(&mut self, key: Uuid, value: AutoCommit) -> BTreeResult<()> {
    self.0.insert(key, value).await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    self.0.remove(key).await
  }
}

impl<T> BTreeTransaction<Uuid, AutoCommit> for AutomergeBTreeTransaction<T>
where
  T: BTreeTransaction<Uuid, AutoCommit>,
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
