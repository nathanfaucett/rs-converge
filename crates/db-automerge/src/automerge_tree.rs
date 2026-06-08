#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};

use core::{borrow::Borrow, ops::RangeBounds};

use async_stream::stream;
use automerge::{ActorId, AutoCommit};
use futures::{Stream, StreamExt, pin_mut};
use uuid::Uuid;

use db_btree::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};
use db_core::{MaybeSend, MaybeSendStream};

use crate::{
  AutomergeBTreeTransaction, CompactionPolicy, DocumentChangeKey, DocumentType, ThresholdPolicy,
  hash_heads,
  reconstruction::{ReconstructedDocument, reconstruct_document},
  run_compaction,
  transaction::AutomergeBTreeTransactionInner,
};

#[derive(Clone)]
pub struct AutomergeBTreeInner<B> {
  pub inner: B,
  pub policy: ThresholdPolicy,
}

impl<B> AutomergeBTreeInner<B> {
  pub fn new(inner: B) -> Self {
    Self {
      inner,
      policy: ThresholdPolicy::default(),
    }
  }

  pub fn with_compaction(inner: B, threshold_count: usize, threshold_bytes: usize) -> Self {
    Self {
      inner,
      policy: ThresholdPolicy {
        threshold_count,
        threshold_bytes,
      },
    }
  }
}

impl<B> AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  async fn tx_get_document(
    &self,
    tx: &mut B::Transaction,
    doc_id: Uuid,
    allow_compaction: bool,
  ) -> BTreeResult<Option<(AutoCommit, bool)>> {
    let reconstructed_document = reconstruct_document(tx, doc_id).await?;

    let mut doc = match reconstructed_document.doc {
      Some(doc) => doc,
      None => return Ok(None),
    };

    let compacted = if allow_compaction {
      if self.policy.should_compact(
        reconstructed_document.deltas,
        reconstructed_document.bytes_size,
      ) {
        run_compaction(tx, doc_id, &mut doc).await?;
        true
      } else {
        false
      }
    } else {
      false
    };

    Ok(Some((doc, compacted)))
  }

  async fn tx_remove_document(
    &self,
    tx: &mut B::Transaction,
    doc_id: Uuid,
  ) -> BTreeResult<Option<AutoCommit>> {
    let (keys_to_remove, reconstructed_document) = {
      let stream = tx.range(DocumentChangeKey::range_for(doc_id));
      pin_mut!(stream);

      let mut reconstructed_document = ReconstructedDocument::new(doc_id);

      let mut results = Vec::new();
      while let Some(item) = stream.next().await {
        let (key, data) = item?;
        reconstructed_document.apply(&key, &data)?;
        results.push(key);
      }

      (results, reconstructed_document)
    };

    for key in keys_to_remove {
      tx.remove(&key).await?;
    }

    Ok(reconstructed_document.doc)
  }

  async fn tx_insert_snapshot(
    &self,
    tx: &mut B::Transaction,
    key: Uuid,
    mut value: AutoCommit,
  ) -> BTreeResult<()> {
    let key = DocumentChangeKey {
      doc_id: key,
      doc_type: DocumentType::Snapshot,
      change_hash: hash_heads(value.get_heads()),
    };
    tx.insert(key, value.save()).await?;
    Ok(())
  }

  async fn tx_update<F>(&self, tx: &mut B::Transaction, key: Uuid, update_fn: F) -> BTreeResult<()>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
  {
    let mut current_doc = self
      .tx_get_document(tx, key, false)
      .await?
      .map(|(doc, _)| doc)
      .unwrap_or_else(|| AutoCommit::new().with_actor(ActorId::from(key.as_bytes())));

    update_fn(&mut current_doc)?;

    let key = DocumentChangeKey {
      doc_id: key,
      doc_type: DocumentType::Incremental,
      change_hash: hash_heads(current_doc.get_heads()),
    };
    let delta = current_doc.save_incremental();

    tx.insert(key, delta).await?;

    Ok(())
  }
}

impl<B> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    if let Some((result, compacted)) = self.tx_get_document(&mut tx, *key.borrow(), true).await? {
      if compacted {
        tx.commit().await?;
      }
      Ok(Some(result))
    } else {
      Ok(None)
    }
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let inner_range = DocumentChangeKey::map_uuid_range(range);

      let inner_stream = self.inner.range(inner_range);
      futures::pin_mut!(inner_stream);

      let mut reconstructed_document_option: Option<ReconstructedDocument> = None;

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;

        if let Some(mut doc) = reconstructed_document_option.take() {
            if doc.matches(&k) {
                reconstructed_document_option = Some(doc);
            } else {
                if let Some(completed_doc) = doc.doc.take() {
                    yield Ok((doc.id, completed_doc));
                }
            }
        }

        let reconstructed_document = reconstructed_document_option
            .get_or_insert_with(|| ReconstructedDocument::new(k.doc_id));

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

impl<B> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  async fn insert(&mut self, key: Uuid, value: AutoCommit) -> BTreeResult<()> {
    let mut tx = self.inner.transaction().await?;
    self.tx_insert_snapshot(&mut tx, key, value).await?;
    tx.commit().await
  }

  async fn update<'a, F>(&'a mut self, key: Uuid, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    self.tx_update(&mut tx, key, update_fn).await?;
    tx.commit().await?;
    Ok(Some(()))
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    let result = self.tx_remove_document(&mut tx, *key.borrow()).await?;
    tx.commit().await?;
    Ok(result)
  }
}

impl<B> BTree<Uuid, AutoCommit> for AutomergeBTreeInner<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>>,
{
  type Transaction = AutomergeBTreeTransactionInner<B::Transaction>;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let inner_tx = self.inner.transaction().await?;
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

impl<B> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<Uuid, AutoCommit>,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    self.inner.get(key).await
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl MaybeSendStream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    self.inner.range(range)
  }
}

impl<B> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<Uuid, AutoCommit>,
{
  async fn insert(&mut self, key: Uuid, value: AutoCommit) -> BTreeResult<()> {
    let mut tx = self.inner.transaction().await?;
    tx.insert(key, value).await?;
    tx.commit().await
  }

  async fn update<'a, F>(&'a mut self, key: Uuid, update_fn: F) -> BTreeResult<Option<()>>
  where
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()> + MaybeSend + 'a,
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

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    let value = tx.remove(key).await?;
    tx.commit().await?;
    Ok(value)
  }
}

impl<B> BTree<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<Uuid, AutoCommit>,
{
  type Transaction = AutomergeBTreeTransaction<B::Transaction>;

  async fn transaction(&self) -> BTreeResult<Self::Transaction> {
    let tx = self.inner.transaction().await?;
    Ok(AutomergeBTreeTransaction::new(tx))
  }
}
