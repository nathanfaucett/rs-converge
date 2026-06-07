#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};
use automerge::AutoCommit;

use core::{borrow::Borrow, ops::RangeBounds};

use async_stream::stream;
use futures::{Stream, StreamExt};
use uuid::Uuid;

use db_btree::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};
use db_core::{MaybeSend, MaybeSendStream};

use crate::{
  AutomergeBTreeTransaction, CompactionPolicy, DocumentChangeKey, ThresholdPolicy,
  compaction::build_incremental_write,
  reconstruction::{
    ReconstructionAccumulator, collect_range_keys, flush_reconstructed_doc, scan_document_entries,
  },
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
  ) -> BTreeResult<Option<(Vec<u8>, bool)>> {
    let inner_range = DocumentChangeKey::range_for(doc_id);

    let scan = scan_document_entries(tx.range(inner_range)).await;

    if !scan.has_entries {
      return Ok(None);
    }

    let state = scan.accumulator.finish();

    let compacted = if self
      .policy
      .should_compact(scan.delta_count, scan.delta_bytes)
    {
      run_compaction(tx, doc_id, state.clone()).await?;
      true
    } else {
      false
    };

    Ok(Some((state, compacted)))
  }

  async fn tx_remove_document(&self, tx: &mut B::Transaction, doc_id: Uuid) -> BTreeResult<()> {
    let keys_to_remove = collect_range_keys(tx.range(DocumentChangeKey::range_for(doc_id))).await?;

    for key in keys_to_remove {
      tx.remove(&key).await?;
    }
    Ok(())
  }

  async fn tx_get<'a, Q>(
    &'a self,
    tx: &'a mut B::Transaction,
    key: Q,
  ) -> BTreeResult<(Option<AutoCommit>, bool)>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    match self.tx_get_document(tx, doc_id).await? {
      None => Ok((None, false)),
      Some((bytes, compacted)) => {
        let doc = AutoCommit::load(&bytes).map_err(|e| BTreeError::Custom(e.to_string()))?;
        Ok((Some(doc), compacted))
      }
    }
  }

  async fn tx_insert(
    &self,
    tx: &mut B::Transaction,
    key: Uuid,
    value: AutoCommit,
  ) -> BTreeResult<()> {
    let existing = self
      .tx_get_document(tx, key)
      .await?
      .map(|(bytes, _compacted)| bytes);

    if let Some((internal_key, bytes)) = build_incremental_write(key, value, existing)
      .map_err(|e| BTreeError::Custom(e.to_string()))?
    {
      tx.insert(internal_key, bytes).await?;
    }

    Ok(())
  }

  async fn tx_remove<'a, Q>(
    &self,
    tx: &'a mut B::Transaction,
    key: Q,
  ) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    let prev = self.tx_get_document(tx, doc_id).await?;

    self.tx_remove_document(tx, doc_id).await?;

    match prev {
      None => Ok(None),
      Some((bytes, _compacted)) => {
        let prev_doc = AutoCommit::load(&bytes).map_err(|e| BTreeError::Custom(e.to_string()))?;
        Ok(Some(prev_doc))
      }
    }
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
    let (result, compacted) = self.tx_get(&mut tx, key).await?;
    if compacted {
      tx.commit().await?;
    }
    Ok(result)
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let inner_range = DocumentChangeKey::map_uuid_range(range);

      let inner_stream = self.inner.range(inner_range);
      futures::pin_mut!(inner_stream);

      let mut current_doc: Option<Uuid> = None;
      let mut accumulator = ReconstructionAccumulator::new();

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;

        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        } else if current_doc.as_ref().expect("doc id present") != &k.doc_id {
          match flush_reconstructed_doc(&mut current_doc, &mut accumulator) {
            Ok(Some(pair)) => yield Ok(pair),
            Ok(None) => {},
            Err(e) => yield Err(BTreeError::Custom(e.to_string())),
          }
          current_doc = Some(k.doc_id);
        }

        accumulator.apply(k.doc_type, v.clone());
      }

      match flush_reconstructed_doc(&mut current_doc, &mut accumulator) {
        Ok(Some(pair)) => yield Ok(pair),
        Ok(None) => {},
        Err(e) => yield Err(BTreeError::Custom(e.to_string())),
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
    self.tx_insert(&mut tx, key, value).await?;
    tx.commit().await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<AutoCommit>>
  where
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    let result = self.tx_remove(&mut tx, key).await?;
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
  async fn insert(&mut self, key: Uuid, value: AutoCommit) -> BTreeResult<()>
where {
    let mut tx = self.inner.transaction().await?;
    tx.insert(key, value).await?;
    tx.commit().await
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
