use crate::AutoCommit;
use crate::compaction::{CompactionPolicy, ThresholdPolicy, build_lifecycle_write, run_compaction};
use crate::document_change_key::{DocumentChangeKey, all_document_bounds, document_entry_bounds};
use crate::reconstruction::{
  ReconstructionAccumulator, collect_range_keys, flush_reconstructed_doc, scan_document_entries,
  uuid_in_range,
};
use crate::transaction::AutomergeTransaction;
#[cfg(not(feature = "std"))]
use alloc::string::ToString;
use alloc::vec::Vec;
use async_stream::stream;
use db_engine::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
  MaybeSend, MaybeSendFuture,
};
use futures::{Stream, StreamExt};
use uuid::Uuid;

#[derive(Clone)]
pub struct AutomergeBTree<B> {
  pub inner: B,
  pub policy: ThresholdPolicy,
}

impl<B> AutomergeBTree<B> {
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

impl<B> AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>> + Clone + Send + Sync + 'static,
{
  pub(super) async fn get_document(
    &self,
    tx: &mut B::Transaction,
    doc_id: Uuid,
  ) -> BTreeResult<Option<(Vec<u8>, bool)>> {
    let (start, end) = document_entry_bounds(doc_id);

    let scan = scan_document_entries(tx.range(start.clone()..=end.clone())).await;

    if !scan.has_entries {
      return Ok(None);
    }

    let state = scan.accumulator.finish();

    let compacted = if self
      .policy
      .should_compact(scan.delta_count, scan.delta_bytes)
    {
      run_compaction(tx, start.clone(), end.clone(), doc_id, state.clone()).await?;
      true
    } else {
      false
    };

    Ok(Some((state, compacted)))
  }

  async fn remove_document_keys(
    &self,
    tx: &mut B::Transaction,
    start: DocumentChangeKey,
    end: DocumentChangeKey,
  ) -> Result<(), BTreeError> {
    let keys_to_remove = collect_range_keys(tx.range(start.clone()..=end.clone())).await?;
    for key in keys_to_remove {
      tx.remove(&key).await?;
    }
    Ok(())
  }

  async fn get_with_tx<'a, Q>(
    &'a self,
    tx: &'a mut B::Transaction,
    key: Q,
  ) -> Result<(Option<AutoCommit>, bool), BTreeError>
  where
    Uuid: Ord,
    Q: core::borrow::Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    match self.get_document(tx, doc_id).await? {
      None => Ok((None, false)),
      Some((bytes, compacted)) => {
        let doc = AutoCommit::load(&bytes).map_err(|e| BTreeError::Custom(e.to_string()))?;
        Ok((Some(doc), compacted))
      }
    }
  }

  async fn insert_with_tx(
    &self,
    tx: &mut B::Transaction,
    key: Uuid,
    value: AutoCommit,
  ) -> Result<(), BTreeError> {
    let existing = self
      .get_document(tx, key)
      .await?
      .map(|(bytes, _compacted)| bytes);

    if let Some((internal_key, bytes)) =
      build_lifecycle_write(key, value, existing).map_err(|e| BTreeError::Custom(e.to_string()))?
    {
      tx.insert(internal_key, bytes).await?;
    }

    Ok(())
  }

  async fn remove_with_tx<'a, Q>(
    &self,
    tx: &'a mut B::Transaction,
    key: Q,
  ) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: core::borrow::Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    let prev = self.get_document(tx, doc_id).await?;
    let (start, end) = document_entry_bounds(doc_id);

    self
      .remove_document_keys(tx, start.clone(), end.clone())
      .await?;

    match prev {
      None => Ok(None),
      Some((bytes, _compacted)) => {
        let prev_doc = AutoCommit::load(&bytes).map_err(|e| BTreeError::Custom(e.to_string()))?;
        Ok(Some(prev_doc))
      }
    }
  }
}

#[allow(clippy::needless_lifetimes)]
impl<B> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: core::borrow::Borrow<Uuid> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    let (result, compacted) = self.get_with_tx(&mut tx, key).await?;
    if compacted {
      tx.commit().await?;
    }
    Ok(result)
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + 'a
  where
    Uuid: Ord,
    R: core::ops::RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let (start_doc, end_doc) = all_document_bounds();

      let inner_stream = self.inner.range(start_doc.clone()..=end_doc.clone());
      futures::pin_mut!(inner_stream);

      let mut current_doc: Option<Uuid> = None;
      let mut accumulator = ReconstructionAccumulator::new();

      while let Some(item) = inner_stream.next().await {
        let (k, v) = item?;
        if !uuid_in_range(&range, &k.doc_id) {
          continue;
        }

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

#[allow(clippy::needless_lifetimes)]
impl<B> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>> + Clone + Send + Sync + 'static,
{
  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    let mut tx = self.inner.transaction().await?;
    self.insert_with_tx(&mut tx, key, value).await?;
    tx.commit().await
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: core::borrow::Borrow<Uuid> + MaybeSend + 'a,
  {
    let mut tx = self.inner.transaction().await?;
    let result = self.remove_with_tx(&mut tx, key).await?;
    tx.commit().await?;
    Ok(result)
  }
}

impl<B> BTree<Uuid, AutoCommit> for AutomergeBTree<B>
where
  B: BTree<DocumentChangeKey, Vec<u8>> + Clone + Send + Sync + 'static,
{
  type Transaction = AutomergeTransaction<B::Transaction>;

  fn create<D>(_definition: &D) -> impl MaybeSendFuture<Output = BTreeResult<Self>>
  where
    Self: Sized,
    D: db_engine::BTreeDefinition<Key = Uuid, Value = AutoCommit>,
  {
    async move { Err(BTreeError::UnsupportedOperation) }
  }

  async fn transaction(&self) -> Result<Self::Transaction, BTreeError> {
    let inner_tx = self.inner.transaction().await?;
    Ok(AutomergeTransaction::new(inner_tx))
  }
}
