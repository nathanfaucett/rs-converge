use std::{borrow::Borrow, collections::BTreeMap, ops::RangeBounds};

use async_stream::stream;
use futures::{Stream, StreamExt};

use automerge::AutoCommit;
use db_core::{BTreeError, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor, MaybeSend};
use uuid::Uuid;

use super::codec::encode_doc_key_range_value_codec;
use super::key::{all_document_bounds, document_entry_bounds};
use super::lifecycle::{build_lifecycle_write, load_autocommit, reconstruct_state};
use super::scan::uuid_in_range;
use super::{AutomergeEntry, DocumentChangeKey, encode_doc_key_range};

fn flush_current_doc(
  merged: &mut BTreeMap<Uuid, Vec<u8>>,
  current_doc: &mut Option<Uuid>,
  latest_snapshot: &mut Option<Vec<u8>>,
  deltas_after_snapshot: &mut Vec<Vec<u8>>,
) {
  if let Some(doc_id) = current_doc.take() {
    let state = reconstruct_state(latest_snapshot.take(), deltas_after_snapshot);
    deltas_after_snapshot.clear();
    merged.insert(doc_id, state);
  }
}

fn apply_pending_overrides<R>(
  range: &R,
  pending: &BTreeMap<Uuid, Option<AutoCommit>>,
  merged: &mut BTreeMap<Uuid, Vec<u8>>,
) where
  R: RangeBounds<Uuid>,
{
  for (doc_id, op) in pending {
    if !uuid_in_range(range, doc_id) {
      continue;
    }

    if let Some(doc) = op {
      merged.insert(*doc_id, doc.clone().save());
    } else {
      merged.remove(doc_id);
    }
  }
}

fn decode_entry_or_raw<VC>(data: &[u8]) -> Vec<u8>
where
  VC: db_core::ValueCodec<AutomergeEntry>,
{
  <VC as db_core::ValueCodec<AutomergeEntry>>::decode_checked(data)
    .unwrap_or_else(|_| data.to_vec())
}

pub struct AutomergeTransaction<T> {
  inner_tx: T,
  pending: BTreeMap<Uuid, Option<AutoCommit>>,
}

impl<T> AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  pub fn new(inner_tx: T) -> Self {
    Self {
      inner_tx,
      pending: BTreeMap::new(),
    }
  }

  async fn reconstruct_inner_doc(&self, doc_id: Uuid) -> Result<Option<Vec<u8>>, BTreeError> {
    let (start, end) = document_entry_bounds(doc_id);
    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas: Vec<Vec<u8>> = Vec::new();

    let stream = self.inner_tx.range(start..=end);
    futures::pin_mut!(stream);
    let mut has_entries = false;
    while let Some(item) = stream.next().await {
      let (key, entry) = item?;
      has_entries = true;
      if key.doc_type.is_snapshot() {
        latest_snapshot = Some(entry);
      } else {
        deltas.push(entry);
      }
    }

    if !has_entries {
      return Ok(None);
    }

    Ok(Some(reconstruct_state(latest_snapshot, &deltas)))
  }

  async fn load_existing_state(
    inner_tx: &mut T,
    doc_id: Uuid,
  ) -> Result<Option<Vec<u8>>, BTreeError> {
    let (start, end) = document_entry_bounds(doc_id);
    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas: Vec<Vec<u8>> = Vec::new();
    let mut has_entries = false;

    let stream = inner_tx.range(start..=end);
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (key, entry) = item?;
      has_entries = true;
      if key.doc_type.is_snapshot() {
        latest_snapshot = Some(entry);
      } else {
        deltas.push(entry);
      }
    }

    if has_entries {
      Ok(Some(reconstruct_state(latest_snapshot, &deltas)))
    } else {
      Ok(None)
    }
  }

  async fn commit_pending_changes(
    inner_tx: &mut T,
    pending: BTreeMap<Uuid, Option<AutoCommit>>,
  ) -> Result<(), BTreeError> {
    for (doc_id, op) in pending {
      Self::commit_pending_change(inner_tx, doc_id, op).await?;
    }
    Ok(())
  }

  async fn commit_pending_change(
    inner_tx: &mut T,
    doc_id: Uuid,
    op: Option<AutoCommit>,
  ) -> Result<(), BTreeError> {
    match op {
      Some(snapshot_doc) => {
        let existing_state = Self::load_existing_state(inner_tx, doc_id).await?;

        if let Some((entry_key, entry_bytes)) =
          build_lifecycle_write(doc_id, snapshot_doc, existing_state).map_err(BTreeError::other)?
        {
          inner_tx.insert(entry_key, entry_bytes).await?;
        }
      }
      None => {
        Self::remove_doc_entries(inner_tx, doc_id).await?;
      }
    }
    Ok(())
  }

  async fn remove_doc_entries(inner_tx: &mut T, doc_id: Uuid) -> Result<(), BTreeError> {
    let (start, end) = document_entry_bounds(doc_id);
    let keys_to_remove: alloc::vec::Vec<DocumentChangeKey> = {
      let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
      let stream = inner_tx.range(start.clone()..=end.clone());
      futures::pin_mut!(stream);
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

impl<T> BTreeTransaction<Uuid, AutoCommit> for AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  async fn commit(self) -> Result<(), BTreeError> {
    let AutomergeTransaction {
      mut inner_tx,
      pending,
    } = self;
    Self::commit_pending_changes(&mut inner_tx, pending).await?;
    inner_tx.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner_tx.rollback().await
  }
}

#[allow(clippy::needless_lifetimes)]
impl<T> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(pending) = self.pending.get(&doc_id) {
      return Ok(pending.clone());
    }
    match self.reconstruct_inner_doc(doc_id).await? {
      Some(bytes) => load_autocommit(&bytes).map(Some),
      None => Ok(None),
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + 'a
  where
    Uuid: Ord,
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      // Map to internal document key range
      let (start_doc, end_doc) = all_document_bounds();

      // Collect per-doc reconstructed state from inner_tx
      let mut merged: std::collections::BTreeMap<Uuid, Vec<u8>> = std::collections::BTreeMap::new();

      let mut current_doc: Option<Uuid> = None;
      let mut latest_snapshot: Option<Vec<u8>> = None;
      let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

      let stream = self.inner_tx.range(start_doc.clone()..=end_doc.clone());
      futures::pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (k, v) = item?;
        if current_doc.is_none() {
          current_doc = Some(k.doc_id);
        }

        if current_doc.unwrap() != k.doc_id {
          flush_current_doc(
            &mut merged,
            &mut current_doc,
            &mut latest_snapshot,
            &mut deltas_after_snapshot,
          );
          current_doc = Some(k.doc_id);
        }

        if k.doc_type.is_snapshot() {
          latest_snapshot = Some(v);
        } else {
          deltas_after_snapshot.push(v);
        }
      }

      flush_current_doc(
        &mut merged,
        &mut current_doc,
        &mut latest_snapshot,
        &mut deltas_after_snapshot,
      );

      apply_pending_overrides(&range, &self.pending, &mut merged);

      for (doc_id, state) in merged.into_iter() {
        if !uuid_in_range(&range, &doc_id) {
          continue;
        }
        match load_autocommit(&state) {
          Ok(doc) => yield Ok((doc_id, doc)),
          Err(e) => yield Err(e),
        }
      }
    }
  }
}

#[allow(clippy::needless_lifetimes)]
impl<T> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeTransaction<T>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry> + Send,
{
  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    self.pending.insert(key, Some(value));
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();

    if let Some(existing) = self.pending.remove(&doc_id) {
      self.pending.insert(doc_id, None);
      return Ok(existing);
    }

    let existing_bytes = self.reconstruct_inner_doc(doc_id).await?;
    if existing_bytes.is_some() {
      self.pending.insert(doc_id, None);
    }
    match existing_bytes {
      Some(bytes) => load_autocommit(&bytes).map(Some),
      None => Ok(None),
    }
  }
}

/// Encoded-backed transaction that wraps an inner transaction storing
/// `Vec<u8>` keys/values and uses codecs to translate to `DocumentChangeKey`/
/// `AutomergeEntry`.
pub struct AutomergeEncodedTransaction<T, KC, VC> {
  inner_tx: T,
  pending: BTreeMap<Uuid, Option<AutoCommit>>,
  key_codec: KC,
  _val_codec: VC,
}

impl<T, KC, VC> AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::ValueCodec<DocumentChangeKey>
    + db_core::FastKeyCodec<DocumentChangeKey>
    + Clone
    + Send
    + Sync
    + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  pub fn new(inner_tx: T, key_codec: KC, val_codec: VC) -> Self {
    Self {
      inner_tx,
      pending: BTreeMap::new(),
      key_codec,
      _val_codec: val_codec,
    }
  }

  async fn reconstruct_inner_doc(&self, doc_id: Uuid) -> Result<Option<Vec<u8>>, BTreeError> {
    let (start_enc, end_enc) = encode_doc_key_range_value_codec::<KC>(doc_id);

    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

    let stream = self.inner_tx.range(start_enc.clone()..=end_enc.clone());
    futures::pin_mut!(stream);
    let mut has_entries = false;
    while let Some(item) = stream.next().await {
      let (k_enc, entry_enc) = item?;
      has_entries = true;
      let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
        Ok(kd) => kd,
        Err(_) => continue,
      };
      if k.doc_type.is_snapshot() {
        let v = decode_entry_or_raw::<VC>(&entry_enc);
        latest_snapshot = Some(v);
        deltas_after_snapshot.clear();
      } else {
        let v = decode_entry_or_raw::<VC>(&entry_enc);
        deltas_after_snapshot.push(v);
      }
    }

    if !has_entries {
      return Ok(None);
    }

    Ok(Some(reconstruct_state(
      latest_snapshot,
      &deltas_after_snapshot,
    )))
  }

  async fn apply_snapshot_doc(
    inner_tx: &mut T,
    doc_id: Uuid,
    snapshot_doc: AutoCommit,
    key_codec: &KC,
  ) -> Result<(), BTreeError> {
    let existing_state = Self::load_existing_state(inner_tx, doc_id, key_codec).await?;

    if let Some((entry_key, entry_bytes)) =
      build_lifecycle_write(doc_id, snapshot_doc, existing_state).map_err(BTreeError::other)?
    {
      Self::write_pending_snapshot(inner_tx, key_codec, entry_key, entry_bytes).await?;
    }

    Ok(())
  }

  async fn write_pending_snapshot(
    inner_tx: &mut T,
    key_codec: &KC,
    entry_key: DocumentChangeKey,
    entry_bytes: AutomergeEntry,
  ) -> Result<(), BTreeError> {
    let mut key_scratch = db_core::KeyScratch::with_capacity(49);
    <KC as db_core::FastKeyCodec<DocumentChangeKey>>::encode_into(
      key_codec,
      &entry_key,
      &mut key_scratch,
    );
    let value_encoded: Vec<u8> = <VC as db_core::ValueCodec<AutomergeEntry>>::encode(&entry_bytes)
      .as_ref()
      .to_vec();
    inner_tx.insert(key_scratch.buf, value_encoded).await?;
    Ok(())
  }
}

impl<T, KC, VC> AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn load_existing_state(
    inner_tx: &mut T,
    doc_id: Uuid,
    key_codec: &KC,
  ) -> Result<Option<Vec<u8>>, BTreeError> {
    let (start_enc, end_enc) = encode_doc_key_range(doc_id, key_codec);
    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();
    let mut has_entries = false;

    let stream = inner_tx.range(start_enc.clone()..=end_enc.clone());
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (k_enc, entry_enc) = item?;
      let key = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) {
        Ok(decoded) => decoded,
        Err(_) => continue,
      };
      has_entries = true;
      if key.doc_type.is_snapshot() {
        latest_snapshot = Some(decode_entry_or_raw::<VC>(&entry_enc));
        deltas_after_snapshot.clear();
      } else {
        deltas_after_snapshot.push(decode_entry_or_raw::<VC>(&entry_enc));
      }
    }

    if has_entries {
      Ok(Some(reconstruct_state(
        latest_snapshot,
        &deltas_after_snapshot,
      )))
    } else {
      Ok(None)
    }
  }

  async fn remove_doc_entries(
    inner_tx: &mut T,
    doc_id: Uuid,
    key_codec: &KC,
  ) -> Result<(), BTreeError> {
    let (start_enc, end_enc) = encode_doc_key_range(doc_id, key_codec);
    let keys_to_remove = {
      let mut collected: alloc::vec::Vec<Vec<u8>> = alloc::vec::Vec::new();
      let stream = inner_tx.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(stream);
      while let Some(item) = stream.next().await {
        let (k_enc, _v_enc) = item?;
        collected.push(k_enc);
      }
      collected
    };

    for k in keys_to_remove {
      inner_tx.remove(&k).await?;
    }

    Ok(())
  }

  async fn commit_pending_changes(
    inner_tx: &mut T,
    pending: BTreeMap<Uuid, Option<AutoCommit>>,
    key_codec: &KC,
  ) -> Result<(), BTreeError> {
    for (doc_id, op) in pending {
      Self::commit_pending_change(inner_tx, doc_id, op, key_codec).await?;
    }
    Ok(())
  }

  async fn commit_pending_change(
    inner_tx: &mut T,
    doc_id: Uuid,
    op: Option<AutoCommit>,
    key_codec: &KC,
  ) -> Result<(), BTreeError> {
    match op {
      Some(snapshot_doc) => {
        Self::apply_snapshot_doc(inner_tx, doc_id, snapshot_doc, key_codec).await?
      }
      None => Self::remove_doc_entries(inner_tx, doc_id, key_codec).await?,
    }
    Ok(())
  }
}

impl<T, KC, VC> BTreeTransaction<Uuid, AutoCommit> for AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn commit(self) -> Result<(), BTreeError> {
    let AutomergeEncodedTransaction {
      mut inner_tx,
      pending,
      key_codec,
      _val_codec: _,
    } = self;

    Self::commit_pending_changes(&mut inner_tx, pending, &key_codec).await?;
    inner_tx.commit().await
  }

  async fn rollback(self) -> Result<(), BTreeError> {
    self.inner_tx.rollback().await
  }
}

#[allow(clippy::needless_lifetimes)]
impl<T, KC, VC> BTreeReadExecutor<Uuid, AutoCommit> for AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn get<'a, Q>(&'a self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(pending) = self.pending.get(&doc_id) {
      return Ok(pending.clone());
    }
    match self.reconstruct_inner_doc(doc_id).await? {
      Some(bytes) => load_autocommit(&bytes).map(Some),
      None => Ok(None),
    }
  }

  fn range<'a, R>(
    &'a self,
    range: R,
  ) -> impl Stream<Item = Result<(Uuid, AutoCommit), BTreeError>> + 'a
  where
    Uuid: Ord,
    R: RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let (start_enc, _) = encode_doc_key_range(Uuid::from_u128(0), &self.key_codec);
      let (_, end_enc) = encode_doc_key_range(Uuid::from_u128(u128::MAX), &self.key_codec);

      let mut merged: std::collections::BTreeMap<Uuid, Vec<u8>> = std::collections::BTreeMap::new();

      let mut current_doc: Option<Uuid> = None;
      let mut latest_snapshot: Option<Vec<u8>> = None;
      let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

      let stream = self.inner_tx.range(start_enc.clone()..=end_enc.clone());
      futures::pin_mut!(stream);

      while let Some(item) = stream.next().await {
        let (k_enc, v_enc) = item?;
        let k = match <KC as db_core::ValueCodec<DocumentChangeKey>>::decode_checked(&k_enc) { Ok(kd) => kd, Err(_) => continue };
        if current_doc.is_none() { current_doc = Some(k.doc_id); }
        if current_doc.unwrap() != k.doc_id {
          flush_current_doc(
            &mut merged,
            &mut current_doc,
            &mut latest_snapshot,
            &mut deltas_after_snapshot,
          );
          current_doc = Some(k.doc_id);
        }
        if k.doc_type.is_snapshot() { latest_snapshot = Some(decode_entry_or_raw::<VC>(&v_enc)); deltas_after_snapshot.clear(); }
        else { deltas_after_snapshot.push(decode_entry_or_raw::<VC>(&v_enc)); }
      }

      flush_current_doc(
        &mut merged,
        &mut current_doc,
        &mut latest_snapshot,
        &mut deltas_after_snapshot,
      );

      apply_pending_overrides(&range, &self.pending, &mut merged);

      for (doc_id, state) in merged.into_iter() {
        if !uuid_in_range(&range, &doc_id) {
          continue;
        }
        match load_autocommit(&state) { Ok(doc) => yield Ok((doc_id, doc)), Err(e) => yield Err(e), }
      }
    }
  }
}

#[allow(clippy::needless_lifetimes)]
impl<T, KC, VC> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeEncodedTransaction<T, KC, VC>
where
  T: BTreeTransaction<Vec<u8>, Vec<u8>> + Send,
  KC: db_core::FastKeyCodec<DocumentChangeKey> + Clone + Send + Sync + 'static,
  VC: db_core::ValueCodec<AutomergeEntry> + Clone + Send + Sync + 'static,
{
  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> Result<(), BTreeError>
  where
    Uuid: Ord,
  {
    self.pending.insert(key, Some(value));
    Ok(())
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> Result<Option<AutoCommit>, BTreeError>
  where
    Uuid: Ord,
    Q: Borrow<Uuid> + MaybeSend + 'a,
  {
    let doc_id = *key.borrow();
    if let Some(existing) = self.pending.remove(&doc_id) {
      self.pending.insert(doc_id, None);
      return Ok(existing);
    }
    let existing_bytes = self.reconstruct_inner_doc(doc_id).await?;
    if existing_bytes.is_some() {
      self.pending.insert(doc_id, None);
    }
    match existing_bytes {
      Some(bytes) => load_autocommit(&bytes).map(Some),
      None => Ok(None),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{AutomergeEntry, DocumentChangeKey};
  use automerge::AutoCommit;
  use automerge::transaction::Transactable;
  use db_core::{
    BTree, BTreeError, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor, block_on,
  };
  use db_in_memory::InMemoryBTree;
  use futures::StreamExt;
  use uuid::Uuid;

  use super::super::{AutomergeBTree, AutomergeBTreeEncoded};

  struct TxParityObservation {
    pending_get_bytes: Vec<u8>,
    pending_range_ids: Vec<Uuid>,
    committed_get_bytes: Vec<u8>,
    removed_bytes: Vec<u8>,
    range_after_pending_remove_ids: Vec<Uuid>,
  }

  fn doc_with_value(value: &str) -> AutoCommit {
    let mut doc = AutoCommit::new();
    doc.put(&automerge::ROOT, "v", value).expect("put");
    doc
  }

  async fn collect_ids_from_range<S>(stream: S) -> Result<Vec<Uuid>, BTreeError>
  where
    S: futures::Stream<Item = Result<(Uuid, AutoCommit), BTreeError>>,
  {
    let mut ids = Vec::new();
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (id, _doc) = item?;
      ids.push(id);
    }
    Ok(ids)
  }

  async fn run_transaction_observation<S>(
    store: &S,
    doc_a: Uuid,
    doc_b: Uuid,
    doc_a_value: &AutoCommit,
    doc_b_value: &AutoCommit,
  ) -> Result<TxParityObservation, BTreeError>
  where
    S: BTree<Uuid, AutoCommit>,
  {
    let mut tx = store.transaction().await?;
    tx.insert(doc_a, doc_a_value.clone()).await?;
    tx.insert(doc_b, doc_b_value.clone()).await?;

    let pending_get_bytes = tx.get(&doc_a).await?.expect("missing pending doc").save();

    let pending_range_ids =
      collect_ids_from_range(tx.range(Uuid::nil()..=Uuid::from_u128(u128::MAX))).await?;
    tx.commit().await?;

    let committed_get_bytes = store
      .get(&doc_a)
      .await?
      .expect("missing committed doc")
      .save();

    let mut tx2 = store.transaction().await?;
    let removed_bytes = tx2
      .remove(&doc_a)
      .await?
      .expect("missing removed doc")
      .save();

    let range_after_pending_remove_ids =
      collect_ids_from_range(tx2.range(Uuid::nil()..=Uuid::from_u128(u128::MAX))).await?;
    tx2.rollback().await?;

    Ok(TxParityObservation {
      pending_get_bytes,
      pending_range_ids,
      committed_get_bytes,
      removed_bytes,
      range_after_pending_remove_ids,
    })
  }

  #[test]
  fn transaction_get_reflects_pending_changes() {
    block_on(async {
      let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let store = AutomergeBTree::new(underlying.clone());
      let mut tx = store.transaction().await.expect("start transaction");
      let doc_id = Uuid::new_v4();

      let mut doc = AutoCommit::new();
      doc.put(&automerge::ROOT, "v", "hello").expect("put");
      let mut expected = doc.clone();

      tx.insert(doc_id, doc).await.expect("insert pending delta");
      let got = tx.get(&doc_id).await.expect("tx get");
      let mut got_doc = got.expect("missing");
      assert_eq!(got_doc.save(), expected.save());

      tx.rollback().await.expect("rollback");
    });
  }

  #[test]
  fn transaction_commit_applies_pending_changes() {
    block_on(async {
      let underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let store = AutomergeBTree::new(underlying.clone());
      let mut tx = store.transaction().await.expect("start transaction");
      let doc_id = Uuid::new_v4();

      let mut doc = AutoCommit::new();
      doc.put(&automerge::ROOT, "v", "hello").expect("put");
      let mut expected = doc.clone();

      tx.insert(doc_id, doc).await.expect("insert pending delta");
      tx.commit().await.expect("commit");

      let got = store.get(&doc_id).await.expect("get after commit");
      let mut got_doc = got.expect("missing");
      assert_eq!(got_doc.save(), expected.save());
    });
  }

  #[test]
  fn encoded_transaction_matches_plain_transaction_semantics() {
    block_on(async {
      let plain_underlying = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
      let plain_store = AutomergeBTree::new(plain_underlying);

      let encoded_underlying = InMemoryBTree::<Vec<u8>, Vec<u8>>::new();
      let encoded_store = AutomergeBTreeEncoded::new(encoded_underlying);

      let doc_a = Uuid::from_u128(1);
      let doc_b = Uuid::from_u128(2);
      let doc_a_value = doc_with_value("a");
      let doc_b_value = doc_with_value("b");

      let plain =
        run_transaction_observation(&plain_store, doc_a, doc_b, &doc_a_value, &doc_b_value)
          .await
          .expect("plain transaction flow");
      let encoded =
        run_transaction_observation(&encoded_store, doc_a, doc_b, &doc_a_value, &doc_b_value)
          .await
          .expect("encoded transaction flow");

      assert_eq!(plain.pending_get_bytes, encoded.pending_get_bytes);
      assert_eq!(plain.pending_range_ids, encoded.pending_range_ids);
      assert_eq!(plain.committed_get_bytes, encoded.committed_get_bytes);
      assert_eq!(plain.removed_bytes, encoded.removed_bytes);
      assert_eq!(
        plain.range_after_pending_remove_ids,
        encoded.range_after_pending_remove_ids
      );
    });
  }
}
