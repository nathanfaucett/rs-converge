use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use async_stream::stream;
use core::{borrow::Borrow, ops::RangeBounds};
use db_engine::{
  BTree, BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeValue,
  BTreeWriteExecutor, MaybeSend, MaybeSendStream,
};
use futures::{Stream, StreamExt, pin_mut};
use uuid::Uuid;

use crate::{
  DocumentChangeKey,
  automerge_serde::AutoCommit,
  compaction::build_lifecycle_write,
  document_change_key::document_entry_bounds,
  reconstruction::{reconstruct_state, uuid_in_range},
};

pub(crate) struct AutomergeBTreeTransactionInner<T> {
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
  async fn reconstruct_inner_doc(&self, doc_id: Uuid) -> BTreeResult<Option<Vec<u8>>> {
    let (start, end) = document_entry_bounds(doc_id);
    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas: Vec<Vec<u8>> = Vec::new();

    let stream = self.inner_tx.range(start..=end);
    pin_mut!(stream);
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

  async fn load_existing_state(inner_tx: &mut T, doc_id: Uuid) -> BTreeResult<Option<Vec<u8>>> {
    let (start, end) = document_entry_bounds(doc_id);
    let mut latest_snapshot: Option<Vec<u8>> = None;
    let mut deltas: Vec<Vec<u8>> = Vec::new();
    let mut has_entries = false;

    let stream = inner_tx.range(start..=end);
    pin_mut!(stream);
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

  async fn remove_doc_entries(inner_tx: &mut T, doc_id: Uuid) -> BTreeResult<()> {
    let (start, end) = document_entry_bounds(doc_id);
    let keys_to_remove: Vec<DocumentChangeKey> = {
      let mut collected: Vec<DocumentChangeKey> = Vec::new();
      let stream = inner_tx.range(start.clone()..=end.clone());
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
    match self.reconstruct_inner_doc(doc_id).await? {
      Some(bytes) => crate::compaction::load_autocommit(&bytes).map(Some),
      None => Ok(None),
    }
  }

  fn range<'a, R>(&'a self, range: R) -> impl Stream<Item = BTreeResult<(Uuid, AutoCommit)>> + 'a
  where
    Uuid: Ord,
    R: core::ops::RangeBounds<Uuid> + MaybeSend + 'a,
  {
    stream! {
      let (start_doc, end_doc) = crate::document_change_key::all_document_bounds();

      let mut merged: BTreeMap<Uuid, Vec<u8>> = BTreeMap::new();

      let mut current_doc: Option<Uuid> = None;
      let mut latest_snapshot: Option<Vec<u8>> = None;
      let mut deltas_after_snapshot: Vec<Vec<u8>> = Vec::new();

      let stream = self.inner_tx.range(start_doc.clone()..=end_doc.clone());
      pin_mut!(stream);

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
        match crate::compaction::load_autocommit(&state) {
          Ok(doc) => yield Ok((doc_id, doc)),
          Err(e) => yield Err(e),
        }
      }
    }
  }
}

impl<T> BTreeWriteExecutor<Uuid, AutoCommit> for AutomergeBTreeTransactionInner<T>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>> + Send,
{
  async fn insert<'a>(&'a mut self, key: Uuid, value: AutoCommit) -> BTreeResult<()>
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

    let existing_bytes = self.reconstruct_inner_doc(doc_id).await?;
    if existing_bytes.is_some() {
      self.pending.insert(doc_id, None);
    }
    match existing_bytes {
      Some(bytes) => crate::compaction::load_autocommit(&bytes).map(Some),
      None => Ok(None),
    }
  }
}

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

pub struct AutomergeBTreeTransaction<T>(T);

impl<T> AutomergeBTreeTransaction<T> {
  pub fn new(inner: T) -> Self {
    Self(inner)
  }
}

impl<T, K, V> BTreeReadExecutor<K, V> for AutomergeBTreeTransaction<T>
where
  T: BTree<Uuid, AutoCommit>,
  K: BTreeKey,
  V: BTreeValue,
{
  async fn get<'a, Q>(&'a self, key: Q) -> BTreeResult<Option<V>>
  where
    Q: Borrow<K> + MaybeSend + 'a,
  {
    todo!()
  }

  fn range<'a, R>(&'a self, range: R) -> impl MaybeSendStream<Item = BTreeResult<(K, V)>> + 'a
  where
    R: RangeBounds<K> + MaybeSend + 'a,
  {
    todo!()
  }
}

impl<T, K, V> BTreeWriteExecutor<K, V> for AutomergeBTreeTransaction<T>
where
  T: BTree<Uuid, AutoCommit>,
  K: BTreeKey,
  V: BTreeValue,
{
  async fn insert<'a>(&'a mut self, key: K, value: V) -> BTreeResult<()> {
    todo!()
  }

  async fn remove<'a, Q>(&'a mut self, key: Q) -> BTreeResult<Option<V>>
  where
    Q: Borrow<K> + MaybeSend + 'a,
  {
    todo!()
  }
}

impl<T, K, V> BTreeTransaction<K, V> for AutomergeBTreeTransaction<T>
where
  T: BTree<Uuid, AutoCommit>,
  K: BTreeKey,
  V: BTreeValue,
{
  async fn commit(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    todo!()
  }

  async fn rollback(self) -> BTreeResult<()>
  where
    Self: Sized,
  {
    todo!()
  }
}
