use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

use automerge::AutomergeError;
use futures::{Stream, StreamExt, pin_mut};
use uuid::Uuid;

use db_btree::BTreeError;

use crate::{DocumentChangeKey, DocumentType, automerge_serde::AutoCommit};

pub(super) struct ReconstructionAccumulator {
  latest_snapshot: Option<Vec<u8>>,
  deltas_after_snapshot: Vec<Vec<u8>>,
}

pub(super) struct ScannedDocumentState {
  pub accumulator: ReconstructionAccumulator,
  pub delta_count: usize,
  pub delta_bytes: usize,
  pub has_entries: bool,
}

impl ReconstructionAccumulator {
  pub(super) fn new() -> Self {
    Self {
      latest_snapshot: None,
      deltas_after_snapshot: Vec::new(),
    }
  }

  pub(super) fn apply(&mut self, doc_type: DocumentType, entry: Vec<u8>) {
    if doc_type.is_snapshot() {
      self.latest_snapshot = Some(entry);
    } else {
      self.deltas_after_snapshot.push(entry);
    }
  }

  pub(super) fn finish(self) -> Vec<u8> {
    let state = self.latest_snapshot.unwrap_or_default();
    reconstruct_state(Some(state), &self.deltas_after_snapshot)
  }
}

pub(super) async fn scan_document_entries<S>(stream: S) -> ScannedDocumentState
where
  S: Stream<Item = Result<(DocumentChangeKey, Vec<u8>), BTreeError>>,
{
  let mut accumulator = ReconstructionAccumulator::new();
  let mut delta_count = 0usize;
  let mut delta_bytes = 0usize;
  let mut has_entries = false;

  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    let (key, entry) = match item {
      Ok(pair) => pair,
      Err(_) => continue,
    };
    has_entries = true;
    if key.doc_type.is_snapshot() {
      delta_count = 0;
      delta_bytes = 0;
    } else {
      delta_count += 1;
      delta_bytes += entry.len();
    }
    accumulator.apply(key.doc_type, entry);
  }

  ScannedDocumentState {
    accumulator,
    delta_count,
    delta_bytes,
    has_entries,
  }
}

pub(super) async fn collect_range_keys<S, K, V>(stream: S) -> Result<Vec<K>, BTreeError>
where
  S: Stream<Item = Result<(K, V), BTreeError>>,
{
  let mut keys = Vec::new();
  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    let (key, _value) = item?;
    keys.push(key);
  }
  Ok(keys)
}

pub(super) fn uuid_in_range<R>(range: &R, doc_id: &Uuid) -> bool
where
  R: RangeBounds<Uuid>,
{
  let start_ok = match range.start_bound() {
    Bound::Included(lower) => doc_id >= lower,
    Bound::Excluded(lower) => doc_id > lower,
    Bound::Unbounded => true,
  };
  let end_ok = match range.end_bound() {
    Bound::Included(upper) => doc_id <= upper,
    Bound::Excluded(upper) => doc_id < upper,
    Bound::Unbounded => true,
  };
  start_ok && end_ok
}

pub(super) fn flush_reconstructed_doc(
  current_doc: &mut Option<Uuid>,
  accumulator: &mut ReconstructionAccumulator,
) -> Result<Option<(Uuid, AutoCommit)>, AutomergeError> {
  if let Some(doc_id) = current_doc.take() {
    let state = core::mem::replace(accumulator, ReconstructionAccumulator::new()).finish();
    AutoCommit::load(&state).map(|doc| Some((doc_id, doc)))
  } else {
    Ok(None)
  }
}

pub(super) fn reconstruct_state(
  latest_snapshot: Option<Vec<u8>>,
  deltas_after_snapshot: &[Vec<u8>],
) -> Vec<u8> {
  let state = latest_snapshot.unwrap_or_default();
  reconstruct(&state, deltas_after_snapshot)
}

fn reconstruct(state: &[u8], deltas_after_snapshot: &[Vec<u8>]) -> Vec<u8> {
  let mut doc = AutoCommit::load(state).unwrap_or_else(|_| AutoCommit::new());
  for delta in deltas_after_snapshot {
    let _ = doc.load_incremental(delta);
  }
  doc.save()
}
