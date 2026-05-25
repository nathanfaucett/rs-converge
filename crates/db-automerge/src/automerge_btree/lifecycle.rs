use automerge::{AutoCommit, AutomergeError};
use db_core::BTreeError;
use uuid::Uuid;

use super::hash::{hash_hashes, hash_heads};
use super::key::{AutomergeEntry, DocumentChangeKey, DocumentType};
use super::reconstruction::reconstruct;

pub(super) fn reconstruct_state(
  latest_snapshot: Option<Vec<u8>>,
  deltas_after_snapshot: &[Vec<u8>],
) -> Vec<u8> {
  let state = latest_snapshot.unwrap_or_default();
  reconstruct(&state, deltas_after_snapshot)
}

pub(super) fn build_lifecycle_write(
  doc_id: Uuid,
  mut desired_doc: AutoCommit,
  existing_doc_bytes: Option<Vec<u8>>,
) -> Result<Option<(DocumentChangeKey, AutomergeEntry)>, AutomergeError> {
  if let Some(current_doc_bytes) = existing_doc_bytes {
    let mut current_doc = AutoCommit::load(&current_doc_bytes)?;

    let changes = desired_doc.get_changes(&current_doc.get_heads());
    if changes.is_empty() {
      return Ok(None);
    }

    let mut delta_bytes = Vec::new();
    let mut change_hashes = Vec::with_capacity(changes.len());
    for change in &changes {
      delta_bytes.extend_from_slice(change.raw_bytes());
      change_hashes.push(change.hash().0);
    }

    let change_hash = hash_hashes(change_hashes);
    let key = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash,
    };

    return Ok(Some((key, delta_bytes)));
  }

  let change_hash = hash_heads(&desired_doc.get_heads());
  let key = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash,
  };
  let bytes = desired_doc.save();
  Ok(Some((key, bytes)))
}

pub(super) fn load_autocommit(bytes: &[u8]) -> Result<AutoCommit, BTreeError> {
  AutoCommit::load(bytes).map_err(BTreeError::other)
}
