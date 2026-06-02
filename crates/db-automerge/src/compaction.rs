use alloc::vec::Vec;
use db_engine::{BTreeError, BTreeResult, BTreeTransaction};
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{DocumentChangeKey, DocumentType, automerge_serde::AutoCommit};

pub fn hash_hashes<I>(hashes: I) -> [u8; 32]
where
  I: IntoIterator<Item = [u8; 32]>,
{
  let mut hasher = Sha256::new();
  for hash in hashes {
    hasher.update(hash);
  }
  let result = hasher.finalize();
  let mut out = [0u8; 32];
  out.copy_from_slice(&result);
  out
}

pub fn hash_heads(heads: &[automerge::ChangeHash]) -> [u8; 32] {
  hash_hashes(heads.iter().map(|head| head.0))
}

pub trait CompactionPolicy: Send + Sync {
  fn should_compact(&self, delta_count: usize, delta_bytes: usize) -> bool;
}

#[derive(Clone)]
pub struct ThresholdPolicy {
  pub threshold_count: usize,
  pub threshold_bytes: usize,
}

impl ThresholdPolicy {
  pub const DEFAULT_COUNT: usize = 100;
  pub const DEFAULT_BYTES: usize = 1024 * 1024;
}

impl Default for ThresholdPolicy {
  fn default() -> Self {
    Self {
      threshold_count: Self::DEFAULT_COUNT,
      threshold_bytes: Self::DEFAULT_BYTES,
    }
  }
}

impl CompactionPolicy for ThresholdPolicy {
  fn should_compact(&self, delta_count: usize, delta_bytes: usize) -> bool {
    (self.threshold_count > 0 && delta_count >= self.threshold_count)
      || (self.threshold_bytes > 0 && delta_bytes >= self.threshold_bytes)
  }
}

pub async fn run_compaction<T>(
  tx: &mut T,
  start: DocumentChangeKey,
  end: DocumentChangeKey,
  doc_id: Uuid,
  state: Vec<u8>,
) -> BTreeResult<()>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
  let mut compacted_doc = AutoCommit::load(&state).map_err(BTreeError::other)?;
  let new_hash = hash_heads(&compacted_doc.get_heads());

  let to_remove: Vec<DocumentChangeKey> = {
    let range_stream = tx.range(start.clone()..=end.clone());
    pin_mut!(range_stream);
    let mut collected: Vec<DocumentChangeKey> = Vec::new();
    while let Some(item) = range_stream.next().await {
      let (k, _v) = match item {
        Ok(pair) => pair,
        Err(err) => return Err(err),
      };
      if k.doc_type.is_snapshot() && k.change_hash == new_hash {
        continue;
      }
      collected.push(k);
    }
    collected
  };

  let new_key = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash: new_hash,
  };

  tx.insert(new_key.clone(), state).await?;

  for k in to_remove {
    tx.remove(k).await?;
  }

  Ok(())
}

pub(super) fn build_lifecycle_write(
  doc_id: Uuid,
  mut desired_doc: AutoCommit,
  existing_doc_bytes: Option<Vec<u8>>,
) -> Result<Option<(DocumentChangeKey, Vec<u8>)>, automerge::AutomergeError> {
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
