use core::fmt;

use automerge::AutoCommit;
use db_core::{BTreeError, BTreeTransaction};
use futures::StreamExt;
use uuid::Uuid;

use super::hash::hash_heads;
use super::{AutomergeEntry, DocumentChangeKey, DocumentType};

pub trait CompactionPolicy: Send + Sync {
  fn should_compact(&self, delta_count: usize, delta_bytes: usize) -> bool;
}

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

#[derive(Debug)]
pub enum CompactionError {
  Scan(BTreeError),
  DecodeState(BTreeError),
  Insert(BTreeError),
  Remove(DocumentChangeKey, BTreeError),
  Commit(BTreeError),
  Rollback(BTreeError),
}

impl fmt::Display for CompactionError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      CompactionError::Scan(source) => write!(f, "compaction scan failed: {source}"),
      CompactionError::DecodeState(source) => {
        write!(f, "compaction state decode failed: {source}")
      }
      CompactionError::Insert(source) => write!(f, "compaction insert failed: {source}"),
      CompactionError::Remove(key, source) => {
        write!(f, "compaction remove failed for key {:?}: {source}", key)
      }
      CompactionError::Commit(source) => write!(f, "compaction commit failed: {source}"),
      CompactionError::Rollback(source) => write!(f, "compaction rollback failed: {source}"),
    }
  }
}

impl std::error::Error for CompactionError {}

/// Perform transactional compaction: insert a snapshot for `doc_id` with `state` and remove older change keys.
pub async fn run_compaction<T>(
  mut tx: T,
  start: DocumentChangeKey,
  end: DocumentChangeKey,
  doc_id: Uuid,
  state: Vec<u8>,
) -> Result<(), CompactionError>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry>,
{
  let mut compacted_doc = AutoCommit::load(&state)
    .map_err(BTreeError::other)
    .map_err(CompactionError::DecodeState)?;
  let new_hash = hash_heads(&compacted_doc.get_heads());

  let new_entry = state;

  let to_remove: alloc::vec::Vec<DocumentChangeKey> = {
    let range_stream = tx.range(start.clone()..=end.clone());
    futures::pin_mut!(range_stream);
    let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
    while let Some(item) = range_stream.next().await {
      let (k, _v) = match item {
        Ok(pair) => pair,
        Err(err) => return Err(CompactionError::Scan(err)),
      };
      // Remove all old entries: any Incremental (deltas) or Snapshot with mismatched hash
      if k.doc_type.is_snapshot() && k.change_hash == new_hash {
        // Skip the new compacted snapshot itself
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

  if let Err(err) = tx.insert(new_key.clone(), new_entry).await {
    if let Err(rollback_err) = tx.rollback().await {
      return Err(CompactionError::Rollback(rollback_err));
    }
    return Err(CompactionError::Insert(err));
  }

  for k in to_remove {
    if let Err(err) = tx.remove(k.clone()).await {
      let rollback_err = tx.rollback().await;
      eprintln!(
        "Automerge compaction remove failed for key {:?}, attempting rollback: {:?}",
        k, rollback_err
      );
      return Err(CompactionError::Remove(k, err));
    }
  }

  tx.commit().await.map_err(CompactionError::Commit)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn thresholds_respected() {
    let policy = ThresholdPolicy {
      threshold_count: 5,
      threshold_bytes: 512,
    };
    assert!(policy.should_compact(10, 0));
    assert!(policy.should_compact(0, 1024));
    assert!(!policy.should_compact(2, 10));
  }

  #[test]
  fn compaction_error_display_messages_include_context() {
    let key = DocumentChangeKey {
      doc_id: Uuid::nil(),
      doc_type: DocumentType::Incremental,
      change_hash: [0u8; 32],
    };

    let errors = [
      CompactionError::Scan(BTreeError::other(std::io::Error::other("scan"))),
      CompactionError::DecodeState(BTreeError::other(std::io::Error::other("decode"))),
      CompactionError::Insert(BTreeError::other(std::io::Error::other("insert"))),
      CompactionError::Remove(key, BTreeError::other(std::io::Error::other("remove"))),
      CompactionError::Commit(BTreeError::other(std::io::Error::other("commit"))),
      CompactionError::Rollback(BTreeError::other(std::io::Error::other("rollback"))),
    ];

    for error in errors {
      let message = error.to_string();
      assert!(message.starts_with("compaction "));
      assert!(!message.is_empty());
    }
  }
}
