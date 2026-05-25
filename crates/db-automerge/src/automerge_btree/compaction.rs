use automerge::AutoCommit;
use db_core::{BTreeError, BTreeResult, BTreeTransaction};
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

/// Perform transactional compaction: insert a snapshot for `doc_id` with `state` and remove older change keys.
pub async fn run_compaction<T>(
  tx: &mut T,
  start: DocumentChangeKey,
  end: DocumentChangeKey,
  doc_id: Uuid,
  state: Vec<u8>,
) -> BTreeResult<()>
where
  T: BTreeTransaction<DocumentChangeKey, AutomergeEntry>,
{
  let mut compacted_doc = AutoCommit::load(&state).map_err(BTreeError::other)?;
  let new_hash = hash_heads(&compacted_doc.get_heads());

  let new_entry = state;

  let to_remove: alloc::vec::Vec<DocumentChangeKey> = {
    let range_stream = tx.range(start.clone()..=end.clone());
    futures::pin_mut!(range_stream);
    let mut collected: alloc::vec::Vec<DocumentChangeKey> = alloc::vec::Vec::new();
    while let Some(item) = range_stream.next().await {
      let (k, _v) = match item {
        Ok(pair) => pair,
        Err(err) => return Err(err),
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

  tx.insert(new_key.clone(), new_entry).await?;

  for k in to_remove {
    tx.remove(k.clone()).await?;
  }

  Ok(())
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
}
