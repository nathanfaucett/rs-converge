use automerge::{AutoCommit, ChangeHash};
use futures::{StreamExt, pin_mut};
use sha2::{Digest, Sha256};

use db_btree::{BTreeResult, BTreeTransaction};

use crate::{DocumentChangeKey, DocumentType, document_change_key::DocumentId};

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

pub fn hash_heads<I>(heads: I) -> [u8; 32]
where
  I: IntoIterator<Item = ChangeHash>,
{
  hash_hashes(heads.into_iter().map(|head| head.0))
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
  doc_id: DocumentId,
  compacted_doc: &mut AutoCommit,
) -> BTreeResult<()>
where
  T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
  let to_remove: Vec<DocumentChangeKey> = {
    let range_stream = tx.range(DocumentChangeKey::range_for(doc_id.clone()));
    pin_mut!(range_stream);

    let mut results: Vec<DocumentChangeKey> = Vec::new();

    while let Some(item) = range_stream.next().await {
      let (k, _v) = match item {
        Ok(pair) => pair,
        Err(err) => return Err(err),
      };
      results.push(k);
    }
    results
  };
  let new_key = DocumentChangeKey {
    doc_id,
    doc_type: DocumentType::Snapshot,
    change_hash: hash_heads(compacted_doc.get_heads()),
  };

  tx.insert(new_key, compacted_doc.save()).await?;

  for k in to_remove {
    tx.remove(k).await?;
  }

  Ok(())
}
