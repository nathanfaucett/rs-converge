#[cfg(not(feature = "std"))]
use alloc::{format, vec::Vec};

use automerge::AutoCommit;
use db_btree::{BTreeError, BTreeReadExecutor, BTreeResult};
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use crate::DocumentChangeKey;

#[derive(Debug, Clone)]
pub struct ReconstructedDocument {
  pub id: Uuid,
  pub doc: Option<AutoCommit>,
  pub deltas: usize,
  pub bytes_size: usize,
}

impl ReconstructedDocument {
  pub fn new(id: Uuid) -> Self {
    Self {
      id,
      doc: None,
      deltas: 0,
      bytes_size: 0,
    }
  }

  pub fn matches(&self, key: &DocumentChangeKey) -> bool {
    self.id == key.doc_id
  }

  pub fn apply(&mut self, key: &DocumentChangeKey, data: &[u8]) -> BTreeResult<()> {
    if let Some(doc) = self.doc.as_mut() {
      doc.load_incremental(data).map_err(BTreeError::custom)?;
    } else {
      if key.doc_type.is_snapshot() {
        self.doc.replace(
          AutoCommit::load(data)
            .map_err(BTreeError::custom)?
            .with_actor(key.actor_id()),
        );
      } else {
        return Err(BTreeError::custom(format!(
          "Expected delta for document {}, but found snapshot",
          key.doc_id
        )));
      }
    }

    self.deltas += 1;
    self.bytes_size += data.len();

    Ok(())
  }
}

pub async fn reconstruct_document<T>(tx: &T, doc_id: Uuid) -> BTreeResult<ReconstructedDocument>
where
  T: BTreeReadExecutor<DocumentChangeKey, Vec<u8>>,
{
  let stream = tx.range(DocumentChangeKey::range_for(doc_id));
  pin_mut!(stream);

  let mut reconstructed_document = ReconstructedDocument::new(doc_id);

  while let Some(item) = stream.next().await {
    let (key, data) = item?;

    reconstructed_document.apply(&key, &data)?;
  }

  Ok(reconstructed_document)
}
