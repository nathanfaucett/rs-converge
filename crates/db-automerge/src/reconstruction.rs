use automerge::AutoCommit;
use db_btree::{BTreeError, BTreeReadExecutor, BTreeResult};
use futures::{StreamExt, pin_mut};

use crate::{DocumentChangeKey, document_change_key::DocumentId};

#[derive(Debug, Clone)]
pub struct ReconstructedDocument {
  pub id: DocumentId,
  pub doc: Option<AutoCommit>,
  pub deltas: usize,
  pub bytes_size: usize,
}

impl ReconstructedDocument {
  pub fn new(id: DocumentId) -> Self {
    Self {
      id,
      doc: None,
      deltas: 0,
      bytes_size: 0,
    }
  }

  pub fn same_id(&self, key: &DocumentChangeKey) -> bool {
    &self.id == key.id()
  }

  pub fn apply(&mut self, key: &DocumentChangeKey, data: &[u8]) -> BTreeResult<()> {
    if let Some(doc) = self.doc.as_mut() {
      doc.load_incremental(data).map_err(BTreeError::custom)?;
    } else {
      if key.r#type().is_snapshot() {
        self.doc.replace(
          AutoCommit::load(data)
            .map_err(BTreeError::custom)?
            .with_actor(key.actor_id()),
        );
      } else {
        return Err(BTreeError::custom(format!(
          "Expected delta for document {:x?}, but found snapshot",
          key.id()
        )));
      }
    }

    self.deltas += 1;
    self.bytes_size += data.len();

    Ok(())
  }
}

pub async fn reconstruct_document<T>(tx: &T, key: &DocumentId) -> BTreeResult<ReconstructedDocument>
where
  T: BTreeReadExecutor<DocumentChangeKey, Vec<u8>>,
{
  let range = DocumentChangeKey::range_for(key);
  let stream = tx.range(range);
  pin_mut!(stream);

  let mut reconstructed_document_option = None;

  while let Some(item) = stream.next().await {
    let (key, data) = item?;

    let reconstructed_document = reconstructed_document_option
      .get_or_insert_with(|| ReconstructedDocument::new(key.id().clone()));

    reconstructed_document.apply(&key, &data)?;
  }

  reconstructed_document_option.ok_or_else(|| BTreeError::custom("Document not found".to_string()))
}
