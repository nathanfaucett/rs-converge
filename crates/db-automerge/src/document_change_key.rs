use crate::document_type::DocumentType;
use core::cmp::Ordering;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentChangeKey {
  pub doc_id: Uuid,
  pub doc_type: DocumentType,
  pub change_hash: [u8; 32],
}

impl Ord for DocumentChangeKey {
  fn cmp(&self, other: &Self) -> Ordering {
    match self.doc_id.cmp(&other.doc_id) {
      Ordering::Equal => match self.doc_type.cmp(&other.doc_type) {
        Ordering::Equal => self.change_hash.cmp(&other.change_hash),
        ord => ord,
      },
      ord => ord,
    }
  }
}

impl PartialOrd for DocumentChangeKey {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

pub(super) fn document_entry_bounds(doc_id: Uuid) -> (DocumentChangeKey, DocumentChangeKey) {
  (
    DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [0u8; 32],
    },
    DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: [255u8; 32],
    },
  )
}

pub(super) fn all_document_bounds() -> (DocumentChangeKey, DocumentChangeKey) {
  (
    DocumentChangeKey {
      doc_id: Uuid::from_u128(0),
      doc_type: DocumentType::Snapshot,
      change_hash: [0u8; 32],
    },
    DocumentChangeKey {
      doc_id: Uuid::from_u128(u128::MAX),
      doc_type: DocumentType::Incremental,
      change_hash: [255u8; 32],
    },
  )
}
