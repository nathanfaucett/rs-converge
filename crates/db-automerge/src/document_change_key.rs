use core::{
  cmp::Ordering,
  fmt,
  ops::{Bound, RangeBounds},
};

use automerge::ActorId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::document_type::DocumentType;

pub type DocumentId = Vec<u8>;
pub type DocumentChangeHash = [u8; 32];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct DocumentChangeKey {
  pub doc_id: DocumentId,
  pub doc_type: DocumentType,
  pub change_hash: DocumentChangeHash,
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

impl fmt::Display for DocumentChangeKey {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{:x?}|{}|{:x?}",
      self.doc_id, self.doc_type, self.change_hash
    )
  }
}

impl DocumentChangeKey {
  pub fn doc_id_to_uuid(doc_id: &[u8]) -> Uuid {
    if doc_id.len() == 16 {
      if let Some(uuid) = Uuid::from_slice(&doc_id).ok() {
        return uuid;
      }
    }

    Uuid::new_v5(&Uuid::NAMESPACE_DNS, &doc_id)
  }

  pub fn uuid(&self) -> Uuid {
    Self::doc_id_to_uuid(&self.doc_id)
  }

  pub fn actor_id(&self) -> ActorId {
    ActorId::from(self.uuid().as_bytes())
  }

  pub fn as_bytes(&self) -> &[u8] {
    unsafe {
      core::slice::from_raw_parts(
        self as *const Self as *const u8,
        core::mem::size_of::<Self>(),
      )
    }
  }

  pub fn min_for_id(doc_id: DocumentId) -> Self {
    Self {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [0u8; 32],
    }
  }

  pub fn max_for_id(doc_id: DocumentId) -> Self {
    Self {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: [255u8; 32],
    }
  }

  pub fn range_for(doc_id: DocumentId) -> impl RangeBounds<DocumentChangeKey> {
    let start = Bound::Included(Self::min_for_id(doc_id.clone()));
    let end = Bound::Included(Self::max_for_id(doc_id));

    (start, end)
  }

  pub fn map_doc_id_range(
    range: impl RangeBounds<DocumentId>,
  ) -> impl RangeBounds<DocumentChangeKey> {
    let start = match range.start_bound() {
      Bound::Included(doc_id) => Bound::Included(DocumentChangeKey::min_for_id(doc_id.clone())),
      Bound::Excluded(doc_id) => Bound::Excluded(DocumentChangeKey::max_for_id(doc_id.clone())),
      Bound::Unbounded => Bound::Unbounded,
    };

    let end = match range.end_bound() {
      Bound::Included(doc_id) => Bound::Included(DocumentChangeKey::max_for_id(doc_id.clone())),
      Bound::Excluded(doc_id) => Bound::Excluded(DocumentChangeKey::min_for_id(doc_id.clone())),
      Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
  }
}
