use core::{
  cmp::Ordering,
  ops::{Bound, RangeBounds},
};

use automerge::ActorId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::document_type::DocumentType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
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

impl DocumentChangeKey {
  pub fn actor_id(&self) -> ActorId {
    ActorId::from(self.doc_id.as_bytes())
  }

  pub fn as_bytes(&self) -> &[u8] {
    unsafe {
      core::slice::from_raw_parts(
        self as *const Self as *const u8,
        core::mem::size_of::<Self>(),
      )
    }
  }

  pub fn min_for_id(doc_id: Uuid) -> Self {
    Self {
      doc_id,
      doc_type: DocumentType::Snapshot,
      change_hash: [0u8; 32],
    }
  }

  pub fn max_for_id(doc_id: Uuid) -> Self {
    Self {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: [255u8; 32],
    }
  }

  pub fn range_for(doc_id: Uuid) -> impl RangeBounds<DocumentChangeKey> {
    let start = Bound::Included(Self::min_for_id(doc_id));
    let end = Bound::Included(Self::max_for_id(doc_id));

    (start, end)
  }

  pub fn map_uuid_range(uuid_range: impl RangeBounds<Uuid>) -> impl RangeBounds<DocumentChangeKey> {
    let start = match uuid_range.start_bound() {
      Bound::Included(&uuid) => Bound::Included(DocumentChangeKey::min_for_id(uuid)),
      Bound::Excluded(&uuid) => Bound::Excluded(DocumentChangeKey::max_for_id(uuid)),
      Bound::Unbounded => Bound::Unbounded,
    };

    let end = match uuid_range.end_bound() {
      Bound::Included(&uuid) => Bound::Included(DocumentChangeKey::max_for_id(uuid)),
      Bound::Excluded(&uuid) => Bound::Excluded(DocumentChangeKey::min_for_id(uuid)),
      Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
  }
}
