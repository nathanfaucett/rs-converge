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
  pub id: DocumentId,
  pub r#type: DocumentType,
  pub change_hash: DocumentChangeHash,
}

impl DocumentChangeKey {
  pub fn new(id: DocumentId, r#type: DocumentType, change_hash: DocumentChangeHash) -> Self {
    Self {
      id,
      r#type,
      change_hash,
    }
  }

  pub fn new_snapshot(id: DocumentId, change_hash: DocumentChangeHash) -> Self {
    Self::new(id, DocumentType::Snapshot, change_hash)
  }

  pub fn new_incremental(id: DocumentId, change_hash: DocumentChangeHash) -> Self {
    Self::new(id, DocumentType::Incremental, change_hash)
  }

  pub fn id_to_uuid(id: &[u8]) -> Uuid {
    if id.len() == 16
      && let Some(uuid) = Uuid::from_slice(id).ok()
    {
      return uuid;
    }

    Uuid::new_v5(&Uuid::NAMESPACE_DNS, id)
  }

  pub fn id(&self) -> &[u8] {
    &self.id
  }

  pub fn r#type(&self) -> DocumentType {
    self.r#type
  }

  pub fn change_hash(&self) -> &DocumentChangeHash {
    &self.change_hash
  }

  pub fn uuid(&self) -> Uuid {
    Self::id_to_uuid(&self.id)
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

  pub fn to_bytes(&self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }

  pub fn min_for_id(id: DocumentId) -> Self {
    Self {
      id,
      r#type: DocumentType::Snapshot,
      change_hash: [0u8; 32],
    }
  }

  pub fn max_for_id(id: DocumentId) -> Self {
    Self {
      id,
      r#type: DocumentType::Incremental,
      change_hash: [255u8; 32],
    }
  }

  pub fn range_for(id: &DocumentId) -> impl RangeBounds<Self> {
    let start = Bound::Included(Self::min_for_id(id.clone()));
    let end = Bound::Included(Self::max_for_id(id.clone()));

    (start, end)
  }

  pub fn map_document_id_range<R>(range: R) -> impl RangeBounds<Self>
  where
    R: RangeBounds<DocumentId>,
  {
    let start = match range.start_bound() {
      Bound::Included(id) => Bound::Included(Self::min_for_id(id.clone())),
      Bound::Excluded(id) => Bound::Excluded(Self::max_for_id(id.clone())),
      Bound::Unbounded => Bound::Unbounded,
    };

    let end = match range.end_bound() {
      Bound::Included(id) => Bound::Included(Self::max_for_id(id.clone())),
      Bound::Excluded(id) => Bound::Excluded(Self::min_for_id(id.clone())),
      Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
  }
}

impl Ord for DocumentChangeKey {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .id
      .cmp(&other.id)
      .then_with(|| self.r#type.cmp(&other.r#type))
      .then_with(|| self.change_hash.cmp(&other.change_hash))
  }
}

impl PartialOrd for DocumentChangeKey {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl fmt::Display for DocumentChangeKey {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:x?}|{}|{:x?}", self.id, self.r#type, self.change_hash)
  }
}
