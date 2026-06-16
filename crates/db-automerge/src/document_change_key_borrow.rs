use core::{
  cmp::Ordering,
  ops::{Bound, RangeBounds},
};
use std::borrow::Borrow;

use db_btree::BTreeQuery;

use crate::{DocumentChangeHash, DocumentId, document_type::DocumentType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentChangeKeyBorrow<'a, Q>
where
  Q: BTreeQuery<DocumentId> + ?Sized,
  DocumentId: Borrow<Q>,
{
  pub id: &'a Q,
  pub r#type: DocumentType,
  pub change_hash: DocumentChangeHash,
}

impl<'a, Q> DocumentChangeKeyBorrow<'a, Q>
where
  Q: BTreeQuery<DocumentId> + ?Sized,
  DocumentId: Borrow<Q>,
{
  pub fn id(&self) -> &Q {
    self.id
  }

  pub fn r#type(&self) -> DocumentType {
    self.r#type
  }

  pub fn change_hash(&self) -> &DocumentChangeHash {
    &self.change_hash
  }

  pub fn min_for_id(id: &'a Q) -> Self {
    Self {
      id,
      r#type: DocumentType::Snapshot,
      change_hash: [0u8; 32],
    }
  }

  pub fn max_for_id(id: &'a Q) -> Self {
    Self {
      id,
      r#type: DocumentType::Incremental,
      change_hash: [255u8; 32],
    }
  }

  pub fn range_for(id: &'a Q) -> impl RangeBounds<Self> {
    let start = Bound::Included(Self::min_for_id(id));
    let end = Bound::Included(Self::max_for_id(id));

    (start, end)
  }

  pub fn map_document_id_range<R>(range: &'a R) -> impl RangeBounds<Self>
  where
    R: RangeBounds<Q>,
  {
    let start = match range.start_bound() {
      Bound::Included(id) => Bound::Included(Self::min_for_id(id)),
      Bound::Excluded(id) => Bound::Excluded(Self::max_for_id(id)),
      Bound::Unbounded => Bound::Unbounded,
    };

    let end = match range.end_bound() {
      Bound::Included(id) => Bound::Included(Self::max_for_id(id)),
      Bound::Excluded(id) => Bound::Excluded(Self::min_for_id(id)),
      Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
  }
}

impl<'a, Q> Ord for DocumentChangeKeyBorrow<'a, Q>
where
  Q: BTreeQuery<DocumentId> + ?Sized,
  DocumentId: Borrow<Q>,
{
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .id
      .cmp(other.id)
      .then_with(|| self.r#type.cmp(&other.r#type))
      .then_with(|| self.change_hash.cmp(&other.change_hash))
  }
}

impl<'a, Q> PartialOrd for DocumentChangeKeyBorrow<'a, Q>
where
  Q: BTreeQuery<DocumentId> + ?Sized,
  DocumentId: Borrow<Q>,
{
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}
