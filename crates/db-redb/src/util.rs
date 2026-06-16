use std::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use db_btree::{BTreeKey, BTreeQuery, BTreeValue};
use redb::TableDefinition;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedbCodecError {
  #[error("Error: {0}")]
  Custom(String),
}

pub type RedbCodecResult<T> = Result<T, RedbCodecError>;

pub trait RedbValue: BTreeValue
where
  Self: Sized,
{
  fn encode(&self) -> RedbCodecResult<Vec<u8>>;
  fn decode(bytes: &[u8]) -> RedbCodecResult<Self>;
}

pub trait RedbKey: BTreeKey + RedbValue {}

pub fn table_definition(name: &str) -> TableDefinition<&'static [u8], &'static [u8]> {
  TableDefinition::new(name)
}

pub fn map_range<K, Q, R>(range: R) -> RedbCodecResult<impl RangeBounds<Vec<u8>>>
where
  Q: BTreeQuery<K> + ?Sized,
  K: RedbKey + Borrow<Q>,
  R: RangeBounds<Q>,
{
  let start = match range.start_bound() {
    Bound::Included(q) => Bound::Included(q.to_key().encode()?),
    Bound::Excluded(q) => Bound::Excluded(q.to_key().encode()?),
    Bound::Unbounded => Bound::Unbounded,
  };

  let end = match range.end_bound() {
    Bound::Included(q) => Bound::Included(q.to_key().encode()?),
    Bound::Excluded(q) => Bound::Excluded(q.to_key().encode()?),
    Bound::Unbounded => Bound::Unbounded,
  };

  Ok((start, end))
}

pub fn range_as_ref<'a, R>(range: &'a R) -> impl RangeBounds<&'a [u8]>
where
  R: RangeBounds<Vec<u8>> + 'a,
{
  let start = match range.start_bound() {
    Bound::Included(q) => Bound::Included(q.as_slice()),
    Bound::Excluded(q) => Bound::Excluded(q.as_slice()),
    Bound::Unbounded => Bound::Unbounded,
  };

  let end = match range.end_bound() {
    Bound::Included(q) => Bound::Included(q.as_slice()),
    Bound::Excluded(q) => Bound::Excluded(q.as_slice()),
    Bound::Unbounded => Bound::Unbounded,
  };

  (start, end)
}
