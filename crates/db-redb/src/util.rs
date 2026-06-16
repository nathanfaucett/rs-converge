use std::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use db_btree::{BTreeKey, BTreeQuery, BTreeValue};
use redb::TableDefinition;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedbError {
  #[error("Error: {0}")]
  Custom(String),
}

impl RedbError {
  pub fn custom<T>(error: T) -> Self
  where
    T: ToString,
  {
    Self::Custom(error.to_string())
  }
}

pub type RedbResult<T> = Result<T, RedbError>;

pub trait RedbValue: BTreeValue
where
  Self: Sized,
{
  fn encode(&self) -> RedbResult<Vec<u8>>;
  fn decode(bytes: &[u8]) -> RedbResult<Self>;
}

impl<T> RedbValue for T
where
  T: BTreeValue + Serialize + DeserializeOwned,
{
  fn encode(&self) -> RedbResult<Vec<u8>> {
    postcard::to_stdvec(self).map_err(RedbError::custom)
  }

  fn decode(bytes: &[u8]) -> RedbResult<Self> {
    postcard::from_bytes(bytes).map_err(RedbError::custom)
  }
}

pub trait RedbKey: BTreeKey + RedbValue {}

impl<T> RedbKey for T where T: BTreeKey + RedbValue {}

#[allow(mismatched_lifetime_syntaxes)]
pub fn table_definition(name: &str) -> TableDefinition<&'static [u8], &'static [u8]> {
  TableDefinition::new(name)
}

pub fn map_range<K, Q, R>(range: R) -> RedbResult<impl RangeBounds<Vec<u8>>>
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
