use std::{
  cmp::Ordering,
  ops::{Bound, RangeBounds},
};

#[derive(Clone, Debug)]
pub struct Key<K>(K);

impl<K> Key<K> {
  pub fn new(key: K) -> Self {
    Self(key)
  }

  pub fn into_inner(self) -> K {
    self.0
  }
}

impl<K> Key<K>
where
  K: Clone,
{
  pub fn range<R>(range: R) -> impl RangeBounds<Key<K>>
  where
    R: RangeBounds<K>,
  {
    let start = match range.start_bound() {
      Bound::Included(k) => Bound::Included(Key(k.clone())),
      Bound::Excluded(k) => Bound::Excluded(Key(k.clone())),
      Bound::Unbounded => Bound::Unbounded,
    };

    let end = match range.end_bound() {
      Bound::Included(k) => Bound::Included(Key(k.clone())),
      Bound::Excluded(k) => Bound::Excluded(Key(k.clone())),
      Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
  }
}

impl<K> PartialEq for Key<K>
where
  K: redb::Key + 'static,
  for<'a> K: redb::Value<SelfType<'a> = K>,
{
  fn eq(&self, other: &Self) -> bool {
    K::compare(
      K::as_bytes(&self.0).as_ref(),
      K::as_bytes(&other.0).as_ref(),
    ) == Ordering::Equal
  }
}

impl<K> Eq for Key<K>
where
  K: redb::Key + 'static,
  for<'a> K: redb::Value<SelfType<'a> = K>,
{
}

impl<K> PartialOrd for Key<K>
where
  K: redb::Key + 'static,
  for<'a> K: redb::Value<SelfType<'a> = K>,
{
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<K> Ord for Key<K>
where
  K: redb::Key + 'static,
  for<'a> K: redb::Value<SelfType<'a> = K>,
{
  fn cmp(&self, other: &Self) -> Ordering {
    K::compare(
      K::as_bytes(&self.0).as_ref(),
      K::as_bytes(&other.0).as_ref(),
    )
  }
}

impl<K> redb::Value for Key<K>
where
  K: redb::Key + 'static,
  for<'a> K: redb::Value<SelfType<'a> = K>,
{
  type SelfType<'a>
    = Key<K::SelfType<'a>>
  where
    Self: 'a;
  type AsBytes<'a>
    = K::AsBytes<'a>
  where
    Self: 'a;

  fn fixed_width() -> Option<usize> {
    K::fixed_width()
  }

  fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
  where
    Self: 'a,
  {
    Key(K::from_bytes(data))
  }

  fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
  where
    Self: 'b,
  {
    K::as_bytes(&value.0)
  }

  fn type_name() -> redb::TypeName {
    redb::TypeName::new(std::any::type_name::<Self>())
  }
}

impl<K> redb::Key for Key<K>
where
  K: redb::Key + 'static,
  for<'a> K: redb::Value<SelfType<'a> = K>,
{
  fn compare(a: &[u8], b: &[u8]) -> Ordering {
    K::compare(a, b)
  }
}
