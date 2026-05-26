use core::{borrow::Borrow, ops::RangeBounds};

#[cfg(not(feature = "std"))]
use alloc::collections::BTreeMap;
#[cfg(feature = "std")]
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum TransactionEntry<V> {
  Present(V),
  Deleted,
}

impl<V> TransactionEntry<V> {
  pub fn as_option(&self) -> Option<&V> {
    match self {
      TransactionEntry::Present(value) => Some(value),
      TransactionEntry::Deleted => None,
    }
  }
}

#[derive(Debug, Clone)]
pub struct TransactionPatch<K, V>(BTreeMap<K, TransactionEntry<V>>);

impl<K, V> Default for TransactionPatch<K, V> {
  fn default() -> Self {
    Self(BTreeMap::new())
  }
}

impl<K, V> TransactionPatch<K, V> {
  pub fn get<Q>(&self, base: &BTreeMap<K, V>, key: &Q) -> Option<V>
  where
    K: Ord,
    V: Clone,
    Q: Borrow<K>,
  {
    match self.0.get(key.borrow()) {
      Some(entry) => entry.as_option().cloned(),
      None => base.get(key.borrow()).cloned(),
    }
  }

  pub fn insert(&mut self, key: K, value: V)
  where
    K: Ord,
  {
    self.0.insert(key, TransactionEntry::Present(value));
  }

  pub fn remove<Q>(&mut self, base: &BTreeMap<K, V>, key: Q) -> Option<V>
  where
    K: Ord + Clone,
    V: Clone,
    Q: Borrow<K>,
  {
    if self.0.contains_key(key.borrow()) {
      return None;
    }
    if let Some(entry) = base.get(key.borrow()) {
      self
        .0
        .insert(key.borrow().clone(), TransactionEntry::Deleted);
      return Some(entry.clone());
    }
    None
  }

  pub fn commit(self, base: &mut BTreeMap<K, V>)
  where
    K: Ord,
  {
    for (key, entry) in self.0 {
      match entry {
        TransactionEntry::Present(value) => {
          base.insert(key, value);
        }
        TransactionEntry::Deleted => {
          base.remove(&key);
        }
      }
    }
  }

  pub fn range<R>(&self, base: &BTreeMap<K, V>, range: R) -> BTreeMap<K, V>
  where
    K: Ord + Clone,
    V: Clone,
    R: RangeBounds<K>,
  {
    merge_range_maps(
      base,
      &self.0,
      range,
      |k| !matches!(self.0.get(k), Some(TransactionEntry::Deleted)),
      |k, entry, merged| match entry {
        TransactionEntry::Present(value) => {
          merged.insert(k.clone(), value.clone());
        }
        TransactionEntry::Deleted => {
          merged.remove(k);
        }
      },
    )
  }
}

fn merge_range_maps<K, V, P, R, FInclude, FApply>(
  base: &BTreeMap<K, V>,
  patch: &BTreeMap<K, P>,
  range: R,
  mut include_base: FInclude,
  mut apply_patch: FApply,
) -> BTreeMap<K, V>
where
  K: Ord + Clone,
  V: Clone,
  R: RangeBounds<K>,
  FInclude: FnMut(&K) -> bool,
  FApply: FnMut(&K, &P, &mut BTreeMap<K, V>),
{
  struct RangeBoundsRef<'a, R>(&'a R);

  impl<'a, R> Copy for RangeBoundsRef<'a, R> {}

  impl<'a, R> Clone for RangeBoundsRef<'a, R> {
    fn clone(&self) -> Self {
      *self
    }
  }

  impl<'a, T: ?Sized, R> RangeBounds<T> for RangeBoundsRef<'a, R>
  where
    R: RangeBounds<T>,
  {
    fn start_bound(&self) -> core::ops::Bound<&T> {
      self.0.start_bound()
    }

    fn end_bound(&self) -> core::ops::Bound<&T> {
      self.0.end_bound()
    }
  }

  let range_ref = RangeBoundsRef(&range);

  let mut merged = BTreeMap::new();

  for (k, v) in base.range(range_ref) {
    if include_base(k) {
      merged.insert(k.clone(), v.clone());
    }
  }

  for (k, p) in patch.range(range_ref) {
    apply_patch(k, p, &mut merged);
  }

  merged
}
