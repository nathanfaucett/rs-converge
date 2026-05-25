use core::ops::RangeBounds;

#[cfg(not(feature = "std"))]
use alloc::collections::BTreeMap;
#[cfg(feature = "std")]
use std::collections::BTreeMap;

pub fn merge_range_maps<K, V, P, R, FInclude, FApply>(
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
