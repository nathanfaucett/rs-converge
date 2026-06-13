use std::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use redb::{ReadableTable, Table, TableDefinition};

use db_btree::{BTreeError, BTreeResult};

#[allow(mismatched_lifetime_syntaxes)]
pub fn table_definition(name: &str) -> TableDefinition<&'static [u8], &'static [u8]> {
  TableDefinition::new(name)
}

pub fn tx_read<K, V, T>(table: &T, key: &Q) -> BTreeResult<Option<V>>
where
  K: Borrow<Q> + Codec,
  Q: Codex + ?Sized,
  V: Codec,
  T: ReadableTable<&'static [u8], &'static [u8]>,
{
  let key_bytes = key.to_bytes().map_err(BTreeError::custom)?;
  let key_bytes_ref = key_bytes.as_slice();

  if let Some(value_bytes) = table.get(key_bytes_ref).map_err(BTreeError::custom)? {
    let value = V::from_bytes(value_bytes.value()).map_err(BTreeError::custom)?;

    Ok(Some(value))
  } else {
    Ok(None)
  }
}

pub fn tx_range<K, V, R, T>(table: &T, range: R) -> BTreeResult<Vec<(K, V)>>
where
  K: Codec,
  V: Codec,
  R: RangeBounds<K>,
  T: ReadableTable<&'static [u8], &'static [u8]>,
{
  let mapped_range = map_range(range).map_err(BTreeError::custom)?;
  let mapped_range_ref = map_vec_range(&mapped_range);
  let iter = table.range(mapped_range_ref).map_err(BTreeError::custom)?;

  let mut results = Vec::new();

  for item in iter {
    let (key_bytes, value_bytes) = item.map_err(BTreeError::custom)?;

    let key = K::from_bytes(key_bytes.value()).map_err(BTreeError::custom)?;
    let value = V::from_bytes(value_bytes.value()).map_err(BTreeError::custom)?;

    results.push((key, value));
  }

  Ok(results)
}

pub fn tx_update<K, V, F>(
  tx: &mut Table<&[u8], &[u8]>,
  key: K,
  update_fn: F,
) -> BTreeResult<Option<()>>
where
  K: Codec,
  V: Codec,
  F: FnOnce(Option<V>) -> BTreeResult<Option<V>>,
{
  let key_bytes = key.to_bytes().map_err(BTreeError::custom)?;
  let key_bytes_ref = key_bytes.as_slice();

  let new_value_option = if let Some(current) = tx.get(key_bytes_ref).map_err(BTreeError::custom)? {
    let current_value = V::from_bytes(current.value()).map_err(BTreeError::custom)?;

    update_fn(Some(current_value))?
  } else {
    update_fn(None)?
  };

  if let Some(new_value) = new_value_option {
    let new_value_bytes = new_value.to_bytes().map_err(BTreeError::custom)?;
    let new_value_bytes_ref = new_value_bytes.as_slice();

    tx.insert(key_bytes_ref, new_value_bytes_ref)
      .map_err(BTreeError::custom)?;

    Ok(Some(()))
  } else {
    Ok(None)
  }
}

pub fn tx_insert<K, V>(tx: &mut Table<&[u8], &[u8]>, key: K, value: V) -> BTreeResult<()>
where
  K: Codec,
  V: Codec,
{
  let key_bytes = key.to_bytes().map_err(BTreeError::custom)?;
  let key_bytes_ref = key_bytes.as_slice();
  let value_bytes = value.to_bytes().map_err(BTreeError::custom)?;
  let value_bytes_ref = value_bytes.as_slice();

  tx.insert(key_bytes_ref, value_bytes_ref)
    .map_err(BTreeError::custom)?;

  Ok(())
}

pub fn tx_remove<K, V>(tx: &mut Table<&[u8], &[u8]>, key: &Q) -> BTreeResult<Option<V>>
where
  K: Borrow<Q> + Codec,
  Q: Codex + ?Sized,
  V: Codec,
{
  let key_bytes = key.to_bytes().map_err(BTreeError::custom)?;
  let key_bytes_ref = key_bytes.as_slice();

  if let Some(value_bytes) = tx.remove(key_bytes_ref).map_err(BTreeError::custom)? {
    let value = V::from_bytes(value_bytes.value()).map_err(BTreeError::custom)?;

    Ok(Some(value))
  } else {
    Ok(None)
  }
}

pub fn map_range<K, R>(range: R) -> CodecResult<impl RangeBounds<Vec<u8>>>
where
  K: Codec,
  R: RangeBounds<K>,
{
  let start = match range.start_bound() {
    Bound::Included(key) => Bound::Included(key.to_bytes()?),
    Bound::Excluded(key) => Bound::Excluded(key.to_bytes()?),
    Bound::Unbounded => Bound::Unbounded,
  };

  let end = match range.end_bound() {
    Bound::Included(key) => Bound::Included(key.to_bytes()?),
    Bound::Excluded(key) => Bound::Excluded(key.to_bytes()?),
    Bound::Unbounded => Bound::Unbounded,
  };

  Ok((start, end))
}

pub fn map_vec_range<'a, R>(range: &'a R) -> impl RangeBounds<&'a [u8]>
where
  R: RangeBounds<Vec<u8>> + 'a,
{
  let start = match range.start_bound() {
    Bound::Included(key) => Bound::Included(key.as_slice()),
    Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
    Bound::Unbounded => Bound::Unbounded,
  };

  let end = match range.end_bound() {
    Bound::Included(key) => Bound::Included(key.as_slice()),
    Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
    Bound::Unbounded => Bound::Unbounded,
  };

  (start, end)
}
