use db_core::{BTreeError, Cursor, DecodeError, decode_bytes, encode_bytes_into_sink};
use db_types::codec::{decode_store_key, decode_store_value, encode_store_key, encode_store_value};
use db_types::{EngineKey, StoreKey, StoreValue};

pub(crate) trait SnapshotAdapter {
  type Key: Clone + Eq;
  type Value: Clone;

  fn decode_key(cursor: &mut Cursor<'_>) -> Result<Self::Key, BTreeError>;
  fn decode_value(cursor: &mut Cursor<'_>) -> Result<Self::Value, BTreeError>;
  fn encode_key(buffer: &mut Vec<u8>, key: &Self::Key);
  fn encode_value(buffer: &mut Vec<u8>, value: &Self::Value);
}

type SnapshotEntries<A> = Vec<(<A as SnapshotAdapter>::Key, <A as SnapshotAdapter>::Value)>;
#[cfg(test)]
type SnapshotRemoveOutcome<A> = (Option<<A as SnapshotAdapter>::Value>, Option<Vec<u8>>);

pub(crate) struct StoreSnapshotAdapter;

fn decode_snapshot_value<T>(
  cursor: &mut Cursor<'_>,
  decode: impl FnOnce(&mut Cursor<'_>) -> Result<T, DecodeError>,
) -> Result<T, BTreeError> {
  decode(cursor).map_err(BTreeError::other)
}

impl SnapshotAdapter for StoreSnapshotAdapter {
  type Key = StoreKey;
  type Value = StoreValue;

  fn decode_key(cursor: &mut Cursor<'_>) -> Result<Self::Key, BTreeError> {
    decode_snapshot_value(cursor, decode_store_key)
  }

  fn decode_value(cursor: &mut Cursor<'_>) -> Result<Self::Value, BTreeError> {
    decode_snapshot_value(cursor, decode_store_value)
  }

  fn encode_key(buffer: &mut Vec<u8>, key: &Self::Key) {
    encode_store_key(buffer, key);
  }

  fn encode_value(buffer: &mut Vec<u8>, value: &Self::Value) {
    encode_store_value(buffer, value);
  }
}

pub(crate) struct EngineSnapshotAdapter;

impl SnapshotAdapter for EngineSnapshotAdapter {
  type Key = EngineKey;
  type Value = Vec<u8>;

  fn decode_key(cursor: &mut Cursor<'_>) -> Result<Self::Key, BTreeError> {
    decode_bytes(cursor).map_err(BTreeError::other)
  }

  fn decode_value(cursor: &mut Cursor<'_>) -> Result<Self::Value, BTreeError> {
    decode_bytes(cursor).map_err(BTreeError::other)
  }

  fn encode_key(buffer: &mut Vec<u8>, key: &Self::Key) {
    db_core::encode_with_version(buffer, |sink| encode_bytes_into_sink(sink, key));
  }

  fn encode_value(buffer: &mut Vec<u8>, value: &Self::Value) {
    db_core::encode_with_version(buffer, |sink| encode_bytes_into_sink(sink, value));
  }
}

pub(crate) fn parse_entries<A: SnapshotAdapter>(
  buf: &[u8],
) -> Result<SnapshotEntries<A>, BTreeError> {
  let mut out = Vec::new();
  let mut cursor = Cursor::new(buf);

  loop {
    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(DecodeError::Truncated) => break,
      Err(e) => return Err(BTreeError::other(e)),
    }

    let key = A::decode_key(&mut cursor)?;

    match db_core::decode_version(&mut cursor) {
      Ok(()) => {}
      Err(e) => return Err(BTreeError::other(e)),
    }

    let value = A::decode_value(&mut cursor)?;
    out.push((key, value));
  }

  Ok(out)
}

pub(crate) fn encode_entries<A: SnapshotAdapter>(entries: &[(A::Key, A::Value)]) -> Vec<u8> {
  let mut buf = Vec::new();
  for (key, value) in entries {
    A::encode_key(&mut buf, key);
    A::encode_value(&mut buf, value);
  }
  buf
}

pub(crate) fn find_entry<A: SnapshotAdapter>(
  buf: &[u8],
  needle: &A::Key,
) -> Result<Option<A::Value>, BTreeError> {
  for (key, value) in parse_entries::<A>(buf)? {
    if &key == needle {
      return Ok(Some(value));
    }
  }
  Ok(None)
}

pub(crate) fn key_in_range<K, R>(key: &K, range: &R) -> bool
where
  K: Ord,
  R: core::ops::RangeBounds<K>,
{
  use core::ops::Bound;

  let start = match range.start_bound() {
    Bound::Included(lower) => key >= lower,
    Bound::Excluded(lower) => key > lower,
    Bound::Unbounded => true,
  };
  let end = match range.end_bound() {
    Bound::Included(upper) => key <= upper,
    Bound::Excluded(upper) => key < upper,
    Bound::Unbounded => true,
  };
  start && end
}

pub(crate) fn set_entry<A: SnapshotAdapter>(
  buf: Option<&[u8]>,
  key: &A::Key,
  value: &A::Value,
) -> Result<Vec<u8>, BTreeError> {
  let mut entries = if let Some(buf) = buf {
    parse_entries::<A>(buf)?
  } else {
    Vec::new()
  };

  if let Some((_, existing)) = entries.iter_mut().find(|(existing, _)| existing == key) {
    *existing = value.clone();
  } else {
    entries.push((key.clone(), value.clone()));
  }

  Ok(encode_entries::<A>(&entries))
}

#[cfg(test)]
pub(crate) fn remove_entry<A: SnapshotAdapter>(
  buf: Option<&[u8]>,
  key: &A::Key,
) -> Result<SnapshotRemoveOutcome<A>, BTreeError> {
  let mut entries = if let Some(buf) = buf {
    parse_entries::<A>(buf)?
  } else {
    Vec::new()
  };

  let mut removed = None;
  entries.retain(|(existing, value)| {
    if existing == key {
      removed = Some(value.clone());
      false
    } else {
      true
    }
  });

  if removed.is_none() {
    return Ok((None, buf.map(|bytes| bytes.to_vec())));
  }

  if entries.is_empty() {
    Ok((removed, None))
  } else {
    Ok((removed, Some(encode_entries::<A>(&entries))))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_types::EngineValue;
  use db_types::key_encoding::{DefaultEncoding, KeyEncoding, RowEncoding};

  fn key(values: Vec<EngineValue>) -> EngineKey {
    <DefaultEncoding as KeyEncoding>::encode_values(&values)
  }

  fn row(values: Vec<EngineValue>) -> Vec<u8> {
    <DefaultEncoding as RowEncoding>::encode_values(&values)
  }

  #[test]
  fn store_snapshot_roundtrip_and_remove() {
    let key = StoreKey::table_row("users".to_string(), key(vec![EngineValue::Integer(1)]));
    let value = StoreValue::Row(vec![EngineValue::Text("alice".to_string())]);

    let bytes = set_entry::<StoreSnapshotAdapter>(None, &key, &value).expect("set");
    assert_eq!(
      find_entry::<StoreSnapshotAdapter>(&bytes, &key).expect("find"),
      Some(value.clone())
    );

    let (removed, updated) =
      remove_entry::<StoreSnapshotAdapter>(Some(&bytes), &key).expect("remove");
    assert_eq!(removed, Some(value));
    assert_eq!(updated, None);
  }

  #[test]
  fn engine_snapshot_updates_existing_key() {
    let key = key(vec![EngineValue::Integer(7)]);
    let value1 = row(vec![EngineValue::Text("first".to_string())]);
    let value2 = row(vec![EngineValue::Text("second".to_string())]);

    let first = set_entry::<EngineSnapshotAdapter>(None, &key, &value1).expect("first");
    let second = set_entry::<EngineSnapshotAdapter>(Some(&first), &key, &value2).expect("second");

    assert_eq!(
      find_entry::<EngineSnapshotAdapter>(&second, &key).expect("find"),
      Some(value2)
    );
  }
}
