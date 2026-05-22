use automerge::AutoCommit;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::Value;
use automerge::transaction::Transactable;
use db_core::{
  BTreeError, Cursor, DecodeError, decode_bytes, decode_with_version, encode_bytes_into_sink,
};
use db_types::codec::{decode_store_key, decode_store_value, encode_store_key, encode_store_value};
use db_types::key_encoding::{DefaultEncoding, RowEncoding};
use db_types::{EngineKey, EngineValue, StoreKey, StoreValue};

use base64::{Engine as _, engine::general_purpose};

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

const ROW_FIELD: &str = "row";
const STORE_KEY_FIELD: &str = "store_key";

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

pub(crate) fn decode_snapshot_base64(value: impl ToString) -> Result<Vec<u8>, BTreeError> {
  let text = value.to_string();
  let encoded = text
    .strip_prefix('"')
    .and_then(|s| s.strip_suffix('"'))
    .unwrap_or(&text);

  general_purpose::STANDARD
    .decode(encoded.as_bytes())
    .map_err(BTreeError::other)
}

pub(crate) fn encode_snapshot_base64(bytes: &[u8]) -> String {
  general_purpose::STANDARD.encode(bytes)
}

pub(crate) fn snapshot_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, "snapshot") {
    Ok(Some(decode_snapshot_base64(value)?))
  } else {
    Ok(None)
  }
}

pub(crate) fn snapshot_doc(snapshot: &[u8]) -> Result<AutoCommit, BTreeError> {
  let snapshot_str = encode_snapshot_base64(snapshot);
  let mut doc = AutoCommit::new();
  doc
    .put(&automerge::ROOT, "snapshot", snapshot_str)
    .map_err(BTreeError::other)?;
  Ok(doc)
}

fn encode_row_cell(value: &EngineValue) -> Vec<u8> {
  <DefaultEncoding as RowEncoding>::encode_values(core::slice::from_ref(value))
}

fn decode_row_cell(bytes: &[u8]) -> Result<EngineValue, BTreeError> {
  let values = <DefaultEncoding as RowEncoding>::decode_values(bytes).map_err(BTreeError::other)?;
  if values.len() == 1 {
    Ok(values[0].clone())
  } else {
    Err(BTreeError::UnsupportedOperation)
  }
}

fn scalar_bytes(value: Value<'_>) -> Result<Vec<u8>, BTreeError> {
  match value {
    Value::Scalar(scalar) => match scalar.as_ref() {
      ScalarValue::Bytes(bytes) => Ok(bytes.to_vec()),
      _ => Err(BTreeError::UnsupportedOperation),
    },
    _ => Err(BTreeError::UnsupportedOperation),
  }
}

pub(crate) fn set_row_columns(doc: &mut AutoCommit, row: &[EngineValue]) -> Result<(), BTreeError> {
  if doc.get(&automerge::ROOT, ROW_FIELD).is_ok() {
    doc
      .delete(&automerge::ROOT, ROW_FIELD)
      .map_err(BTreeError::other)?;
  }

  let row_obj = doc
    .put_object(&automerge::ROOT, ROW_FIELD, ObjType::List)
    .map_err(BTreeError::other)?;

  for (index, value) in row.iter().enumerate() {
    let encoded = encode_row_cell(value);
    doc
      .insert(&row_obj, index, encoded)
      .map_err(BTreeError::other)?;
  }

  Ok(())
}

pub(crate) fn read_row_columns(doc: &AutoCommit) -> Result<Option<Vec<EngineValue>>, BTreeError> {
  let Some((value, row_obj)) = doc
    .get(&automerge::ROOT, ROW_FIELD)
    .map_err(BTreeError::other)?
  else {
    return Ok(None);
  };

  if !matches!(value, Value::Object(ObjType::List)) {
    return Err(BTreeError::UnsupportedOperation);
  }

  let mut row = Vec::with_capacity(doc.length(&row_obj));
  for index in 0..doc.length(&row_obj) {
    let Some((value, _)) = doc.get(&row_obj, index).map_err(BTreeError::other)? else {
      return Err(BTreeError::UnsupportedOperation);
    };
    let bytes = scalar_bytes(value)?;
    row.push(decode_row_cell(&bytes)?);
  }
  Ok(Some(row))
}

pub(crate) fn set_store_key_metadata(
  doc: &mut AutoCommit,
  key: &StoreKey,
) -> Result<(), BTreeError> {
  let mut encoded = Vec::new();
  encode_store_key(&mut encoded, key);
  doc
    .put(&automerge::ROOT, STORE_KEY_FIELD, encoded)
    .map_err(BTreeError::other)?;
  Ok(())
}

pub(crate) fn read_store_key_metadata(doc: &AutoCommit) -> Result<Option<StoreKey>, BTreeError> {
  let Some((value, _)) = doc
    .get(&automerge::ROOT, STORE_KEY_FIELD)
    .map_err(BTreeError::other)?
  else {
    return Ok(None);
  };
  let bytes = scalar_bytes(value)?;
  decode_with_version(&bytes, decode_store_key)
    .map(Some)
    .map_err(BTreeError::other)
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

  #[test]
  fn row_columns_roundtrip() {
    let mut doc = AutoCommit::new();
    let row = vec![
      EngineValue::Integer(1),
      EngineValue::Text("alice".to_string()),
      EngineValue::Null,
    ];

    set_row_columns(&mut doc, &row).expect("set row");
    assert_eq!(read_row_columns(&doc).expect("read row"), Some(row));
  }
}
