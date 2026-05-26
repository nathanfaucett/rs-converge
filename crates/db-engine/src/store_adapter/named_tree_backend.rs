use super::EngineNamedTreeTransaction;
use crate::key_encoding::{DefaultEncoding, KeyEncoding, RowEncoding};
use crate::{EngineError, EngineKey, EngineRow, EngineValue, PrimaryKey};
use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};

#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Raw named-tree storage access for engine-backed stores.
///
/// This module keeps the engine implementation from depending directly on
/// low-level `get`/`insert`/`remove` calls on a `NamedTreeTransaction`.
pub(super) async fn get_bytes<'a, T>(
  tx: &'a mut T,
  tree: &'a str,
  key: &'a EngineKey,
) -> Result<Option<Vec<u8>>, EngineError>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  tx.get(tree, key).await.map_err(EngineError::from)
}

pub(super) async fn insert_bytes<'a, T>(
  tx: &'a mut T,
  tree: &'a str,
  key: EngineKey,
  value: Vec<u8>,
) -> Result<(), EngineError>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  tx.insert(tree, key, value).await.map_err(EngineError::from)
}

pub(super) async fn remove_bytes<'a, T>(
  tx: &'a mut T,
  tree: &'a str,
  key: &'a EngineKey,
) -> Result<Option<Vec<u8>>, EngineError>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  tx.remove(tree, key).await.map_err(EngineError::from)
}

pub(super) fn range_bytes<'a, T>(
  tx: &'a T,
  tree: String,
) -> impl Stream<Item = Result<(EngineKey, Vec<u8>), EngineError>> + 'a
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  stream! {
    let s = tx.range(&tree, ..);
    pin_mut!(s);
    while let Some(item) = s.next().await {
      yield item.map_err(EngineError::from);
    }
  }
}

pub(super) fn encode_row_bytes(row: &EngineRow) -> Vec<u8> {
  <DefaultEncoding as RowEncoding>::encode_values(row)
}

pub(super) fn decode_row_bytes(bytes: &[u8]) -> Result<EngineRow, EngineError> {
  <DefaultEncoding as RowEncoding>::decode_values(bytes)
    .map_err(|error| EngineError::SchemaMismatch(format!("row decode error: {}", error)))
}

pub(super) fn primary_key_from_engine_key(key: &EngineKey) -> Result<PrimaryKey, EngineError> {
  let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
    .map_err(|e| EngineError::SchemaMismatch(format!("decode key error: {}", e)))?;

  if values.len() != 1 {
    return Err(EngineError::SchemaMismatch(
      "row primary key must be single UUID value".into(),
    ));
  }

  match &values[0] {
    EngineValue::Uuid(bytes) => Ok(PrimaryKey::new(*bytes)),
    _ => Err(EngineError::SchemaMismatch(
      "row primary key must be UUID".into(),
    )),
  }
}

pub(super) async fn collect_tree_rows<T>(
  tx: &T,
  tree_name: &str,
) -> Result<Vec<EngineRow>, EngineError>
where
  T: EngineNamedTreeTransaction<EngineKey, Vec<u8>>,
{
  let stream = tx.range(tree_name, ..);
  pin_mut!(stream);

  let mut rows = Vec::new();
  while let Some(item) = stream.next().await {
    let (_key, row_bytes) = item.map_err(EngineError::from)?;
    rows.push(decode_row_bytes(&row_bytes)?);
  }
  Ok(rows)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::EngineValue;

  fn uuid_bytes() -> [u8; 16] {
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  }

  #[test]
  fn encode_and_decode_roundtrip_engine_row() {
    let row = vec![EngineValue::Uuid(uuid_bytes()), EngineValue::Integer(42)];
    let bytes = encode_row_bytes(&row);
    let decoded = decode_row_bytes(&bytes).expect("decode should succeed");
    assert_eq!(decoded, row);
  }

  #[test]
  fn primary_key_from_engine_key_returns_uuid_primary_key() {
    let key = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Uuid(uuid_bytes())]);
    let pk = primary_key_from_engine_key(&key).expect("primary key decode");
    assert_eq!(*pk.as_bytes(), uuid_bytes());
  }
}
