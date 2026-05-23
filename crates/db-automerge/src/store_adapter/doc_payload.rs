use automerge::AutoCommit;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::Value;
use automerge::transaction::Transactable;
use db_core::{BTreeError, decode_with_version};
use db_types::codec::{decode_store_key, encode_store_key};
use db_types::key_encoding::{DefaultEncoding, RowEncoding};
use db_types::{EngineValue, StoreKey};

const ROW_FIELD: &str = "row";
const STORE_KEY_FIELD: &str = "store_key";
const VALUE_FIELD: &str = "value";
const TOMBSTONE_FIELD: &str = "deleted";

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

pub(crate) fn clear_doc_fields(doc: &mut AutoCommit) -> Result<(), BTreeError> {
  for field in [ROW_FIELD, VALUE_FIELD, TOMBSTONE_FIELD] {
    if let Ok(Some(_)) = doc.get(&automerge::ROOT, field) {
      doc
        .delete(&automerge::ROOT, field)
        .map_err(BTreeError::other)?;
    }
  }
  Ok(())
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
