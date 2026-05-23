use automerge::AutoCommit;
use automerge::ObjType;
use automerge::ReadDoc;
use automerge::ScalarValue;
use automerge::Value;
use automerge::transaction::Transactable;
use db_core::{BTreeError, decode_with_version};
use db_types::codec::{decode_store_key, decode_store_value, encode_store_key, encode_store_value};
use db_types::key_encoding::{DefaultEncoding, RowEncoding};
use db_types::{EngineValue, StoreKey, StoreValue};

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

pub(crate) fn set_doc_value(doc: &mut AutoCommit, value: &StoreValue) -> Result<(), BTreeError> {
  clear_doc_fields(doc)?;
  let mut encoded = Vec::new();
  encode_store_value(&mut encoded, value);
  doc
    .put(&automerge::ROOT, VALUE_FIELD, encoded)
    .map_err(BTreeError::other)?;
  Ok(())
}

pub(crate) fn read_value_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, VALUE_FIELD) {
    return Ok(Some(scalar_bytes(value)?));
  }
  Ok(None)
}

pub(crate) fn set_tombstone(doc: &mut AutoCommit) -> Result<(), BTreeError> {
  clear_doc_fields(doc)?;
  doc
    .put(&automerge::ROOT, TOMBSTONE_FIELD, true)
    .map_err(BTreeError::other)?;
  Ok(())
}

pub(crate) fn is_tombstone(doc: &AutoCommit) -> Result<bool, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, TOMBSTONE_FIELD) {
    return match value {
      Value::Scalar(scalar) => match scalar.as_ref() {
        ScalarValue::Boolean(b) => Ok(*b),
        _ => Err(BTreeError::UnsupportedOperation),
      },
      _ => Err(BTreeError::UnsupportedOperation),
    };
  }
  Ok(false)
}

pub(crate) fn read_doc_value(doc: &AutoCommit) -> Result<Option<StoreValue>, BTreeError> {
  if is_tombstone(doc)? {
    return Ok(None);
  }

  if let Some(row) = read_row_columns(doc)? {
    if row.is_empty() {
      return Ok(None);
    }
    return Ok(Some(StoreValue::Row(row)));
  }

  if let Some(bytes) = read_value_bytes(doc)? {
    return decode_with_version(&bytes, decode_store_value)
      .map(Some)
      .map_err(BTreeError::other);
  }

  Ok(None)
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

pub(crate) fn is_direct_document(doc: &AutoCommit) -> Result<bool, BTreeError> {
  if read_store_key_metadata(doc)?.is_none() {
    return Ok(false);
  }
  if is_tombstone(doc)? {
    return Ok(true);
  }
  if read_row_columns(doc)?.is_some() {
    return Ok(true);
  }
  if read_doc_value(doc)?.is_some() {
    return Ok(true);
  }
  Ok(false)
}

pub(crate) fn read_direct_document_entry(
  doc: &AutoCommit,
) -> Result<Option<StoreValue>, BTreeError> {
  read_doc_value(doc)
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
