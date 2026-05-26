//! Automerge document encoding for engine-facing named-tree values.
//!
//! Maps engine keys and row bytes to Automerge document fields. Merge and CRDT
//! semantics live in `db-automerge`; this module only builds and reads documents.

use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, ScalarValue, Value};
use db_core::{BTreeError, decode_with_version};
use db_engine::key_encoding::{DefaultEncoding, RowEncoding};
use db_engine::{EngineKey, EngineValue, StoreKey};
use db_engine::{decode_store_key, encode_store_key};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const NAMED_KEY_FIELD: &str = "named_key";
const NAMED_VALUE_FIELD: &str = "value";
const NAMED_TOMBSTONE_FIELD: &str = "deleted";
const ROW_FIELD: &str = "row";
const STORE_KEY_FIELD: &str = "store_key";
const VALUE_FIELD: &str = "value";
const TOMBSTONE_FIELD: &str = "deleted";

pub(crate) fn is_row_tree(tree: &str) -> bool {
  tree.starts_with("t:")
}

pub(crate) fn doc_id_for_key(key: &EngineKey) -> Uuid {
  let mut hasher = Sha256::new();
  hasher.update(key);
  let key_digest = hasher.finalize();

  let mut bytes = [0u8; 16];
  bytes.copy_from_slice(&key_digest[..16]);
  Uuid::from_bytes(bytes)
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

fn clear_doc_fields(doc: &mut AutoCommit) -> Result<(), BTreeError> {
  for field in [ROW_FIELD, VALUE_FIELD, TOMBSTONE_FIELD] {
    if let Ok(Some(_)) = doc.get(&automerge::ROOT, field) {
      doc
        .delete(&automerge::ROOT, field)
        .map_err(BTreeError::other)?;
    }
  }
  Ok(())
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

fn set_row_columns(doc: &mut AutoCommit, row: &[EngineValue]) -> Result<(), BTreeError> {
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

fn set_store_key_metadata(doc: &mut AutoCommit, key: &StoreKey) -> Result<(), BTreeError> {
  let mut encoded = Vec::new();
  encode_store_key(&mut encoded, key);
  doc
    .put(&automerge::ROOT, STORE_KEY_FIELD, encoded)
    .map_err(BTreeError::other)?;
  Ok(())
}

fn read_store_key_metadata(doc: &AutoCommit) -> Result<Option<StoreKey>, BTreeError> {
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

fn set_named_key_metadata(doc: &mut AutoCommit, key: &EngineKey) -> Result<(), BTreeError> {
  if let Ok(Some(_)) = doc.get(&automerge::ROOT, NAMED_KEY_FIELD) {
    doc
      .delete(&automerge::ROOT, NAMED_KEY_FIELD)
      .map_err(BTreeError::other)?;
  }
  doc
    .put(&automerge::ROOT, NAMED_KEY_FIELD, key.clone())
    .map_err(BTreeError::other)?;
  Ok(())
}

fn set_named_value(doc: &mut AutoCommit, value: &[u8]) -> Result<(), BTreeError> {
  if let Ok(Some(_)) = doc.get(&automerge::ROOT, NAMED_VALUE_FIELD) {
    doc
      .delete(&automerge::ROOT, NAMED_VALUE_FIELD)
      .map_err(BTreeError::other)?;
  }
  doc
    .put(&automerge::ROOT, NAMED_VALUE_FIELD, value.to_vec())
    .map_err(BTreeError::other)?;
  Ok(())
}

fn set_named_tombstone(doc: &mut AutoCommit) -> Result<(), BTreeError> {
  if let Ok(Some(_)) = doc.get(&automerge::ROOT, NAMED_TOMBSTONE_FIELD) {
    doc
      .delete(&automerge::ROOT, NAMED_TOMBSTONE_FIELD)
      .map_err(BTreeError::other)?;
  }
  doc
    .put(&automerge::ROOT, NAMED_TOMBSTONE_FIELD, true)
    .map_err(BTreeError::other)?;
  Ok(())
}

pub(crate) fn is_named_tombstone(doc: &AutoCommit) -> Result<bool, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, NAMED_TOMBSTONE_FIELD) {
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

pub(crate) fn read_named_value_bytes(doc: &AutoCommit) -> Result<Option<Vec<u8>>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, NAMED_VALUE_FIELD) {
    return Ok(Some(scalar_bytes(value)?));
  }
  Ok(None)
}

pub(crate) fn read_named_key_metadata(doc: &AutoCommit) -> Result<Option<EngineKey>, BTreeError> {
  if let Ok(Some((value, _id))) = doc.get(&automerge::ROOT, NAMED_KEY_FIELD) {
    return Ok(Some(scalar_bytes(value)?));
  }
  Ok(None)
}

pub(crate) fn build_named_doc(
  existing: Option<AutoCommit>,
  key: &EngineKey,
  value: &[u8],
) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing.unwrap_or_default();
  clear_doc_fields(&mut doc)?;
  set_named_key_metadata(&mut doc, key)?;
  set_named_value(&mut doc, value)?;
  Ok(doc)
}

fn build_named_tombstone(existing: AutoCommit, key: &EngineKey) -> Result<AutoCommit, BTreeError> {
  let mut doc = existing;
  clear_doc_fields(&mut doc)?;
  set_named_key_metadata(&mut doc, key)?;
  set_named_tombstone(&mut doc)?;
  Ok(doc)
}

pub(crate) fn decode_row_bytes(row: &[u8]) -> Result<Vec<EngineValue>, BTreeError> {
  <DefaultEncoding as RowEncoding>::decode_values(row).map_err(BTreeError::other)
}

pub(crate) fn encode_row_bytes(row: &[EngineValue]) -> Vec<u8> {
  <DefaultEncoding as RowEncoding>::encode_values(row)
}

pub(crate) fn row_key_from_doc(doc: &AutoCommit) -> Result<EngineKey, BTreeError> {
  match read_store_key_metadata(doc)? {
    Some(StoreKey::TableRow { primary_key, .. }) => Ok(primary_key),
    _ => Err(BTreeError::UnsupportedOperation),
  }
}

pub(crate) fn build_named_tree_document(
  tree: &str,
  key: &EngineKey,
  value: Vec<u8>,
  existing: Option<AutoCommit>,
) -> Result<AutoCommit, BTreeError>
where
  EngineKey: Ord,
{
  if is_row_tree(tree) {
    let row = decode_row_bytes(&value)?;
    let mut doc = existing.unwrap_or_default();
    clear_doc_fields(&mut doc)?;
    set_store_key_metadata(
      &mut doc,
      &StoreKey::TableRow {
        table_name: tree.strip_prefix("t:").unwrap_or(tree).to_string(),
        primary_key: key.clone(),
      },
    )?;
    set_row_columns(&mut doc, &row)?;
    Ok(doc)
  } else {
    build_named_doc(existing, key, &value)
  }
}

pub(crate) fn build_removed_named_row_doc(
  mut doc: AutoCommit,
) -> Result<(AutoCommit, Option<Vec<u8>>), BTreeError> {
  let removed = read_row_columns(&doc)?.and_then(|row| {
    if row.is_empty() {
      None
    } else {
      Some(encode_row_bytes(&row))
    }
  });

  if removed.is_some() {
    set_row_columns(&mut doc, &[])?;
    Ok((doc, removed))
  } else {
    Ok((doc, None))
  }
}

pub(crate) fn build_removed_named_doc(
  doc: AutoCommit,
  key: &EngineKey,
) -> Result<(AutoCommit, Option<Vec<u8>>), BTreeError> {
  if is_named_tombstone(&doc)? {
    return Ok((doc, None));
  }

  let removed = read_named_value_bytes(&doc)?;
  if removed.is_none() {
    return Ok((doc, None));
  }

  let next_doc = build_named_tombstone(doc, key)?;
  Ok((next_doc, removed))
}
