use db_core::BTreeError;
use db_types::key_encoding::{DefaultEncoding, KeyEncoding};
use db_types::{EngineKey, EngineValue};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn is_row_tree(tree: &str) -> bool {
  tree.starts_with("t:")
}

pub(crate) fn key_uuid(key: &EngineKey) -> Result<Uuid, BTreeError> {
  let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
    .map_err(|_| BTreeError::UnsupportedOperation)?;

  if values.len() != 1 {
    return Err(BTreeError::UnsupportedOperation);
  }

  match &values[0] {
    EngineValue::Uuid(bytes) => Ok(Uuid::from_bytes(*bytes)),
    _ => Err(BTreeError::UnsupportedOperation),
  }
}

pub(crate) fn row_key_from_doc_id(doc_id: Uuid) -> EngineKey {
  <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Uuid(*doc_id.as_bytes())])
}

pub(crate) fn hashed_doc_id(tree: &str, key: &EngineKey) -> Uuid {
  let mut hasher = Sha256::new();
  hasher.update(b"named:");
  hasher.update(tree.as_bytes());
  let tree_digest = hasher.finalize_reset();

  hasher.update(key);
  let key_digest = hasher.finalize();

  let mut bytes = [0u8; 16];
  bytes[..8].copy_from_slice(&tree_digest[..8]);
  bytes[8..].copy_from_slice(&key_digest[..8]);
  Uuid::from_bytes(bytes)
}

pub(crate) fn tree_uuid_range(tree: &str) -> (Uuid, Uuid) {
  let mut hasher = Sha256::new();
  hasher.update(b"named:");
  hasher.update(tree.as_bytes());
  let digest = hasher.finalize();

  let mut start_bytes = [0u8; 16];
  let mut end_bytes = [0u8; 16];
  start_bytes[..8].copy_from_slice(&digest[..8]);
  end_bytes[..8].copy_from_slice(&digest[..8]);
  end_bytes[8..].fill(0xff);

  (Uuid::from_bytes(start_bytes), Uuid::from_bytes(end_bytes))
}

pub(crate) fn doc_id_for_tree_key(tree: &str, key: &EngineKey) -> Result<Uuid, BTreeError> {
  if is_row_tree(tree) {
    key_uuid(key)
  } else {
    Ok(hashed_doc_id(tree, key))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_types::EngineValue;

  #[test]
  fn row_tree_detects_prefix() {
    assert!(is_row_tree("t:users"));
    assert!(!is_row_tree("n:users"));
  }

  #[test]
  fn doc_id_for_non_row_tree_is_stable() {
    let tree = "index:test";
    let key = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Integer(1)]);

    let first = doc_id_for_tree_key(tree, &key).expect("first");
    let second = doc_id_for_tree_key(tree, &key).expect("second");
    assert_eq!(first, second);
  }
}
