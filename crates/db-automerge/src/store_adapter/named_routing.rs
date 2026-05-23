use db_core::BTreeError;
use db_types::EngineKey;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn is_row_tree(tree: &str) -> bool {
  tree.starts_with("t:")
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
  Ok(hashed_doc_id(tree, key))
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_types::EngineValue;
  use db_types::key_encoding::{DefaultEncoding, KeyEncoding};
  use uuid::Uuid;

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

  #[test]
  fn doc_id_for_row_tree_is_tree_specific() {
    let key = <DefaultEncoding as KeyEncoding>::encode_values(&[EngineValue::Uuid(
      *Uuid::from_u128(1).as_bytes(),
    )]);

    let first = doc_id_for_tree_key("t:users", &key).expect("first");
    let second = doc_id_for_tree_key("t:orders", &key).expect("second");
    assert_ne!(first, second);
  }
}
