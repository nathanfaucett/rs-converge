pub mod btree;
pub mod factory;
pub mod transaction;

pub use btree::RedbBTree;

use serde::{Serialize, de::DeserializeOwned};

/// Encode a value to bytes using postcard, mapping errors to BTreeError
pub(crate) fn encode_value<V: Serialize>(v: &V) -> Result<Vec<u8>, db_engine::BTreeError> {
  postcard::to_stdvec(v)
    .map_err(|e| db_engine::BTreeError::Custom(format!("postcard value ser error: {}", e)))
}

/// Decode a value from bytes using postcard, mapping errors to BTreeError
pub(crate) fn decode_value<V: DeserializeOwned>(bytes: &[u8]) -> Result<V, db_engine::BTreeError> {
  postcard::from_bytes(bytes)
    .map_err(|e| db_engine::BTreeError::Custom(format!("postcard value de error: {}", e)))
}
