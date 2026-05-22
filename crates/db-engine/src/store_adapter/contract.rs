use crate::{EngineError, EngineKey, EngineRow, PrimaryKey};
use db_types::key_encoding::{DefaultEncoding, KeyEncoding, RowEncoding};

pub(crate) fn encode_row_bytes(row: &EngineRow) -> Vec<u8> {
  <DefaultEncoding as RowEncoding>::encode_values(row)
}

pub(crate) fn decode_row_bytes(bytes: &[u8]) -> Result<EngineRow, EngineError> {
  <DefaultEncoding as RowEncoding>::decode_values(bytes)
    .map_err(|error| EngineError::SchemaMismatch(format!("row decode error: {}", error)))
}

pub(crate) fn primary_key_from_engine_key(key: &EngineKey) -> Result<PrimaryKey, EngineError> {
  let values = <DefaultEncoding as KeyEncoding>::decode_values(key)
    .map_err(|e| EngineError::SchemaMismatch(format!("decode key error: {}", e)))?;

  if values.len() != 1 {
    return Err(EngineError::SchemaMismatch(
      "row primary key must be single UUID value".into(),
    ));
  }

  match &values[0] {
    db_types::EngineValue::Uuid(bytes) => Ok(PrimaryKey::new(*bytes)),
    _ => Err(EngineError::SchemaMismatch(
      "row primary key must be UUID".into(),
    )),
  }
}

pub(crate) fn schema_decode_error(error: db_core::DecodeError) -> EngineError {
  EngineError::SchemaMismatch(error.to_string())
}

#[cfg(test)]
mod tests {
  use super::*;
  use db_types::EngineValue;

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
