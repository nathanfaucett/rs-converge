pub use db_types::{EngineKey, EngineRow, EngineType, EngineValue, PrimaryKey};

use db_core::BTreeError;
use thiserror::Error;

// IndexSchema/TableSchema not needed here; avoid unused-import warnings.

#[derive(Error, Debug)]
pub enum EngineError {
  #[error("table not found: {0}")]
  TableNotFound(String),

  #[error("index not found: {0}")]
  IndexNotFound(String),

  #[error("table already exists: {0}")]
  DuplicateTable(String),

  #[error("index already exists: {0}")]
  DuplicateIndex(String),

  #[error("duplicate primary key: {0:?}")]
  DuplicatePrimaryKey(PrimaryKey),

  #[error("unique index violation: {0}")]
  UniqueIndexViolation(String),

  #[error("schema mismatch: {0}")]
  SchemaMismatch(String),

  #[error("type mismatch: {0}")]
  TypeMismatch(String),

  #[error("query limit exceeded: {0}")]
  QueryLimitExceeded(String),

  #[error("query not supported: {0}")]
  QueryNotSupported(String),

  #[error("primary key missing")]
  PrimaryKeyMissing,

  #[error("unsupported index type")]
  UnsupportedIndexType,

  #[error("storage error: {0}")]
  StoreError(#[from] BTreeError),
}

impl From<db_types::schema::SchemaError> for EngineError {
  fn from(e: db_types::schema::SchemaError) -> Self {
    match e {
      db_types::schema::SchemaError::SchemaMismatch(s) => EngineError::SchemaMismatch(s),
      db_types::schema::SchemaError::TypeMismatch(s) => EngineError::TypeMismatch(s),
      db_types::schema::SchemaError::PrimaryKeyMissing => EngineError::PrimaryKeyMissing,
    }
  }
}
