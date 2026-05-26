use crate::{
  EngineRow, TableSchema,
  query::ResultColumn,
  row_deserialize_error::RowDeserializeError,
  row_deserializer::{deserialize_named_row, deserialize_row},
};

/// Trait for types that can be deserialized from a row using a table schema.
///
/// This trait provides a generic way to convert a row with a schema into a typed struct.
/// Implementations are typically generated using serde's `#[derive(Deserialize)]`.
///
/// # Example
///
/// ```ignore
/// use serde::Deserialize;
/// use db_engine::FromRow;
///
/// #[derive(Deserialize)]
/// struct User {
///   id: i64,
///   name: String,
///   email: String,
/// }
///
/// impl FromRow for User {}
///
/// // Usage:
/// let schema = engine.describe_table("users")?;
/// let result = engine.execute_query(query)?;
/// let users: Vec<User> = result.into_typed::<User>(&schema)?;
/// ```
pub trait FromRow: serde::de::DeserializeOwned {
  fn from_row(schema: &TableSchema, row: &EngineRow) -> Result<Self, RowDeserializeError> {
    deserialize_row(schema, row)
  }

  fn from_named_row(
    columns: &[ResultColumn],
    row: &EngineRow,
  ) -> Result<Self, RowDeserializeError> {
    deserialize_named_row(columns, row)
  }
}

/// Blanket implementation for all types that implement Deserialize
impl<T: serde::de::DeserializeOwned> FromRow for T {}
