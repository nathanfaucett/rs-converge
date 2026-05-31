#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use crate::{MaybeSend, MaybeSendFuture, MaybeSync, value::ValueType};

pub type ColumnIndex = u8;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
  pub name: String,
  pub r#type: ValueType,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexSchema {
  pub name: String,
  pub table_name: String,
  pub column_indices: Vec<ColumnIndex>,
  pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
  pub name: String,
  pub columns: Vec<ColumnSchema>,
  pub primary_key_index: Vec<ColumnIndex>,
}

pub trait DescribeSchema: MaybeSend + MaybeSync {
  fn describe_table(&self, table_name: &str) -> impl MaybeSendFuture<Output = Option<TableSchema>>;
}
