#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

mod btree;
mod concurrency;
mod engine;
mod from_row;
#[cfg(feature = "in-memory")]
mod in_memory_btree;
mod query;
mod schema;
mod translator;
mod value;

pub use btree::{
  BTree, BTreeDefinition, BTreeError, BTreeKey, BTreeManager, BTreeReadExecutor, BTreeResult,
  BTreeTransaction, BTreeValue, BTreeWriteExecutor,
};
pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
pub use from_row::{FromRow, RowDeserializeError};
#[cfg(feature = "in-memory")]
pub use in_memory_btree::{InMemoryBTree, InMemoryBTreeManager};
pub use query::{
  Column, Expr, ExprValue, ExtractTables, Join, JoinKind, OrderBy, Query, Result, ResultColumn,
  SelectOptions, UpdateAssignment,
};
pub use schema::{ColumnIndex, ColumnSchema, DescribeSchema, IndexSchema, TableSchema};
pub use translator::{TranslateError, Translator};
pub use value::{Row, Value, ValueType};
