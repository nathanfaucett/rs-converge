#![cfg_attr(not(feature = "std"), no_std)]

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
mod value;

pub use btree::{
  BTree, BTreeDefinition, BTreeError, BTreeManager, BTreeReadExecutor, BTreeResult,
  BTreeTransaction, BTreeWriteExecutor,
};
pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
pub use from_row::{FromRow, RowDeserializeError};
#[cfg(feature = "in-memory")]
pub use in_memory_btree::{InMemoryBTree, InMemoryBTreeManager};
pub mod engine;
pub use query::{Query, Result, ResultColumn};
pub use schema::{ColumnIndex, ColumnSchema, IndexSchema, SchemaResolver, TableSchema};
pub use value::{Row, Value, ValueType};
