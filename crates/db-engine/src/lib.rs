#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

mod btree;
mod catalog;
mod concurrency;
mod default_manager;
mod engine;
mod executor;
#[cfg(feature = "in-memory")]
mod in_memory_btree;
mod json_number;
mod json_value;
mod query;
mod schema;
mod translator;
mod value;

pub use btree::{
  BTree, BTreeDefinition, BTreeError, BTreeFactory, BTreeKey, BTreeManager, BTreeManagerAnyBTree,
  BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeValue, BTreeWriteExecutor,
};
pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
pub use default_manager::DefaultBTreeManager;
pub use engine::{Engine, EngineError, EngineResult};
#[cfg(feature = "in-memory")]
pub use in_memory_btree::{InMemoryBTree, InMemoryBTreeFactory};
pub use json_number::JsonNumber;
pub use json_value::JsonValue;
pub use query::{
  Column, DdlOp, Expr, ExprValue, Join, JoinKind, OrderBy, Query, QueryResult, QueryResultColumn,
  SelectOptions, Statement, TableIndex, UpdateAssignment,
};
pub use schema::{ColumnIndex, ColumnSchema, DescribeSchema, IndexSchema, TableSchema};
pub use translator::{QueryParams, TranslateError, Translator};
pub use value::{Row, Value, ValueType};
