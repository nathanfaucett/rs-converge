#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

mod access_control;
#[cfg(feature = "in-memory")]
mod backends;
mod change_event;
mod engine;
mod engine_kernel;
mod from_row;
mod json_ops;
mod predicate;
mod query;
mod row_deserialize_error;
mod row_deserializer;
mod schema_resolver;
mod store_adapter;
mod subscriptions;
mod types;

pub use access_control::SyncScope;
pub use change_event::{ChangeEvent, ChangeListener};
pub use db_types::schema::{ColumnSchema, IndexSchema, TableSchema};
pub use engine::{EngineDatabase, EngineReadTransaction, EngineTransaction};
pub use from_row::FromRow;
pub use json_ops::{json_extract, json_merge, json_valid};
pub use query::{
  Aggregate, HavingPredicate, JoinClause, JoinKind, JoinOn, OrderBy, QualifiedColumn,
  QualifiedOperand, QualifiedPredicate, RefOrAgg, SelectOptions, SortDirection, UpdateAssignment,
  UpdateValueExpr,
};
pub use query::{EngineQuery, EngineResult, ResultColumn};
pub use row_deserialize_error::RowDeserializeError;
pub use schema_resolver::SchemaResolver;
pub use store_adapter::{
  BackendCapability, EngineNamedTreeBackend, EngineNamedTreeTransaction, EngineStore,
  EngineStoreReadTransaction, EngineStoreTransaction, NamedTreeEngineStore, TransactionContract,
  fetch_rows_by_primary_keys, lookup_primary_keys_by_index_predicate,
};
pub use subscriptions::{Subscriber, SubscriptionId};
pub use types::{EngineError, EngineKey, EngineRow, EngineType, EngineValue, PrimaryKey};

// Internal exports for engine module
pub(crate) use change_event::ChangeListenerRegistry;
