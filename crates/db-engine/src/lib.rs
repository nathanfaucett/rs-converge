#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

mod change_event;
mod engine;
mod engine_kernel;
mod from_row;
pub mod key_encoding;
mod persistence;
mod predicate;
mod query;
mod row_deserialize_error;
mod row_deserializer;
mod schema;
mod schema_resolver;
mod store_backend;
mod subscriptions;
mod types;

pub use store_backend::{EngineStoreBackend, EngineStoreTransaction};

pub use change_event::{ChangeEvent, ChangeListener};
pub use engine::{EngineDatabase, EngineReadTransaction, EngineTransaction};
pub use from_row::FromRow;
pub use key_encoding::{DefaultEncoding, EngineKeyCodec, EngineRowCodec, KeyEncoding, RowEncoding};
pub use persistence::{StoreKey, decode_store_key, encode_store_key};
pub use query::{
  Aggregate, HavingPredicate, JoinClause, JoinKind, JoinOn, OrderBy, QualifiedColumn,
  QualifiedOperand, QualifiedPredicate, RefOrAgg, SelectOptions, SortDirection, UpdateAssignment,
  UpdateValueExpr,
};
pub use query::{EngineQuery, EngineResult, ResultColumn};
pub use row_deserialize_error::RowDeserializeError;
pub use schema::{ColumnSchema, IndexSchema, TableSchema};
pub use schema_resolver::SchemaResolver;
pub use subscriptions::{Subscriber, SubscriptionId};
pub use types::{EngineError, EngineKey, EngineRow, EngineType, EngineValue, PrimaryKey};

// Internal exports for engine module
pub(crate) use change_event::ChangeListenerRegistry;
