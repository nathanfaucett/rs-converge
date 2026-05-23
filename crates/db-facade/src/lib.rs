#![cfg_attr(not(feature = "std"), no_std)]

mod api;
#[cfg(feature = "automerge")]
mod automerge_named_store;
mod types;

pub use db_sql_to_engine::SqlParams;
#[cfg(feature = "automerge")]
pub use types::AutomergeSyncMetrics;
#[cfg(feature = "redb")]
pub use types::RedbEngineStore;
pub use types::{
  Database, DatabaseError, FacadeStore, InMemoryEngineStore, ReadTransaction, Row, Transaction,
};

// Re-export subscription types from db_engine for convenience
pub use db_engine::{Subscriber, SubscriptionId, SyncScope};
