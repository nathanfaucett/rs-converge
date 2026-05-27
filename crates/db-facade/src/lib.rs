#![cfg_attr(not(feature = "std"), no_std)]

mod api;
mod types;

#[cfg(feature = "automerge")]
pub use db_automerge::{
  AutomergeFormatAdapter, AutomergeSyncMetrics, automerge_layout_metrics, sync_automerge_layouts,
};
pub use db_sql_to_engine::SqlParams;
#[cfg(feature = "redb")]
pub use types::RedbEngineStore;
pub use types::{
  Database, DatabaseError, FacadeStore, InMemoryEngineStore, ReadTransaction, Row, Transaction,
};
#[cfg(feature = "automerge")]
pub use types::{InMemoryAutomergeLayoutBackend, InMemoryAutomergeStore};
#[cfg(all(feature = "automerge", feature = "redb"))]
pub use types::{RedbAutomergeLayoutBackend, RedbAutomergeStore};

// Re-export subscription types from db_engine for convenience
pub use db_engine::{Subscriber, SubscriptionId};
