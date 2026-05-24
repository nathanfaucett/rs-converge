#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(feature = "automerge")]
pub use db_facade::{
  AutomergeSyncMetrics, LayoutFormatBridge, automerge_layout_metrics, sync_automerge_layouts,
};
pub use db_facade::{Database, DatabaseError, Row, SqlParams};
