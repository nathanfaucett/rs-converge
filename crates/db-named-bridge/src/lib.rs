#![cfg_attr(not(feature = "std"), no_std)]

//! Glue between **layout** backends (named B-trees) and **format** handlers
//! (how engine keys/values are stored in each layout tree).
//!
//! - Layout: `NamedBTreeMap<LayoutKey, LayoutValue>` — physical tree storage.
//! - Format: per-tree encoding/decoding exposed as `NamedBTreeMap` + `EngineNamedTreeBackend` for engine keys/values.
//! - Catalog & sync: layout concerns; operate on layout backends, not the SQL facade.

mod glue;
mod layout_catalog;
mod layout_sync;

#[cfg(feature = "automerge")]
pub mod automerge;

pub use glue::LayoutFormatBridge;
pub use layout_catalog::TreeLayoutCatalog;
pub use layout_sync::{PerTreeFormatSync, sync_cataloged_layouts};

/// Layout-level Automerge document metrics (not a facade/SQL concern).
#[cfg(feature = "automerge")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AutomergeSyncMetrics {
  pub document_count: usize,
  pub total_document_bytes: usize,
}
