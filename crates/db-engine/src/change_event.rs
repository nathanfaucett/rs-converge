use crate::{EngineRow, PrimaryKey};
#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::fmt;
#[cfg(not(feature = "std"))]
use spin::RwLock;
#[cfg(feature = "std")]
use std::string::String;
#[cfg(feature = "std")]
use std::sync::Arc;
#[cfg(feature = "std")]
use std::sync::RwLock;

/// A change event emitted when data in the engine mutates.
/// Subscribers listen to these events and recompute affected queries.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum ChangeEvent {
  /// A row was inserted into a table.
  RowInserted {
    table: String,
    pk: PrimaryKey,
    row: EngineRow,
  },
  /// A row was deleted from a table.
  RowDeleted {
    table: String,
    pk: PrimaryKey,
    row: EngineRow,
  },
  /// A row was updated in a table.
  RowUpdated {
    table: String,
    pk: PrimaryKey,
    old_row: EngineRow,
    new_row: EngineRow,
  },
}

impl ChangeEvent {
  /// Get the table name for this change.
  pub fn table(&self) -> &str {
    match self {
      Self::RowInserted { table, .. } => table,
      Self::RowDeleted { table, .. } => table,
      Self::RowUpdated { table, .. } => table,
    }
  }

  /// Get the primary key for this change.
  pub fn pk(&self) -> &PrimaryKey {
    match self {
      Self::RowInserted { pk, .. } => pk,
      Self::RowDeleted { pk, .. } => pk,
      Self::RowUpdated { pk, .. } => pk,
    }
  }
}

/// Trait for objects that want to listen to change events.
pub trait ChangeListener: Send + Sync {
  /// Called when a change event occurs.
  fn on_change(&self, event: ChangeEvent);
}

/// Internal registry that maintains listeners and broadcasts change events.
pub(crate) struct ChangeListenerRegistry {
  listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
}

impl fmt::Debug for ChangeListenerRegistry {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    #[cfg(feature = "std")]
    let listener_count = self.listeners.read().unwrap().len();
    #[cfg(not(feature = "std"))]
    let listener_count = self.listeners.read().len();

    f.debug_struct("ChangeListenerRegistry")
      .field("listener_count", &listener_count)
      .finish()
  }
}

impl ChangeListenerRegistry {
  pub(crate) fn new() -> Self {
    Self {
      listeners: RwLock::new(Vec::new()),
    }
  }

  /// Register a change listener.
  #[allow(dead_code)]
  pub(crate) fn register(&self, listener: Arc<dyn ChangeListener>) {
    #[cfg(feature = "std")]
    let mut listeners = self.listeners.write().unwrap();
    #[cfg(not(feature = "std"))]
    let mut listeners = self.listeners.write();
    listeners.push(listener);
  }

  /// Emit a change event to all registered listeners.
  pub(crate) fn emit(&self, event: ChangeEvent) {
    #[cfg(feature = "std")]
    let listeners = self.listeners.read().unwrap();
    #[cfg(not(feature = "std"))]
    let listeners = self.listeners.read();
    for listener in listeners.iter() {
      listener.on_change(event.clone());
    }
  }

  /// Clear all listeners.
  #[allow(dead_code)]
  pub(crate) fn clear(&self) {
    #[cfg(feature = "std")]
    let mut listeners = self.listeners.write().unwrap();
    #[cfg(not(feature = "std"))]
    let mut listeners = self.listeners.write();
    listeners.clear();
  }
}
