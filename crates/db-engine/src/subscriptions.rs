use crate::{ChangeEvent, EngineError, EngineQuery, EngineResult, SyncScope};
#[cfg(not(feature = "std"))]
use alloc::string::String;
#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(not(feature = "std"))]
use core::sync::atomic::{AtomicU64, Ordering};
use core::{fmt, mem};
#[cfg(not(feature = "std"))]
use hashbrown::HashMap;
#[cfg(not(feature = "std"))]
use spin::RwLock;
#[cfg(feature = "std")]
use std::collections::HashMap;
#[cfg(feature = "std")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "std")]
use std::sync::{Arc, RwLock};

/// Unique identifier for a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "wasm",
  derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct SubscriptionId(u64);

impl SubscriptionId {
  pub(crate) fn next() -> Self {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    SubscriptionId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
  }
}

/// Trait for objects that want to receive subscription updates.
/// Called when a subscribed query's results change.
pub trait Subscriber: Send + Sync {
  /// Called with new query results, or an error if recomputation failed.
  fn on_results(&self, result: Result<EngineResult, EngineError>);
}

/// Internal representation of a subscription.
pub(crate) struct QuerySubscription {
  pub(crate) id: SubscriptionId,
  pub(crate) query: EngineQuery,
  pub(crate) scope: SyncScope,
  pub(crate) subscriber: Arc<dyn Subscriber>,
  pub(crate) last_results: RwLock<Option<EngineResult>>,
}

impl fmt::Debug for QuerySubscription {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("QuerySubscription")
      .field("id", &self.id)
      .field("query", &self.query)
      .field("scope", &self.scope)
      .field("subscriber", &"<dyn Subscriber>")
      .field("last_results", &"<stored>")
      .finish()
  }
}

impl QuerySubscription {
  /// Check if the change event is visible to this subscription's scope.
  pub(crate) fn matches_scope(&self, event: &ChangeEvent) -> bool {
    self.scope.matches(event)
  }
}

/// Internal registry managing active subscriptions.
#[derive(Debug)]
pub(crate) struct SubscriptionRegistry {
  subscriptions: RwLock<HashMap<SubscriptionId, Arc<QuerySubscription>>>,
  /// Map from table name to subscription IDs that reference it.
  /// Used for efficient lookup when a change event occurs.
  table_to_subscriptions: RwLock<HashMap<String, Vec<SubscriptionId>>>,
}

impl SubscriptionRegistry {
  pub(crate) fn new() -> Self {
    Self {
      subscriptions: RwLock::new(HashMap::new()),
      table_to_subscriptions: RwLock::new(HashMap::new()),
    }
  }

  /// Register a subscription.
  pub(crate) fn register(&self, subscription: Arc<QuerySubscription>) {
    let id = subscription.id;

    // Add to subscriptions map
    {
      #[cfg(feature = "std")]
      let mut subs = self.subscriptions.write().unwrap();
      #[cfg(not(feature = "std"))]
      let mut subs = self.subscriptions.write();
      subs.insert(id, subscription.clone());
    }

    // Add to table-to-subscriptions index
    {
      #[cfg(feature = "std")]
      let mut table_map = self.table_to_subscriptions.write().unwrap();
      #[cfg(not(feature = "std"))]
      let mut table_map = self.table_to_subscriptions.write();
      for table in subscription.query.tables() {
        table_map.entry(table).or_default().push(id);
      }
    }
  }

  /// Unregister a subscription.
  pub(crate) fn unregister(&self, id: SubscriptionId) {
    #[cfg(feature = "std")]
    let mut subs = self.subscriptions.write().unwrap();
    #[cfg(not(feature = "std"))]
    let mut subs = self.subscriptions.write();
    if let Some(sub) = subs.remove(&id) {
      // Remove from table index
      #[cfg(feature = "std")]
      let mut table_map = self.table_to_subscriptions.write().unwrap();
      #[cfg(not(feature = "std"))]
      let mut table_map = self.table_to_subscriptions.write();
      for table in sub.query.tables() {
        if let Some(ids) = table_map.get_mut(&table) {
          ids.retain(|&sid| sid != id);
          if ids.is_empty() {
            table_map.remove(&table);
          }
        }
      }
    }
  }

  /// Get all subscriptions affected by a change to a specific table.
  pub(crate) fn subscriptions_for_table(&self, table: &str) -> Vec<Arc<QuerySubscription>> {
    #[cfg(feature = "std")]
    let table_map = self.table_to_subscriptions.read().unwrap();
    #[cfg(not(feature = "std"))]
    let table_map = self.table_to_subscriptions.read();
    #[cfg(feature = "std")]
    let subs = self.subscriptions.read().unwrap();
    #[cfg(not(feature = "std"))]
    let subs = self.subscriptions.read();

    if let Some(ids) = table_map.get(table) {
      ids.iter().filter_map(|id| subs.get(id).cloned()).collect()
    } else {
      Vec::new()
    }
  }

  pub(crate) fn get_subscription(&self, id: SubscriptionId) -> Option<Arc<QuerySubscription>> {
    #[cfg(feature = "std")]
    let subs = self.subscriptions.read().unwrap();
    #[cfg(not(feature = "std"))]
    let subs = self.subscriptions.read();
    subs.get(&id).cloned()
  }

  /// Collect all subscriptions affected by a change event.
  /// Returns subscriptions that should be recomputed.
  pub(crate) fn affected_by_change(&self, event: &ChangeEvent) -> Vec<Arc<QuerySubscription>> {
    let table = event.table();
    let mut affected = Vec::new();

    // Get all subscriptions that reference this table
    for sub in self.subscriptions_for_table(table) {
      // Check if this subscription's scope matches the change
      if sub.matches_scope(event) {
        affected.push(sub);
      }
    }

    affected
  }
}

/// Helper for batching subscription updates during sync.
pub(crate) struct SubscriptionBatch {
  invalidated: RwLock<Vec<SubscriptionId>>,
}

impl SubscriptionBatch {
  pub(crate) fn new() -> Self {
    Self {
      invalidated: RwLock::new(Vec::new()),
    }
  }

  pub(crate) fn invalidate(&self, id: SubscriptionId) {
    #[cfg(feature = "std")]
    let mut inv = self.invalidated.write().unwrap();
    #[cfg(not(feature = "std"))]
    let mut inv = self.invalidated.write();
    if !inv.contains(&id) {
      inv.push(id);
    }
  }

  pub(crate) fn take_invalidated(&self) -> Vec<SubscriptionId> {
    #[cfg(feature = "std")]
    let mut inv = self.invalidated.write().unwrap();
    #[cfg(not(feature = "std"))]
    let mut inv = self.invalidated.write();
    mem::take(&mut *inv)
  }
}
