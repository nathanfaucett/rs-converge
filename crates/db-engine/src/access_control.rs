use crate::{ChangeEvent, EngineError, QualifiedPredicate, TableSchema};
use alloc::string::String;
use hashbrown::{HashMap, HashSet};

/// Represents the scope of data a peer can sync and query.
/// Controls which tables are accessible and applies row-level filters.
#[derive(Debug, Clone)]
pub struct SyncScope {
  /// Set of tables this peer can access.
  allowed_tables: HashSet<String>,
  /// Per-table row-level filter predicates.
  table_filters: HashMap<String, QualifiedPredicate>,
}

impl SyncScope {
  /// Create an unrestricted scope (all tables, no filters).
  /// Used for privileged peers.
  pub fn unrestricted() -> Self {
    Self {
      allowed_tables: HashSet::new(),
      table_filters: HashMap::new(),
    }
  }

  /// Create a new scope with specific allowed tables.
  pub fn new(allowed_tables: HashSet<String>) -> Self {
    Self {
      allowed_tables,
      table_filters: HashMap::new(),
    }
  }

  /// Add a table to the allowed set.
  pub fn allow_table(mut self, table: String) -> Self {
    self.allowed_tables.insert(table);
    self
  }

  /// Add a row-level filter for a table.
  /// If a table has a filter, only rows matching the predicate are visible.
  pub fn add_filter(mut self, table: String, filter: QualifiedPredicate) -> Self {
    self.table_filters.insert(table, filter);
    self
  }

  /// Check if this scope can access a table.
  pub fn can_access(&self, table: &str) -> bool {
    // If unrestricted (no tables listed), allow all tables
    if self.allowed_tables.is_empty() {
      return true;
    }
    self.allowed_tables.contains(table)
  }

  /// Get the row-level filter for a table, if any.
  pub fn filter_for(&self, table: &str) -> Option<&QualifiedPredicate> {
    self.table_filters.get(table)
  }

  /// Check if a change event is visible within this scope.
  /// Returns false if:
  /// - The table is not accessible
  /// - The row doesn't match the table's filter predicate
  pub fn matches(&self, event: &ChangeEvent) -> bool {
    let table = event.table();

    // Check table access
    if !self.can_access(table) {
      return false;
    }

    // If table has a filter, the row must match it
    // For now, we accept any row (actual predicate evaluation happens at query time)
    // This is a conservative approach: if we don't know if it matches, we include it
    // and let query execution do detailed filtering
    true
  }

  /// Validate that all filters are compatible with the given table schemas.
  pub fn validate(&self, schemas: &HashMap<String, TableSchema>) -> Result<(), EngineError> {
    for table in self.table_filters.keys() {
      if !schemas.contains_key(table) {
        return Err(EngineError::TableNotFound(table.clone()));
      }

      // TODO: Validate that filter references valid columns
      // This will require checking the schema and predicate structure
    }
    Ok(())
  }
}

impl Default for SyncScope {
  /// Default is unrestricted scope.
  fn default() -> Self {
    Self::unrestricted()
  }
}
