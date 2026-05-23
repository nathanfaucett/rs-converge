use crate::EngineError;
/// Explicit backend capabilities and contract documentation.
///
/// Different backends provide different transactional guarantees.
/// This module makes those guarantees explicit and testable.
use alloc::string::ToString;

/// Transactional guarantees provided by a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCapability {
  /// Single-tree transactions only; multiple trees are NOT atomic together.
  /// Useful for: in-memory stores, simple backends.
  SingleTreeAtomicity,

  /// Multiple trees can be updated in a single atomic transaction.
  /// All mutations across all trees commit together, or none do.
  /// Useful for: embedded databases (REDB, automerge), strongly ACID backends.
  MultiTreeAtomicity,
}

/// Transactional contract that each backend MUST honor.
pub struct TransactionContract {
  /// What level of atomicity this backend provides.
  pub atomicity: BackendCapability,

  /// Whether this backend supports multi-table writes as a single unit.
  /// If MultiTreeAtomicity, this MUST be true.
  pub multi_tree_write_atomicity: bool,

  /// Whether schema changes (table/index create/drop) are atomic with data writes.
  /// If true, all catalog updates commit with data mutations.
  pub schema_mutation_atomicity: bool,
}

impl TransactionContract {
  /// Multi-tree atomicity with schema coupling (e.g., automerge, REDB).
  pub fn coupled_multi_tree() -> Self {
    Self {
      atomicity: BackendCapability::MultiTreeAtomicity,
      multi_tree_write_atomicity: true,
      schema_mutation_atomicity: true,
    }
  }

  /// Single-tree atomicity (e.g., simple in-memory stores).
  pub fn single_tree() -> Self {
    Self {
      atomicity: BackendCapability::SingleTreeAtomicity,
      multi_tree_write_atomicity: false,
      schema_mutation_atomicity: false,
    }
  }

  /// Validate that a contract is internally consistent.
  pub fn validate(&self) -> Result<(), EngineError> {
    if matches!(self.atomicity, BackendCapability::MultiTreeAtomicity)
      && !self.multi_tree_write_atomicity
    {
      return Err(EngineError::SchemaMismatch(
        "MultiTreeAtomicity backend MUST have multi_tree_write_atomicity=true".to_string(),
      ));
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn multi_tree_atomicity_contract_validates() {
    let contract = TransactionContract::coupled_multi_tree();
    assert!(contract.validate().is_ok());
  }

  #[test]
  fn single_tree_atomicity_contract_validates() {
    let contract = TransactionContract::single_tree();
    assert!(contract.validate().is_ok());
  }

  #[test]
  fn invalid_contract_multi_tree_without_write_atomicity_fails() {
    let contract = TransactionContract {
      atomicity: BackendCapability::MultiTreeAtomicity,
      multi_tree_write_atomicity: false,
      schema_mutation_atomicity: true,
    };
    assert!(contract.validate().is_err());
  }
}
