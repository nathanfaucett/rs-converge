use core::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DocumentType {
  Snapshot = 0,
  Incremental = 1,
}

impl fmt::Display for DocumentType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      DocumentType::Snapshot => write!(f, "Snapshot"),
      DocumentType::Incremental => write!(f, "Incremental"),
    }
  }
}

impl DocumentType {
  pub fn is_snapshot(self) -> bool {
    matches!(self, DocumentType::Snapshot)
  }

  pub fn is_incremental(self) -> bool {
    matches!(self, DocumentType::Incremental)
  }
}
