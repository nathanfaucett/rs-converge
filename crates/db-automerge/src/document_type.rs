use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DocumentType {
  Snapshot = 0,
  Incremental = 1,
}

impl DocumentType {
  pub fn is_snapshot(self) -> bool {
    matches!(self, DocumentType::Snapshot)
  }

  pub fn is_incremental(self) -> bool {
    matches!(self, DocumentType::Incremental)
  }
}
