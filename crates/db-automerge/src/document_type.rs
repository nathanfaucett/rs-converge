use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DocumentType {
  Incremental = 0,
  Snapshot = 1,
}

impl DocumentType {
  pub fn is_snapshot(self) -> bool {
    matches!(self, DocumentType::Snapshot)
  }
}
