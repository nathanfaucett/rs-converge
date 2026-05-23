// Keep this crate root thin: implementation lives in `automerge_btree`.
extern crate alloc;

mod automerge_btree;
pub use automerge_btree::*;
mod store_adapter;
pub use automerge::AutoCommit;
pub use store_adapter::{
  AutomergeEngineStore, apply_documents, automerge_metrics, collect_documents,
  sync_automerge_stores,
};
