#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod automerge_serde;
mod automerge_tree;
mod compaction;
mod document_change_key;
mod document_type;
mod reconstruction;
mod transaction;

pub use automerge_tree::AutomergeBTree;
pub use compaction::{CompactionPolicy, ThresholdPolicy, hash_hashes, hash_heads, run_compaction};
pub use document_change_key::DocumentChangeKey;
pub use document_type::DocumentType;
pub use transaction::AutomergeBTreeTransaction;
