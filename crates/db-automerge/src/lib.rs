#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

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
pub use transaction::AutomergeTransaction;

pub type AutomergeEntry = Vec<u8>;
