#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod btree;
#[cfg(feature = "in-memory")]
mod in_memory_btree;

pub use btree::{
  BTree, BTreeError, BTreeKey, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeValue,
  BTreeWriteExecutor,
};
#[cfg(feature = "in-memory")]
pub use in_memory_btree::InMemoryBTree;
