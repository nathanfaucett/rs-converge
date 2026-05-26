#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod in_memory_btree;
mod in_memory_named;

pub use in_memory_btree::{InMemoryBTree, InMemoryBTreeTransaction};
pub use in_memory_named::{InMemoryNamedBTree, InMemoryNamedTree};
