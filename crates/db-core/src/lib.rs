#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;
#[cfg(not(feature = "std"))]
extern crate core;
extern crate futures;
extern crate thiserror;

mod btree;
mod concurrency;
#[cfg(feature = "in-memory")]
mod in_memory_btree;

pub use btree::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};
pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
#[cfg(feature = "in-memory")]
pub use in_memory_btree::InMemoryBTree;
