#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;
#[cfg(not(feature = "std"))]
extern crate core;
extern crate futures;
extern crate thiserror;

mod btree;
mod concurrency;
mod named_tree;
mod transaction_patch;

pub use btree::{
  BTree, BTreeError, BTreeReadExecutor, BTreeResult, BTreeTransaction, BTreeWriteExecutor,
};
pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
pub use named_tree::NamedBTreeMap;
pub use transaction_patch::{TransactionEntry, TransactionPatch};
