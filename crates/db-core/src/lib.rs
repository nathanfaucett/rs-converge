#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;
#[cfg(not(feature = "std"))]
extern crate core;
extern crate futures;
extern crate thiserror;

mod blocking;
mod btree;
mod codec;
mod concurrency;
mod named_tree;
mod range_merge;
#[cfg(feature = "test-helpers")]
mod test_helpers;
mod transaction_patch;

pub use blocking::block_on;
pub use btree::{BTree, BTreeError, BTreeExecutor, BTreeResult, BTreeTransaction};
pub use codec::{
  BufferSink, CURRENT_CODEC_VERSION, Cursor, DecodeError, FastKeyCodec, KeyCodec, KeyScratch,
  ValueCodec, canonical_f64_bits, canonical_f64_bits_into_sink, decode_bool, decode_bytes,
  decode_from_slice, decode_len, decode_string, decode_usize, decode_version, decode_with_version,
  encode_bool_into_sink, encode_bytes_into_sink, encode_i64_into_sink, encode_len_into_sink,
  encode_string_into_sink, encode_u32_into_sink, encode_u64_into_sink, encode_usize_into_sink,
  encode_version_into_sink, encode_with_version,
};
pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};

pub use range_merge::merge_range_maps;

pub use transaction_patch::{TransactionEntry, TransactionPatch};

#[cfg(feature = "test-helpers")]
pub use test_helpers::MockBTree;

pub use named_tree::NamedBTreeMap;
