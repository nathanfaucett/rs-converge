#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate core;
#[cfg(not(feature = "std"))]
#[allow(unused_imports)]
#[macro_use]
extern crate alloc;

pub mod codec;
pub mod engine_types;
pub mod key_encoding;
pub mod persistence;
pub mod schema;
pub mod storage_codec;
pub mod store;

pub use engine_types::{EngineKey, EngineRow, EngineType, EngineValue, PrimaryKey};
pub use schema::{ColumnSchema, IndexSchema, TableSchema};
pub use storage_codec::{EngineKeyCodec, EngineRowCodec};
pub use store::{StoreKey, StoreValue};
