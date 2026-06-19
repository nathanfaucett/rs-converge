#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

mod catalog;
mod engine;
mod executor;
mod kernel;
#[cfg(feature = "redb")]
mod redb;

pub use engine::{Engine, EngineError, EngineResult};
pub use kernel::EngineKernel;
#[cfg(feature = "redb")]
pub use redb::RedbEngineKernel;
