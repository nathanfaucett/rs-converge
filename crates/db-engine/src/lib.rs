#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod catalog;
mod change;
mod codec;
mod engine;
mod executor;
#[cfg(feature = "in-memory")]
mod in_memory;
mod index;
mod kernel;

pub use catalog::ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID;
pub use change::{Change, ChangeKey, ChangeReplication};
pub use codec::{DirectRowCodec, RowCodec};
pub use engine::{Engine, EngineError, EngineResult};
#[cfg(feature = "in-memory")]
pub use in_memory::{InMemoryKernel, InMemoryKernelTransaction};
pub use kernel::{Kernel, KernelTransaction};
