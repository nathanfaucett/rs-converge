#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
#[macro_use]
extern crate alloc;

mod catalog;
mod default_manager;
mod engine;
mod executor;

pub use default_manager::DefaultBTreeManager;
pub use engine::{Engine, EngineError, EngineResult};
