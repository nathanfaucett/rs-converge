#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod catalog;
mod engine;
mod executor;
#[cfg(feature = "in-memory")]
mod in_memory;
mod kernel;
mod reconciler;

pub use engine::{Engine, EngineError, EngineResult};
#[cfg(feature = "in-memory")]
pub use in_memory::{InMemoryKernel, InMemoryKernelTransaction};
pub use kernel::{Kernel, KernelTransaction};
pub use reconciler::{DirectRowReconciler, RowReconciler};
