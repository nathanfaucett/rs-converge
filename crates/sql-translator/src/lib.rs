#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod sql_translator;

pub use sql_translator::SqlTranslator;
