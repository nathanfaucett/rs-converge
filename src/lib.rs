#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod api;
mod database;

pub use api::*;
pub use database::Database;
