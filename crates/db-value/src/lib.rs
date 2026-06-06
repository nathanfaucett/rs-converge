#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

mod json_number;
mod json_value;
mod value;

pub use json_number::JsonNumber;
pub use json_value::JsonValue;
pub use value::{Row, Value, ValueType};
