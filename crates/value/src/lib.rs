#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

mod from_row;
mod json_number;
mod json_value;
mod value;

#[cfg(all(test, feature = "std"))]
mod tests;

pub use from_row::{FromRow, FromRowError, FromValue, decode, value};
pub use json_number::JsonNumber;
pub use json_value::JsonValue;
pub use value::{Row, Value, ValueType};
