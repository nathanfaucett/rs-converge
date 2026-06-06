#![cfg_attr(not(feature = "std"), no_std)]

mod concurrency;

pub use concurrency::{MaybeSend, MaybeSendFuture, MaybeSendStream, MaybeSync};
