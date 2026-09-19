#![no_std]

extern crate alloc;

mod protocol;
mod session;
mod transport;

pub use protocol::{PROTOCOL_VERSION, SyncHello, SyncMessage};
pub use session::{SessionConfig, SyncError, SyncResult, SyncRole, synchronize};
pub use transport::SyncTransport;
