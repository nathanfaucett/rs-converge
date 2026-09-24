#![no_std]

extern crate alloc;

mod codec;
mod protocol;
mod session;
mod state;
mod transport;

pub use codec::SyncRowCodec;
pub use protocol::{
    PROTOCOL_VERSION, SyncHello, SyncIncrementalChange, SyncMessage, SyncRowInventory,
    SyncSnapshotRequest,
};
pub use session::{
    SessionConfig, SyncError, SyncResult, SyncRole, apply_incremental_changes_for,
    apply_sync_state_for, export_sync_state_for, sync_manifest_for, synchronize,
};
pub use state::{StateDigest, SyncChangeId, SyncKey, SyncManifest, SyncStateUnit};
pub use transport::SyncTransport;
