use alloc::{string::String, vec::Vec};

use crate::{SyncChangeId, SyncKey, SyncManifest, SyncStateUnit};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 3;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncHello {
    pub protocol_version: u16,
    pub manifest: SyncManifest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncRowInventory {
    pub table: String,
    pub row: Uuid,
    pub changes: Vec<SyncChangeId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncIncrementalChange {
    pub table: String,
    pub row: Uuid,
    pub id: SyncChangeId,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SyncSnapshotRequest {
    pub table: String,
    pub row: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage {
    Hello(SyncHello),
    Manifest(SyncManifest),
    Inventory(Vec<SyncRowInventory>),
    State(Vec<SyncStateUnit>),
    Changes(Vec<SyncIncrementalChange>),
    RequestSnapshots(Vec<SyncSnapshotRequest>),
    Abort(String),
    Done,
}

impl SyncRowInventory {
    pub fn key(&self) -> SyncKey {
        SyncKey::Row {
            table: self.table.clone(),
            row: self.row,
        }
    }
}
