use alloc::vec::Vec;

use db_engine::Frontier;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncHello {
    pub protocol_version: u16,
    pub replication_domain: [u8; 32],
    pub row_codec: [u8; 32],
    pub frontier: Frontier,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SyncMessage {
    Hello(SyncHello),
    Frontier(Frontier),
    Checkpoint(Vec<u8>),
    Envelopes(Vec<Vec<u8>>),
    Done,
}
