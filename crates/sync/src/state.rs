use alloc::vec::Vec;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use engine::TableGenerationId;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SyncChangeId(pub Vec<u8>);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum SyncKey {
    Table { id: Uuid },
    TableField { id: Uuid },
    Index { id: Uuid },
    IndexField { index: Uuid, position: u32 },
    Row { table: TableGenerationId, row: Uuid },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct StateDigest(pub [u8; 32]);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncStateUnit {
    pub key: SyncKey,
    pub state: Vec<u8>,
    pub metadata: Vec<u8>,
    pub digest: StateDigest,
}

impl SyncStateUnit {
    pub fn new(key: SyncKey, state: Vec<u8>, metadata: Vec<u8>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update((state.len() as u64).to_le_bytes());
        hasher.update(&state);
        hasher.update((metadata.len() as u64).to_le_bytes());
        hasher.update(&metadata);
        Self {
            key,
            state,
            metadata,
            digest: StateDigest(hasher.finalize().into()),
        }
    }

    pub fn verify_digest(&self) -> bool {
        Self::new(self.key.clone(), self.state.clone(), self.metadata.clone()).digest == self.digest
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncManifest {
    pub entries: Vec<(SyncKey, StateDigest)>,
}

impl SyncManifest {
    pub fn new(mut entries: Vec<(SyncKey, StateDigest)>) -> Self {
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        Self { entries }
    }

    pub fn contains(&self, key: &SyncKey, digest: StateDigest) -> bool {
        self.entries
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
            .is_ok_and(|index| self.entries[index].1 == digest)
    }
}
