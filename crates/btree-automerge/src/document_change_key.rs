use core::{
    cmp::Ordering,
    fmt,
    ops::{Bound, RangeBounds},
};

use automerge::ActorId;
use btree::BTreeError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::document_type::DocumentType;

pub type DocumentId = Vec<u8>;
pub type DocumentChangeHash = [u8; 32];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct DocumentChangeKey {
    pub id: DocumentId,
    pub r#type: DocumentType,
    pub change_hash: DocumentChangeHash,
}

impl DocumentChangeKey {
    pub fn new(id: DocumentId, r#type: DocumentType, change_hash: DocumentChangeHash) -> Self {
        Self {
            id,
            r#type,
            change_hash,
        }
    }

    pub fn new_snapshot(id: DocumentId, change_hash: DocumentChangeHash) -> Self {
        Self::new(id, DocumentType::Snapshot, change_hash)
    }

    pub fn new_incremental(id: DocumentId, change_hash: DocumentChangeHash) -> Self {
        Self::new(id, DocumentType::Incremental, change_hash)
    }

    pub fn id_to_uuid(id: &[u8]) -> Uuid {
        if id.len() == 16
            && let Some(uuid) = Uuid::from_slice(id).ok()
        {
            return uuid;
        }

        Uuid::new_v5(&Uuid::NAMESPACE_DNS, id)
    }

    pub fn id(&self) -> &DocumentId {
        &self.id
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.id
    }

    pub fn r#type(&self) -> DocumentType {
        self.r#type
    }

    pub fn change_hash(&self) -> &DocumentChangeHash {
        &self.change_hash
    }

    pub fn uuid(&self) -> Uuid {
        Self::id_to_uuid(&self.id)
    }

    pub fn actor_id(&self) -> ActorId {
        ActorId::from(self.uuid().as_bytes())
    }

    pub fn encode_ordered(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.id.len() + 35);
        for byte in &self.id {
            if *byte == 0 {
                bytes.extend_from_slice(&[0, 255]);
            } else {
                bytes.push(*byte);
            }
        }
        bytes.extend_from_slice(&[0, 0, self.r#type as u8]);
        bytes.extend_from_slice(&self.change_hash);
        bytes
    }

    pub fn decode_ordered(bytes: &[u8]) -> Result<Self, BTreeError> {
        let mut id = Vec::new();
        let mut index = 0;
        loop {
            match (bytes.get(index), bytes.get(index + 1)) {
                (Some(0), Some(0)) => {
                    index += 2;
                    break;
                }
                (Some(0), Some(255)) => {
                    id.push(0);
                    index += 2;
                }
                (Some(byte), _) => {
                    id.push(*byte);
                    index += 1;
                }
                _ => return Err(BTreeError::InvalidDocument),
            }
        }
        let r#type = match bytes.get(index) {
            Some(0) => DocumentType::Snapshot,
            Some(1) => DocumentType::Incremental,
            _ => return Err(BTreeError::InvalidDocument),
        };
        let hash: DocumentChangeHash = bytes
            .get(index + 1..)
            .ok_or(BTreeError::InvalidDocument)?
            .try_into()
            .map_err(|_| BTreeError::InvalidDocument)?;
        Ok(Self::new(id, r#type, hash))
    }

    pub fn min_for_id(id: DocumentId) -> Self {
        Self {
            id,
            r#type: DocumentType::Snapshot,
            change_hash: [0u8; 32],
        }
    }

    pub fn max_for_id(id: DocumentId) -> Self {
        Self {
            id,
            r#type: DocumentType::Incremental,
            change_hash: [255u8; 32],
        }
    }

    pub fn range_for(id: &DocumentId) -> impl RangeBounds<Self> {
        let start = Bound::Included(Self::min_for_id(id.clone()));
        let end = Bound::Included(Self::max_for_id(id.clone()));

        (start, end)
    }

    pub fn map_document_id_range<R>(range: R) -> impl RangeBounds<Self>
    where
        R: RangeBounds<DocumentId>,
    {
        let start = match range.start_bound() {
            Bound::Included(id) => Bound::Included(Self::min_for_id(id.clone())),
            Bound::Excluded(id) => Bound::Excluded(Self::max_for_id(id.clone())),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(id) => Bound::Included(Self::max_for_id(id.clone())),
            Bound::Excluded(id) => Bound::Excluded(Self::min_for_id(id.clone())),
            Bound::Unbounded => Bound::Unbounded,
        };

        (start, end)
    }
}

impl Ord for DocumentChangeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.r#type.cmp(&other.r#type))
            .then_with(|| self.change_hash.cmp(&other.change_hash))
    }
}

impl PartialOrd for DocumentChangeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for DocumentChangeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:x?}|{}|{:x?}", self.id, self.r#type, self.change_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::{DocumentChangeKey, DocumentType};

    #[test]
    fn ordered_codec_round_trips_and_preserves_order() {
        let keys = [
            DocumentChangeKey::new(vec![], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![0], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![0, 1], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![1], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![1], DocumentType::Incremental, [0; 32]),
            DocumentChangeKey::new(vec![1], DocumentType::Incremental, [1; 32]),
        ];
        for pair in keys.windows(2) {
            assert_eq!(
                pair[0].cmp(&pair[1]),
                pair[0].encode_ordered().cmp(&pair[1].encode_ordered())
            );
            assert_eq!(
                DocumentChangeKey::decode_ordered(&pair[0].encode_ordered()).unwrap(),
                pair[0]
            );
        }
    }

    #[test]
    fn ordered_codec_rejects_malformed_bytes() {
        for bytes in [vec![], vec![0], vec![0, 1], vec![0, 0, 2], vec![0, 0, 0]] {
            assert!(DocumentChangeKey::decode_ordered(&bytes).is_err());
        }
    }
}
