use core::{
    cmp::Ordering,
    ops::{Bound, RangeBounds},
};

use async_stream::stream;
use db_btree::{BTreeError, BTreeRead, BTreeResult, BTreeTransaction};
use db_btree_automerge::{DocumentChangeKey, DocumentType};
use db_btree_redb::RedbBTreeScopedTransaction;
use futures::Stream;
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ChangeKey(Vec<u8>);

impl redb::Value for ChangeKey {
    type SelfType<'a>
        = Self
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(data.to_vec())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        &value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(core::any::type_name::<Self>())
    }
}

impl redb::Key for ChangeKey {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

pub struct RedbChangeLogTransaction<'a> {
    inner: RedbBTreeScopedTransaction<'a, ChangeKey, Vec<u8>>,
}

impl<'a> RedbChangeLogTransaction<'a> {
    pub(crate) fn new(inner: RedbBTreeScopedTransaction<'a, ChangeKey, Vec<u8>>) -> Self {
        Self { inner }
    }
}

fn encode(key: &DocumentChangeKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(key.id.len() + 35);
    for byte in &key.id {
        if *byte == 0 {
            bytes.extend_from_slice(&[0, 255]);
        } else {
            bytes.push(*byte);
        }
    }
    bytes.extend_from_slice(&[0, 0]);
    bytes.push(key.r#type as u8);
    bytes.extend_from_slice(&key.change_hash);
    bytes
}

fn decode(bytes: &[u8]) -> BTreeResult<DocumentChangeKey> {
    let mut id = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            0 if bytes.get(index + 1) == Some(&0) => {
                index += 2;
                break;
            }
            0 if bytes.get(index + 1) == Some(&255) => {
                id.push(0);
                index += 2;
            }
            byte => {
                id.push(byte);
                index += 1;
            }
        }
    }
    let r#type = match bytes.get(index) {
        Some(0) => DocumentType::Snapshot,
        Some(1) => DocumentType::Incremental,
        _ => return Err(BTreeError::InvalidDocument),
    };
    index += 1;
    let Some(hash) = bytes.get(index..) else {
        return Err(BTreeError::InvalidDocument);
    };
    let hash: [u8; 32] = hash.try_into().map_err(|_| BTreeError::InvalidDocument)?;
    Ok(DocumentChangeKey::new(id, r#type, hash))
}

fn map_range<R>(range: R) -> (Bound<ChangeKey>, Bound<ChangeKey>)
where
    R: RangeBounds<DocumentChangeKey>,
{
    let start = match range.start_bound() {
        Bound::Included(key) => Bound::Included(ChangeKey(encode(key))),
        Bound::Excluded(key) => Bound::Excluded(ChangeKey(encode(key))),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Bound::Included(ChangeKey(encode(key))),
        Bound::Excluded(key) => Bound::Excluded(ChangeKey(encode(key))),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

impl BTreeRead<DocumentChangeKey, Vec<u8>> for RedbChangeLogTransaction<'_> {
    async fn get(&self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        self.inner.get(&ChangeKey(encode(key))).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let entries = self.inner.range(map_range(range));
        stream! {
            for await entry in entries {
                let (key, value) = entry?;
                yield Ok((decode(&key.0)?, value));
            }
        }
    }
}

impl BTreeTransaction<DocumentChangeKey, Vec<u8>> for RedbChangeLogTransaction<'_> {
    async fn insert(&mut self, key: DocumentChangeKey, value: Vec<u8>) -> BTreeResult<()> {
        self.inner.insert(ChangeKey(encode(&key)), value).await
    }

    async fn update<F>(&mut self, key: DocumentChangeKey, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> BTreeResult<()>,
    {
        self.inner.update(ChangeKey(encode(&key)), update_fn).await
    }

    async fn remove(&mut self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        self.inner.remove(&ChangeKey(encode(key))).await
    }

    fn remove_range<R>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let entries = self.inner.remove_range(map_range(range));
        stream! {
            for await entry in entries {
                let (key, value) = entry?;
                yield Ok((decode(&key.0)?, value));
            }
        }
    }

    async fn commit(self) -> BTreeResult<()> {
        self.inner.commit().await
    }

    async fn rollback(self) -> BTreeResult<()> {
        self.inner.rollback().await
    }
}

#[cfg(test)]
mod tests {
    use core::ops::RangeBounds;

    use db_btree_automerge::{DocumentChangeKey, DocumentType};

    use super::{decode, encode, map_range};

    #[test]
    fn encoding_preserves_document_change_key_order() {
        let keys = [
            DocumentChangeKey::new(vec![0], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![0, 1], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![1], DocumentType::Snapshot, [0; 32]),
            DocumentChangeKey::new(vec![1], DocumentType::Incremental, [0; 32]),
        ];
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(encode(&pair[0]) < encode(&pair[1]));
            assert_eq!(decode(&encode(&pair[0])).unwrap(), pair[0]);
        }
    }

    #[test]
    fn document_range_excludes_adjacent_document_ids() {
        let id = vec![0, 1];
        let previous = DocumentChangeKey::new(vec![0], DocumentType::Incremental, [255; 32]);
        let first = DocumentChangeKey::new(id.clone(), DocumentType::Snapshot, [0; 32]);
        let last = DocumentChangeKey::new(id.clone(), DocumentType::Incremental, [255; 32]);
        let next = DocumentChangeKey::new(vec![0, 2], DocumentType::Snapshot, [0; 32]);
        let range = map_range(DocumentChangeKey::range_for(&id));
        assert!(range.contains(&super::ChangeKey(encode(&first))));
        assert!(range.contains(&super::ChangeKey(encode(&last))));
        assert!(!range.contains(&super::ChangeKey(encode(&previous))));
        assert!(!range.contains(&super::ChangeKey(encode(&next))));
    }
}
