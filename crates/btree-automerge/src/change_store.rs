use core::ops::{Bound, RangeBounds};

use async_stream::stream;
use btree::{BTreeRead, BTreeResult, BTreeTransaction};
use futures::{Stream, StreamExt, pin_mut};

use crate::DocumentChangeKey;

pub struct AutomergeChangeStore<T> {
    inner: T,
}

impl<T> AutomergeChangeStore<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

fn bounds<R>(range: R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
where
    R: RangeBounds<DocumentChangeKey>,
{
    let map = |bound: Bound<&DocumentChangeKey>| match bound {
        Bound::Included(key) => Bound::Included(key.encode_ordered()),
        Bound::Excluded(key) => Bound::Excluded(key.encode_ordered()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (map(range.start_bound()), map(range.end_bound()))
}

fn contains(range: &(Bound<Vec<u8>>, Bound<Vec<u8>>), key: &[u8]) -> bool {
    match &range.0 {
        Bound::Included(start) if key < start => return false,
        Bound::Excluded(start) if key <= start => return false,
        _ => {}
    }
    match &range.1 {
        Bound::Included(end) if key > end => false,
        Bound::Excluded(end) if key >= end => false,
        _ => true,
    }
}

impl<T> BTreeRead<DocumentChangeKey, Vec<u8>> for AutomergeChangeStore<T>
where
    T: BTreeRead<Vec<u8>, Vec<u8>>,
{
    async fn get(&self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        self.inner.get(&key.encode_ordered()).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let range = bounds(range);
        let entries = self.inner.range(..);
        stream! {
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                if contains(&range, &key) {
                    yield Ok((DocumentChangeKey::decode_ordered(&key)?, value));
                }
            }
        }
    }
}

impl<T> BTreeTransaction<DocumentChangeKey, Vec<u8>> for AutomergeChangeStore<T>
where
    T: BTreeTransaction<Vec<u8>, Vec<u8>>,
{
    async fn insert(&mut self, key: DocumentChangeKey, value: Vec<u8>) -> BTreeResult<()> {
        self.inner.insert(key.encode_ordered(), value).await
    }

    async fn update<F>(&mut self, key: DocumentChangeKey, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> BTreeResult<()>,
    {
        self.inner.update(key.encode_ordered(), update_fn).await
    }

    async fn remove(&mut self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        self.inner.remove(&key.encode_ordered()).await
    }

    fn remove_range<R>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let entries = self.inner.remove_range(bounds(range));
        stream! {
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                yield Ok((DocumentChangeKey::decode_ordered(&key)?, value));
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
