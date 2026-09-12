use core::ops::{Bound, RangeBounds};

use async_stream::stream;
use db_btree::{BTreeError, BTreeRead, BTreeResult, BTreeTransaction};
use db_btree_automerge::{DocumentChangeKey, DocumentType};
use db_engine::KernelTransaction;
use db_value::{Row, Value};
use futures::{Stream, StreamExt, pin_mut};

pub(crate) struct ChangeLogRead<'a, T> {
    transaction: &'a T,
    table: String,
}

impl<'a, T> ChangeLogRead<'a, T> {
    pub(crate) fn new(transaction: &'a T, table: impl Into<String>) -> Self {
        Self {
            transaction,
            table: table.into(),
        }
    }
}

pub(crate) struct ChangeLogTransaction<'a, T> {
    transaction: &'a mut T,
    table: String,
}

impl<'a, T> ChangeLogTransaction<'a, T> {
    pub(crate) fn new(transaction: &'a mut T, table: impl Into<String>) -> Self {
        Self {
            transaction,
            table: table.into(),
        }
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
    let hash = hash.try_into().map_err(|_| BTreeError::InvalidDocument)?;
    Ok(DocumentChangeKey::new(id, r#type, hash))
}

fn row_key(key: &DocumentChangeKey) -> Row {
    Row::new(vec![Value::Blob(encode(key))])
}

fn decode_entry(key: Row, value: Row) -> BTreeResult<(DocumentChangeKey, Vec<u8>)> {
    let Some(Value::Blob(key)) = key.values.first() else {
        return Err(BTreeError::InvalidDocument);
    };
    let Some(Value::Blob(value)) = value.values.first() else {
        return Err(BTreeError::InvalidDocument);
    };
    Ok((decode(key)?, value.clone()))
}

fn range_contains(
    range: &(Bound<DocumentChangeKey>, Bound<DocumentChangeKey>),
    key: &DocumentChangeKey,
) -> bool {
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

fn bounds<R>(range: R) -> (Bound<DocumentChangeKey>, Bound<DocumentChangeKey>)
where
    R: RangeBounds<DocumentChangeKey>,
{
    let start = match range.start_bound() {
        Bound::Included(key) => Bound::Included(key.clone()),
        Bound::Excluded(key) => Bound::Excluded(key.clone()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Bound::Included(key.clone()),
        Bound::Excluded(key) => Bound::Excluded(key.clone()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

impl<T> BTreeRead<DocumentChangeKey, Vec<u8>> for ChangeLogRead<'_, T>
where
    T: KernelTransaction,
{
    async fn get(&self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        let row = self
            .transaction
            .get_entry(&self.table, &row_key(key))
            .await
            .map_err(BTreeError::custom)?;
        row.map(|value| decode_entry(row_key(key), value).map(|(_, value)| value))
            .transpose()
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let range = bounds(range);
        let entries = self.transaction.scan_entries(&self.table);
        stream! {
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry.map_err(BTreeError::custom)?;
                let (key, value) = decode_entry(key, value)?;
                if range_contains(&range, &key) { yield Ok((key, value)); }
            }
        }
    }
}

impl<T> BTreeRead<DocumentChangeKey, Vec<u8>> for ChangeLogTransaction<'_, T>
where
    T: KernelTransaction + Send,
{
    async fn get(&self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        let row = self
            .transaction
            .get_entry(&self.table, &row_key(key))
            .await
            .map_err(BTreeError::custom)?;
        row.map(|value| decode_entry(row_key(key), value).map(|(_, value)| value))
            .transpose()
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let range = bounds(range);
        let entries = self.transaction.scan_entries(&self.table);
        stream! {
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry.map_err(BTreeError::custom)?;
                let (key, value) = decode_entry(key, value)?;
                if range_contains(&range, &key) { yield Ok((key, value)); }
            }
        }
    }
}

impl<T> BTreeTransaction<DocumentChangeKey, Vec<u8>> for ChangeLogTransaction<'_, T>
where
    T: KernelTransaction + Send,
{
    async fn insert(&mut self, key: DocumentChangeKey, value: Vec<u8>) -> BTreeResult<()> {
        self.transaction
            .put_entry(
                &self.table,
                row_key(&key),
                Row::new(vec![Value::Blob(value)]),
            )
            .await
            .map_err(BTreeError::custom)
    }

    async fn update<F>(&mut self, key: DocumentChangeKey, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut Vec<u8>) -> BTreeResult<()>,
    {
        let Some(mut value) = self.get(&key).await? else {
            return Ok(None);
        };
        update_fn(&mut value)?;
        self.insert(key, value).await?;
        Ok(Some(()))
    }

    async fn remove(&mut self, key: &DocumentChangeKey) -> BTreeResult<Option<Vec<u8>>> {
        let value = self
            .transaction
            .remove_entry(&self.table, &row_key(key))
            .await
            .map_err(BTreeError::custom)?;
        value
            .map(|value| decode_entry(row_key(key), value).map(|(_, value)| value))
            .transpose()
    }

    fn remove_range<R>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>
    where
        R: RangeBounds<DocumentChangeKey>,
    {
        let _ = range;
        stream! {
            yield Err(BTreeError::UnsupportedOperation);
        }
    }

    async fn commit(self) -> BTreeResult<()> {
        Ok(())
    }
    async fn rollback(self) -> BTreeResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{bounds, decode, encode, range_contains};

    use db_btree_automerge::{DocumentChangeKey, DocumentType};

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
        let range = bounds(DocumentChangeKey::range_for(&id));
        assert!(range_contains(&range, &first));
        assert!(range_contains(&range, &last));
        assert!(!range_contains(&range, &previous));
        assert!(!range_contains(&range, &next));
    }
}
