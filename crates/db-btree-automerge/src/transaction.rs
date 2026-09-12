use core::ops::RangeBounds;

use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt, pin_mut};

use db_btree::{BTreeRead, BTreeResult, BTreeTransaction};

use crate::{
    DocumentChangeKey, ThresholdPolicy,
    document_change_key::DocumentId,
    reconstruction::{reconstruct_document, reconstruct_documents},
    util::{tx_insert_snapshot, tx_remove_document, tx_update},
};

pub async fn get_document<T>(transaction: &T, key: &DocumentId) -> BTreeResult<Option<AutoCommit>>
where
    T: BTreeRead<DocumentChangeKey, Vec<u8>>,
{
    Ok(reconstruct_document(transaction, key)
        .await?
        .and_then(|document| document.doc))
}

pub struct AutomergeBTreeTransaction<T>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    inner_tx: T,
    policy: ThresholdPolicy,
}

unsafe impl<T> Send for AutomergeBTreeTransaction<T> where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>
{
}

impl<T> AutomergeBTreeTransaction<T>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    pub fn new(inner_tx: T, policy: ThresholdPolicy) -> Self {
        Self { inner_tx, policy }
    }
}

impl<T> BTreeRead<DocumentId, AutoCommit> for AutomergeBTreeTransaction<T>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    async fn get(&self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
        get_document(&self.inner_tx, key).await
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
    where
        R: RangeBounds<DocumentId>,
    {
        stream!({
            let inner_range = DocumentChangeKey::map_document_id_range(range);
            let documents = reconstruct_documents(self.inner_tx.range(inner_range));
            pin_mut!(documents);

            while let Some(document) = documents.next().await {
                match document.result {
                    Ok(mut document) => {
                        if let Some(doc) = document.doc.take() {
                            yield Ok((document.id, doc));
                        }
                    }
                    Err(error) => yield Err(error),
                }
            }
        })
    }
}

impl<T> BTreeTransaction<DocumentId, AutoCommit> for AutomergeBTreeTransaction<T>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    async fn insert(&mut self, key: DocumentId, value: AutoCommit) -> BTreeResult<()>
    where
        DocumentId: Ord,
    {
        tx_insert_snapshot(&mut self.inner_tx, key, value).await
    }

    async fn update<F>(&mut self, key: DocumentId, update_fn: F) -> BTreeResult<Option<()>>
    where
        F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
    {
        tx_update(&mut self.inner_tx, key, update_fn, &self.policy).await?;
        Ok(Some(()))
    }

    async fn remove(&mut self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
        tx_remove_document(&mut self.inner_tx, key).await
    }

    fn remove_range<R>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
    where
        R: RangeBounds<DocumentId>,
    {
        stream!({
            let keys_to_remove = {
                let inner_range = DocumentChangeKey::map_document_id_range(range);
                let documents = reconstruct_documents(self.inner_tx.range(inner_range));
                pin_mut!(documents);
                let mut keys_to_remove = Vec::new();

                while let Some(document) = documents.next().await {
                    keys_to_remove.extend(document.keys);
                    match document.result {
                        Ok(mut document) => {
                            if let Some(doc) = document.doc.take() {
                                yield Ok((document.id, doc));
                            }
                        }
                        Err(error) => yield Err(error),
                    }
                }

                keys_to_remove
            };

            for key in keys_to_remove {
                self.inner_tx.remove(&key).await?;
            }
        })
    }

    async fn commit(self) -> BTreeResult<()> {
        let AutomergeBTreeTransaction { inner_tx, .. } = self;
        inner_tx.commit().await
    }

    async fn rollback(self) -> BTreeResult<()> {
        self.inner_tx.rollback().await
    }
}
