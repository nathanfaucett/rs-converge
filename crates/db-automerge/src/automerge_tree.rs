use core::ops::RangeBounds;
use std::sync::Arc;

use async_lock::RwLock;
use async_stream::stream;
use automerge::AutoCommit;
use futures::{Stream, StreamExt, pin_mut};

use db_btree::{BTree, BTreeError, BTreeRead, BTreeResult, BTreeTransaction};

use crate::{
    AutomergeBTreeTransaction, DocumentChangeKey, ThresholdPolicy, document_change_key::DocumentId,
    reconstruction::ReconstructedDocument, util::tx_get_document,
};

#[derive(Clone)]
pub struct AutomergeBTree<B>
where
    B: BTree<DocumentChangeKey, Vec<u8>>,
{
    inner: Arc<RwLock<B>>,
    policy: ThresholdPolicy,
}

impl<B> AutomergeBTree<B>
where
    B: BTree<DocumentChangeKey, Vec<u8>>,
{
    pub fn new(inner: B) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
            policy: ThresholdPolicy::default(),
        }
    }
    pub fn with_compaction(inner: B, threshold_count: usize, threshold_bytes: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
            policy: ThresholdPolicy {
                threshold_count,
                threshold_bytes,
            },
        }
    }
}

impl<B> BTreeRead<DocumentId, AutoCommit> for AutomergeBTree<B>
where
    B: BTree<DocumentChangeKey, Vec<u8>>,
{
    async fn get(&self, key: &DocumentId) -> BTreeResult<Option<AutoCommit>> {
        let inner_guard = self.inner.read().await;
        let mut tx = inner_guard.transaction().await?;
        if let Some((result, compacted)) = tx_get_document(&mut tx, key, true, &self.policy).await?
        {
            if compacted {
                tx.commit().await?;
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    fn range<R>(&self, range: R) -> impl Stream<Item = BTreeResult<(DocumentId, AutoCommit)>>
    where
        R: RangeBounds<DocumentId>,
    {
        stream!({
            let inner_range = DocumentChangeKey::map_document_id_range(range);

            let inner_guard = self.inner.read().await;
            let inner_stream = inner_guard.range(inner_range);
            pin_mut!(inner_stream);

            let mut reconstructed_document_option: Option<ReconstructedDocument> = None;

            while let Some(item) = inner_stream.next().await {
                let (k, v) = item?;

                if let Some(mut doc) = reconstructed_document_option.take() {
                    if doc.same_id(&k) {
                        reconstructed_document_option = Some(doc);
                    } else {
                        if let Some(completed_doc) = doc.doc.take() {
                            yield Ok((doc.id, completed_doc));
                        }
                    }
                }

                let reconstructed_document = reconstructed_document_option
                    .get_or_insert_with(|| ReconstructedDocument::new(k.id().clone()));

                if let Err(e) = reconstructed_document.apply(&k, &v) {
                    yield Err(BTreeError::Custom(e.to_string()));
                    continue;
                }
            }

            if let Some(mut doc) = reconstructed_document_option
                && let Some(completed_doc) = doc.doc.take()
            {
                yield Ok((doc.id, completed_doc));
            }
        })
    }
}

impl<B> BTree<DocumentId, AutoCommit> for AutomergeBTree<B>
where
    B: BTree<DocumentChangeKey, Vec<u8>>,
{
    type Transaction = AutomergeBTreeTransaction<B::Transaction>;

    async fn transaction(&self) -> BTreeResult<Self::Transaction> {
        let inner_guard = self.inner.read().await;
        let inner_tx = inner_guard.transaction().await?;
        Ok(AutomergeBTreeTransaction::new(
            inner_tx,
            self.policy.clone(),
        ))
    }
}
