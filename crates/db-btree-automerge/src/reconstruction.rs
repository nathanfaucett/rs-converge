use async_stream::stream;
use automerge::AutoCommit;
use db_btree::{BTreeError, BTreeRead, BTreeResult};
use futures::{Stream, StreamExt, pin_mut};

use crate::{DocumentChangeKey, document_change_key::DocumentId};

#[derive(Debug, Clone)]
pub struct ReconstructedDocument {
    pub id: DocumentId,
    pub doc: Option<AutoCommit>,
    pub deltas: usize,
    pub bytes_size: usize,
}

impl ReconstructedDocument {
    pub fn new(id: DocumentId) -> Self {
        Self {
            id,
            doc: None,
            deltas: 0,
            bytes_size: 0,
        }
    }

    pub fn same_id(&self, key: &DocumentChangeKey) -> bool {
        &self.id == key.id()
    }

    pub fn apply(&mut self, key: &DocumentChangeKey, data: &[u8]) -> BTreeResult<()> {
        if let Some(doc) = self.doc.as_mut() {
            doc.load_incremental(data).map_err(BTreeError::custom)?;
        } else {
            if key.r#type().is_snapshot() {
                self.doc.replace(
                    AutoCommit::load(data)
                        .map_err(BTreeError::custom)?
                        .with_actor(key.actor_id()),
                );
            } else {
                return Err(BTreeError::custom(format!(
                    "Expected delta for document {:x?}, but found snapshot",
                    key.id()
                )));
            }
        }

        self.deltas += 1;
        self.bytes_size += data.len();

        Ok(())
    }
}

pub(crate) struct ReconstructedRangeDocument {
    pub keys: Vec<DocumentChangeKey>,
    pub result: BTreeResult<ReconstructedDocument>,
}

struct PendingDocument {
    document: ReconstructedDocument,
    keys: Vec<DocumentChangeKey>,
    error: Option<BTreeError>,
}

impl PendingDocument {
    fn new(key: DocumentChangeKey, data: Vec<u8>) -> Self {
        let mut document = ReconstructedDocument::new(key.id().clone());
        let error = document.apply(&key, &data).err();
        Self {
            document,
            keys: vec![key],
            error,
        }
    }

    fn apply(&mut self, key: DocumentChangeKey, data: Vec<u8>) {
        self.keys.push(key.clone());
        if self.error.is_none()
            && let Err(error) = self.document.apply(&key, &data)
        {
            self.error = Some(error);
        }
    }

    fn finish(self) -> ReconstructedRangeDocument {
        ReconstructedRangeDocument {
            keys: self.keys,
            result: self.error.map_or_else(|| Ok(self.document), Err),
        }
    }
}

pub(crate) fn reconstruct_documents<S>(source: S) -> impl Stream<Item = ReconstructedRangeDocument>
where
    S: Stream<Item = BTreeResult<(DocumentChangeKey, Vec<u8>)>>,
{
    stream!({
        pin_mut!(source);
        let mut pending = None;

        while let Some(item) = source.next().await {
            let (key, data) = match item {
                Ok(item) => item,
                Err(error) => {
                    yield ReconstructedRangeDocument {
                        keys: Vec::new(),
                        result: Err(error),
                    };
                    continue;
                }
            };

            if pending
                .as_ref()
                .is_some_and(|document: &PendingDocument| !document.document.same_id(&key))
            {
                yield pending
                    .take()
                    .expect("pending document is present")
                    .finish();
            }

            match pending.as_mut() {
                Some(document) => document.apply(key, data),
                None => pending = Some(PendingDocument::new(key, data)),
            }
        }

        if let Some(document) = pending {
            yield document.finish();
        }
    })
}

pub async fn reconstruct_document<T>(
    tx: &T,
    key: &DocumentId,
) -> BTreeResult<Option<ReconstructedDocument>>
where
    T: BTreeRead<DocumentChangeKey, Vec<u8>>,
{
    let source = tx.range(DocumentChangeKey::range_for(key));
    let documents = reconstruct_documents(source);
    pin_mut!(documents);

    match documents.next().await {
        Some(document) => document.result.map(Some),
        None => Ok(None),
    }
}
