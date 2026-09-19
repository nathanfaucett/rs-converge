use automerge::{ActorId, AutoCommit};
use futures::{StreamExt, pin_mut};

use btree::{BTreeResult, BTreeTransaction};

use crate::{
    CompactionPolicy, DocumentChangeKey, ThresholdPolicy,
    document_change_key::DocumentId,
    hash_heads,
    reconstruction::{reconstruct_document, reconstruct_documents},
    run_compaction,
};

pub async fn tx_get_document<T>(
    tx: &mut T,
    doc_id: &DocumentId,
    allow_compaction: bool,
    policy: &ThresholdPolicy,
) -> BTreeResult<Option<(AutoCommit, bool)>>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    let Some(reconstructed_document) = reconstruct_document(tx, doc_id).await? else {
        return Ok(None);
    };

    let mut doc = match reconstructed_document.doc {
        Some(doc) => doc,
        None => return Ok(None),
    };

    let compacted = if allow_compaction {
        if policy.should_compact(
            reconstructed_document.deltas,
            reconstructed_document.bytes_size,
        ) {
            run_compaction(tx, &reconstructed_document.id, &mut doc).await?;
            true
        } else {
            false
        }
    } else {
        false
    };

    Ok(Some((doc, compacted)))
}

pub async fn tx_remove_document<T>(
    tx: &mut T,
    doc_id: &DocumentId,
) -> BTreeResult<Option<AutoCommit>>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    let (keys_to_remove, result) = {
        let documents = reconstruct_documents(tx.range(DocumentChangeKey::range_for(doc_id)));
        pin_mut!(documents);

        match documents.next().await {
            Some(document) => (document.keys, document.result),
            None => return Ok(None),
        }
    };

    for key in keys_to_remove {
        tx.remove(&key).await?;
    }

    Ok(result?.doc)
}

pub async fn tx_insert_snapshot<T>(
    tx: &mut T,
    key: DocumentId,
    mut value: AutoCommit,
) -> BTreeResult<()>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
{
    let key = DocumentChangeKey::new_snapshot(key, hash_heads(value.get_heads()));
    tx.insert(key, value.save()).await?;
    Ok(())
}

pub async fn tx_update<T, F>(
    tx: &mut T,
    doc_id: DocumentId,
    update_fn: F,
    policy: &ThresholdPolicy,
) -> BTreeResult<()>
where
    T: BTreeTransaction<DocumentChangeKey, Vec<u8>>,
    F: FnOnce(&mut AutoCommit) -> BTreeResult<()>,
{
    let mut current_doc = tx_get_document(tx, &doc_id, false, policy)
        .await?
        .map(|(doc, _)| doc)
        .unwrap_or_else(|| {
            AutoCommit::new().with_actor(ActorId::from(
                DocumentChangeKey::id_to_uuid(doc_id.as_slice()).as_bytes(),
            ))
        });

    update_fn(&mut current_doc)?;

    let key = DocumentChangeKey::new_incremental(doc_id, hash_heads(current_doc.get_heads()));
    let delta = current_doc.save_incremental();

    tx.insert(key, delta).await?;

    Ok(())
}
