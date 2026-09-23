use automerge::{AutoCommit, ROOT, transaction::Transactable};
use btree::{BTree, BTreeRead, BTreeTransaction, InMemoryBTree};
use btree_automerge::{AutomergeBTree, DocumentChangeKey, DocumentId, DocumentType};
use futures::{StreamExt, executor::block_on, pin_mut};

fn inner_tree() -> InMemoryBTree<DocumentChangeKey, Vec<u8>> {
    InMemoryBTree::new()
}

#[test]
fn missing_document_returns_none() {
    block_on(async {
        let tree = AutomergeBTree::new(inner_tree());

        assert!(tree.get(&DocumentId::from([1])).await.unwrap().is_none());
    });
}

#[test]
fn metadatas_are_included_in_snapshot_and_incremental_ranges() {
    block_on(async {
        let inner = inner_tree();
        let id = DocumentId::from([1]);
        let mut document = AutoCommit::new();
        let snapshot = DocumentChangeKey::new_snapshot(id.clone(), [0; 32]);
        let incremental = DocumentChangeKey::new_incremental(id.clone(), [1; 32]);
        let metadata = DocumentChangeKey::new_metadata(id.clone());

        let mut tx = inner.transaction().await.unwrap();
        tx.insert(snapshot, document.save()).await.unwrap();
        document.put(ROOT, "value", "changed").unwrap();
        tx.insert(incremental, document.save_incremental())
            .await
            .unwrap();
        tx.insert(metadata, Vec::new()).await.unwrap();
        tx.commit().await.unwrap();

        let tree = AutomergeBTree::new(inner.clone());
        assert!(tree.get(&id).await.unwrap().is_none());
        let stream = tree.range(id.clone()..=id.clone());
        pin_mut!(stream);
        assert!(stream.next().await.is_none());

        let range_id = id.clone();
        let raw = inner.range(DocumentChangeKey::range_for(&range_id));
        pin_mut!(raw);
        assert_eq!(
            raw.next().await.unwrap().unwrap().0.r#type(),
            DocumentType::Snapshot
        );
        assert_eq!(
            raw.next().await.unwrap().unwrap().0.r#type(),
            DocumentType::Incremental
        );
        assert_eq!(
            raw.next().await.unwrap().unwrap().0.r#type(),
            DocumentType::Metadata
        );
        assert!(raw.next().await.is_none());

        let key = DocumentChangeKey::new(id, DocumentType::Metadata, [0; 32]);
        assert_eq!(
            DocumentChangeKey::decode_ordered(&key.encode_ordered()).unwrap(),
            key
        );
    });
}

#[test]
fn remove_range_skips_malformed_documents_and_removes_their_changes() {
    block_on(async {
        let inner = inner_tree();
        let malformed_id = DocumentId::from([1]);
        let valid_id = DocumentId::from([2]);
        let malformed_key = DocumentChangeKey::new_incremental(malformed_id.clone(), [0; 32]);
        let valid_key = DocumentChangeKey::new_snapshot(valid_id.clone(), [1; 32]);
        let mut document = AutoCommit::new();

        let mut inner_tx = inner.transaction().await.unwrap();
        inner_tx
            .insert(malformed_key.clone(), vec![0])
            .await
            .unwrap();
        inner_tx
            .insert(valid_key.clone(), document.save())
            .await
            .unwrap();
        inner_tx.commit().await.unwrap();

        let tree = AutomergeBTree::new(inner.clone());
        let mut tx = tree.transaction().await.unwrap();
        {
            let stream = tx.remove_range(malformed_id.clone()..=valid_id.clone());
            pin_mut!(stream);

            assert!(stream.next().await.unwrap().is_err());
            assert_eq!(stream.next().await.unwrap().unwrap().0, valid_id);
            assert!(stream.next().await.is_none());
        }
        tx.commit().await.unwrap();

        assert_eq!(inner.get(&malformed_key).await.unwrap(), None);
        assert_eq!(inner.get(&valid_key).await.unwrap(), None);
    });
}
