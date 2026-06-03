use automerge::transaction::Transactable;
use db_engine::{BTree, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor, InMemoryBTree};
use futures::{StreamExt, executor::block_on};
use std::fs;
use uuid::Uuid;

use db_automerge::{AutoCommit, AutomergeBTree, DocumentChangeKey, DocumentType, hash_hashes};

fn tmp_path(name: &str) -> std::path::PathBuf {
  let mut path = std::env::temp_dir();
  path.push(format!("db-automerge-{}.db", name));
  path
}

#[test]
fn insert_and_get_latest() {
  let _ = fs::remove_file(tmp_path("basic"));

  let underlying = InMemoryBTree::<DocumentChangeKey, Vec<u8>>::new();
  let mut store = AutomergeBTree::new_automerge(underlying);

  let doc_id = Uuid::new_v4();
  let doc = AutoCommit::new();
  let mut expected = doc.clone();

  block_on(store.insert(doc_id, doc)).expect("insert");
  let mut got: AutoCommit = block_on(store.get(&doc_id)).expect("get").expect("missing");
  let expected_bytes = expected.save();
  let got_bytes = got.save();
  assert_eq!(got_bytes, expected_bytes);
}

#[test]
fn range_ordering() {
  let underlying = InMemoryBTree::<DocumentChangeKey, Vec<u8>>::new();
  let mut store = AutomergeBTree::new_automerge(underlying);

  let mut ids: Vec<Uuid> = Vec::new();
  for i in 0..3 {
    let id = Uuid::new_v4();
    ids.push(id);
    let mut doc = AutoCommit::new();
    doc
      .put(&automerge::ROOT, "v", format!("v{}", i))
      .expect("put");
    block_on(store.insert(id, doc)).expect("insert");
    std::thread::sleep(std::time::Duration::from_millis(1));
  }

  let start = &Uuid::nil();
  let end = &Uuid::from_u128(u128::MAX);

  let s = store.range(start..=end);
  let items: Vec<(Uuid, AutoCommit)> = block_on(async move {
    let mut collected: Vec<(Uuid, AutoCommit)> = Vec::new();
    futures::pin_mut!(s);
    while let Some(item) = s.next().await {
      let (k, v) = item.expect("range failed");
      collected.push((k, v));
    }
    collected
  });

  assert_eq!(items.len(), 3);
}

#[test]
fn compaction_with_concurrent_writer() {
  block_on(async {
    let underlying = InMemoryBTree::<DocumentChangeKey, Vec<u8>>::new();
    let automerge = AutomergeBTree::with_compaction_automerge(underlying.clone(), 1, 1);
    let doc_id = Uuid::new_v4();

    let mut base_doc = AutoCommit::new();
    base_doc
      .put(&automerge::ROOT, "message", "hello")
      .expect("put base value");
    let base_changes = base_doc.get_changes(&[]);
    let mut base_delta = Vec::new();
    for change in &base_changes {
      base_delta.extend_from_slice(change.raw_bytes());
    }

    let delta_key = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: hash_hashes(base_changes.iter().map(|c| c.hash().0)),
    };

    {
      let mut tx = underlying.transaction().await.expect("start tx");
      tx.insert(delta_key.clone(), base_delta.clone())
        .await
        .expect("insert delta");
      tx.commit().await.expect("commit tx");
    }

    // trigger compaction before the concurrent writer inserts
    let _ = automerge.get(&doc_id).await;

    let writer_store = underlying.clone();
    let mut writer_doc = base_doc.clone();
    writer_doc
      .put(&automerge::ROOT, "tail", "!")
      .expect("put writer value");
    let writer_changes = writer_doc.get_changes(&base_doc.get_heads());
    let mut writer_delta = Vec::new();
    for change in &writer_changes {
      writer_delta.extend_from_slice(change.raw_bytes());
    }
    let writer_key = DocumentChangeKey {
      doc_id,
      doc_type: DocumentType::Incremental,
      change_hash: hash_hashes(writer_changes.iter().map(|c| c.hash().0)),
    };

    {
      let mut tx = writer_store.transaction().await.expect("start writer tx");
      tx.insert(writer_key.clone(), writer_delta.clone())
        .await
        .expect("insert writer delta");
      tx.commit().await.expect("commit writer tx");
    }

    let final_store = AutomergeBTree::new_automerge(underlying.clone());
    let mut final_doc: AutoCommit = final_store
      .get(&doc_id)
      .await
      .expect("get final")
      .expect("missing final");
    assert_eq!(final_doc.save(), writer_doc.save());

    let actual_writer_value = underlying
      .get(&writer_key)
      .await
      .expect("get writer key failed")
      .expect("writer key missing");
    assert_eq!(actual_writer_value, writer_delta);
  });
}

#[test]
fn encoded_remove_only_deletes_target_document() {
  block_on(async {
    let underlying = InMemoryBTree::<DocumentChangeKey, Vec<u8>>::new();
    let mut store = AutomergeBTree::new_automerge(underlying);

    let doc_a = Uuid::from_u128(1);
    let doc_b = Uuid::from_u128(2);

    let mut first = AutoCommit::new();
    first.put(&automerge::ROOT, "v", "a").expect("put a");
    let mut second = AutoCommit::new();
    second.put(&automerge::ROOT, "v", "b").expect("put b");

    store.insert(doc_a, first.clone()).await.expect("insert a");
    store.insert(doc_b, second.clone()).await.expect("insert b");

    let mut removed: AutoCommit = store
      .remove(&doc_a)
      .await
      .expect("remove a")
      .expect("removed doc");
    assert_eq!(removed.save(), first.save());

    assert!(store.get(&doc_a).await.expect("get removed").is_none());
    let mut remaining: AutoCommit = store
      .get(&doc_b)
      .await
      .expect("get remaining")
      .expect("remaining doc");
    assert_eq!(remaining.save(), second.save());
  });
}
