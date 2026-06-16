use automerge::{AutoCommit, transaction::Transactable};
use db_btree::{BTreeReadExecutor, BTreeWriteExecutor, InMemoryBTree};
use futures::executor::block_on;
use std::fs;
use uuid::Uuid;

use db_automerge::{AutomergeBTree, DocumentChangeKey};

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

  let doc_id = Uuid::now_v7().as_bytes().to_vec();
  let doc = AutoCommit::new();
  let mut expected = doc.clone();

  block_on(store.insert(doc_id.clone(), doc)).expect("insert");
  let mut got: AutoCommit = block_on(store.get(&doc_id)).expect("get").expect("missing");
  let expected_bytes = expected.save();
  let got_bytes = got.save();
  assert_eq!(got_bytes, expected_bytes);
}

#[test]
fn encoded_remove_only_deletes_target_document() {
  block_on(async {
    let underlying = InMemoryBTree::<DocumentChangeKey, Vec<u8>>::new();
    let mut store = AutomergeBTree::new_automerge(underlying);

    let doc_a = Uuid::from_u128(1).as_bytes().to_vec();
    let doc_b = Uuid::from_u128(2).as_bytes().to_vec();

    let mut first = AutoCommit::new();
    first.put(&automerge::ROOT, "v", "a").expect("put a");
    let mut second = AutoCommit::new();
    second.put(&automerge::ROOT, "v", "b").expect("put b");

    store
      .insert(doc_a.clone(), first.clone())
      .await
      .expect("insert a");
    store
      .insert(doc_b.clone(), second.clone())
      .await
      .expect("insert b");

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
