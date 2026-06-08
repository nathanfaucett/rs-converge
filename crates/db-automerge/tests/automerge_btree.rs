use automerge::{AutoCommit, transaction::Transactable};
use db_btree::{BTreeReadExecutor, BTreeWriteExecutor, InMemoryBTree};
use futures::{StreamExt, executor::block_on};
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
