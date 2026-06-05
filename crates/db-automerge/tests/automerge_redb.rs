use automerge::transaction::Transactable;
use db_automerge::{AutoCommit, AutomergeBTree, DocumentChangeKey};
use db_engine::{BTreeDefinition, BTreeFactory, BTreeReadExecutor, BTreeWriteExecutor};
use db_redb::RedbBTreeFactory;
use futures::executor::block_on;
use std::{
  path::PathBuf,
  process,
  time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone)]
struct ChangeLogDefinition {
  id: String,
}

impl ChangeLogDefinition {
  fn new(id: &str) -> Self {
    Self { id: id.to_string() }
  }
}

impl BTreeDefinition for ChangeLogDefinition {
  type Key = DocumentChangeKey;
  type Value = Vec<u8>;

  fn id(&self) -> &str {
    &self.id
  }
}

fn create_temp_db() -> (redb::Database, PathBuf) {
  let mut path = std::env::temp_dir();
  let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .expect("clock before epoch")
    .as_nanos();
  path.push(format!("db-automerge-redb-{}-{}.db", process::id(), now));

  if path.exists() {
    let _ = std::fs::remove_file(&path);
  }

  let db = redb::Database::create(&path).expect("create redb db");
  (db, path)
}

#[test]
fn automerge_on_redb_insert_get_round_trip() {
  block_on(async {
    let (db, path) = create_temp_db();

    let factory = RedbBTreeFactory::new(db);
    let tree_def = ChangeLogDefinition::new("automerge_docs_test");
    let underlying = factory.create(&tree_def).await.expect("create redb tree");

    let mut store = AutomergeBTree::new_automerge(underlying);

    let doc_id = Uuid::new_v4();
    let mut doc = AutoCommit::new();
    doc
      .put(&automerge::ROOT, "name", "alice")
      .expect("write automerge value");

    let mut expected = doc.clone();
    store.insert(doc_id, doc).await.expect("insert doc");

    let mut loaded = store
      .get(&doc_id)
      .await
      .expect("read doc")
      .expect("missing doc");

    assert_eq!(loaded.save(), expected.save());

    drop(store);
    let _ = std::fs::remove_file(path);
  });
}
