use db_engine::{BTree, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor};
use db_redb::RedbBTree;
use futures::{StreamExt, executor::block_on, pin_mut};
use redb::TableDefinition;
use std::{
  path::PathBuf,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

fn create_temp_db() -> (redb::Database, PathBuf) {
  let mut path = std::env::temp_dir();
  path.push(format!(
    "aicacia_db_redb_test_{}.db",
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_nanos()
  ));
  // ensure no stale file
  if path.exists() {
    let _ = std::fs::remove_file(&path);
  }
  let db = redb::Database::create(&path).expect("create redb db");
  (db, path)
}

#[test]
fn transaction_commit_and_rollback() {
  block_on(async {
    let (db, path) = create_temp_db();
    let arc_db = Arc::new(db);

    let static_name: &'static str =
      Box::leak("aicacia_btree_tx_commit".to_string().into_boxed_str());
    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_name);

    let mut store = RedbBTree::<i32, i32>::new(arc_db.clone(), "test-commit", table_def);

    store
      .insert(1, 100)
      .await
      .expect("insert initial value into store");
    let mut tx = store.transaction().await.expect("start transaction");
    tx.insert(2, 200)
      .await
      .expect("insert value in transaction");
    tx.remove(&1).await.expect("remove failed");
    tx.commit().await.expect("commit transaction");

    assert_eq!(store.get(&1).await.expect("get failed"), None);
    assert_eq!(store.get(&2).await.expect("get failed"), Some(200));

    drop(arc_db);
    let _ = std::fs::remove_file(path);
  });
}

#[test]
fn transaction_range_merges_pending_changes() {
  block_on(async {
    let (db, path) = create_temp_db();
    let arc_db = Arc::new(db);

    let static_name: &'static str =
      Box::leak("aicacia_btree_range_tx".to_string().into_boxed_str());
    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_name);

    let mut store = RedbBTree::<i32, i32>::new(arc_db.clone(), "test-range", table_def);

    store
      .insert(1, 100)
      .await
      .expect("insert initial value into store");
    store
      .insert(3, 300)
      .await
      .expect("insert second value into store");

    let mut tx = store.transaction().await.expect("start transaction");
    tx.insert(2, 200)
      .await
      .expect("insert value in transaction");
    tx.remove(&3).await.expect("remove failed");

    let mut values = Vec::new();
    let stream = tx.range(0..10);
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
      let (key, value) = item.expect("range item failed");
      values.push((key, value));
    }

    assert_eq!(values, Vec::from([(1, 100), (2, 200)]));

    drop(arc_db);
    let _ = std::fs::remove_file(path);
  });
}

#[test]
fn transaction_get_honors_pending_delete() {
  block_on(async {
    let (db, path) = create_temp_db();
    let arc_db = Arc::new(db);

    let static_name: &'static str = Box::leak("aicacia_btree_get_tx".to_string().into_boxed_str());
    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_name);

    let mut store = RedbBTree::<i32, i32>::new(arc_db.clone(), "test-get", table_def);

    store
      .insert(1, 100)
      .await
      .expect("insert initial value into store");

    let mut tx = store.transaction().await.expect("start transaction");
    tx.remove(&1).await.expect("remove failed");

    assert_eq!(tx.get(&1).await.expect("get failed"), None);

    drop(arc_db);
    let _ = std::fs::remove_file(path);
  });
}

#[test]
fn transaction_rollback_discards_changes() {
  block_on(async {
    let (db, path) = create_temp_db();
    let arc_db = Arc::new(db);

    let static_name: &'static str =
      Box::leak("aicacia_btree_rollback_tx".to_string().into_boxed_str());
    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_name);

    let mut store = RedbBTree::<i32, i32>::new(arc_db.clone(), "test-rollback", table_def);

    store
      .insert(1, 100)
      .await
      .expect("insert initial value into store");

    let mut tx = store.transaction().await.expect("start transaction");
    tx.insert(2, 200)
      .await
      .expect("insert value in transaction");
    tx.remove(&1).await.expect("remove failed");
    tx.rollback().await.expect("rollback transaction");

    assert_eq!(store.get(&1).await.expect("get failed"), Some(100));
    assert_eq!(store.get(&2).await.expect("get failed"), None);

    drop(arc_db);
    let _ = std::fs::remove_file(path);
  });
}

#[test]
fn transaction_remove_pending_insert_returns_old_value() {
  block_on(async {
    let (db, path) = create_temp_db();
    let arc_db = Arc::new(db);

    let static_name: &'static str = Box::leak(
      "aicacia_btree_remove_pending_tx"
        .to_string()
        .into_boxed_str(),
    );
    let table_def = TableDefinition::<&'static [u8], &'static [u8]>::new(static_name);

    let store = RedbBTree::<i32, i32>::new(arc_db.clone(), "test-remove-pending", table_def);

    let mut tx = store.transaction().await.expect("start tx");
    tx.insert(1, 100).await.expect("insert in tx");

    assert_eq!(tx.remove(&1).await.expect("remove failed"), Some(100));
    assert_eq!(tx.get(&1).await.expect("get failed"), None);

    tx.commit().await.expect("commit failed");
    assert_eq!(store.get(&1).await.expect("get failed"), None);

    drop(arc_db);
    let _ = std::fs::remove_file(path);
  });
}
