use core::{
  future::Future,
  pin::pin,
  task::{Context, Poll, Waker},
};
use db_core::{BTree, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor};
use futures::{StreamExt, pin_mut};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

fn block_on<F: Future>(future: F) -> F::Output {
  let waker = Waker::noop();
  let mut context = Context::from_waker(waker);
  let mut future = pin!(future);

  loop {
    match future.as_mut().poll(&mut context) {
      Poll::Ready(output) => return output,
      Poll::Pending => core::hint::spin_loop(),
    }
  }
}

fn redb_contract_path() -> std::path::PathBuf {
  static NEXT_ID: AtomicU64 = AtomicU64::new(0);

  let mut path = std::env::temp_dir();
  let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
  let nanos = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("clock before unix epoch")
    .as_nanos();

  path.push(format!(
    "aicacia_btree_contract_{}_{}_{}.db",
    std::process::id(),
    nanos,
    id
  ));
  path
}

async fn commit_and_rollback_contract<S>(mut store: S)
where
  S: BTree<u64, u64> + BTreeWriteExecutor<u64, u64>,
{
  store.insert(1, 100).await.expect("insert initial value");

  let mut tx = store.transaction().await.expect("start tx");
  tx.insert(2, 200).await.expect("insert in tx");
  let _ = tx.remove(&1).await;
  tx.commit().await.expect("commit");

  assert_eq!(store.get(&1).await.expect("get failed"), None);
  assert_eq!(store.get(&2).await.expect("get failed"), Some(200));
}

async fn transaction_range_merges_contract<S>(mut store: S)
where
  S: BTree<u64, u64> + BTreeWriteExecutor<u64, u64>,
{
  store.insert(1, 100).await.expect("insert initial");
  store.insert(3, 300).await.expect("insert second");

  let mut tx = store.transaction().await.expect("start tx");
  tx.insert(2, 200).await.expect("insert in tx");
  tx.remove(&3).await.expect("remove failed");

  let mut values = Vec::new();
  let stream = tx.range(0..10);
  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    let (k, v) = item.expect("range item failed");
    values.push((k, v));
  }

  assert_eq!(values, Vec::from([(1u64, 100u64), (2u64, 200u64)]));
}

#[test]
fn inmemory_transaction_commit_and_rollback_contract() {
  block_on(async {
    let store = db_in_memory::InMemoryBTree::<u64, u64>::new();
    commit_and_rollback_contract(store).await;
  });
}

#[test]
fn inmemory_transaction_range_merges_contract() {
  block_on(async {
    let store = db_in_memory::InMemoryBTree::<u64, u64>::new();
    transaction_range_merges_contract(store).await;
  });
}

#[test]
fn redb_transaction_commit_and_rollback_contract() {
  block_on(async {
    let path = redb_contract_path();
    let store = db_redb::REDBBTree::<u64, u64>::open(&path, "contract_table").expect("open redb");
    let _ = std::fs::remove_file(&path);
    commit_and_rollback_contract(store).await;

    let _ = std::fs::remove_file(path);
  });
}

async fn read_transaction_observes_committed_state<S>(mut store: S)
where
  S: BTree<u64, u64> + BTreeWriteExecutor<u64, u64>,
{
  store.insert(1, 100).await.expect("insert initial value");

  let tx = store.transaction().await.expect("start read tx");
  assert_eq!(tx.get(&1).await.expect("get value"), Some(100));

  let mut rows = Vec::new();
  let stream = tx.range(0..10);
  pin_mut!(stream);
  while let Some(item) = stream.next().await {
    let (k, v) = item.expect("range item failed");
    rows.push((k, v));
  }

  assert_eq!(rows, vec![(1u64, 100u64)]);
}

#[test]
fn inmemory_read_transaction_observes_committed_state() {
  block_on(async {
    let store = db_in_memory::InMemoryBTree::<u64, u64>::new();
    read_transaction_observes_committed_state(store).await;
  });
}

#[test]
fn redb_read_transaction_observes_committed_state() {
  block_on(async {
    let path = redb_contract_path();
    let store = db_redb::REDBBTree::<u64, u64>::open(&path, "contract_table").expect("open redb");
    let _ = std::fs::remove_file(&path);
    read_transaction_observes_committed_state(store).await;

    let _ = std::fs::remove_file(path);
  });
}

#[test]
fn redb_transaction_range_merges_contract() {
  block_on(async {
    let path = redb_contract_path();
    let store = db_redb::REDBBTree::<u64, u64>::open(&path, "contract_table").expect("open redb");
    let _ = std::fs::remove_file(&path);
    transaction_range_merges_contract(store).await;

    let _ = std::fs::remove_file(path);
  });
}
