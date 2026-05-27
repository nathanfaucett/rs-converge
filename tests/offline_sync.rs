#[cfg(feature = "automerge")]
mod tests {
  use db::{Database, automerge_layout_metrics, sync_automerge_layouts};
  use db_core::NamedBTreeMap;
  use db_engine::EngineStoreBackend;
  use db_engine::{
    EngineKey, EngineQuery, EngineValue, QualifiedColumn, QualifiedOperand, QualifiedPredicate,
    UpdateAssignment,
  };
  use futures::executor::block_on;

  #[cfg(feature = "redb")]
  use std::fs;
  #[cfg(feature = "redb")]
  use std::path::PathBuf;
  #[cfg(feature = "redb")]
  use std::sync::atomic::{AtomicU64, Ordering};
  #[cfg(feature = "redb")]
  use std::time::{SystemTime, UNIX_EPOCH};

  fn uuid_value(id: u128) -> EngineValue {
    EngineValue::Uuid(id.to_be_bytes())
  }

  fn uuid_lit(id: u128) -> String {
    let hex = format!("{id:032x}");
    format!(
      "'{}-{}-{}-{}-{}'::uuid",
      &hex[0..8],
      &hex[8..12],
      &hex[12..16],
      &hex[16..20],
      &hex[20..32],
    )
  }

  #[cfg(feature = "redb")]
  fn temp_redb_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system time after unix epoch")
      .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    path.push(format!(
      "aicacia_automerge_sync_{}_{}_{}.db",
      label, nanos, id
    ));
    path
  }

  fn expected_user_rows() -> Vec<Vec<EngineValue>> {
    vec![
      vec![uuid_value(1), EngineValue::Text("Alice".into())],
      vec![uuid_value(2), EngineValue::Text("Bob".into())],
    ]
  }

  trait SyncStore:
    Clone
    + NamedBTreeMap<EngineKey, Vec<u8>>
    + EngineStoreBackend<EngineKey, Vec<u8>>
    + Send
    + Sync
    + 'static
  {
  }

  impl<T> SyncStore for T where
    T: Clone
      + NamedBTreeMap<EngineKey, Vec<u8>>
      + EngineStoreBackend<EngineKey, Vec<u8>>
      + Send
      + Sync
      + 'static
  {
  }

  fn sort_rows(rows: &mut [Vec<EngineValue>]) {
    rows.sort_by(|l, r| format!("{:?}", l).cmp(&format!("{:?}", r)));
  }

  async fn select_sorted<S>(db: &mut Database<S>, sql: &str) -> Vec<Vec<EngineValue>>
  where
    S: SyncStore,
  {
    let mut rows = db.execute_sql(sql).await.expect("select rows").rows;
    sort_rows(&mut rows);
    rows
  }

  async fn assert_query_rows_match<S>(
    left: &mut Database<S>,
    right: &mut Database<S>,
    sql: &str,
    expected: Vec<Vec<EngineValue>>,
  ) where
    S: SyncStore,
  {
    let left_rows = select_sorted(left, sql).await;
    let right_rows = select_sorted(right, sql).await;

    assert_eq!(left_rows, expected);
    assert_eq!(right_rows, expected);
    assert_eq!(left_rows, right_rows);
  }

  async fn assert_user_rows_match<S>(left: &mut Database<S>, right: &mut Database<S>)
  where
    S: SyncStore,
  {
    assert_query_rows_match(
      left,
      right,
      "SELECT id, name FROM users;",
      expected_user_rows(),
    )
    .await;
  }

  macro_rules! sync_layout_peers {
    ($left:expr, $right:expr) => {{
      sync_automerge_layouts($left.automerge_layout(), $right.automerge_layout())
        .await
        .expect("layout sync");
      $left.reload_schema().await.expect("reload left schema");
      $right.reload_schema().await.expect("reload right schema");
    }};
  }

  fn eq_pred(table: &str, column_index: usize, value: EngineValue) -> QualifiedPredicate {
    QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: table.into(),
        column_index,
      }),
      QualifiedOperand::Value(value),
    )
  }

  macro_rules! run_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create users on left");

      sync_layout_peers!($left, $right);

      $right
        .execute_sql(&format!(
          "INSERT INTO users (id, name) VALUES ({}, 'Bob');",
          uuid_lit(2)
        ))
        .await
        .expect("insert bob on right");
      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name) VALUES ({}, 'Alice');",
          uuid_lit(1)
        ))
        .await
        .expect("insert alice on left");

      sync_layout_peers!($left, $right);
      assert_user_rows_match(&mut $left, &mut $right).await;

      sync_layout_peers!($left, $right);
      assert_user_rows_match(&mut $left, &mut $right).await;
    }};
  }

  macro_rules! run_lifecycle_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, level INT);")
        .await
        .expect("create users on left");
      $left
        .execute_sql("CREATE TABLE teams (id UUID PRIMARY KEY, title TEXT);")
        .await
        .expect("create teams on left");

      sync_layout_peers!($left, $right);

      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name, level) VALUES ({}, 'Alice', 10);",
          uuid_lit(1)
        ))
        .await
        .expect("insert alice on left");
      $left
        .execute_sql(&format!(
          "INSERT INTO teams (id, title) VALUES ({}, 'Core');",
          uuid_lit(101)
        ))
        .await
        .expect("insert core team on left");

      $right
        .execute_sql(&format!(
          "INSERT INTO users (id, name, level) VALUES ({}, 'Bob', 20);",
          uuid_lit(2)
        ))
        .await
        .expect("insert bob on right");
      $right
        .execute_sql(&format!(
          "INSERT INTO teams (id, title) VALUES ({}, 'Infra');",
          uuid_lit(102)
        ))
        .await
        .expect("insert infra team on right");

      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, level FROM users;",
        vec![
          vec![
            uuid_value(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(10),
          ],
          vec![
            uuid_value(2),
            EngineValue::Text("Bob".into()),
            EngineValue::Integer(20),
          ],
        ],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![
          vec![uuid_value(101), EngineValue::Text("Core".into())],
          vec![uuid_value(102), EngineValue::Text("Infra".into())],
        ],
      )
      .await;

      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name, level) VALUES ({}, 'Cara', 30);",
          uuid_lit(3)
        ))
        .await
        .expect("insert cara on left");
      $left
        .execute_sql(&format!(
          "INSERT INTO teams (id, title) VALUES ({}, 'Ops');",
          uuid_lit(103)
        ))
        .await
        .expect("insert ops team on left");
      $right
        .execute_sql(&format!(
          "INSERT INTO users (id, name, level) VALUES ({}, 'Dan', 40);",
          uuid_lit(4)
        ))
        .await
        .expect("insert dan on right");

      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, level FROM users;",
        vec![
          vec![
            uuid_value(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(10),
          ],
          vec![
            uuid_value(2),
            EngineValue::Text("Bob".into()),
            EngineValue::Integer(20),
          ],
          vec![
            uuid_value(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(30),
          ],
          vec![
            uuid_value(4),
            EngineValue::Text("Dan".into()),
            EngineValue::Integer(40),
          ],
        ],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![
          vec![uuid_value(101), EngineValue::Text("Core".into())],
          vec![uuid_value(102), EngineValue::Text("Infra".into())],
          vec![uuid_value(103), EngineValue::Text("Ops".into())],
        ],
      )
      .await;

      $right
        .execute_sql("CREATE TABLE profiles (id UUID PRIMARY KEY, alias TEXT, status TEXT);")
        .await
        .expect("create profiles on right");
      sync_layout_peers!($right, $left);

      $left
        .execute_sql(&format!(
          "INSERT INTO profiles (id, alias, status) VALUES ({}, 'Dora', 'active');",
          uuid_lit(7)
        ))
        .await
        .expect("insert dora on left");
      $right
        .execute_sql(&format!(
          "INSERT INTO profiles (id, alias, status) VALUES ({}, 'Eli', 'offline');",
          uuid_lit(8)
        ))
        .await
        .expect("insert eli on right");
      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, alias, status FROM profiles;",
        vec![
          vec![
            uuid_value(7),
            EngineValue::Text("Dora".into()),
            EngineValue::Text("active".into()),
          ],
          vec![
            uuid_value(8),
            EngineValue::Text("Eli".into()),
            EngineValue::Text("offline".into()),
          ],
        ],
      )
      .await;

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT status, id FROM profiles;",
        vec![
          vec![EngineValue::Text("active".into()), uuid_value(7)],
          vec![EngineValue::Text("offline".into()), uuid_value(8)],
        ],
      )
      .await;

      sync_layout_peers!($left, $right);
      sync_layout_peers!($right, $left);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, alias, status FROM profiles;",
        vec![
          vec![
            uuid_value(7),
            EngineValue::Text("Dora".into()),
            EngineValue::Text("active".into()),
          ],
          vec![
            uuid_value(8),
            EngineValue::Text("Eli".into()),
            EngineValue::Text("offline".into()),
          ],
        ],
      )
      .await;
    }};
  }

  macro_rules! run_mutation_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, score INT);")
        .await
        .expect("create users on left");
      sync_layout_peers!($left, $right);

      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name, score) VALUES ({}, 'Alice', 10);",
          uuid_lit(1)
        ))
        .await
        .expect("insert alice on left");
      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name, score) VALUES ({}, 'Bob', 20);",
          uuid_lit(2)
        ))
        .await
        .expect("insert bob on left");
      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name, score) VALUES ({}, 'Cara', 30);",
          uuid_lit(3)
        ))
        .await
        .expect("insert cara on left");

      sync_layout_peers!($left, $right);

      $left
        .execute_query(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(2, EngineValue::Integer(11))],
          predicate: Some(eq_pred("users", 0, uuid_value(1))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update score for alice on left");

      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            uuid_value(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(11),
          ],
          vec![
            uuid_value(2),
            EngineValue::Text("Bob".into()),
            EngineValue::Integer(20),
          ],
          vec![
            uuid_value(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(30),
          ],
        ],
      )
      .await;

      $right
        .execute_query(EngineQuery::Delete {
          table: "users".into(),
          predicate: Some(eq_pred("users", 0, uuid_value(2))),
          returning: None,
        })
        .await
        .expect("delete bob on right");

      sync_layout_peers!($right, $left);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            uuid_value(1),
            EngineValue::Text("Alice".into()),
            EngineValue::Integer(11),
          ],
          vec![
            uuid_value(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(30),
          ],
        ],
      )
      .await;

      $left
        .execute_query(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(
            1,
            EngineValue::Text("Alice L".into()),
          )],
          predicate: Some(eq_pred("users", 0, uuid_value(1))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update alice name on left");

      $right
        .execute_query(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(2, EngineValue::Integer(31))],
          predicate: Some(eq_pred("users", 0, uuid_value(3))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update cara score on right");

      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            uuid_value(1),
            EngineValue::Text("Alice L".into()),
            EngineValue::Integer(11),
          ],
          vec![
            uuid_value(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(31),
          ],
        ],
      )
      .await;

      sync_layout_peers!($left, $right);
      sync_layout_peers!($right, $left);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name, score FROM users;",
        vec![
          vec![
            uuid_value(1),
            EngineValue::Text("Alice L".into()),
            EngineValue::Integer(11),
          ],
          vec![
            uuid_value(3),
            EngineValue::Text("Cara".into()),
            EngineValue::Integer(31),
          ],
        ],
      )
      .await;
    }};
  }

  macro_rules! run_efficiency_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create users on left");

      for i in 1..=8 {
        $left
          .execute_sql(&format!(
            "INSERT INTO users (id, name) VALUES ({}, 'user_{i}');",
            uuid_lit(i as u128),
          ))
          .await
          .expect("insert seed user on left");
      }

      sync_layout_peers!($left, $right);

      let left_before = automerge_layout_metrics($left.automerge_layout())
        .await
        .expect("layout metrics");
      let right_before = automerge_layout_metrics($right.automerge_layout())
        .await
        .expect("layout metrics");

      sync_layout_peers!($left, $right);
      sync_layout_peers!($right, $left);

      let left_after = automerge_layout_metrics($left.automerge_layout())
        .await
        .expect("layout metrics");
      let right_after = automerge_layout_metrics($right.automerge_layout())
        .await
        .expect("layout metrics");

      assert_eq!(left_after.document_count, left_before.document_count);
      assert_eq!(right_after.document_count, right_before.document_count);
      assert!(left_after.total_document_bytes <= left_before.total_document_bytes + 128);
      assert!(right_after.total_document_bytes <= right_before.total_document_bytes + 128);

      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name) VALUES ({}, 'new_user');",
          uuid_lit(100)
        ))
        .await
        .expect("insert one delta row on left");
      sync_layout_peers!($left, $right);

      let left_delta = automerge_layout_metrics($left.automerge_layout())
        .await
        .expect("layout metrics");
      let right_delta = automerge_layout_metrics($right.automerge_layout())
        .await
        .expect("layout metrics");

      assert!(left_delta.document_count >= left_after.document_count);
      assert!(right_delta.document_count >= right_after.document_count);
      assert!(left_delta.total_document_bytes >= left_after.total_document_bytes);
      assert!(right_delta.total_document_bytes >= right_after.total_document_bytes);
    }};
  }

  macro_rules! run_tree_isolation_sync_scenario {
    ($left:expr, $right:expr) => {{
      $left
        .execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create users on left");
      $left
        .execute_sql("CREATE TABLE teams (id UUID PRIMARY KEY, title TEXT);")
        .await
        .expect("create teams on left");

      sync_layout_peers!($left, $right);

      $left
        .execute_sql(&format!(
          "INSERT INTO users (id, name) VALUES ({}, 'Alice');",
          uuid_lit(1)
        ))
        .await
        .expect("insert users row");
      $left
        .execute_sql(&format!(
          "INSERT INTO teams (id, title) VALUES ({}, 'Core');",
          uuid_lit(1)
        ))
        .await
        .expect("insert teams row");

      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name FROM users;",
        vec![vec![uuid_value(1), EngineValue::Text("Alice".into())]],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![vec![uuid_value(1), EngineValue::Text("Core".into())]],
      )
      .await;

      $left
        .execute_sql(&format!(
          "UPDATE users SET name = 'Alice v2' WHERE id = {};",
          uuid_lit(1)
        ))
        .await
        .expect("update users row only");

      sync_layout_peers!($left, $right);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name FROM users;",
        vec![vec![uuid_value(1), EngineValue::Text("Alice v2".into())]],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![vec![uuid_value(1), EngineValue::Text("Core".into())]],
      )
      .await;

      $right
        .execute_sql(&format!("DELETE FROM users WHERE id = {};", uuid_lit(1)))
        .await
        .expect("delete users row only");

      sync_layout_peers!($right, $left);

      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, name FROM users;",
        vec![],
      )
      .await;
      assert_query_rows_match(
        &mut $left,
        &mut $right,
        "SELECT id, title FROM teams;",
        vec![vec![uuid_value(1), EngineValue::Text("Core".into())]],
      )
      .await;
    }};
  }

  async fn offline_sync_in_memory_scenario() {
    let mut left = Database::open_automerge_in_memory()
      .await
      .expect("open left in-memory db");
    let mut right = Database::open_automerge_in_memory()
      .await
      .expect("open right in-memory db");

    run_sync_scenario!(left, right);
  }

  #[test]
  fn automerge_in_memory_offline_sync_converges() {
    block_on(async {
      offline_sync_in_memory_scenario().await;
    });
  }

  #[test]
  fn automerge_in_memory_offline_sync_lifecycle_converges() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_lifecycle_sync_scenario!(left, right);
    });
  }

  #[test]
  fn automerge_in_memory_offline_sync_mutations_converge() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_mutation_sync_scenario!(left, right);
    });
  }

  #[cfg(feature = "redb")]
  async fn offline_sync_redb_scenario() {
    let left_path = temp_redb_path("left");
    let right_path = temp_redb_path("right");

    let mut left = Database::open_automerge_with_redb(&left_path)
      .await
      .expect("open left redb db");
    let mut right = Database::open_automerge_with_redb(&right_path)
      .await
      .expect("open right redb db");

    run_sync_scenario!(left, right);

    let _ = fs::remove_file(left_path);
    let _ = fs::remove_file(right_path);
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_converges() {
    block_on(async {
      offline_sync_redb_scenario().await;
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_lifecycle_converges() {
    block_on(async {
      let left_path = temp_redb_path("left_lifecycle");
      let right_path = temp_redb_path("right_lifecycle");

      let mut left = Database::open_automerge_with_redb(&left_path)
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path)
        .await
        .expect("open right redb db");

      run_lifecycle_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_mutations_converge() {
    block_on(async {
      let left_path = temp_redb_path("left_mutations");
      let right_path = temp_redb_path("right_mutations");

      let mut left = Database::open_automerge_with_redb(&left_path)
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path)
        .await
        .expect("open right redb db");

      run_mutation_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }

  #[test]
  fn automerge_in_memory_offline_sync_noop_is_data_efficient() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_efficiency_sync_scenario!(left, right);
    });
  }

  #[test]
  fn automerge_in_memory_tree_isolation_same_key() {
    block_on(async {
      let mut left = Database::open_automerge_in_memory()
        .await
        .expect("open left in-memory db");
      let mut right = Database::open_automerge_in_memory()
        .await
        .expect("open right in-memory db");

      run_tree_isolation_sync_scenario!(left, right);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_offline_sync_noop_is_data_efficient() {
    block_on(async {
      let left_path = temp_redb_path("left_efficiency");
      let right_path = temp_redb_path("right_efficiency");

      let mut left = Database::open_automerge_with_redb(&left_path)
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path)
        .await
        .expect("open right redb db");

      run_efficiency_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_tree_isolation_same_key() {
    block_on(async {
      let left_path = temp_redb_path("left_isolation");
      let right_path = temp_redb_path("right_isolation");

      let mut left = Database::open_automerge_with_redb(&left_path)
        .await
        .expect("open left redb db");
      let mut right = Database::open_automerge_with_redb(&right_path)
        .await
        .expect("open right redb db");

      run_tree_isolation_sync_scenario!(left, right);

      let _ = fs::remove_file(left_path);
      let _ = fs::remove_file(right_path);
    });
  }
}
