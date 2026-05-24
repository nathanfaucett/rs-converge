#[cfg(feature = "automerge")]
mod tests {
  use db::{Database, SqlParams};
  use db_engine::EngineValue;
  use futures::executor::block_on;

  const U1: &str = "'00000000-0000-0000-0000-000000000001'::uuid";
  const U2: &str = "'00000000-0000-0000-0000-000000000002'::uuid";
  const U10: &str = "'00000000-0000-0000-0000-00000000000a'::uuid";
  const U20: &str = "'00000000-0000-0000-0000-000000000014'::uuid";

  fn uuid_value(id: u128) -> EngineValue {
    EngineValue::Uuid(id.to_be_bytes())
  }

  #[cfg(feature = "redb")]
  use std::fs;
  #[cfg(feature = "redb")]
  use std::path::PathBuf;
  #[cfg(feature = "redb")]
  use std::sync::atomic::{AtomicU64, Ordering};
  #[cfg(feature = "redb")]
  use std::time::{SystemTime, UNIX_EPOCH};

  #[cfg(feature = "redb")]
  fn temp_redb_path(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system time after unix epoch")
      .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    path.push(format!("aicacia_facade_sql_{}_{}_{}.db", label, nanos, id));
    path
  }

  #[test]
  fn automerge_two_create_tables() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id UUID PRIMARY KEY, user_id UUID);")
        .await
        .expect("create orders");
    });
  }

  #[test]
  fn automerge_in_memory_join_returns_two_rows() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id UUID PRIMARY KEY, user_id UUID, amount INT);")
        .await
        .expect("create orders");

      db.execute_sql(&format!(
        "INSERT INTO users (id, name) VALUES ({U1}, 'Alice');"
      ))
      .await
      .expect("insert user 1");
      db.execute_sql(&format!(
        "INSERT INTO users (id, name) VALUES ({U2}, 'Bob');"
      ))
      .await
      .expect("insert user 2");

      db.execute_sql(&format!(
        "INSERT INTO orders (id, user_id, amount) VALUES ({U10},{U1},100);"
      ))
      .await
      .expect("insert order 1");
      db.execute_sql(&format!(
        "INSERT INTO orders (id, user_id, amount) VALUES ({U20},{U2},200);"
      ))
      .await
      .expect("insert order 2");

      let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
      let res = db.execute_sql(sql).await.expect("execute select");
      assert_eq!(res.rows.len(), 2);
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Alice".into()),
        EngineValue::Integer(100),
      ]));
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Bob".into()),
        EngineValue::Integer(200),
      ]));
    });
  }

  #[test]
  fn automerge_in_memory_empty_table_select_returns_no_rows() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      let result = db
        .execute_sql("SELECT id, score FROM users;")
        .await
        .expect("select empty users");

      assert!(result.rows.is_empty());
    });
  }

  #[test]
  fn automerge_in_memory_update_delete_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!(
        "INSERT INTO users (id, name, score) VALUES ({U1}, 'Alice', 10);"
      ))
      .await
      .expect("insert alice");
      db.execute_sql(&format!(
        "INSERT INTO users (id, name, score) VALUES ({U2}, 'Bob', 20);"
      ))
      .await
      .expect("insert bob");

      db.execute_sql(&format!("UPDATE users SET score = 11 WHERE id = {U1};"))
        .await
        .expect("update alice score");
      db.execute_sql(&format!("DELETE FROM users WHERE id = {U2};"))
        .await
        .expect("delete bob");

      let result = db
        .execute_sql("SELECT id, name, score FROM users;")
        .await
        .expect("select users");

      assert_eq!(result.rows.len(), 1);
      assert_eq!(
        result.rows[0],
        vec![
          uuid_value(1),
          EngineValue::Text("Alice".into()),
          EngineValue::Integer(11),
        ]
      );
    });
  }

  #[test]
  fn automerge_in_memory_update_join_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, team_id UUID, score INT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE teams (id UUID PRIMARY KEY, bonus INT);")
        .await
        .expect("create teams");

      db.execute_sql(&format!(
        "INSERT INTO users (id, team_id, score) VALUES ({U1}, {U10}, 5);"
      ))
      .await
      .expect("insert user 1");
      db.execute_sql(&format!(
        "INSERT INTO users (id, team_id, score) VALUES ({U2}, {U20}, 7);"
      ))
      .await
      .expect("insert user 2");
      db.execute_sql(&format!("INSERT INTO teams (id, bonus) VALUES ({U10}, 3);"))
        .await
        .expect("insert team 10");
      db.execute_sql(&format!("INSERT INTO teams (id, bonus) VALUES ({U20}, 4);"))
        .await
        .expect("insert team 20");

      db.execute_sql(
        &format!("UPDATE users u JOIN teams t ON u.team_id = t.id SET score = score + t.bonus WHERE u.id = {U1};"),
      )
      .await
      .expect("update joined user score");

      let result = db
        .execute_sql("SELECT id, score FROM users;")
        .await
        .expect("select users");

      assert_eq!(result.rows.len(), 2);
      assert!(
        result
          .rows
          .contains(&vec![uuid_value(1), EngineValue::Integer(8),])
      );
      assert!(
        result
          .rows
          .contains(&vec![uuid_value(2), EngineValue::Integer(7),])
      );
    });
  }

  #[test]
  fn automerge_in_memory_update_from_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, team_id UUID, score INT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE teams (id UUID PRIMARY KEY, bonus INT);")
        .await
        .expect("create teams");

      db.execute_sql(&format!(
        "INSERT INTO users (id, team_id, score) VALUES ({U1}, {U10}, 5);"
      ))
      .await
      .expect("insert user 1");
      db.execute_sql(&format!(
        "INSERT INTO users (id, team_id, score) VALUES ({U2}, {U20}, 7);"
      ))
      .await
      .expect("insert user 2");
      db.execute_sql(&format!("INSERT INTO teams (id, bonus) VALUES ({U10}, 3);"))
        .await
        .expect("insert team 10");
      db.execute_sql(&format!("INSERT INTO teams (id, bonus) VALUES ({U20}, 4);"))
        .await
        .expect("insert team 20");

      db.execute_sql(
        &format!("UPDATE users SET score = score + teams.bonus FROM teams WHERE users.team_id = teams.id AND users.id = {U1};"),
      )
      .await
      .expect("update from user score");

      let result = db
        .execute_sql("SELECT id, score FROM users;")
        .await
        .expect("select users");

      assert_eq!(result.rows.len(), 2);
      assert!(
        result
          .rows
          .contains(&vec![uuid_value(1), EngineValue::Integer(8),])
      );
      assert!(
        result
          .rows
          .contains(&vec![uuid_value(2), EngineValue::Integer(7),])
      );
    });
  }

  #[test]
  fn automerge_in_memory_update_returning_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!("INSERT INTO users (id, score) VALUES ({U1}, 10);"))
        .await
        .expect("insert user");

      let result = db
        .execute_sql(&format!(
          "UPDATE users SET score = score + 2 WHERE id = {U1} RETURNING id, score;"
        ))
        .await
        .expect("update returning");

      assert_eq!(
        result.rows,
        vec![vec![uuid_value(1), EngineValue::Integer(12)]],
      );
    });
  }

  #[test]
  fn automerge_in_memory_insert_returning_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      let result = db
        .execute_sql(&format!(
          "INSERT INTO users (id, score) VALUES ({U1}, 10) RETURNING id, score;"
        ))
        .await
        .expect("insert returning");

      assert_eq!(
        result.rows,
        vec![vec![uuid_value(1), EngineValue::Integer(10)]],
      );
    });
  }

  #[test]
  fn automerge_in_memory_insert_returning_expression_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      let result = db
        .execute_sql(&format!(
          "INSERT INTO users (id, score) VALUES ({U1}, 10) RETURNING id, score + 2;"
        ))
        .await
        .expect("insert returning expression");

      assert_eq!(
        result.rows,
        vec![vec![uuid_value(1), EngineValue::Integer(12)]],
      );
    });
  }

  #[test]
  fn automerge_in_memory_delete_returning_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!("INSERT INTO users (id, score) VALUES ({U1}, 10);"))
        .await
        .expect("insert user");

      let result = db
        .execute_sql(&format!(
          "DELETE FROM users WHERE id = {U1} RETURNING id, score;"
        ))
        .await
        .expect("delete returning");

      assert_eq!(
        result.rows,
        vec![vec![uuid_value(1), EngineValue::Integer(10)]],
      );

      let remaining = db
        .execute_sql(&format!("SELECT id, score FROM users WHERE id = {U1};"))
        .await
        .expect("select users");
      assert!(remaining.rows.is_empty());
    });
  }

  #[test]
  fn automerge_in_memory_update_returning_expression_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!("INSERT INTO users (id, score) VALUES ({U1}, 10);"))
        .await
        .expect("insert user");

      let result = db
        .execute_sql(&format!(
          "UPDATE users SET score = score + 2 WHERE id = {U1} RETURNING score * 2;"
        ))
        .await
        .expect("update returning expression");

      assert_eq!(result.rows, vec![vec![EngineValue::Integer(24)]]);
    });
  }

  #[test]
  fn automerge_in_memory_delete_returning_expression_sql_roundtrip() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!("INSERT INTO users (id, score) VALUES ({U1}, 10);"))
        .await
        .expect("insert user");

      let result = db
        .execute_sql(&format!(
          "DELETE FROM users WHERE id = {U1} RETURNING score + 1;"
        ))
        .await
        .expect("delete returning expression");

      assert_eq!(result.rows, vec![vec![EngineValue::Integer(11)]]);
    });
  }

  #[test]
  fn automerge_in_memory_execute_sql_with_named_params() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!("INSERT INTO users (id, score) VALUES ({U1}, 10);"))
        .await
        .expect("insert user");

      let params = SqlParams::named([("user_id", uuid_value(1))]);
      let result = db
        .execute_sql_with_params("SELECT id, score FROM users WHERE id = :user_id;", &params)
        .await
        .expect("select with named params");

      assert_eq!(
        result.rows,
        vec![vec![uuid_value(1), EngineValue::Integer(10)]]
      );
    });
  }

  #[test]
  fn automerge_in_memory_execute_sql_with_positional_params() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, score INT);")
        .await
        .expect("create users");

      db.execute_sql(&format!("INSERT INTO users (id, score) VALUES ({U1}, 10);"))
        .await
        .expect("insert user");

      let params = SqlParams::from(vec![uuid_value(1)]);
      let result = db
        .execute_sql_with_params("SELECT id, score FROM users WHERE id = $1;", &params)
        .await
        .expect("select with positional params");

      assert_eq!(
        result.rows,
        vec![vec![uuid_value(1), EngineValue::Integer(10)]]
      );
    });
  }

  #[cfg(feature = "redb")]
  #[test]
  fn automerge_redb_join_returns_two_rows() {
    block_on(async {
      let path = temp_redb_path("join");
      let _ = fs::remove_file(&path);

      let mut db = Database::open_automerge_with_redb(&path)
        .await
        .expect("open automerge redb");

      db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create users");
      db.execute_sql("CREATE TABLE orders (id UUID PRIMARY KEY, user_id UUID, amount INT);")
        .await
        .expect("create orders");

      db.execute_sql(&format!(
        "INSERT INTO users (id, name) VALUES ({U1}, 'Alice');"
      ))
      .await
      .expect("insert user 1");
      db.execute_sql(&format!(
        "INSERT INTO users (id, name) VALUES ({U2}, 'Bob');"
      ))
      .await
      .expect("insert user 2");

      db.execute_sql(&format!(
        "INSERT INTO orders (id, user_id, amount) VALUES ({U10},{U1},100);"
      ))
      .await
      .expect("insert order 1");
      db.execute_sql(&format!(
        "INSERT INTO orders (id, user_id, amount) VALUES ({U20},{U2},200);"
      ))
      .await
      .expect("insert order 2");

      let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
      let res = db.execute_sql(sql).await.expect("execute select");
      assert_eq!(res.rows.len(), 2);
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Alice".into()),
        EngineValue::Integer(100),
      ]));
      assert!(res.rows.contains(&vec![
        EngineValue::Text("Bob".into()),
        EngineValue::Integer(200),
      ]));

      let _ = fs::remove_file(path);
    });
  }

  #[test]
  fn automerge_if_exists_not_exists() {
    block_on(async {
      let mut db = Database::open_automerge_in_memory()
        .await
        .expect("open automerge in-memory");

      db.execute_sql("CREATE TABLE IF NOT EXISTS test_table (id UUID PRIMARY KEY);")
        .await
        .expect("create if not exists on missing table");
      db.execute_sql("CREATE TABLE IF NOT EXISTS test_table (id UUID PRIMARY KEY);")
        .await
        .expect("create if not exists on existing table");

      db.execute_sql("CREATE TABLE test_table (id UUID PRIMARY KEY);")
        .await
        .expect_err("plain create should fail when table exists");

      db.execute_sql("DROP TABLE IF EXISTS test_table;")
        .await
        .expect("drop if exists on existing table");
      db.execute_sql("DROP TABLE IF EXISTS test_table;")
        .await
        .expect("drop if exists on missing table");

      db.execute_sql("DROP TABLE test_table;")
        .await
        .expect_err("plain drop should fail when table is missing");
    });
  }
}
