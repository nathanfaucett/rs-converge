#![cfg(feature = "redb")]

use db_core::block_on;
use db_engine::EngineValue;
use db_facade::Database;

#[test]
fn open_in_redb_roundtrips() {
  block_on(async {
    let path = std::env::temp_dir().join(format!(
      "aicacia_db_facade_open_in_redb_{}.db",
      std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos()
    ));

    let mut db = Database::open_in_redb(&path).await.expect("open db");
    db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
      .await
      .expect("create table");
    db.execute_sql(
      "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'Alice');",
    )
    .await
    .expect("insert row");

    let result = db
      .execute_sql("SELECT id, name FROM users;")
      .await
      .expect("select");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], EngineValue::Text("Alice".into()));

    let _ = std::fs::remove_file(path);
  });
}
