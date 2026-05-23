use db_core::block_on;
use db_facade::{Database, InMemoryEngineStore};

#[test]
fn facade_execute_index_ddl_if_exists_semantics() {
  block_on(async {
    let store = InMemoryEngineStore::new();
    let mut db = Database::from_store(store);

    db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
      .await
      .expect("create table");

    db.execute_sql("CREATE INDEX idx_users_name ON users (name);")
      .await
      .expect("create index");

    // Second create with IF NOT EXISTS should not error.
    db.execute_sql("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name);")
      .await
      .expect("create index if not exists");

    // Drop the index.
    db.execute_sql("DROP INDEX idx_users_name;")
      .await
      .expect("drop index");

    // Second drop with IF EXISTS should not error.
    db.execute_sql("DROP INDEX IF EXISTS idx_users_name;")
      .await
      .expect("drop index if exists");
  });
}
