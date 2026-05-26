use db_core::block_on;
use db_engine::ColumnSchema;
use db_engine::{EngineQuery, EngineType, EngineValue, TableSchema};
use db_facade::{Database, InMemoryEngineStore, ReadTransaction};

#[test]
fn facade_read_transaction_supports_select_only() {
  block_on(async {
    let store = InMemoryEngineStore::new();
    let mut db = Database::from_store(store);

    db.register_table(
      TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "name".into(),
            data_type: EngineType::Text,
          },
        ],
        primary_key: vec![0],
      },
      false,
    )
    .await
    .expect("register table");

    db.execute_query(EngineQuery::Insert {
      table: "users".into(),
      row: vec![
        EngineValue::Uuid([0; 16]),
        EngineValue::Text("Alice".into()),
      ],
      returning: None,
    })
    .await
    .expect("insert row");

    let tx: ReadTransaction<'_, _> = db.read_transaction();
    let result = tx
      .execute_sql(&db, "SELECT id, name FROM users")
      .await
      .expect("execute select");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], EngineValue::Text("Alice".into()));

    let err = tx
      .execute_sql(
        &db,
        "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000000', 'Bob')",
      )
      .await
      .expect_err("insert in read transaction should fail");

    assert!(format!("{err}").contains("read transaction supports only SELECT statements"));
  });
}
