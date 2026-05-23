// Example: store and retrieve JSON column values via SQL
//
// Run with: cargo run --example json --features automerge
#[cfg(feature = "automerge")]
use db::Database;
#[cfg(feature = "automerge")]
use db_engine::EngineValue;
#[cfg(feature = "automerge")]
use futures::executor::block_on;

#[cfg(feature = "automerge")]
fn main() {
  block_on(async {
    let mut db = Database::open_automerge_in_memory()
      .await
      .expect("open in-memory db");

    db.execute_sql("CREATE TABLE settings (id UUID PRIMARY KEY, name TEXT, config JSON);")
      .await
      .expect("create settings");

    db.execute_sql(
      r#"INSERT INTO settings VALUES (
        '00000000-0000-0000-0000-000000000001'::uuid,
        'alice',
        '{"theme":"dark","notifications":true}'
      );"#,
    )
    .await
    .expect("insert alice");

    db.execute_sql(
      r#"INSERT INTO settings VALUES (
        '00000000-0000-0000-0000-000000000002'::uuid,
        'bob',
        '{"theme":"light","notifications":false}'
      );"#,
    )
    .await
    .expect("insert bob");

    let result = db
      .execute_sql("SELECT name, config FROM settings;")
      .await
      .expect("select settings");

    assert_eq!(result.rows.len(), 2);

    for row in &result.rows {
      if let (Some(EngineValue::Text(name)), Some(EngineValue::Json(config))) =
        (row.first(), row.get(1))
      {
        println!("{name}: {config}");
      }
    }

    // Update alice's config
    db.execute_sql(
      r#"UPDATE settings SET config = '{"theme":"solarized","notifications":true}' WHERE name = 'alice';"#,
    )
    .await
    .expect("update alice");

    let updated = db
      .execute_sql("SELECT config FROM settings WHERE name = 'alice';")
      .await
      .expect("select alice config");

    assert_eq!(updated.rows.len(), 1);
    if let Some(EngineValue::Json(config)) = updated.rows[0].first() {
      assert!(config.contains("solarized"));
      println!("alice updated: {config}");
    }
  });
}
#[cfg(not(feature = "automerge"))]
fn main() {
  eprintln!("Enable the `automerge` feature to run this example.");
}
