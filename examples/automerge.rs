// Example: register two tables, insert rows, and run a SQL JOIN select
// Requires the `automerge` feature; this example fails to build otherwise.
#[cfg(feature = "automerge")]
use db::Database;
#[cfg(feature = "automerge")]
use futures::executor::block_on;

#[cfg(feature = "automerge")]
fn main() {
  block_on(async {
    let mut db = Database::open_automerge_in_memory()
      .await
      .expect("open automerge db (in-memory)");

    // Create tables via SQL using the facade.
    db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);")
      .await
      .expect("create users");
    db.execute_sql("CREATE TABLE orders (id UUID PRIMARY KEY, user_id UUID, amount INT);")
      .await
      .expect("create orders");

    // Insert some users via SQL using the facade
    db.execute_sql("INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Alice');")
      .await
      .expect("insert user 1");
    db.execute_sql(
      "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000002'::uuid, 'Bob');",
    )
    .await
    .expect("insert user 2");

    // Insert some orders via SQL using the facade
    db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES ('00000000-0000-0000-0000-000000000001'::uuid,'00000000-0000-0000-0000-000000000001'::uuid,100);")
      .await
      .expect("insert order 1");
    db.execute_sql("INSERT INTO orders (id, user_id, amount) VALUES ('00000000-0000-0000-0000-000000000002'::uuid,'00000000-0000-0000-0000-000000000002'::uuid,200);")
      .await
      .expect("insert order 2");

    let sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;";
    let res = db.execute_sql(sql).await.expect("execute select");

    println!("Joined rows: {}", res.rows.len());
    for row in res.rows {
      println!("row: {:?}", row);
    }
  });
}

#[cfg(not(feature = "automerge"))]
fn main() {
  eprintln!("Enable the `automerge` feature to run this example.");
}
