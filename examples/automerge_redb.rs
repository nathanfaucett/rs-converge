// Example: register two tables, insert rows, and run a SQL JOIN select.
// Uses Automerge document encoding on a redb named-tree layout backend when
// built with the `automerge` and `redb` features.
#[cfg(all(feature = "automerge", feature = "redb"))]
use db::Database;
#[cfg(all(feature = "automerge", feature = "redb"))]
use futures::executor::block_on;

fn main() {
  #[cfg(all(feature = "automerge", feature = "redb"))]
  block_on(async {
    use std::time;

    let mut path = std::env::temp_dir();
    path.push(format!(
      "aicacia_automerge_redb_{}.db",
      time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs()
    ));

    let mut db = Database::open_automerge_with_redb(path)
      .await
      .expect("open automerge redb");

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
