/// Example: Query to Type - Typed Query Results
///
/// This example demonstrates how to use the FromRow trait to deserialize
/// query results into strongly-typed Rust structs.
///
/// Run with: cargo run --example typed_queries --features automerge
#[cfg(feature = "automerge")]
use db::Database;
#[cfg(feature = "automerge")]
use futures::executor::block_on;
use serde::Deserialize;

/// Define a struct that matches the schema of your table
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct User {
  id: Vec<u8>, // UUID is stored as bytes
  name: String,
  email: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Product {
  id: Vec<u8>, // UUID
  name: String,
  price: f64,
}

#[cfg(feature = "automerge")]
fn main() {
  block_on(async {
    // Create an in-memory database
    let mut db = Database::open_automerge_in_memory()
      .await
      .expect("Failed to open database");

    println!("=== Typed Query Example ===\n");

    // Create tables
    println!("Creating tables...");
    db.execute_sql("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, email TEXT);")
      .await
      .expect("Failed to create users table");

    db.execute_sql("CREATE TABLE products (id UUID PRIMARY KEY, name TEXT, price FLOAT);")
      .await
      .expect("Failed to create products table");

    // Insert some data
    println!("Inserting data...");
    db.execute_sql(
      "INSERT INTO users (id, name, email) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Alice', 'alice@example.com');",
    )
    .await
    .expect("Failed to insert user 1");

    db.execute_sql(
      "INSERT INTO users (id, name, email) VALUES ('00000000-0000-0000-0000-000000000002'::uuid, 'Bob', 'bob@example.com');",
    )
    .await
    .expect("Failed to insert user 2");

    db.execute_sql(
      "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000003'::uuid, 'Charlie');",
    )
    .await
    .expect("Failed to insert user 3");

    db.execute_sql(
      "INSERT INTO products (id, name, price) VALUES ('10000000-0000-0000-0000-000000000001'::uuid, 'Laptop', 999.99);",
    )
    .await
    .expect("Failed to insert product 1");

    db.execute_sql(
      "INSERT INTO products (id, name, price) VALUES ('10000000-0000-0000-0000-000000000002'::uuid, 'Mouse', 29.99);",
    )
    .await
    .expect("Failed to insert product 2");

    db.execute_sql(
      "INSERT INTO products (id, name, price) VALUES ('10000000-0000-0000-0000-000000000003'::uuid, 'Keyboard', 79.99);",
    )
    .await
    .expect("Failed to insert product 3");

    // Query and deserialize into typed structs
    println!("\n--- Users ---");
    let users_result = db
      .execute_sql("SELECT id, name, email FROM users;")
      .await
      .expect("Failed to query users");

    let users: Vec<User> = users_result
      .into_typed_named::<User>()
      .expect("Failed to deserialize users");

    for user in users {
      let email = user.email.unwrap_or_else(|| "N/A".to_string());
      println!("  {} - {}", user.name, email);
    }

    // Query products with typed deserialization
    println!("\n--- Products ---");
    let products_result = db
      .execute_sql("SELECT id, name, price FROM products;")
      .await
      .expect("Failed to query products");

    let products: Vec<Product> = products_result
      .into_typed_named::<Product>()
      .expect("Failed to deserialize products");

    for product in products {
      println!("  {} - ${:.2}", product.name, product.price);
    }

    println!("\n=== Example Complete ===");
  });
}
#[cfg(not(feature = "automerge"))]
fn main() {
  eprintln!("Enable the `automerge` feature to run this example.");
}
