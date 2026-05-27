#[cfg(feature = "std")]
mod tests {
  use db::{Database, SqlParams};
  use db_engine::{EngineError, EngineQuery, EngineResult, Subscriber};
  use futures::executor::block_on;
  use std::sync::{Arc, Mutex};

  #[derive(Clone)]
  struct CountingSubscriber {
    call_count: Arc<Mutex<usize>>,
    last_row_count: Arc<Mutex<usize>>,
  }

  impl CountingSubscriber {
    fn new() -> Self {
      Self {
        call_count: Arc::new(Mutex::new(0)),
        last_row_count: Arc::new(Mutex::new(0)),
      }
    }

    fn get_call_count(&self) -> usize {
      *self.call_count.lock().unwrap()
    }

    fn get_last_row_count(&self) -> usize {
      *self.last_row_count.lock().unwrap()
    }
  }

  impl Subscriber for CountingSubscriber {
    fn on_results(&self, result: Result<EngineResult, EngineError>) {
      *self.call_count.lock().unwrap() += 1;
      if let Ok(results) = result {
        *self.last_row_count.lock().unwrap() = results.rows.len();
      }
    }
  }

  #[test]
  fn test_facade_subscription() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE events (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let query = EngineQuery::select_simple("events".to_string(), vec![0, 1], None);

      // Subscribe through facade
      let _id = db
        .subscribe_query(query, subscriber_arc.clone())
        .await
        .expect("subscribe via facade");

      // Should have initial callback
      assert_eq!(subscriber.get_call_count(), 1);
      assert_eq!(subscriber.get_last_row_count(), 0);

      db.execute_sql(
        "INSERT INTO events (id, name) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'created');",
      )
      .await
      .expect("insert event");

      assert_eq!(subscriber.get_call_count(), 2);
      assert_eq!(subscriber.get_last_row_count(), 1);

      println!("✓ Facade layer subscriptions work");
    });
  }

  #[test]
  fn test_facade_subscription_batches_multi_row_update() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE events (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      db.execute_sql(
        "INSERT INTO events (id, name) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'one');",
      )
      .await
      .expect("insert first row");
      db.execute_sql(
        "INSERT INTO events (id, name) VALUES ('00000000-0000-0000-0000-000000000002'::uuid, 'two');",
      )
      .await
      .expect("insert second row");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let query = EngineQuery::select_simple("events".to_string(), vec![0, 1], None);
      db.subscribe_query(query, subscriber_arc.clone())
        .await
        .expect("subscribe via facade");

      assert_eq!(subscriber.get_call_count(), 1);
      assert_eq!(subscriber.get_last_row_count(), 2);

      db.execute_sql("UPDATE events SET name = 'updated';")
        .await
        .expect("update both rows in one commit");

      assert_eq!(subscriber.get_call_count(), 2);
      assert_eq!(subscriber.get_last_row_count(), 2);

      println!("✓ Facade subscription batching works");
    });
  }

  #[test]
  fn test_facade_subscription_with_scope() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE data (id UUID PRIMARY KEY, value TEXT);")
        .await
        .expect("create table");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let query = EngineQuery::select_simple("data".to_string(), vec![0, 1], None);

      // Subscribe with explicit scope
      let _id = db
        .subscribe_query(query, subscriber_arc.clone())
        .await
        .expect("subscribe with scope");

      assert_eq!(subscriber.get_call_count(), 1);

      println!("✓ Facade subscriptions with scope work");
    });
  }

  #[test]
  fn test_facade_subscribe_sql() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE orders (id UUID PRIMARY KEY, total INT);")
        .await
        .expect("create table");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      // Subscribe via SQL string
      let _id = db
        .subscribe_sql("SELECT id, total FROM orders", subscriber_arc.clone())
        .await
        .expect("subscribe_sql");

      // Should have initial (empty) callback
      assert_eq!(subscriber.get_call_count(), 1);
      assert_eq!(subscriber.get_last_row_count(), 0);

      println!("✓ subscribe_sql on facade works");
    });
  }

  #[test]
  fn test_facade_subscribe_sql_rejects_ddl() {
    block_on(async {
      let db = Database::open_in_memory().await.expect("open db");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let result = db
        .subscribe_sql(
          "CREATE TABLE should_fail (id UUID PRIMARY KEY)",
          subscriber_arc,
        )
        .await;

      assert!(result.is_err(), "DDL should be rejected");
      println!("✓ subscribe_sql correctly rejects DDL");
    });
  }

  #[test]
  fn test_facade_subscribe_sql_with_params() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open db");

      db.execute_sql("CREATE TABLE orders (id UUID PRIMARY KEY, total INT);")
        .await
        .expect("create table");

      let subscriber = CountingSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());
      let params = SqlParams::from(vec![db_engine::EngineValue::Integer(50)]);

      let _id = db
        .subscribe_sql_with_params(
          "SELECT id, total FROM orders WHERE total >= $1",
          &params,
          subscriber_arc,
        )
        .await
        .expect("subscribe_sql_with_params");

      assert_eq!(subscriber.get_call_count(), 1);
      assert_eq!(subscriber.get_last_row_count(), 0);
    });
  }
}
