#[cfg(feature = "std")]
mod tests {
  use db::Database;
  use db_engine::{EngineError, EngineQuery, EngineResult, Subscriber, SyncScope};
  use futures::executor::block_on;
  use std::sync::{Arc, Mutex};

  #[derive(Clone)]
  struct TestSubscriber {
    results: Arc<Mutex<Vec<EngineResult>>>,
    call_count: Arc<Mutex<usize>>,
  }

  impl TestSubscriber {
    fn new() -> Self {
      Self {
        results: Arc::new(Mutex::new(Vec::new())),
        call_count: Arc::new(Mutex::new(0)),
      }
    }

    fn get_results(&self) -> Vec<EngineResult> {
      self.results.lock().unwrap().clone()
    }

    fn get_call_count(&self) -> usize {
      *self.call_count.lock().unwrap()
    }
  }

  impl Subscriber for TestSubscriber {
    fn on_results(&self, result: Result<EngineResult, EngineError>) {
      if let Ok(results) = result {
        self.results.lock().unwrap().push(results);
      }
      *self.call_count.lock().unwrap() += 1;
    }
  }

  #[test]
  fn test_multiple_subscriptions() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open in-memory db");

      db.execute_sql("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      // Create two subscribers
      let sub1 = TestSubscriber::new();
      let sub1_arc = Arc::new(sub1.clone());
      let sub2 = TestSubscriber::new();
      let sub2_arc = Arc::new(sub2.clone());

      // Create query
      let query = EngineQuery::select_simple("items".to_string(), vec![0, 1], None);

      // Subscribe both
      let id1 = db
        .subscribe_query(query.clone(), sub1_arc.clone(), None)
        .await
        .expect("subscribe sub1");
      let id2 = db
        .subscribe_query(query.clone(), sub2_arc.clone(), None)
        .await
        .expect("subscribe sub2");

      // Both should have initial (empty) results
      assert_eq!(sub1.get_call_count(), 1);
      assert_eq!(sub2.get_call_count(), 1);

      // Unsubscribe first one
      db.unsubscribe(id1).await.expect("unsubscribe");

      // Unsubscribe second one
      db.unsubscribe(id2).await.expect("unsubscribe");

      println!("✓ Multiple subscriptions work");
    });
  }

  #[test]
  fn test_subscription_with_restricted_scope() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open in-memory db");

      db.execute_sql("CREATE TABLE products (id UUID PRIMARY KEY, name TEXT);")
        .await
        .expect("create table");

      // Create a subscriber
      let subscriber = TestSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      // Create query with restricted scope
      let query = EngineQuery::select_simple("products".to_string(), vec![0, 1], None);
      let scope = SyncScope::new(vec!["products".to_string()].into_iter().collect());

      // Subscribe should work
      let _id = db
        .subscribe_query(query.clone(), subscriber_arc.clone(), Some(scope))
        .await
        .expect("subscribe with scope");

      // Should have initial results
      assert_eq!(subscriber.get_call_count(), 1);

      println!("✓ Subscriptions with scope work");
    });
  }

  #[test]
  fn test_scope_can_access() {
    // Test unrestricted scope
    let unrestricted_scope = SyncScope::default();
    assert!(unrestricted_scope.can_access("users"));
    assert!(unrestricted_scope.can_access("orders"));
    assert!(unrestricted_scope.can_access("any_table"));

    // Test restricted scope
    let restricted_scope = SyncScope::new(
      vec!["users".to_string(), "products".to_string()]
        .into_iter()
        .collect(),
    );

    assert!(restricted_scope.can_access("users"));
    assert!(restricted_scope.can_access("products"));
    assert!(!restricted_scope.can_access("orders"));

    println!("✓ SyncScope access control works");
  }

  #[test]
  fn test_subscription_initial_empty_results() {
    block_on(async {
      let mut db = Database::open_in_memory().await.expect("open in-memory db");

      db.execute_sql("CREATE TABLE logs (id UUID PRIMARY KEY, message TEXT);")
        .await
        .expect("create table");

      let subscriber = TestSubscriber::new();
      let subscriber_arc = Arc::new(subscriber.clone());

      let query = EngineQuery::select_simple("logs".to_string(), vec![0, 1], None);

      // Subscribe to empty table
      let _id = db
        .subscribe_query(query, subscriber_arc.clone(), None)
        .await
        .expect("subscribe");

      // Should have called subscriber once with empty results
      assert_eq!(subscriber.get_call_count(), 1);
      let results = subscriber.get_results();
      assert_eq!(results.len(), 1);
      assert_eq!(results[0].rows.len(), 0);

      println!("✓ Subscription gets initial empty results");
    });
  }

  #[test]
  fn test_scope_table_filtering() {
    let scope = SyncScope::new(vec!["users".to_string()].into_iter().collect());

    // Allowed table
    assert!(scope.can_access("users"));

    // Not allowed
    assert!(!scope.can_access("admin_logs"));
    assert!(!scope.can_access("orders"));

    println!("✓ Scope table filtering works");
  }
}
