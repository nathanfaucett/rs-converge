use crate::{
  ChangeEvent, ChangeListenerRegistry, EngineError, EngineRow, IndexSchema, Subscriber,
  SubscriptionId, SyncScope, TableSchema,
  engine_kernel::{EngineKernel, EngineWriteTxn},
  query::EngineQuery,
  query::EngineResult,
  query::JoinClause,
  query::QualifiedPredicate,
  query::ResultColumn,
  query::UpdateAssignment,
  query::UpdateValueExpr,
  store_adapter::EngineStore,
  subscriptions::{QuerySubscription, SubscriptionBatch, SubscriptionRegistry},
};

#[cfg(all(test, feature = "std"))]
use crate::store_adapter::NamedTreeEngineStore;

#[cfg(not(feature = "std"))]
use alloc::boxed::Box;
#[cfg(not(feature = "std"))]
use alloc::string::{String, ToString};
#[cfg(not(feature = "std"))]
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(not(feature = "std"))]
use spin::RwLock;
#[cfg(feature = "std")]
use std::boxed::Box;
#[cfg(feature = "std")]
use std::string::ToString;
#[cfg(feature = "std")]
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct EngineDatabase<S> {
  kernel: EngineKernel<S>,
  subscription_registry: Arc<SubscriptionRegistry>,
}

impl<S> EngineDatabase<S>
where
  S: EngineStore,
{
  fn returning_columns(
    &self,
    table_name: &str,
    returning: &[UpdateValueExpr],
  ) -> Result<Vec<ResultColumn>, EngineError> {
    let schema = self
      .describe_table(table_name)
      .ok_or_else(|| EngineError::TableNotFound(table_name.to_string()))?;

    let mut columns = Vec::with_capacity(returning.len());
    for (index, expression) in returning.iter().enumerate() {
      match expression {
        UpdateValueExpr::Column(column_ref) => {
          if column_ref.table != table_name {
            return Err(EngineError::SchemaMismatch(format!(
              "RETURNING column {} must reference target table {}",
              column_ref.table, table_name
            )));
          }

          let column = schema.columns.get(column_ref.column_index).ok_or_else(|| {
            EngineError::SchemaMismatch(format!(
              "RETURNING index {} is out of bounds for table {}",
              column_ref.column_index, table_name
            ))
          })?;

          columns.push(ResultColumn::new(
            column.name.clone(),
            Some(table_name.to_string()),
            Some(column_ref.column_index),
          ));
        }
        _ => {
          columns.push(ResultColumn::new(format!("expr_{}", index + 1), None, None));
        }
      }
    }

    Ok(columns)
  }

  pub fn new(store: S) -> Self {
    debug_assert!(store.transaction_contract().validate().is_ok());

    let change_listener_registry = Arc::new(ChangeListenerRegistry::new());
    Self {
      kernel: EngineKernel::new(store, change_listener_registry.clone()),
      subscription_registry: Arc::new(SubscriptionRegistry::new()),
    }
  }

  pub fn new_checked(store: S) -> Result<Self, EngineError> {
    store.transaction_contract().validate()?;
    Ok(Self::new(store))
  }

  pub async fn open(store: S) -> Result<Self, EngineError> {
    store.transaction_contract().validate()?;
    let change_listener_registry = Arc::new(ChangeListenerRegistry::new());
    Ok(Self {
      kernel: EngineKernel::open(store, change_listener_registry.clone()).await?,
      subscription_registry: Arc::new(SubscriptionRegistry::new()),
    })
  }

  #[doc(hidden)]
  pub fn store(&self) -> &S {
    self.kernel.store()
  }

  #[doc(hidden)]
  pub async fn reload_schema(&mut self) -> Result<(), EngineError> {
    self.kernel.load_schema().await
  }

  pub async fn register_table(
    &mut self,
    schema: TableSchema,
    if_not_exists: bool,
  ) -> Result<(), EngineError> {
    self.kernel.register_table(schema, if_not_exists).await
  }

  pub async fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), EngineError> {
    self.kernel.drop_table(table_name, if_exists).await
  }

  pub async fn register_index(&mut self, schema: IndexSchema) -> Result<(), EngineError> {
    self.kernel.register_index(schema).await
  }

  pub async fn drop_index(&mut self, index_name: &str) -> Result<(), EngineError> {
    self.kernel.drop_index(index_name).await
  }

  pub fn transaction(&self) -> EngineTransaction<'_, S> {
    EngineTransaction {
      db: self,
      inner: self.kernel.writer(),
    }
  }

  pub fn read_transaction(&self) -> EngineReadTransaction<'_, S> {
    EngineReadTransaction { db: self }
  }

  pub fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    self.kernel.table(table_name).ok().cloned()
  }

  pub async fn execute(&self, query: EngineQuery) -> Result<EngineResult, EngineError> {
    let (result, events) = self.kernel.run_with_events(query).await?;
    if !events.is_empty() {
      self.recompute_batched_subscriptions(&events).await?;
    }
    Ok(result)
  }

  pub async fn select(
    &self,
    table_name: &str,
    projection: &[usize],
    predicate: Option<QualifiedPredicate>,
  ) -> Result<EngineResult, EngineError> {
    let projection = projection
      .iter()
      .map(|column_index| crate::query::QualifiedColumn {
        table: table_name.to_string(),
        column_index: *column_index,
      })
      .collect();

    self
      .execute(EngineQuery::Select {
        table: table_name.to_string(),
        projection,
        predicate,
        options: Box::new(crate::query::SelectOptions::default()),
      })
      .await
  }

  /// Subscribe to a query with optional access control scope.
  /// Calls the subscriber immediately with initial results,
  /// then calls it again whenever the query results change.
  pub async fn subscribe(
    &self,
    query: EngineQuery,
    scope: &SyncScope,
    subscriber: Arc<dyn Subscriber>,
  ) -> Result<SubscriptionId, EngineError> {
    // Validate that the query respects the scope
    for table in query.tables() {
      if !scope.can_access(&table) {
        return Err(EngineError::TableNotFound(table));
      }
    }

    // Run the query immediately with scope applied and get initial results
    let initial_results = self.execute_with_scope(query.clone(), scope).await?;

    // Call subscriber with initial results
    subscriber.on_results(Ok(initial_results.clone()));

    // Create subscription with initial results
    let subscription = Arc::new(QuerySubscription {
      id: SubscriptionId::next(),
      query,
      scope: scope.clone(),
      subscriber,
      last_results: RwLock::new(Some(initial_results)),
    });

    // Register subscription
    self.subscription_registry.register(subscription.clone());

    Ok(subscription.id)
  }

  /// Unsubscribe from a previously registered subscription.
  pub async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), EngineError> {
    self.subscription_registry.unregister(id);
    Ok(())
  }

  /// Execute a query with access control scope applied.
  /// Adds scope predicates to the WHERE clause and filters results.
  pub async fn execute_with_scope(
    &self,
    query: EngineQuery,
    scope: &SyncScope,
  ) -> Result<EngineResult, EngineError> {
    let scoped_query = self.apply_scope_to_query(query, scope)?;
    self.kernel.run(scoped_query).await
  }

  fn apply_scope_to_query(
    &self,
    query: EngineQuery,
    scope: &SyncScope,
  ) -> Result<EngineQuery, EngineError> {
    for table in query.tables() {
      if !scope.can_access(&table) {
        return Err(EngineError::TableNotFound(table));
      }
    }

    let add_filter = |predicate: Option<QualifiedPredicate>, table: String| {
      if let Some(filter) = scope.filter_for(&table) {
        let combined = match predicate {
          Some(existing) => QualifiedPredicate::And(Box::new(existing), Box::new(filter.clone())),
          None => filter.clone(),
        };
        Some(combined)
      } else {
        predicate
      }
    };

    Ok(match query {
      EngineQuery::Select {
        table: query_table,
        projection,
        predicate,
        options,
      } => EngineQuery::Select {
        table: query_table.clone(),
        projection,
        predicate: add_filter(predicate, query_table.clone()),
        options,
      },
      EngineQuery::Update {
        table: query_table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => EngineQuery::Update {
        table: query_table.clone(),
        assignments,
        predicate: add_filter(predicate, query_table.clone()),
        joins,
        from_tables,
        returning,
      },
      EngineQuery::Delete {
        table: query_table,
        predicate,
        returning,
      } => EngineQuery::Delete {
        table: query_table.clone(),
        predicate: add_filter(predicate, query_table.clone()),
        returning,
      },
      EngineQuery::Insert {
        table,
        row,
        returning,
      } => EngineQuery::Insert {
        table,
        row,
        returning,
      },
    })
  }

  pub(crate) async fn recompute_batched_subscriptions(
    &self,
    events: &[ChangeEvent],
  ) -> Result<(), EngineError> {
    self.recompute_batched_subscriptions_impl(events).await
  }

  async fn recompute_batched_subscriptions_impl(
    &self,
    events: &[ChangeEvent],
  ) -> Result<(), EngineError> {
    let ids = self.collect_invalidated_subscription_ids(events);
    self.recompute_subscriptions(ids).await;
    Ok(())
  }

  fn collect_invalidated_subscription_ids(&self, events: &[ChangeEvent]) -> Vec<SubscriptionId> {
    let batch = SubscriptionBatch::new();
    for event in events {
      for sub in self.subscription_registry.affected_by_change(event) {
        batch.invalidate(sub.id);
      }
    }
    batch.take_invalidated()
  }

  async fn recompute_subscriptions(&self, ids: Vec<SubscriptionId>) {
    for id in ids {
      if let Some(sub) = self.subscription_registry.get_subscription(id) {
        self.recompute_subscription(sub).await;
      }
    }
  }

  async fn recompute_subscription(&self, sub: Arc<QuerySubscription>) {
    match self.execute_with_scope(sub.query.clone(), &sub.scope).await {
      Ok(new_results) => {
        if subscription_results_changed(&sub, &new_results) {
          update_subscription_results(&sub, new_results);
        }
      }
      Err(e) => {
        sub.subscriber.on_results(Err(e));
      }
    }
  }
}

fn subscription_results_changed(sub: &QuerySubscription, new_results: &EngineResult) -> bool {
  #[cfg(feature = "std")]
  let guard = sub.last_results.read().unwrap();
  #[cfg(not(feature = "std"))]
  let guard = sub.last_results.read();

  match &*guard {
    None => true,
    Some(old) => old.rows != new_results.rows || old.columns != new_results.columns,
  }
}

fn update_subscription_results(sub: &QuerySubscription, new_results: EngineResult) {
  #[cfg(feature = "std")]
  let mut last_results = sub.last_results.write().unwrap();
  #[cfg(not(feature = "std"))]
  let mut last_results = sub.last_results.write();
  *last_results = Some(new_results.clone());
  sub.subscriber.on_results(Ok(new_results));
}

pub struct EngineReadTransaction<'db, S>
where
  S: EngineStore,
{
  db: &'db EngineDatabase<S>,
}

impl<'db, S> EngineReadTransaction<'db, S>
where
  S: EngineStore,
{
  pub async fn execute(&self, query: EngineQuery) -> Result<EngineResult, EngineError> {
    match query {
      EngineQuery::Select { .. } => self.db.execute(query).await,
      _ => Err(EngineError::QueryNotSupported(
        "read transaction supports only SELECT queries".into(),
      )),
    }
  }

  pub async fn select(
    &self,
    table_name: &str,
    projection: &[usize],
    predicate: Option<QualifiedPredicate>,
  ) -> Result<EngineResult, EngineError> {
    self.db.select(table_name, projection, predicate).await
  }
}

pub struct EngineTransaction<'db, S>
where
  S: EngineStore,
{
  db: &'db EngineDatabase<S>,
  inner: EngineWriteTxn<'db, S>,
}

impl<'db, S> EngineTransaction<'db, S>
where
  S: EngineStore,
{
  pub async fn insert_row(&mut self, table_name: &str, row: EngineRow) -> Result<(), EngineError> {
    self.inner.insert(table_name, row).await
  }

  pub async fn insert_row_with_returning(
    &mut self,
    table_name: &str,
    row: EngineRow,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<EngineResult, EngineError> {
    let returning_columns = match &returning {
      Some(columns) => Some(self.db.returning_columns(table_name, columns)?),
      None => None,
    };
    let rows = self
      .inner
      .insert_returning(table_name, row, returning)
      .await?;
    Ok(match returning_columns {
      Some(columns) => EngineResult::new_with_columns(rows, columns),
      None => EngineResult::new(rows),
    })
  }

  pub async fn delete_rows(
    &mut self,
    table_name: &str,
    predicate: Option<QualifiedPredicate>,
  ) -> Result<(), EngineError> {
    let _ = self.inner.delete(table_name, predicate, None).await?;
    Ok(())
  }

  pub async fn delete_rows_with_returning(
    &mut self,
    table_name: &str,
    predicate: Option<QualifiedPredicate>,
    returning: Option<Vec<crate::query::UpdateValueExpr>>,
  ) -> Result<EngineResult, EngineError> {
    let returning_columns = match &returning {
      Some(columns) => Some(self.db.returning_columns(table_name, columns)?),
      None => None,
    };
    let rows = self.inner.delete(table_name, predicate, returning).await?;
    Ok(match returning_columns {
      Some(columns) => EngineResult::new_with_columns(rows, columns),
      None => EngineResult::new(rows),
    })
  }

  pub async fn update_rows(
    &mut self,
    table_name: &str,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
  ) -> Result<(), EngineError> {
    let _ = self
      .inner
      .update(
        table_name,
        assignments,
        predicate,
        Vec::new(),
        Vec::new(),
        None,
      )
      .await?;
    Ok(())
  }

  pub async fn update_rows_with_joins(
    &mut self,
    table_name: &str,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
  ) -> Result<(), EngineError> {
    let _ = self
      .inner
      .update(table_name, assignments, predicate, joins, Vec::new(), None)
      .await?;
    Ok(())
  }

  pub async fn update_rows_with_sources(
    &mut self,
    table_name: &str,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
    from_tables: Vec<String>,
  ) -> Result<(), EngineError> {
    let _ = self
      .inner
      .update(table_name, assignments, predicate, joins, from_tables, None)
      .await?;
    Ok(())
  }

  pub async fn update_rows_with_sources_and_returning(
    &mut self,
    table_name: &str,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
    from_tables: Vec<String>,
    returning: Option<Vec<crate::query::UpdateValueExpr>>,
  ) -> Result<EngineResult, EngineError> {
    let returning_columns = match &returning {
      Some(columns) => Some(self.db.returning_columns(table_name, columns)?),
      None => None,
    };
    let rows = self
      .inner
      .update(
        table_name,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      )
      .await?;
    Ok(match returning_columns {
      Some(columns) => EngineResult::new_with_columns(rows, columns),
      None => EngineResult::new(rows),
    })
  }

  pub async fn commit(self) -> Result<(), EngineError> {
    let events = self.inner.commit().await?;
    self.db.recompute_batched_subscriptions(&events).await
  }

  pub async fn rollback(self) -> Result<(), EngineError> {
    self.inner.rollback().await
  }
}

#[cfg(all(test, feature = "std"))]
mod tests {
  use super::*;
  use crate::query::{
    JoinClause, JoinKind, JoinOn, QualifiedColumn, QualifiedOperand, QualifiedPredicate,
    SelectOptions, UpdateValueExpr,
  };
  use crate::{
    ColumnSchema, EngineError, EngineKey, EngineQuery, EngineType, EngineValue, IndexSchema,
    TableSchema, UpdateAssignment,
  };
  use db_in_memory::InMemoryNamedBTree;
  use futures::executor::block_on;

  use uuid::Uuid;

  #[test]
  fn engine_read_transaction_supports_select_only() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

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
      .expect("register users");

      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![
          EngineValue::Uuid([0; 16]),
          EngineValue::Text("Alice".into()),
        ],
        returning: None,
      })
      .await
      .expect("insert row");

      let tx = db.read_transaction();
      let result = tx
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select query");
      assert_eq!(result.rows.len(), 1);

      let error = tx
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![EngineValue::Uuid([1; 16]), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect_err("insert in read transaction should fail");
      assert!(matches!(
        error,
        EngineError::SchemaMismatch(_) | EngineError::QueryNotSupported(_)
      ));
    });
  }

  fn eq_pred(table: &str, column_index: usize, value: EngineValue) -> QualifiedPredicate {
    QualifiedPredicate::Equals(
      QualifiedOperand::Column(QualifiedColumn {
        table: table.into(),
        column_index,
      }),
      QualifiedOperand::Value(value),
    )
  }

  fn uuid(id: u128) -> EngineValue {
    EngineValue::Uuid(*Uuid::from_u128(id).as_bytes())
  }

  #[test]
  fn insert_and_select_from_table() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let schema = TableSchema {
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
      };

      database
        .register_table(schema, false)
        .await
        .expect("register users table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect("execute insert query");

      let result = database
        .execute(EngineQuery::select_simple(
          "users".into(),
          vec![1],
          Some(eq_pred("users", 0, uuid(1))),
        ))
        .await
        .expect("execute select query");

      assert_eq!(result.rows.len(), 1);
      assert_eq!(result.rows[0], vec![EngineValue::Text("Alice".into())]);
    });
  }

  #[test]
  fn insert_and_select_float_value() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let schema = TableSchema {
        name: "measurements".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "value".into(),
            data_type: EngineType::Float,
          },
        ],
        primary_key: vec![0],
      };

      database
        .register_table(schema, false)
        .await
        .expect("register measurements table");

      database
        .execute(EngineQuery::Insert {
          table: "measurements".into(),
          row: vec![uuid(1), EngineValue::Float(1.23)],
          returning: None,
        })
        .await
        .expect("execute insert query");

      let result = database
        .execute(EngineQuery::select_simple(
          "measurements".into(),
          vec![1],
          Some(eq_pred("measurements", 0, uuid(1))),
        ))
        .await
        .expect("execute select query");

      assert_eq!(result.rows, vec![vec![EngineValue::Float(1.23)]]);
    });
  }

  #[test]
  fn insert_and_select_blob_value() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let schema = TableSchema {
        name: "files".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "data".into(),
            data_type: EngineType::Blob,
          },
        ],
        primary_key: vec![0],
      };

      let blob = vec![0xde, 0xad, 0xbe, 0xef];

      database
        .register_table(schema, false)
        .await
        .expect("register files table");

      database
        .execute(EngineQuery::Insert {
          table: "files".into(),
          row: vec![uuid(1), EngineValue::Blob(blob.clone())],
          returning: None,
        })
        .await
        .expect("execute insert query");

      let result = database
        .execute(EngineQuery::select_simple(
          "files".into(),
          vec![1],
          Some(eq_pred("files", 0, uuid(1))),
        ))
        .await
        .expect("execute select query");

      assert_eq!(result.rows, vec![vec![EngineValue::Blob(blob)]]);
    });
  }

  #[test]
  fn transaction_returning_methods_include_columns_and_rows() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");

      let returning = Some(vec![UpdateValueExpr::Column(QualifiedColumn {
        table: "users".into(),
        column_index: 1,
      })]);

      let mut tx = database.transaction();
      let inserted = tx
        .insert_row_with_returning(
          "users",
          vec![uuid(10), EngineValue::Text("Alice".into())],
          returning.clone(),
        )
        .await
        .expect("insert with returning");
      tx.commit().await.expect("commit insert");

      assert_eq!(inserted.rows, vec![vec![EngineValue::Text("Alice".into())]]);
      assert_eq!(inserted.columns.len(), 1);
      assert_eq!(inserted.columns[0].name, "name");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(11), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect("insert second row");

      let mut tx = database.transaction();
      let updated = tx
        .update_rows_with_sources_and_returning(
          "users",
          vec![UpdateAssignment::value(
            1,
            EngineValue::Text("Bobby".into()),
          )],
          Some(eq_pred("users", 0, uuid(11))),
          Vec::new(),
          Vec::new(),
          returning.clone(),
        )
        .await
        .expect("update with returning");
      tx.commit().await.expect("commit update");

      assert_eq!(updated.rows, vec![vec![EngineValue::Text("Bobby".into())]]);
      assert_eq!(updated.columns[0].name, "name");

      let mut tx = database.transaction();
      let deleted = tx
        .delete_rows_with_returning("users", Some(eq_pred("users", 0, uuid(10))), returning)
        .await
        .expect("delete with returning");
      tx.commit().await.expect("commit delete");

      assert_eq!(deleted.rows, vec![vec![EngineValue::Text("Alice".into())]]);
      assert_eq!(deleted.columns[0].name, "name");
    });
  }

  #[test]
  fn select_uses_index_when_available() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");

      database
        .register_index(IndexSchema {
          name: "users_name_idx".into(),
          table_name: "users".into(),
          column_indices: vec![1],
          unique: true,
        })
        .await
        .expect("register users_name_idx index");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect("execute first insert query");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(2), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect("execute second insert query");

      let result = database
        .execute(EngineQuery::select_simple(
          "users".into(),
          vec![0, 1],
          Some(eq_pred("users", 1, EngineValue::Text("Bob".into()))),
        ))
        .await
        .expect("execute select query");

      assert_eq!(
        result.rows,
        vec![vec![uuid(2), EngineValue::Text("Bob".into())]]
      );
    });
  }

  #[test]
  fn inner_join_simple() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };

      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");

      // Insert users
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user 1");

      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(2), EngineValue::Text("Bob".into())],
        returning: None,
      })
      .await
      .expect("insert user 2");

      // Insert orders
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(1), EngineValue::Integer(100)],
        returning: None,
      })
      .await
      .expect("insert order 1");

      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(2), uuid(2), EngineValue::Integer(200)],
        returning: None,
      })
      .await
      .expect("insert order 2");

      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(3), uuid(1), EngineValue::Integer(50)],
        returning: None,
      })
      .await
      .expect("insert order 3");

      let left_col = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let right_col = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };

      let join = JoinClause {
        kind: JoinKind::Inner,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: left_col.clone(),
          right: right_col.clone(),
        },
      };

      let projection = vec![
        QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        }, // name
        QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        }, // amount
      ];

      let options = SelectOptions {
        joins: vec![join],
        aggregates: vec![],
        group_by: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection,
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute join");

      // Expect 3 joined rows: Alice (100), Alice (50), Bob (200)
      assert_eq!(res.rows.len(), 3);
    });
  }

  #[test]
  fn group_by_count_and_sum() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };

      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");

      // Insert users
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user 1");

      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(2), EngineValue::Text("Bob".into())],
        returning: None,
      })
      .await
      .expect("insert user 2");

      // Insert orders
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(1), EngineValue::Integer(100)],
        returning: None,
      })
      .await
      .expect("insert order 1");

      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(2), uuid(2), EngineValue::Integer(200)],
        returning: None,
      })
      .await
      .expect("insert order 2");

      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(3), uuid(1), EngineValue::Integer(50)],
        returning: None,
      })
      .await
      .expect("insert order 3");

      let left_col = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let right_col = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };

      let join = JoinClause {
        kind: JoinKind::Inner,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: left_col.clone(),
          right: right_col.clone(),
        },
      };

      let group_by = vec![QualifiedColumn {
        table: "users".into(),
        column_index: 1,
      }];

      let aggregates = vec![
        crate::query::Aggregate::Count(None),
        crate::query::Aggregate::Sum(QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        }),
      ];

      let options = SelectOptions {
        joins: vec![join],
        aggregates,
        group_by,
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection: vec![],
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute grouped join");

      // Expect 2 groups: Alice (count 2, sum 150), Bob (count 1, sum 200)
      assert_eq!(res.rows.len(), 2);

      for row in res.rows {
        if row[0] == EngineValue::Text("Alice".into()) {
          // count then sum
          match &row[1] {
            EngineValue::Integer(c) => assert_eq!(*c, 2),
            _ => panic!("expected count integer"),
          }
          match &row[2] {
            EngineValue::Float(s) => assert!((*s - 150.0).abs() < f64::EPSILON),
            EngineValue::Integer(i) => assert_eq!(*i, 150),
            _ => panic!("expected sum numeric"),
          }
        } else if row[0] == EngineValue::Text("Bob".into()) {
          match &row[1] {
            EngineValue::Integer(c) => assert_eq!(*c, 1),
            _ => panic!("expected count integer"),
          }
          match &row[2] {
            EngineValue::Float(s) => assert!((*s - 200.0).abs() < f64::EPSILON),
            EngineValue::Integer(i) => assert_eq!(*i, 200),
            _ => panic!("expected sum numeric"),
          }
        } else {
          panic!("unexpected group key: {:?}", row[0]);
        }
      }
    });
  }

  #[test]
  fn order_by_and_limit() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };

      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");

      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user 1");
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(2), EngineValue::Text("Bob".into())],
        returning: None,
      })
      .await
      .expect("insert user 2");

      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(1), EngineValue::Integer(100)],
        returning: None,
      })
      .await
      .expect("insert order 1");
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(2), uuid(2), EngineValue::Integer(200)],
        returning: None,
      })
      .await
      .expect("insert order 2");
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(3), uuid(1), EngineValue::Integer(50)],
        returning: None,
      })
      .await
      .expect("insert order 3");

      let left_col = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let right_col = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };

      let join = JoinClause {
        kind: JoinKind::Inner,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: left_col.clone(),
          right: right_col.clone(),
        },
      };

      let projection = vec![
        QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        },
        QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        },
      ];

      let order_by = vec![crate::query::OrderBy {
        expr: QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        },
        direction: crate::query::SortDirection::Desc,
      }];

      let options = SelectOptions {
        joins: vec![join],
        aggregates: vec![],
        group_by: vec![],
        order_by,
        limit: Some(2),
        offset: Some(0),
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection,
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute ordered join");

      // With ORDER BY amount DESC and LIMIT 2, expect amounts [200,100]
      assert_eq!(res.rows.len(), 2);
      match &res.rows[0][1] {
        EngineValue::Integer(i) => assert_eq!(*i, 200),
        _ => panic!("expected integer amount"),
      }
      match &res.rows[1][1] {
        EngineValue::Integer(i) => assert_eq!(*i, 100),
        _ => panic!("expected integer amount"),
      }
    });
  }

  #[test]
  fn left_join_simple() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };

      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");

      // Insert users: include a user with no orders
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user 1");
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(2), EngineValue::Text("Bob".into())],
        returning: None,
      })
      .await
      .expect("insert user 2");
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(3), EngineValue::Text("Charlie".into())],
        returning: None,
      })
      .await
      .expect("insert user 3");

      // Insert orders for Alice and Bob only
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(1), EngineValue::Integer(100)],
        returning: None,
      })
      .await
      .expect("insert order 1");
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(2), uuid(2), EngineValue::Integer(200)],
        returning: None,
      })
      .await
      .expect("insert order 2");

      let left_col = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let right_col = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };

      let join = JoinClause {
        kind: JoinKind::Left,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: left_col.clone(),
          right: right_col.clone(),
        },
      };

      let projection = vec![
        QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        },
        QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        },
      ];

      let options = SelectOptions {
        joins: vec![join],
        aggregates: vec![],
        group_by: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection,
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute left join");

      // Expect 3 rows, Charlie's order amount should be NULL
      assert_eq!(res.rows.len(), 3);
      let mut found_charlie = false;
      for row in res.rows {
        if row[0] == EngineValue::Text("Charlie".into()) {
          found_charlie = true;
          assert!(matches!(row[1], EngineValue::Null));
        }
      }
      assert!(found_charlie);
    });
  }

  #[test]
  fn right_join_simple() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };
      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");

      // Insert a user and an order that references a missing user
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user 1");
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(999), EngineValue::Integer(55)],
        returning: None,
      })
      .await
      .expect("insert order missing user");

      let left_col = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let right_col = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };

      let join = JoinClause {
        kind: JoinKind::Right,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: left_col.clone(),
          right: right_col.clone(),
        },
      };

      let projection = vec![
        QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        },
        QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        },
      ];

      let options = SelectOptions {
        joins: vec![join],
        aggregates: vec![],
        group_by: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection,
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute right join");

      // Expect 1 row where user is NULL and amount == 55
      assert_eq!(res.rows.len(), 1);
      assert!(matches!(res.rows[0][0], EngineValue::Null));
      match &res.rows[0][1] {
        EngineValue::Integer(i) => assert_eq!(*i, 55),
        _ => panic!("expected integer amount"),
      }
    });
  }

  #[test]
  fn full_join_simple() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };
      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");

      // user 1 exists, user 2 has no orders; order 3 references missing user 3
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user 1");
      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(2), EngineValue::Text("Bob".into())],
        returning: None,
      })
      .await
      .expect("insert user 2");

      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(1), EngineValue::Integer(100)],
        returning: None,
      })
      .await
      .expect("insert order 1");
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(2), uuid(3), EngineValue::Integer(55)],
        returning: None,
      })
      .await
      .expect("insert order missing user");

      let left_col = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let right_col = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };

      let join = JoinClause {
        kind: JoinKind::Full,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: left_col.clone(),
          right: right_col.clone(),
        },
      };

      let projection = vec![
        QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        },
        QualifiedColumn {
          table: "orders".into(),
          column_index: 2,
        },
      ];

      let options = SelectOptions {
        joins: vec![join],
        aggregates: vec![],
        group_by: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection,
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute full join");

      // Expect 3 rows: Alice with 100, Bob with NULL, NULL with 55
      assert_eq!(res.rows.len(), 3);
    });
  }

  #[test]
  fn multiple_joins_chain() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut db = EngineDatabase::new(NamedTreeEngineStore::new(store));

      let users = TableSchema {
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
      };
      let orders = TableSchema {
        name: "orders".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "user_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "product_id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "amount".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };
      let products = TableSchema {
        name: "products".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "title".into(),
            data_type: EngineType::Text,
          },
        ],
        primary_key: vec![0],
      };

      db.register_table(users, false)
        .await
        .expect("register users");
      db.register_table(orders, false)
        .await
        .expect("register orders");
      db.register_table(products, false)
        .await
        .expect("register products");

      db.execute(EngineQuery::Insert {
        table: "users".into(),
        row: vec![uuid(1), EngineValue::Text("Alice".into())],
        returning: None,
      })
      .await
      .expect("insert user");
      db.execute(EngineQuery::Insert {
        table: "products".into(),
        row: vec![uuid(10), EngineValue::Text("Gadget".into())],
        returning: None,
      })
      .await
      .expect("insert product");
      db.execute(EngineQuery::Insert {
        table: "orders".into(),
        row: vec![uuid(1), uuid(1), uuid(10), EngineValue::Integer(99)],
        returning: None,
      })
      .await
      .expect("insert order");

      // Join users -> orders, then orders -> products
      let u_id = QualifiedColumn {
        table: "users".into(),
        column_index: 0,
      };
      let o_user = QualifiedColumn {
        table: "orders".into(),
        column_index: 1,
      };
      let o_prod = QualifiedColumn {
        table: "orders".into(),
        column_index: 2,
      };
      let p_id = QualifiedColumn {
        table: "products".into(),
        column_index: 0,
      };

      let join1 = JoinClause {
        kind: JoinKind::Inner,
        left_table: "users".into(),
        right_table: "orders".into(),
        on: JoinOn::ColumnEq {
          left: u_id.clone(),
          right: o_user.clone(),
        },
      };
      let join2 = JoinClause {
        kind: JoinKind::Inner,
        left_table: "orders".into(),
        right_table: "products".into(),
        on: JoinOn::ColumnEq {
          left: o_prod.clone(),
          right: p_id.clone(),
        },
      };

      let projection = vec![
        QualifiedColumn {
          table: "users".into(),
          column_index: 1,
        },
        QualifiedColumn {
          table: "orders".into(),
          column_index: 3,
        },
        QualifiedColumn {
          table: "products".into(),
          column_index: 1,
        },
      ];

      let options = SelectOptions {
        joins: vec![join1, join2],
        aggregates: vec![],
        group_by: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      };

      let res = db
        .execute(EngineQuery::Select {
          table: "users".into(),
          projection,
          predicate: None,
          options: Box::new(options),
        })
        .await
        .expect("execute multi-join");

      assert_eq!(res.rows.len(), 1);
    });
  }

  #[test]
  fn reopen_database_recovers_schema_from_store() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store.clone()));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");
      database
        .register_index(IndexSchema {
          name: "users_name_idx".into(),
          table_name: "users".into(),
          column_indices: vec![1],
          unique: true,
        })
        .await
        .expect("register users_name_idx index");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect("execute insert query");

      let reopened = EngineDatabase::open(NamedTreeEngineStore::new(store))
        .await
        .expect("open database from store");
      let result = reopened
        .execute(EngineQuery::select_simple(
          "users".into(),
          vec![0, 1],
          Some(eq_pred("users", 1, EngineValue::Text("Bob".into()))),
        ))
        .await
        .expect("execute select query");

      assert_eq!(
        result.rows,
        vec![vec![uuid(1), EngineValue::Text("Bob".into())]],
      );
    });
  }

  #[test]
  fn update_row_and_maintain_indexes() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");
      database
        .register_index(IndexSchema {
          name: "users_name_idx".into(),
          table_name: "users".into(),
          column_indices: vec![1],
          unique: true,
        })
        .await
        .expect("register users_name_idx index");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect("insert first row");
      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(2), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect("insert second row");

      database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(
            1,
            EngineValue::Text("Robert".into()),
          )],
          predicate: Some(eq_pred("users", 0, uuid(2))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update row");

      let result = database
        .execute(EngineQuery::select_simple(
          "users".into(),
          vec![0, 1],
          Some(eq_pred("users", 1, EngineValue::Text("Robert".into()))),
        ))
        .await
        .expect("select updated row");

      assert_eq!(
        result.rows,
        vec![vec![uuid(2), EngineValue::Text("Robert".into())]],
      );

      let stale_result = database
        .execute(EngineQuery::select_simple(
          "users".into(),
          vec![0, 1],
          Some(eq_pred("users", 1, EngineValue::Text("Bob".into()))),
        ))
        .await
        .expect("select stale indexed row");

      assert!(stale_result.rows.is_empty());
    });
  }

  #[test]
  fn unique_index_violates_on_update() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");
      database
        .register_index(IndexSchema {
          name: "users_name_idx".into(),
          table_name: "users".into(),
          column_indices: vec![1],
          unique: true,
        })
        .await
        .expect("register users_name_idx index");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect("insert first row");
      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(2), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect("insert second row");

      let error = database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment::value(
            1,
            EngineValue::Text("Alice".into()),
          )],
          predicate: Some(eq_pred("users", 0, uuid(2))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect_err("update duplicate unique index row");

      assert!(matches!(error, EngineError::UniqueIndexViolation(name) if name == "users_name_idx"));

      let unchanged = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select unchanged rows after failed update");

      assert_eq!(
        unchanged.rows,
        vec![
          vec![uuid(1), EngineValue::Text("Alice".into())],
          vec![uuid(2), EngineValue::Text("Bob".into())],
        ],
      );
    });
  }

  #[test]
  fn delete_rows_with_predicate() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect("insert first row");
      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(2), EngineValue::Text("Bob".into())],
          returning: None,
        })
        .await
        .expect("insert second row");

      database
        .execute(EngineQuery::Delete {
          table: "users".into(),
          predicate: Some(eq_pred("users", 0, uuid(1))),
          returning: None,
        })
        .await
        .expect("delete first row");

      let result = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select remaining rows");

      assert_eq!(
        result.rows,
        vec![vec![uuid(2), EngineValue::Text("Bob".into())]]
      );
    });
  }

  #[test]
  fn update_row_with_expression_assignment() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Integer(10)],
          returning: None,
        })
        .await
        .expect("insert row");

      database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment {
            column_index: 1,
            value: UpdateValueExpr::Add(
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "users".into(),
                column_index: 1,
              })),
              Box::new(UpdateValueExpr::Value(EngineValue::Integer(5))),
            ),
          }],
          predicate: Some(eq_pred("users", 0, uuid(1))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("update row with expression");

      let result = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select row");

      assert_eq!(result.rows, vec![vec![uuid(1), EngineValue::Integer(15)]],);
    });
  }

  #[test]
  fn update_division_by_zero_rolls_back_changes() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Integer,
          },
        ],
        primary_key: vec![0],
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Integer(10)],
          returning: None,
        })
        .await
        .expect("insert row");

      let error = database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment {
            column_index: 1,
            value: UpdateValueExpr::Divide(
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "users".into(),
                column_index: 1,
              })),
              Box::new(UpdateValueExpr::Value(EngineValue::Integer(0))),
            ),
          }],
          predicate: Some(eq_pred("users", 0, uuid(1))),
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect_err("division by zero should fail update");

      assert!(
        matches!(error, EngineError::TypeMismatch(message) if message.contains("division by zero"))
      );

      let unchanged = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select unchanged row");

      assert_eq!(
        unchanged.rows,
        vec![vec![uuid(1), EngineValue::Integer(10)]],
      );
    });
  }

  #[test]
  fn multi_row_update_failure_rolls_back_partial_changes() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
        name: "users".into(),
        columns: vec![
          ColumnSchema {
            name: "id".into(),
            data_type: EngineType::Uuid,
          },
          ColumnSchema {
            name: "score".into(),
            data_type: EngineType::Float,
          },
          ColumnSchema {
            name: "divisor".into(),
            data_type: EngineType::Float,
          },
        ],
        primary_key: vec![0],
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Float(10.0), EngineValue::Float(2.0)],
          returning: None,
        })
        .await
        .expect("insert first row");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(2), EngineValue::Float(7.0), EngineValue::Float(0.0)],
          returning: None,
        })
        .await
        .expect("insert second row");

      let error = database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment {
            column_index: 1,
            value: UpdateValueExpr::Divide(
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "users".into(),
                column_index: 1,
              })),
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "users".into(),
                column_index: 2,
              })),
            ),
          }],
          predicate: None,
          joins: Vec::new(),
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect_err("division by zero should fail multi-row update");

      assert!(
        matches!(error, EngineError::TypeMismatch(message) if message.contains("division by zero"))
      );

      let unchanged = database
        .execute(EngineQuery::select_simple(
          "users".into(),
          vec![0, 1, 2],
          None,
        ))
        .await
        .expect("select unchanged rows");

      assert_eq!(
        unchanged.rows,
        vec![
          vec![uuid(1), EngineValue::Float(10.0), EngineValue::Float(2.0),],
          vec![uuid(2), EngineValue::Float(7.0), EngineValue::Float(0.0),],
        ],
      );
    });
  }

  #[test]
  fn update_row_with_join_assignment_expression() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));

      database
        .register_table(
          TableSchema {
            name: "users".into(),
            columns: vec![
              ColumnSchema {
                name: "id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "team_id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "score".into(),
                data_type: EngineType::Integer,
              },
            ],
            primary_key: vec![0],
          },
          false,
        )
        .await
        .expect("register users table");

      database
        .register_table(
          TableSchema {
            name: "teams".into(),
            columns: vec![
              ColumnSchema {
                name: "id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "bonus".into(),
                data_type: EngineType::Integer,
              },
            ],
            primary_key: vec![0],
          },
          false,
        )
        .await
        .expect("register teams table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), uuid(10), EngineValue::Integer(5)],
          returning: None,
        })
        .await
        .expect("insert user row");
      database
        .execute(EngineQuery::Insert {
          table: "teams".into(),
          row: vec![uuid(10), EngineValue::Integer(3)],
          returning: None,
        })
        .await
        .expect("insert team row");

      database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment {
            column_index: 2,
            value: UpdateValueExpr::Add(
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "users".into(),
                column_index: 2,
              })),
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "teams".into(),
                column_index: 1,
              })),
            ),
          }],
          predicate: None,
          joins: vec![JoinClause {
            kind: JoinKind::Inner,
            left_table: "users".into(),
            right_table: "teams".into(),
            on: JoinOn::ColumnEq {
              left: QualifiedColumn {
                table: "users".into(),
                column_index: 1,
              },
              right: QualifiedColumn {
                table: "teams".into(),
                column_index: 0,
              },
            },
          }],
          from_tables: Vec::new(),
          returning: None,
        })
        .await
        .expect("join update");

      let result = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 2], None))
        .await
        .expect("select updated user row");

      assert_eq!(result.rows, vec![vec![uuid(1), EngineValue::Integer(8)]],);
    });
  }

  #[test]
  fn update_join_rejects_multiple_matches_for_target_row() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));

      database
        .register_table(
          TableSchema {
            name: "users".into(),
            columns: vec![
              ColumnSchema {
                name: "id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "team_id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "score".into(),
                data_type: EngineType::Integer,
              },
            ],
            primary_key: vec![0],
          },
          false,
        )
        .await
        .expect("register users table");

      database
        .register_table(
          TableSchema {
            name: "teams".into(),
            columns: vec![
              ColumnSchema {
                name: "id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "dept_id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "bonus".into(),
                data_type: EngineType::Integer,
              },
            ],
            primary_key: vec![0],
          },
          false,
        )
        .await
        .expect("register teams table");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), uuid(10), EngineValue::Integer(5)],
          returning: None,
        })
        .await
        .expect("insert user row");

      database
        .execute(EngineQuery::Insert {
          table: "teams".into(),
          row: vec![uuid(100), uuid(10), EngineValue::Integer(3)],
          returning: None,
        })
        .await
        .expect("insert first team row");

      database
        .execute(EngineQuery::Insert {
          table: "teams".into(),
          row: vec![uuid(101), uuid(10), EngineValue::Integer(4)],
          returning: None,
        })
        .await
        .expect("insert second team row");

      let result = database
        .execute(EngineQuery::Update {
          table: "users".into(),
          assignments: vec![UpdateAssignment {
            column_index: 2,
            value: UpdateValueExpr::Add(
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "users".into(),
                column_index: 2,
              })),
              Box::new(UpdateValueExpr::Column(QualifiedColumn {
                table: "teams".into(),
                column_index: 2,
              })),
            ),
          }],
          predicate: None,
          joins: vec![JoinClause {
            kind: JoinKind::Inner,
            left_table: "users".into(),
            right_table: "teams".into(),
            on: JoinOn::ColumnEq {
              left: QualifiedColumn {
                table: "users".into(),
                column_index: 1,
              },
              right: QualifiedColumn {
                table: "teams".into(),
                column_index: 1,
              },
            },
          }],
          from_tables: Vec::new(),
          returning: None,
        })
        .await;

      match result {
        Err(EngineError::SchemaMismatch(message)) => {
          assert!(message.contains("matched target row more than once"));
        }
        other => panic!("expected SchemaMismatch duplicate-match error, got {other:?}"),
      }

      let unchanged = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 2], None))
        .await
        .expect("select unchanged users after failed join update");

      assert_eq!(unchanged.rows, vec![vec![uuid(1), EngineValue::Integer(5)]],);
    });
  }

  #[test]
  fn empty_table_select_returns_no_rows() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));

      database
        .register_table(
          TableSchema {
            name: "users".into(),
            columns: vec![
              ColumnSchema {
                name: "id".into(),
                data_type: EngineType::Uuid,
              },
              ColumnSchema {
                name: "score".into(),
                data_type: EngineType::Integer,
              },
            ],
            primary_key: vec![0],
          },
          false,
        )
        .await
        .expect("register users table");

      let result = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select empty users");

      assert!(result.rows.is_empty());
    });
  }

  #[test]
  fn unique_index_violates_on_insert() {
    block_on(async {
      let store: InMemoryNamedBTree<EngineKey, Vec<u8>> = InMemoryNamedBTree::new();
      let mut database = EngineDatabase::new(NamedTreeEngineStore::new(store));
      let users = TableSchema {
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
      };

      database
        .register_table(users, false)
        .await
        .expect("register users table");
      database
        .register_index(IndexSchema {
          name: "users_name_idx".into(),
          table_name: "users".into(),
          column_indices: vec![1],
          unique: true,
        })
        .await
        .expect("register users_name_idx index");

      database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(1), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect("insert first row");

      let error = database
        .execute(EngineQuery::Insert {
          table: "users".into(),
          row: vec![uuid(2), EngineValue::Text("Alice".into())],
          returning: None,
        })
        .await
        .expect_err("insert duplicate unique index row");

      assert!(matches!(error, EngineError::UniqueIndexViolation(name) if name == "users_name_idx"));

      let unchanged = database
        .execute(EngineQuery::select_simple("users".into(), vec![0, 1], None))
        .await
        .expect("select unchanged rows after failed insert");

      assert_eq!(
        unchanged.rows,
        vec![vec![uuid(1), EngineValue::Text("Alice".into())]],
      );
    });
  }
}
