#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use alloc::{format, vec::Vec};
#[cfg(feature = "std")]
use std::vec::Vec;

#[cfg(feature = "automerge")]
use db_automerge::{AutomergeEngineStore, AutomergeEntry, DocumentChangeKey};
#[cfg(feature = "redb")]
use db_engine::EngineKey;
use db_engine::{
  EngineDatabase, EngineQuery, EngineResult, FromRow, IndexSchema, Subscriber, SubscriptionId,
  SyncScope, TableSchema,
};
#[cfg(feature = "automerge")]
use db_in_memory::InMemoryBTree;
use db_in_memory::InMemoryNamedBTree;
#[cfg(all(feature = "automerge", feature = "redb"))]
use db_redb::REDBBTree;
#[cfg(feature = "redb")]
use db_redb::REDBNamedBTree;
#[cfg(feature = "redb")]
use db_types::EngineKeyCodec;
#[cfg(feature = "redb")]
use std::path::Path;

use db_sql_to_engine::{
  CanonicalStatement, DdlOp, SchemaResolver, SqlParams, parse_and_translate,
  parse_and_translate_statement, parse_and_translate_statement_with_params,
  parse_and_translate_with_params,
};

#[cfg(all(feature = "automerge", feature = "redb"))]
use super::types::RedbAutomergeStore;
#[cfg(feature = "redb")]
use super::types::RedbEngineStore;
#[cfg(feature = "automerge")]
use super::types::{AutomergeSyncMetrics, InMemoryAutomergeStore};
use super::types::{
  Database, DatabaseError, FacadeStore, InMemoryEngineStore, ReadTransaction, Row, Transaction,
};
#[cfg(all(feature = "automerge", feature = "redb"))]
use super::types::{FacadeDocumentChangeKeyCodec, FacadeVecBytesCodec};

impl<S> SchemaResolver for Database<S>
where
  S: FacadeStore,
{
  fn describe_table(&self, name: &str) -> Option<TableSchema> {
    self.engine.describe_table(name)
  }
}

impl Database<InMemoryEngineStore> {
  pub fn open_in_memory_sync() -> Self {
    let store = InMemoryNamedBTree::new();
    let engine = EngineDatabase::new(store.into_engine_store());
    Self { engine }
  }

  /// Open an in-memory database (dev/test convenience).
  pub async fn open_in_memory() -> Result<Self, DatabaseError> {
    Ok(Self::open_in_memory_sync())
  }
}

#[cfg(feature = "automerge")]
impl Database<InMemoryAutomergeStore> {
  /// Open an Automerge-backed database (feature-gated).
  pub async fn open_automerge_in_memory() -> Result<Self, DatabaseError> {
    let backend = InMemoryBTree::<DocumentChangeKey, AutomergeEntry>::new();
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store.into_engine_store());
    Ok(Self { engine })
  }

  /// Merge Automerge documents from each peer and reload schema on both sides.
  pub async fn sync_with(&mut self, other: &mut Self) -> Result<(), DatabaseError> {
    db_automerge::sync_automerge_stores(self.engine.store().inner(), other.engine.store().inner())
      .await
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    self.engine.reload_schema().await?;
    other.engine.reload_schema().await?;
    Ok(())
  }

  pub async fn automerge_sync_metrics(&self) -> Result<AutomergeSyncMetrics, DatabaseError> {
    let docs = db_automerge::collect_documents(self.engine.store().inner())
      .await
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let (document_count, total_document_bytes) = db_automerge::automerge_metrics(&docs);
    Ok(AutomergeSyncMetrics {
      document_count,
      total_document_bytes,
    })
  }
}

#[cfg(all(feature = "automerge", feature = "redb"))]
impl Database<RedbAutomergeStore> {
  pub async fn open_automerge_with_redb(
    path: impl AsRef<Path>,
    table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let backend = REDBBTree::<
      DocumentChangeKey,
      AutomergeEntry,
      FacadeDocumentChangeKeyCodec,
      FacadeVecBytesCodec,
    >::open_with_codecs(path, table_name)
    .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let store = AutomergeEngineStore::new_with_backend(backend);
    let engine = EngineDatabase::new(store.into_engine_store());
    Ok(Self { engine })
  }

  /// Merge Automerge documents from each peer and reload schema on both sides.
  pub async fn sync_with(&mut self, other: &mut Self) -> Result<(), DatabaseError> {
    db_automerge::sync_automerge_stores(self.engine.store().inner(), other.engine.store().inner())
      .await
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    self.engine.reload_schema().await?;
    other.engine.reload_schema().await?;
    Ok(())
  }

  pub async fn automerge_sync_metrics(&self) -> Result<AutomergeSyncMetrics, DatabaseError> {
    let docs = db_automerge::collect_documents(self.engine.store().inner())
      .await
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let (document_count, total_document_bytes) = db_automerge::automerge_metrics(&docs);
    Ok(AutomergeSyncMetrics {
      document_count,
      total_document_bytes,
    })
  }
}

#[cfg(feature = "redb")]
impl Database<RedbEngineStore> {
  pub async fn open_in_redb(
    path: impl AsRef<Path>,
    _table_name: &'static str,
  ) -> Result<Self, DatabaseError> {
    let store = REDBNamedBTree::<EngineKey, Vec<u8>, EngineKeyCodec>::open_with_codecs(path)
      .map_err(|e| DatabaseError::Engine(format!("{e}")))?;
    let engine = EngineDatabase::new(store.into_engine_store());
    Ok(Self { engine })
  }
}

impl<S> Database<S>
where
  S: FacadeStore,
{
  pub fn from_store(store: S) -> Self {
    let engine = EngineDatabase::new(store.into_engine_store());
    Self { engine }
  }

  pub async fn open_with_store(store: S) -> Result<Self, DatabaseError> {
    let engine = EngineDatabase::open(store.into_engine_store()).await?;
    Ok(Self { engine })
  }

  pub fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    self.engine.describe_table(table_name)
  }

  /// Register a table schema with the engine.
  pub async fn register_table(
    &mut self,
    schema: TableSchema,
    if_not_exists: bool,
  ) -> Result<(), DatabaseError> {
    self.engine.register_table(schema, if_not_exists).await?;
    Ok(())
  }

  pub async fn drop_table(
    &mut self,
    table_name: &str,
    if_exists: bool,
  ) -> Result<(), DatabaseError> {
    self.engine.drop_table(table_name, if_exists).await?;
    Ok(())
  }

  pub async fn register_index(&mut self, schema: IndexSchema) -> Result<(), DatabaseError> {
    self.engine.register_index(schema).await?;
    Ok(())
  }

  pub async fn drop_index(&mut self, index_name: &str) -> Result<(), DatabaseError> {
    self.engine.drop_index(index_name).await?;
    Ok(())
  }

  /// Execute an `EngineQuery` directly against the engine.
  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, DatabaseError> {
    let result = self.engine.execute(query).await?;
    Ok(result)
  }

  /// Execute an `EngineQuery` and deserialize results into typed structs.
  ///
  /// # Arguments
  ///
  /// * `query` - The engine query to execute
  /// * `table_name` - The name of the table being queried (used to fetch schema)
  ///
  /// # Returns
  ///
  /// A vector of typed structs, one per result row, or an error if query or deserialization fails
  ///
  /// # Example
  ///
  /// ```ignore
  /// use serde::Deserialize;
  /// use db_facade::FromRow;
  ///
  /// #[derive(Deserialize)]
  /// struct User {
  ///   id: i64,
  ///   name: String,
  /// }
  ///
  /// let query = EngineQuery::select_simple("users".to_string(), vec![0, 1], None);
  /// let users: Vec<User> = db.execute_query_typed::<User>(query, "users").await?;
  /// ```
  pub async fn execute_query_typed<T: FromRow>(
    &self,
    query: EngineQuery,
    table_name: &str,
  ) -> Result<Vec<T>, DatabaseError> {
    let result = self.execute_query(query).await?;
    if !result.columns.is_empty() {
      return result
        .into_typed_named::<T>()
        .map_err(|e| DatabaseError::Other(e.to_string()));
    }

    let schema = self
      .engine
      .describe_table(table_name)
      .ok_or_else(|| DatabaseError::Other(format!("Table '{}' not found", table_name)))?;
    result
      .into_typed::<T>(&schema)
      .map_err(|e| DatabaseError::Other(e.to_string()))
  }

  /// Execute an `EngineQuery` and deserialize results using result column metadata.
  ///
  /// This is useful for projections, joins, and grouped queries whose output columns
  /// do not directly align to a single table schema.
  pub async fn execute_query_typed_named<T: FromRow>(
    &self,
    query: EngineQuery,
  ) -> Result<Vec<T>, DatabaseError> {
    let result = self.execute_query(query).await?;
    result
      .into_typed_named::<T>()
      .map_err(|e| DatabaseError::Other(e.to_string()))
  }

  /// Execute a SQL string using the database schema catalog.
  pub async fn execute_sql(&mut self, sql: &str) -> Result<EngineResult, DatabaseError> {
    match parse_and_translate_statement(sql, self) {
      Ok(CanonicalStatement::Query(query)) => self.execute_query(query).await,
      Ok(CanonicalStatement::Ddl(DdlOp::CreateTable(schema, if_not_exists))) => {
        self.register_table(schema, if_not_exists).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropTable(name, if_exists))) => {
        self.engine.drop_table(&name, if_exists).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::CreateIndex(schema))) => {
        self.engine.register_index(schema).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropIndex(name))) => {
        self.engine.drop_index(&name).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Err(e) => Err(DatabaseError::Other(format!("{e}"))),
    }
  }

  /// Execute a SQL string using the database schema catalog with bound parameters.
  pub async fn execute_sql_with_params(
    &mut self,
    sql: &str,
    params: &SqlParams,
  ) -> Result<EngineResult, DatabaseError> {
    match parse_and_translate_statement_with_params(sql, self, params) {
      Ok(CanonicalStatement::Query(query)) => self.execute_query(query).await,
      Ok(CanonicalStatement::Ddl(DdlOp::CreateTable(schema, if_not_exists))) => {
        self.register_table(schema, if_not_exists).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropTable(name, if_exists))) => {
        self.engine.drop_table(&name, if_exists).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::CreateIndex(schema))) => {
        self.engine.register_index(schema).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Ok(CanonicalStatement::Ddl(DdlOp::DropIndex(name))) => {
        self.engine.drop_index(&name).await?;
        Ok(EngineResult::new(Vec::new()))
      }
      Err(e) => Err(DatabaseError::Other(format!("{e}"))),
    }
  }

  /// Convenience: run a closure in a transaction context.
  pub async fn transaction<F, Fut, T>(&self, f: F) -> Result<T, DatabaseError>
  where
    F: FnOnce(&mut Transaction<'_, S>) -> Fut,
    Fut: core::future::Future<Output = Result<T, DatabaseError>>,
  {
    let mut tx = Transaction {
      inner: self.engine.transaction(),
    };
    let out = f(&mut tx).await;
    match out {
      Ok(v) => {
        tx.inner.commit().await?;
        Ok(v)
      }
      Err(e) => {
        let _ = tx.inner.rollback().await;
        Err(e)
      }
    }
  }

  /// Open a read-only transaction.
  pub fn read_transaction(&self) -> ReadTransaction<'_, S> {
    ReadTransaction { db: self }
  }

  /// Subscribe to an `EngineQuery` with optional scope.
  pub async fn subscribe_query(
    &self,
    query: EngineQuery,
    subscriber: std::sync::Arc<dyn Subscriber>,
    scope: Option<SyncScope>,
  ) -> Result<SubscriptionId, DatabaseError> {
    let scope = scope.unwrap_or_default();
    self
      .engine
      .subscribe(query, &scope, subscriber)
      .await
      .map_err(Into::into)
  }

  /// Subscribe to a SQL SELECT string with optional scope.
  /// Returns an error if the SQL is not a SELECT statement.
  pub async fn subscribe_sql(
    &self,
    sql: &str,
    subscriber: std::sync::Arc<dyn Subscriber>,
    scope: Option<SyncScope>,
  ) -> Result<SubscriptionId, DatabaseError> {
    let query = match parse_and_translate_statement(sql, self) {
      Ok(CanonicalStatement::Query(q)) => q,
      Ok(_) => {
        return Err(DatabaseError::Other(
          "subscribe_sql only accepts SELECT statements".into(),
        ));
      }
      Err(e) => return Err(DatabaseError::Other(format!("{e}"))),
    };
    self.subscribe_query(query, subscriber, scope).await
  }

  /// Subscribe to a SQL SELECT string with optional scope and bound parameters.
  /// Returns an error if the SQL is not a SELECT statement.
  pub async fn subscribe_sql_with_params(
    &self,
    sql: &str,
    params: &SqlParams,
    subscriber: std::sync::Arc<dyn Subscriber>,
    scope: Option<SyncScope>,
  ) -> Result<SubscriptionId, DatabaseError> {
    let query = match parse_and_translate_statement_with_params(sql, self, params) {
      Ok(CanonicalStatement::Query(q)) => q,
      Ok(_) => {
        return Err(DatabaseError::Other(
          "subscribe_sql_with_params only accepts SELECT statements".into(),
        ));
      }
      Err(e) => return Err(DatabaseError::Other(format!("{e}"))),
    };
    self.subscribe_query(query, subscriber, scope).await
  }

  /// Subscribe to a query (unrestricted).
  pub async fn subscribe_unrestricted(
    &self,
    query: EngineQuery,
    subscriber: std::sync::Arc<dyn Subscriber>,
  ) -> Result<SubscriptionId, DatabaseError> {
    self.subscribe_query(query, subscriber, None).await
  }

  /// Unsubscribe from a subscription.
  pub async fn unsubscribe(&self, id: SubscriptionId) -> Result<(), DatabaseError> {
    self.engine.unsubscribe(id).await.map_err(Into::into)
  }

  /// Execute a query with scope.
  pub async fn execute_with_scope(
    &self,
    query: EngineQuery,
    scope: &SyncScope,
  ) -> Result<EngineResult, DatabaseError> {
    self
      .engine
      .execute_with_scope(query, scope)
      .await
      .map_err(Into::into)
  }
}

impl<'db, S> Transaction<'db, S>
where
  S: FacadeStore,
{
  pub async fn insert_row(&mut self, table: &str, row: Row) -> Result<(), DatabaseError> {
    self.inner.insert_row(table, row).await?;
    Ok(())
  }

  /// Execute a SQL string inside this transaction. Supports INSERT/UPDATE/DELETE.
  /// SELECT is not supported in a write transaction — use `Database::execute_sql`.
  pub async fn execute_sql(
    &mut self,
    resolver: &dyn SchemaResolver,
    sql: &str,
  ) -> Result<EngineResult, DatabaseError> {
    let query =
      parse_and_translate(sql, resolver).map_err(|e| DatabaseError::Other(format!("{e}")))?;
    match query {
      EngineQuery::Insert {
        table,
        row,
        returning,
      } => self
        .inner
        .insert_row_with_returning(&table, row, returning)
        .await
        .map_err(Into::into),
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => {
        let result = self
          .inner
          .update_rows_with_sources_and_returning(
            &table,
            assignments,
            predicate,
            joins,
            from_tables,
            returning,
          )
          .await?;
        Ok(result)
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => self
        .inner
        .delete_rows_with_returning(&table, predicate, returning)
        .await
        .map_err(Into::into),
      EngineQuery::Select { .. } => Err(DatabaseError::Other(
        "SELECT inside transaction not supported; use Database::execute_sql instead".into(),
      )),
    }
  }

  /// Execute a SQL string inside this transaction with bound parameters.
  pub async fn execute_sql_with_params(
    &mut self,
    resolver: &dyn SchemaResolver,
    sql: &str,
    params: &SqlParams,
  ) -> Result<EngineResult, DatabaseError> {
    let query = parse_and_translate_with_params(sql, resolver, params)
      .map_err(|e| DatabaseError::Other(format!("{e}")))?;
    match query {
      EngineQuery::Insert {
        table,
        row,
        returning,
      } => self
        .inner
        .insert_row_with_returning(&table, row, returning)
        .await
        .map_err(Into::into),
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => {
        let result = self
          .inner
          .update_rows_with_sources_and_returning(
            &table,
            assignments,
            predicate,
            joins,
            from_tables,
            returning,
          )
          .await?;
        Ok(result)
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => self
        .inner
        .delete_rows_with_returning(&table, predicate, returning)
        .await
        .map_err(Into::into),
      EngineQuery::Select { .. } => Err(DatabaseError::Other(
        "SELECT inside transaction not supported; use Database::execute_sql instead".into(),
      )),
    }
  }

  pub async fn commit(self) -> Result<(), DatabaseError> {
    self.inner.commit().await?;
    Ok(())
  }

  pub async fn rollback(self) -> Result<(), DatabaseError> {
    self.inner.rollback().await?;
    Ok(())
  }
}

impl<'db, S> ReadTransaction<'db, S>
where
  S: FacadeStore,
{
  /// Execute a SQL string inside this read-only transaction.
  /// Only SELECT statements are supported.
  pub async fn execute_sql(
    &self,
    resolver: &dyn SchemaResolver,
    sql: &str,
  ) -> Result<EngineResult, DatabaseError> {
    let query =
      parse_and_translate(sql, resolver).map_err(|e| DatabaseError::Other(format!("{e}")))?;
    match query {
      EngineQuery::Select { .. } => self.db.execute_query(query).await,
      _ => Err(DatabaseError::Other(
        "read transaction supports only SELECT statements".into(),
      )),
    }
  }

  /// Execute a SQL string inside this read-only transaction with bound parameters.
  /// Only SELECT statements are supported.
  pub async fn execute_sql_with_params(
    &self,
    resolver: &dyn SchemaResolver,
    sql: &str,
    params: &SqlParams,
  ) -> Result<EngineResult, DatabaseError> {
    let query = parse_and_translate_with_params(sql, resolver, params)
      .map_err(|e| DatabaseError::Other(format!("{e}")))?;
    match query {
      EngineQuery::Select { .. } => self.db.execute_query(query).await,
      _ => Err(DatabaseError::Other(
        "read transaction supports only SELECT statements".into(),
      )),
    }
  }

  pub async fn execute_query(&self, query: EngineQuery) -> Result<EngineResult, DatabaseError> {
    match query {
      EngineQuery::Select { .. } => self.db.execute_query(query).await,
      _ => Err(DatabaseError::Other(
        "read transaction supports only SELECT queries".into(),
      )),
    }
  }
}
