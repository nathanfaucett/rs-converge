use super::catalog::EngineCatalog;
use super::join_builder::{
  JoinedRowState, apply_join_clauses, build_join_template, collect_table_rows_map, collect_tables,
  expand_with_from_tables, seed_joined_row_states,
};
use super::transaction_lifecycle::TransactionLifecycle;
use crate::predicate::{EvalContext, PredicateEvaluator};
use crate::store_adapter::{
  EngineStore, EngineStoreTransaction, collect_table_rows, delete_row, find_conflicting_index_entry,
};
use crate::{
  ChangeEvent, ChangeListenerRegistry, EngineError, EngineRow, EngineValue, IndexSchema,
  PrimaryKey, query::JoinClause, query::QualifiedPredicate, query::UpdateAssignment,
  query::UpdateValueExpr,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

async fn ensure_indexes_unique<TX>(
  tx: &mut TX,
  indexes: &[IndexSchema],
  row: &EngineRow,
  row_pk: &PrimaryKey,
) -> Result<(), EngineError>
where
  TX: crate::store_adapter::EngineStoreTransaction,
{
  for index in indexes.iter().filter(|index| index.unique) {
    let index_key = index.key_for(row)?;
    if find_conflicting_index_entry(tx, index, &index_key, row_pk)
      .await?
      .is_some()
    {
      return Err(EngineError::UniqueIndexViolation(index.name.clone()));
    }
  }
  Ok(())
}

async fn insert_all_index_entries<TX>(
  tx: &mut TX,
  indexes: &[IndexSchema],
  row: &EngineRow,
  primary_key: &PrimaryKey,
) -> Result<(), EngineError>
where
  TX: crate::store_adapter::EngineStoreTransaction,
{
  for index in indexes {
    let index_key = index.key_for(row)?;
    tx.insert_index_entry(index, &index_key, primary_key)
      .await?;
  }
  Ok(())
}

#[derive(Debug)]
pub(crate) struct EngineWriteTxn<'db, S>
where
  S: EngineStore,
  S::Transaction: EngineStoreTransaction,
{
  pub(crate) store: &'db S,
  pub(crate) catalog: &'db EngineCatalog,
  pub(crate) lifecycle: TransactionLifecycle<S::Transaction>,
  pub(crate) change_listener_registry: Arc<ChangeListenerRegistry>,
  pub(crate) pending_events: Vec<ChangeEvent>,
}

impl<'db, S> EngineWriteTxn<'db, S>
where
  S: EngineStore,
  S::Transaction: EngineStoreTransaction,
{
  pub(crate) async fn transaction(&mut self) -> Result<&mut S::Transaction, EngineError> {
    self
      .lifecycle
      .transaction(|| self.store.engine_transaction())
      .await
  }

  pub(crate) async fn insert(
    &mut self,
    table_name: &str,
    row: EngineRow,
  ) -> Result<(), EngineError> {
    let table = self.catalog.table(table_name)?;
    table.validate_row(&row)?;
    let pk = table.primary_key(&row)?;
    let indexes = self.catalog.indexes_for_table(table_name);
    let event = ChangeEvent::RowInserted {
      table: table_name.to_string(),
      pk,
      row: row.clone(),
    };

    let tx = self.transaction().await?;
    if tx.get_table_row(table_name, &pk).await?.is_some() {
      return Err(EngineError::DuplicatePrimaryKey(pk));
    }

    ensure_indexes_unique(tx, &indexes, &row, &pk).await?;

    tx.insert_table_row(table_name, pk, row.clone()).await?;

    insert_all_index_entries(tx, &indexes, &row, &pk).await?;

    self.pending_events.push(event);

    Ok(())
  }

  pub(crate) async fn insert_returning(
    &mut self,
    table_name: &str,
    row: EngineRow,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<Vec<EngineRow>, EngineError> {
    let inserted_row = row.clone();
    self.insert(table_name, row).await?;

    if let Some(expressions) = returning.as_ref() {
      return Ok(vec![Self::evaluate_returning_row(
        table_name,
        &inserted_row,
        expressions,
      )?]);
    }

    Ok(Vec::new())
  }

  pub(crate) async fn delete(
    &mut self,
    table_name: &str,
    predicate: Option<QualifiedPredicate>,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<Vec<EngineRow>, EngineError> {
    self.catalog.table(table_name)?;
    let indexes = self.catalog.indexes_for_table(table_name);
    let mut pending_events = Vec::new();
    let mut returning_rows: Vec<EngineRow> = Vec::new();

    {
      let tx = self.transaction().await?;
      let rows = collect_table_rows(tx, table_name, predicate).await?;

      for (primary_key, row) in rows {
        if let Some(expressions) = returning.as_ref() {
          returning_rows.push(Self::evaluate_returning_row(table_name, &row, expressions)?);
        }
        delete_row(tx, table_name, &primary_key, &row, &indexes).await?;

        pending_events.push(ChangeEvent::RowDeleted {
          table: table_name.to_string(),
          pk: primary_key,
          row,
        });
      }
    }

    self.pending_events.extend(pending_events);

    Ok(returning_rows)
  }

  pub(crate) async fn update(
    &mut self,
    table_name: &str,
    assignments: Vec<UpdateAssignment>,
    predicate: Option<QualifiedPredicate>,
    joins: Vec<JoinClause>,
    from_tables: Vec<String>,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<Vec<EngineRow>, EngineError> {
    let table = self.catalog.table(table_name)?.clone();
    if assignments.is_empty() {
      return Ok(Vec::new());
    }

    let indexes = self.catalog.indexes_for_table(table_name);
    let mut pending_events = Vec::new();
    let mut returning_rows: Vec<EngineRow> = Vec::new();

    {
      let tx = self.transaction().await?;
      let rows = if joins.is_empty() && from_tables.is_empty() {
        collect_table_rows(tx, table_name, predicate)
          .await?
          .into_iter()
          .map(|(pk, row)| (pk, row, None))
          .collect::<Vec<_>>()
      } else {
        Self::collect_join_update_rows(
          tx,
          &table,
          table_name,
          &joins,
          &from_tables,
          predicate.as_ref(),
        )
        .await?
      };

      for (old_pk, old_row, joined_state) in rows {
        let updated_row =
          Self::apply_assignments(&old_row, &assignments, table_name, joined_state.as_ref())?;
        table.validate_row(&updated_row)?;
        let new_pk = table.primary_key(&updated_row)?;

        if new_pk != old_pk && tx.get_table_row(table_name, &new_pk).await?.is_some() {
          return Err(EngineError::DuplicatePrimaryKey(new_pk));
        }

        delete_row(tx, table_name, &old_pk, &old_row, &indexes).await?;
        ensure_indexes_unique(tx, &indexes, &updated_row, &new_pk).await?;

        tx.insert_table_row(table_name, new_pk, updated_row.clone())
          .await?;

        insert_all_index_entries(tx, &indexes, &updated_row, &new_pk).await?;

        pending_events.push(ChangeEvent::RowUpdated {
          table: table_name.to_string(),
          pk: new_pk,
          old_row,
          new_row: updated_row.clone(),
        });

        if let Some(expressions) = returning.as_ref() {
          returning_rows.push(Self::evaluate_returning_row(
            table_name,
            &updated_row,
            expressions,
          )?);
        }
      }
    }

    self.pending_events.extend(pending_events);

    Ok(returning_rows)
  }

  fn evaluate_returning_row(
    table_name: &str,
    row: &EngineRow,
    expressions: &[UpdateValueExpr],
  ) -> Result<EngineRow, EngineError> {
    let mut output: EngineRow = Vec::with_capacity(expressions.len());
    for expression in expressions {
      output.push(Self::evaluate_update_expr(
        row, expression, table_name, None,
      )?);
    }
    Ok(output)
  }

  fn apply_assignments(
    row: &EngineRow,
    assignments: &[UpdateAssignment],
    target_table: &str,
    joined_state: Option<&JoinedRowState>,
  ) -> Result<EngineRow, EngineError> {
    let mut updated = row.clone();

    for assignment in assignments {
      let value = Self::evaluate_update_expr(row, &assignment.value, target_table, joined_state)?;
      let cell = updated.get_mut(assignment.column_index).ok_or_else(|| {
        EngineError::SchemaMismatch(format!(
          "update index {} is out of bounds",
          assignment.column_index
        ))
      })?;
      *cell = value;
    }

    Ok(updated)
  }

  fn evaluate_update_expr(
    row: &EngineRow,
    expr: &UpdateValueExpr,
    target_table: &str,
    joined_state: Option<&JoinedRowState>,
  ) -> Result<EngineValue, EngineError> {
    match expr {
      UpdateValueExpr::Value(value) => Ok(value.clone()),
      UpdateValueExpr::Column(column) => {
        if column.table == target_table {
          return row.get(column.column_index).cloned().ok_or_else(|| {
            EngineError::SchemaMismatch(format!(
              "column index {} is out of bounds",
              column.column_index
            ))
          });
        }

        let state = joined_state.ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "column {}.{} requires join context",
            column.table, column.column_index
          ))
        })?;

        let joined_row = state
          .get(&column.table)
          .and_then(|entry| entry.as_ref())
          .ok_or_else(|| {
            EngineError::SchemaMismatch(format!(
              "join table {} has no row for expression",
              column.table
            ))
          })?;

        joined_row.get(column.column_index).cloned().ok_or_else(|| {
          EngineError::SchemaMismatch(format!(
            "column index {} is out of bounds",
            column.column_index
          ))
        })
      }
      UpdateValueExpr::Add(left, right) => Self::evaluate_numeric_binary(
        row,
        left,
        right,
        "+",
        |l, r| l + r,
        target_table,
        joined_state,
      ),
      UpdateValueExpr::Subtract(left, right) => Self::evaluate_numeric_binary(
        row,
        left,
        right,
        "-",
        |l, r| l - r,
        target_table,
        joined_state,
      ),
      UpdateValueExpr::Multiply(left, right) => Self::evaluate_numeric_binary(
        row,
        left,
        right,
        "*",
        |l, r| l * r,
        target_table,
        joined_state,
      ),
      UpdateValueExpr::Divide(left, right) => {
        let left_value = Self::evaluate_update_expr(row, left, target_table, joined_state)?;
        let right_value = Self::evaluate_update_expr(row, right, target_table, joined_state)?;
        if matches!(&right_value, EngineValue::Integer(0)) {
          return Err(EngineError::TypeMismatch("division by zero".into()));
        }
        if matches!(&right_value, EngineValue::Float(v) if *v == 0.0) {
          return Err(EngineError::TypeMismatch("division by zero".into()));
        }
        let left_number = Self::engine_value_to_f64(&left_value, "/")?;
        let right_number = Self::engine_value_to_f64(&right_value, "/")?;
        Ok(EngineValue::Float(left_number / right_number))
      }
    }
  }

  fn evaluate_numeric_binary<F>(
    row: &EngineRow,
    left: &UpdateValueExpr,
    right: &UpdateValueExpr,
    op: &str,
    apply: F,
    target_table: &str,
    joined_state: Option<&JoinedRowState>,
  ) -> Result<EngineValue, EngineError>
  where
    F: Fn(f64, f64) -> f64,
  {
    let left_value = Self::evaluate_update_expr(row, left, target_table, joined_state)?;
    let right_value = Self::evaluate_update_expr(row, right, target_table, joined_state)?;

    if left_value == EngineValue::Null || right_value == EngineValue::Null {
      return Ok(EngineValue::Null);
    }

    let left_number = Self::engine_value_to_f64(&left_value, op)?;
    let right_number = Self::engine_value_to_f64(&right_value, op)?;
    let output = apply(left_number, right_number);

    if let (EngineValue::Integer(_), EngineValue::Integer(_)) = (&left_value, &right_value)
      && op != "/"
    {
      return Ok(EngineValue::Integer(output as i64));
    }

    Ok(EngineValue::Float(output))
  }

  fn engine_value_to_f64(value: &EngineValue, op: &str) -> Result<f64, EngineError> {
    match value {
      EngineValue::Integer(number) => Ok(*number as f64),
      EngineValue::Float(number) => Ok(*number),
      other => Err(EngineError::TypeMismatch(format!(
        "operator {op} requires numeric values, got {other:?}"
      ))),
    }
  }

  async fn collect_join_update_rows(
    tx: &mut S::Transaction,
    table: &crate::TableSchema,
    table_name: &str,
    joins: &[JoinClause],
    from_tables: &[String],
    predicate: Option<&QualifiedPredicate>,
  ) -> Result<Vec<(PrimaryKey, EngineRow, Option<JoinedRowState>)>, EngineError> {
    let all_tables = collect_tables(table_name, from_tables, joins);
    let table_rows_map = collect_table_rows_map::<S>(tx, &all_tables).await?;
    let template = build_join_template(&all_tables);
    let partial_results = seed_joined_row_states(table_name, &table_rows_map, &template);
    let partial_results = expand_with_from_tables(partial_results, from_tables, &table_rows_map)?;
    let mut partial_results =
      apply_join_clauses(joins, &table_rows_map, &template, partial_results)?;

    if let Some(pred) = predicate {
      let eval_ctx = EvalContext::empty();
      let evaluator = PredicateEvaluator::new(&eval_ctx);
      partial_results.retain(|partial| evaluator.matches_joined_row(pred, partial));
    }

    let mut matched: HashMap<PrimaryKey, (EngineRow, JoinedRowState)> = HashMap::new();
    let mut seen: HashSet<PrimaryKey> = HashSet::new();

    for partial in partial_results {
      let Some(Some(base_row)) = partial.get(table_name) else {
        continue;
      };

      let pk = table.primary_key(base_row)?;
      if seen.contains(&pk) {
        return Err(EngineError::SchemaMismatch(
          "UPDATE JOIN matched target row more than once".into(),
        ));
      }
      seen.insert(pk);
      matched.insert(pk, (base_row.clone(), partial));
    }

    Ok(
      matched
        .into_iter()
        .map(|(pk, (row, partial))| (pk, row, Some(partial)))
        .collect(),
    )
  }

  pub(crate) async fn commit(mut self) -> Result<Vec<ChangeEvent>, EngineError> {
    if let Some(tx) = self.lifecycle.take_for_commit() {
      tx.commit().await?;
    }
    let events = core::mem::take(&mut self.pending_events);
    for event in &events {
      self.change_listener_registry.emit(event.clone());
    }
    Ok(events)
  }

  pub(crate) async fn rollback(mut self) -> Result<(), EngineError> {
    if let Some(tx) = self.lifecycle.take_for_rollback() {
      tx.rollback().await?;
    }
    Ok(())
  }
}
