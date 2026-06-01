#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use futures::{StreamExt, pin_mut};

use crate::{
  BTree, BTreeManager, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor, Column, Engine,
  EngineError, EngineResult, Expr, ExprValue, Query, Row, UpdateAssignment, Value,
  engine::EngineBTreeDefinition,
};

pub async fn execute<M>(engine: &Engine<M>, query: Query) -> EngineResult<Vec<Row>>
where
  M: BTreeManager,
{
  match query {
    Query::Select {
      table,
      projection,
      predicate,
      ..
    } => execute_select(engine, &table, &projection, predicate.as_ref()).await,
    Query::Insert { table, row, .. } => execute_insert(engine, &table, row).await,
    Query::Update {
      table,
      assignments,
      predicate,
      ..
    } => execute_update(engine, &table, &assignments, predicate.as_ref()).await,
    Query::Delete {
      table, predicate, ..
    } => execute_delete(engine, &table, predicate.as_ref()).await,
  }
}

async fn execute_select<M>(
  engine: &Engine<M>,
  table: &str,
  projection: &[Column],
  predicate: Option<&Expr>,
) -> EngineResult<Vec<Row>>
where
  M: BTreeManager,
{
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.get(&definition).await?;

  let mut rows = Vec::new();
  let stream = btree.range(..);
  pin_mut!(stream);

  while let Some(entry) = stream.next().await {
    let (_key, row) = entry?;

    if matches_predicate(predicate, &row, table)? {
      rows.push(project_row(&row, table, projection)?);
    }
  }

  Ok(rows)
}

async fn execute_insert<M>(engine: &Engine<M>, table: &str, row: Row) -> EngineResult<Vec<Row>>
where
  M: BTreeManager,
{
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.get(&definition).await?;
  let mut tx = btree.transaction().await?;

  let key = primary_key_from_row(&row)?;
  if let Err(error) = tx.insert(key, row).await {
    tx.rollback().await?;
    return Err(error.into());
  }

  tx.commit().await?;
  Ok(Vec::new())
}

async fn execute_update<M>(
  engine: &Engine<M>,
  table: &str,
  assignments: &[UpdateAssignment],
  predicate: Option<&Expr>,
) -> EngineResult<Vec<Row>>
where
  M: BTreeManager,
{
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.get(&definition).await?;
  let mut tx = btree.transaction().await?;

  let mut to_update = Vec::new();
  {
    let stream = tx.range(..);
    pin_mut!(stream);

    while let Some(entry) = stream.next().await {
      let (key, row) = entry?;
      if matches_predicate(predicate, &row, table)? {
        to_update.push((key, row));
      }
    }
  }

  for (old_key, old_row) in to_update {
    let mut new_row = old_row.clone();

    for assignment in assignments {
      if assignment.column.table != table {
        tx.rollback().await?;
        return Err(EngineError::Unsupported(
          "update assignment references another table",
        ));
      }

      let index = usize::from(assignment.column.column_index);
      if index >= new_row.len() {
        tx.rollback().await?;
        return Err(EngineError::InvalidQuery("update assignment out of bounds"));
      }

      new_row[index] = eval_expr_value_for_row(&assignment.value, &old_row, table)?;
    }

    let new_key = primary_key_from_row(&new_row)?;
    if new_key != old_key
      && let Err(error) = tx.remove(old_key).await
    {
      tx.rollback().await?;
      return Err(error.into());
    }

    if let Err(error) = tx.insert(new_key, new_row).await {
      tx.rollback().await?;
      return Err(error.into());
    }
  }

  tx.commit().await?;
  Ok(Vec::new())
}

async fn execute_delete<M>(
  engine: &Engine<M>,
  table: &str,
  predicate: Option<&Expr>,
) -> EngineResult<Vec<Row>>
where
  M: BTreeManager,
{
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.get(&definition).await?;
  let mut tx = btree.transaction().await?;

  let mut keys = Vec::new();
  {
    let stream = tx.range(..);
    pin_mut!(stream);

    while let Some(entry) = stream.next().await {
      let (key, row) = entry?;
      if matches_predicate(predicate, &row, table)? {
        keys.push(key);
      }
    }
  }

  for key in keys {
    if let Err(error) = tx.remove(key).await {
      tx.rollback().await?;
      return Err(error.into());
    }
  }

  tx.commit().await?;
  Ok(Vec::new())
}

fn project_row(row: &Row, table: &str, projection: &[Column]) -> EngineResult<Row> {
  if projection.is_empty() {
    return Ok(row.clone());
  }

  let mut projected = Vec::with_capacity(projection.len());
  for column in projection {
    if column.table != table {
      return Err(EngineError::Unsupported(
        "projection references another table",
      ));
    }

    let index = usize::from(column.column_index);
    let value = row
      .get(index)
      .ok_or(EngineError::InvalidQuery("projection out of bounds"))?;
    projected.push(value.clone());
  }

  Ok(projected)
}

fn matches_predicate(predicate: Option<&Expr>, row: &Row, table: &str) -> EngineResult<bool> {
  match predicate {
    Some(expr) => eval_expr(expr, row, table),
    None => Ok(true),
  }
}

fn eval_expr(expr: &Expr, row: &Row, table: &str) -> EngineResult<bool> {
  match expr {
    Expr::Equals(left, right) => {
      Ok(eval_expr_value_for_row(left, row, table)? == eval_expr_value_for_row(right, row, table)?)
    }
    Expr::NotEquals(left, right) => {
      Ok(eval_expr_value_for_row(left, row, table)? != eval_expr_value_for_row(right, row, table)?)
    }
    Expr::LessThan(left, right) => {
      Ok(eval_expr_value_for_row(left, row, table)? < eval_expr_value_for_row(right, row, table)?)
    }
    Expr::LessThanOrEquals(left, right) => {
      Ok(eval_expr_value_for_row(left, row, table)? <= eval_expr_value_for_row(right, row, table)?)
    }
    Expr::GreaterThan(left, right) => {
      Ok(eval_expr_value_for_row(left, row, table)? > eval_expr_value_for_row(right, row, table)?)
    }
    Expr::GreaterThanOrEquals(left, right) => {
      Ok(eval_expr_value_for_row(left, row, table)? >= eval_expr_value_for_row(right, row, table)?)
    }
    Expr::IsNull(value) => Ok(matches!(
      eval_expr_value_for_row(value, row, table)?,
      Value::Null
    )),
    Expr::IsNotNull(value) => Ok(!matches!(
      eval_expr_value_for_row(value, row, table)?,
      Value::Null
    )),
    Expr::InList {
      expr,
      list,
      negated,
    } => {
      let value = eval_expr_value_for_row(expr, row, table)?;
      let contains = list.iter().any(|item| item == &value);
      Ok(if *negated { !contains } else { contains })
    }
    Expr::InSubquery { .. } => Err(EngineError::Unsupported("in-subquery predicate")),
    Expr::Like {
      expr,
      pattern,
      negated,
    } => {
      let expr_value = eval_expr_value_for_row(expr, row, table)?;
      let pattern_value = eval_expr_value_for_row(pattern, row, table)?;
      let matches = match (expr_value, pattern_value) {
        (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
        _ => false,
      };
      Ok(if *negated { !matches } else { matches })
    }
    Expr::And(left, right) => Ok(eval_expr(left, row, table)? && eval_expr(right, row, table)?),
    Expr::Or(left, right) => Ok(eval_expr(left, row, table)? || eval_expr(right, row, table)?),
    Expr::Not(inner) => Ok(!eval_expr(inner, row, table)?),
  }
}

fn eval_expr_value_for_row(expr: &ExprValue, row: &Row, table: &str) -> EngineResult<Value> {
  match expr {
    ExprValue::Value(value) => Ok(value.clone()),
    ExprValue::Column(column) => {
      if column.table != table {
        return Err(EngineError::Unsupported(
          "expression references another table",
        ));
      }

      let index = usize::from(column.column_index);
      let value = row
        .get(index)
        .ok_or(EngineError::InvalidQuery("expression column out of bounds"))?;
      Ok(value.clone())
    }
  }
}

fn primary_key_from_row(row: &Row) -> EngineResult<Vec<Value>> {
  let value = row.first().ok_or(EngineError::InvalidQuery(
    "row must contain at least one value",
  ))?;
  Ok(vec![value.clone()])
}

fn like_matches(value: &str, pattern: &str) -> bool {
  like_matches_bytes(value.as_bytes(), pattern.as_bytes())
}

fn like_matches_bytes(value: &[u8], pattern: &[u8]) -> bool {
  if pattern.is_empty() {
    return value.is_empty();
  }

  match pattern[0] {
    b'%' => {
      if like_matches_bytes(value, &pattern[1..]) {
        return true;
      }

      if value.is_empty() {
        return false;
      }

      like_matches_bytes(&value[1..], pattern)
    }
    b'_' => {
      if value.is_empty() {
        return false;
      }
      like_matches_bytes(&value[1..], &pattern[1..])
    }
    ch => {
      if value.first().copied() != Some(ch) {
        return false;
      }
      like_matches_bytes(&value[1..], &pattern[1..])
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn like_matches_handles_wildcards() {
    assert!(like_matches("alice", "a%"));
    assert!(like_matches("alice", "a_i_e"));
    assert!(!like_matches("alice", "b%"));
  }
}
