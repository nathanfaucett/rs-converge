#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  vec::Vec,
};

use futures::{StreamExt, pin_mut};

use db_btree::{
  BTree, BTreeFactory, BTreeManager, BTreeReadExecutor, BTreeTransaction, BTreeWriteExecutor,
};
use db_query::{
  DataDefinition, Query, QueryColumn, QueryExpr, QueryExprValue, QueryJoin, QueryJoinKind,
  QueryResult, QueryResultColumn, QuerySelectOptions, QueryTableIndex, QueryUpdateAssignment,
  Statement,
};
use db_schema::{ColumnSchemaIndex, DescribeSchema, TableSchema};
use db_value::{Row, Value};

use crate::engine::{Engine, EngineBTreeDefinition, EngineError, EngineResult};

pub async fn execute_statement<M, F>(
  engine: &Engine<M, F>,
  statement: Statement,
) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  match statement {
    Statement::Query(query) => execute_query(engine, query).await,
    Statement::DataDefinition(ddl) => execute_ddl(engine, ddl).await,
  }
}

pub async fn execute_query<M, F>(engine: &Engine<M, F>, query: Query) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  match query {
    Query::Select {
      tables,
      table_index,
      projection,
      predicate,
      options,
      ..
    } => {
      execute_select(
        engine,
        &tables,
        table_index,
        &projection,
        predicate.as_ref(),
        options.as_deref(),
      )
      .await
    }
    Query::Insert {
      tables,
      table_index,
      row,
      returning,
      ..
    } => execute_insert(engine, &tables, table_index, row, returning).await,
    Query::Update {
      tables,
      table_index,
      assignments,
      predicate,
      returning,
      ..
    } => {
      execute_update(
        engine,
        &tables,
        table_index,
        &assignments,
        predicate.as_ref(),
        returning,
      )
      .await
    }
    Query::Delete {
      tables,
      table_index,
      predicate,
      returning,
      ..
    } => execute_delete(engine, &tables, table_index, predicate.as_ref(), returning).await,
  }
}

async fn execute_ddl<M, F>(engine: &Engine<M, F>, ddl: DataDefinition) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  match ddl {
    DataDefinition::CreateTable {
      schema,
      if_not_exists: _,
    } => {
      engine.register_table_schema(&schema).await?;
      Ok(QueryResult::new(Vec::new()))
    }
    DataDefinition::CreateIndex {
      schema,
      if_not_exists: _,
    } => {
      engine.register_index_schema(&schema).await?;
      Ok(QueryResult::new(Vec::new()))
    }
    DataDefinition::DropTable { .. } | DataDefinition::DropIndex { .. } => {
      Err(EngineError::Unsupported("DDL operation not supported"))
    }
  }
}

async fn execute_select<M, F>(
  engine: &Engine<M, F>,
  tables: &[String],
  table_index: QueryTableIndex,
  projection: &[QueryColumn],
  predicate: Option<&QueryExpr>,
  options: Option<&QuerySelectOptions>,
) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  if options.map(|opts| opts.joins.is_empty()).unwrap_or(true) {
    let table = resolve_table_name(tables, table_index)?;
    let definition = EngineBTreeDefinition::from(table);
    let btree = engine.manager.entry(&definition).await?;

    let columns = build_query_result_columns(engine, table, table_index, projection).await?;
    let mut rows = Vec::new();
    let stream = btree.range(..);
    pin_mut!(stream);

    while let Some(entry) = stream.next().await {
      let (_key, row) = entry?;

      if matches_predicate(predicate, &row, table_index)? {
        rows.push(project_row(&row, table_index, projection)?);
      }
    }

    return Ok(QueryResult::new_with_columns(rows, columns));
  }

  let table_rows = collect_table_rows(engine, tables).await?;
  let mut contexts = Vec::new();
  for row in &table_rows[0] {
    let mut context = vec![None; tables.len()];
    context[0] = Some(row.clone());
    contexts.push(context);
  }

  if let Some(opts) = options {
    for join in &opts.joins {
      contexts = apply_join(&table_rows, contexts, join)?;
    }
  }

  let columns = build_query_result_columns_for_query(engine, tables, projection).await?;
  let mut rows = Vec::new();

  for context in contexts {
    if matches_context_predicate(predicate, &context)? {
      rows.push(project_join_row(&context, projection)?);
    }
  }

  Ok(QueryResult::new_with_columns(rows, columns))
}

async fn collect_table_rows<M, F>(
  engine: &Engine<M, F>,
  tables: &[String],
) -> EngineResult<Vec<Vec<Row>>>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let mut all_rows = Vec::with_capacity(tables.len());

  for table in tables {
    let definition = EngineBTreeDefinition::from(table.as_str());
    let btree = engine.manager.entry(&definition).await?;
    let mut rows = Vec::new();
    let stream = btree.range(..);
    pin_mut!(stream);

    while let Some(entry) = stream.next().await {
      let (_key, row) = entry?;
      rows.push(row);
    }

    all_rows.push(rows);
  }

  Ok(all_rows)
}

fn apply_join(
  table_rows: &[Vec<Row>],
  contexts: Vec<Vec<Option<Row>>>,
  join: &QueryJoin,
) -> EngineResult<Vec<Vec<Option<Row>>>> {
  let mut next_contexts = Vec::new();
  let joined_rows = &table_rows[join.table_index as usize];
  let mut matched_right = vec![false; joined_rows.len()];

  for existing_context in contexts.into_iter() {
    let mut matched = false;

    for (right_index, right_row) in joined_rows.iter().enumerate() {
      let mut candidate = existing_context.clone();
      candidate[join.table_index as usize] = Some(right_row.clone());

      if matches_context_predicate(Some(&join.on), &candidate)? {
        next_contexts.push(candidate);
        matched = true;
        matched_right[right_index] = true;
      }
    }

    if !matched && matches!(join.kind, QueryJoinKind::Left | QueryJoinKind::Full) {
      let mut candidate = existing_context.clone();
      candidate[join.table_index as usize] = None;
      next_contexts.push(candidate);
    }
  }

  if matches!(join.kind, QueryJoinKind::Right | QueryJoinKind::Full) {
    for (right_index, right_row) in joined_rows.iter().enumerate() {
      if !matched_right[right_index] {
        let mut candidate = vec![None; table_rows.len()];
        candidate[join.table_index as usize] = Some(right_row.clone());
        next_contexts.push(candidate);
      }
    }
  }

  Ok(next_contexts)
}

fn project_join_row(context: &[Option<Row>], projection: &[QueryColumn]) -> EngineResult<Row> {
  let mut projected = Vec::with_capacity(projection.len());

  for column in projection {
    let row = context
      .get(column.table_index as usize)
      .ok_or(EngineError::InvalidQuery("table index out of bounds"))?;

    if let Some(row) = row {
      let index = column.column_index as usize;
      projected.push(
        row
          .get(index)
          .ok_or(EngineError::InvalidQuery("projection out of bounds"))?
          .clone(),
      );
    } else {
      projected.push(Value::Null);
    }
  }

  Ok(projected)
}

fn matches_context_predicate(
  predicate: Option<&QueryExpr>,
  context: &[Option<Row>],
) -> EngineResult<bool> {
  match predicate {
    Some(expr) => eval_expr_context(expr, context),
    None => Ok(true),
  }
}

fn eval_expr_context(expr: &QueryExpr, context: &[Option<Row>]) -> EngineResult<bool> {
  match expr {
    QueryExpr::Equals(left, right) => {
      let left_value = eval_expr_value_for_row_context(left, context)?;
      let right_value = eval_expr_value_for_row_context(right, context)?;
      Ok(
        !matches!(left_value, Value::Null)
          && !matches!(right_value, Value::Null)
          && left_value == right_value,
      )
    }
    QueryExpr::NotEquals(left, right) => {
      let left_value = eval_expr_value_for_row_context(left, context)?;
      let right_value = eval_expr_value_for_row_context(right, context)?;
      Ok(
        !matches!(left_value, Value::Null)
          && !matches!(right_value, Value::Null)
          && left_value != right_value,
      )
    }
    QueryExpr::LessThan(left, right) => {
      let left_value = eval_expr_value_for_row_context(left, context)?;
      let right_value = eval_expr_value_for_row_context(right, context)?;
      Ok(
        !matches!(left_value, Value::Null)
          && !matches!(right_value, Value::Null)
          && left_value < right_value,
      )
    }
    QueryExpr::LessThanOrEquals(left, right) => {
      let left_value = eval_expr_value_for_row_context(left, context)?;
      let right_value = eval_expr_value_for_row_context(right, context)?;
      Ok(
        !matches!(left_value, Value::Null)
          && !matches!(right_value, Value::Null)
          && left_value <= right_value,
      )
    }
    QueryExpr::GreaterThan(left, right) => {
      let left_value = eval_expr_value_for_row_context(left, context)?;
      let right_value = eval_expr_value_for_row_context(right, context)?;
      Ok(
        !matches!(left_value, Value::Null)
          && !matches!(right_value, Value::Null)
          && left_value > right_value,
      )
    }
    QueryExpr::GreaterThanOrEquals(left, right) => {
      let left_value = eval_expr_value_for_row_context(left, context)?;
      let right_value = eval_expr_value_for_row_context(right, context)?;
      Ok(
        !matches!(left_value, Value::Null)
          && !matches!(right_value, Value::Null)
          && left_value >= right_value,
      )
    }
    QueryExpr::IsNull(value) => Ok(matches!(
      eval_expr_value_for_row_context(value, context)?,
      Value::Null
    )),
    QueryExpr::IsNotNull(value) => Ok(!matches!(
      eval_expr_value_for_row_context(value, context)?,
      Value::Null
    )),
    QueryExpr::And(left, right) => {
      Ok(eval_expr_context(left, context)? && eval_expr_context(right, context)?)
    }
    QueryExpr::Or(left, right) => {
      Ok(eval_expr_context(left, context)? || eval_expr_context(right, context)?)
    }
    QueryExpr::Not(inner) => Ok(!eval_expr_context(inner, context)?),
    QueryExpr::InList { .. } | QueryExpr::InSubquery { .. } | QueryExpr::Like { .. } => Err(
      EngineError::Unsupported("predicate not supported in join context"),
    ),
  }
}

fn eval_expr_value_for_row_context(
  expr: &QueryExprValue,
  context: &[Option<Row>],
) -> EngineResult<Value> {
  match expr {
    QueryExprValue::Value(value) => Ok(value.clone()),
    QueryExprValue::Column(column) => {
      let row = context
        .get(column.table_index as usize)
        .ok_or(EngineError::InvalidQuery("table index out of bounds"))?;

      if let Some(row) = row {
        let index = column.column_index as usize;
        let value = row
          .get(index)
          .ok_or(EngineError::InvalidQuery("expression column out of bounds"))?;
        Ok(value.clone())
      } else {
        Ok(Value::Null)
      }
    }
  }
}

async fn execute_insert<M, F>(
  engine: &Engine<M, F>,
  tables: &[String],
  table_index: QueryTableIndex,
  row: Row,
  returning: Option<Vec<QueryColumn>>,
) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let table = resolve_table_name(tables, table_index)?;
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.entry(&definition).await?;
  let mut tx = btree.transaction().await?;

  let row_clone = row.clone();
  let key = primary_key_from_row(&row)?;

  if let Some(returning_columns) = returning.as_deref() {
    let _ = build_query_result_columns(engine, table, table_index, returning_columns).await?;
  }

  if let Err(error) = tx.insert(key, row).await {
    tx.rollback().await?;
    return Err(error.into());
  }

  tx.commit().await?;
  execute_returning(
    engine,
    table,
    table_index,
    returning.as_deref(),
    vec![row_clone],
  )
  .await
}

async fn execute_update<M, F>(
  engine: &Engine<M, F>,
  tables: &[String],
  table_index: QueryTableIndex,
  assignments: &[QueryUpdateAssignment],
  predicate: Option<&QueryExpr>,
  returning: Option<Vec<QueryColumn>>,
) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let table = resolve_table_name(tables, table_index)?;
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.entry(&definition).await?;
  let mut tx = btree.transaction().await?;

  let returning_columns = returning.as_deref();
  if let Some(returning_columns) = returning_columns {
    let _ = build_query_result_columns(engine, table, table_index, returning_columns).await?;
  }

  let mut to_update = Vec::new();
  {
    let stream = tx.range(..);
    pin_mut!(stream);

    while let Some(entry) = stream.next().await {
      let (key, row) = entry?;
      if matches_predicate(predicate, &row, table_index)? {
        to_update.push((key, row));
      }
    }
  }

  let mut updated_rows = Vec::new();

  for (old_key, old_row) in to_update {
    let mut new_row = old_row.clone();

    for assignment in assignments {
      if assignment.column.table_index != table_index {
        tx.rollback().await?;
        return Err(EngineError::Unsupported(
          "update assignment references another table",
        ));
      }

      let index = assignment.column.column_index as usize;
      if index >= new_row.len() {
        tx.rollback().await?;
        return Err(EngineError::InvalidQuery("update assignment out of bounds"));
      }

      new_row[index] = eval_expr_value_for_row(&assignment.value, &old_row, table_index)?;
    }

    if returning_columns.is_some() {
      updated_rows.push(new_row.clone());
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
  execute_returning(engine, table, table_index, returning_columns, updated_rows).await
}

async fn execute_delete<M, F>(
  engine: &Engine<M, F>,
  tables: &[String],
  table_index: QueryTableIndex,
  predicate: Option<&QueryExpr>,
  returning: Option<Vec<QueryColumn>>,
) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let table = resolve_table_name(tables, table_index)?;
  let definition = EngineBTreeDefinition::from(table);
  let btree = engine.manager.entry(&definition).await?;
  let mut tx = btree.transaction().await?;

  let returning_columns = returning.as_deref();
  if let Some(returning_columns) = returning_columns {
    let _ = build_query_result_columns(engine, table, table_index, returning_columns).await?;
  }

  let mut keys = Vec::new();
  let mut deleted_rows = Vec::new();
  {
    let stream = tx.range(..);
    pin_mut!(stream);

    while let Some(entry) = stream.next().await {
      let (key, row) = entry?;
      if matches_predicate(predicate, &row, table_index)? {
        if returning_columns.is_some() {
          deleted_rows.push(row.clone());
        }
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
  execute_returning(engine, table, table_index, returning_columns, deleted_rows).await
}

async fn build_query_result_columns_for_query<M, F>(
  engine: &Engine<M, F>,
  tables: &[String],
  projection: &[QueryColumn],
) -> EngineResult<Vec<QueryResultColumn>>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let mut columns = Vec::with_capacity(projection.len());
  for column in projection {
    let table = resolve_table_name(tables, column.table_index)?;
    let schema = engine
      .describe_table(table)
      .await
      .ok_or(EngineError::InvalidQuery("unknown table"))?;
    columns.push(build_query_result_column(
      &schema,
      table,
      column.table_index,
      column,
    )?);
  }

  Ok(columns)
}

async fn build_query_result_columns<M, F>(
  engine: &Engine<M, F>,
  table: &str,
  table_index: QueryTableIndex,
  projection: &[QueryColumn],
) -> EngineResult<Vec<QueryResultColumn>>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let schema = engine
    .describe_table(table)
    .await
    .ok_or(EngineError::InvalidQuery("unknown table"))?;

  if projection.is_empty() {
    return Ok(
      schema
        .columns
        .iter()
        .enumerate()
        .map(|(column_index, column_schema)| QueryResultColumn {
          name: column_schema.name.clone(),
          source_table: Some(table.to_string()),
          source_column_index: Some(column_index as ColumnSchemaIndex),
        })
        .collect(),
    );
  }

  let mut columns = Vec::with_capacity(projection.len());
  for column in projection {
    columns.push(build_query_result_column(
      &schema,
      table,
      table_index,
      column,
    )?);
  }

  Ok(columns)
}

fn build_query_result_column(
  schema: &TableSchema,
  table: &str,
  table_index: QueryTableIndex,
  column: &QueryColumn,
) -> EngineResult<QueryResultColumn> {
  if column.table_index != table_index {
    return Err(EngineError::Unsupported(
      "projection references another table",
    ));
  }

  let index = column.column_index as usize;
  let column_schema = schema
    .columns
    .get(index)
    .ok_or(EngineError::InvalidQuery("projection out of bounds"))?;

  Ok(QueryResultColumn {
    name: column_schema.name.clone(),
    source_table: Some(table.to_string()),
    source_column_index: Some(column.column_index),
  })
}

async fn execute_returning<M, F>(
  engine: &Engine<M, F>,
  table: &str,
  table_index: QueryTableIndex,
  returning: Option<&[QueryColumn]>,
  rows: Vec<Row>,
) -> EngineResult<QueryResult>
where
  M: BTreeManager<F>,
  F: BTreeFactory,
{
  let returning_columns = match returning {
    Some(columns) => columns,
    None => return Ok(QueryResult::new(Vec::new())),
  };

  let columns = build_query_result_columns(engine, table, table_index, returning_columns).await?;
  let mut projected_rows = Vec::with_capacity(rows.len());

  for row in rows {
    projected_rows.push(project_row(&row, table_index, returning_columns)?);
  }

  Ok(QueryResult::new_with_columns(projected_rows, columns))
}

fn project_row(
  row: &Row,
  table_index: QueryTableIndex,
  projection: &[QueryColumn],
) -> EngineResult<Row> {
  if projection.is_empty() {
    return Ok(row.clone());
  }

  let mut projected = Vec::with_capacity(projection.len());
  for column in projection {
    if column.table_index != table_index {
      return Err(EngineError::Unsupported(
        "projection references another table",
      ));
    }

    let index = column.column_index as usize;
    let value = row
      .get(index)
      .ok_or(EngineError::InvalidQuery("projection out of bounds"))?;
    projected.push(value.clone());
  }

  Ok(projected)
}

fn matches_predicate(
  predicate: Option<&QueryExpr>,
  row: &Row,
  table_index: QueryTableIndex,
) -> EngineResult<bool> {
  match predicate {
    Some(expr) => eval_expr(expr, row, table_index),
    None => Ok(true),
  }
}

fn eval_expr(expr: &QueryExpr, row: &Row, table_index: QueryTableIndex) -> EngineResult<bool> {
  match expr {
    QueryExpr::Equals(left, right) => Ok(
      eval_expr_value_for_row(left, row, table_index)?
        == eval_expr_value_for_row(right, row, table_index)?,
    ),
    QueryExpr::NotEquals(left, right) => Ok(
      eval_expr_value_for_row(left, row, table_index)?
        != eval_expr_value_for_row(right, row, table_index)?,
    ),
    QueryExpr::LessThan(left, right) => Ok(
      eval_expr_value_for_row(left, row, table_index)?
        < eval_expr_value_for_row(right, row, table_index)?,
    ),
    QueryExpr::LessThanOrEquals(left, right) => Ok(
      eval_expr_value_for_row(left, row, table_index)?
        <= eval_expr_value_for_row(right, row, table_index)?,
    ),
    QueryExpr::GreaterThan(left, right) => Ok(
      eval_expr_value_for_row(left, row, table_index)?
        > eval_expr_value_for_row(right, row, table_index)?,
    ),
    QueryExpr::GreaterThanOrEquals(left, right) => Ok(
      eval_expr_value_for_row(left, row, table_index)?
        >= eval_expr_value_for_row(right, row, table_index)?,
    ),
    QueryExpr::IsNull(value) => Ok(matches!(
      eval_expr_value_for_row(value, row, table_index)?,
      Value::Null
    )),
    QueryExpr::IsNotNull(value) => Ok(!matches!(
      eval_expr_value_for_row(value, row, table_index)?,
      Value::Null
    )),
    QueryExpr::InList {
      expr,
      list,
      negated,
    } => {
      let value = eval_expr_value_for_row(expr, row, table_index)?;
      let contains = list.iter().any(|item| item == &value);
      Ok(if *negated { !contains } else { contains })
    }
    QueryExpr::InSubquery { .. } => Err(EngineError::Unsupported("in-subquery predicate")),
    QueryExpr::Like {
      expr,
      pattern,
      negated,
    } => {
      let expr_value = eval_expr_value_for_row(expr, row, table_index)?;
      let pattern_value = eval_expr_value_for_row(pattern, row, table_index)?;
      let matches = match (expr_value, pattern_value) {
        (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
        _ => false,
      };
      Ok(if *negated { !matches } else { matches })
    }
    QueryExpr::And(left, right) => {
      Ok(eval_expr(left, row, table_index)? && eval_expr(right, row, table_index)?)
    }
    QueryExpr::Or(left, right) => {
      Ok(eval_expr(left, row, table_index)? || eval_expr(right, row, table_index)?)
    }
    QueryExpr::Not(inner) => Ok(!eval_expr(inner, row, table_index)?),
  }
}

fn eval_expr_value_for_row(
  expr: &QueryExprValue,
  row: &Row,
  table_index: QueryTableIndex,
) -> EngineResult<Value> {
  match expr {
    QueryExprValue::Value(value) => Ok(value.clone()),
    QueryExprValue::Column(column) => {
      if column.table_index != table_index {
        return Err(EngineError::Unsupported(
          "expression references another table",
        ));
      }

      let index = column.column_index as usize;
      let value = row
        .get(index)
        .ok_or(EngineError::InvalidQuery("expression column out of bounds"))?;
      Ok(value.clone())
    }
  }
}

fn resolve_table_name(tables: &[String], table_index: QueryTableIndex) -> EngineResult<&str> {
  tables
    .get(table_index as usize)
    .map(|name| name.as_str())
    .ok_or(EngineError::InvalidQuery("table index out of bounds"))
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
