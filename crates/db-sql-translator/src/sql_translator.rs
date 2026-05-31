use async_trait::async_trait;
use db_engine::{DescribeSchema, Query, TranslateError, Translator, Value};
use sqlparser::ast::{
  BinaryOperator, Expr as SQLExpr, ObjectName, ObjectNamePart, SelectItem,
  Statement as SQLStatement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SqlTranslator;

// Small helper to track parameter state while translating. Supports both
// Postgres-style indexed parameters ($1, $2, ...) and positional `?` which
// consume params in order.
struct ParamState<'a> {
  params: Option<&'a [Value]>,
  next: usize,
}

impl<'a> ParamState<'a> {
  fn new(params: Option<&'a [Value]>) -> Self {
    Self { params, next: 0 }
  }

  fn take_next(&mut self) -> Result<Value, TranslateError> {
    match self.params {
      Some(p) => {
        if self.next >= p.len() {
          return Err(TranslateError::Custom(format!(
            "missing parameter at position {}",
            self.next + 1
          )));
        }
        let v = p[self.next].clone();
        self.next += 1;
        Ok(v)
      }
      None => Err(TranslateError::Custom(
        "no parameters provided for positional placeholder".into(),
      )),
    }
  }

  fn get_indexed(&self, idx0: usize) -> Result<Value, TranslateError> {
    match self.params {
      Some(p) => p
        .get(idx0)
        .cloned()
        .ok_or_else(|| TranslateError::Custom(format!("missing parameter ${}", idx0 + 1))),
      None => Err(TranslateError::Custom(
        "no parameters provided for indexed placeholder".into(),
      )),
    }
  }
}

fn object_name_to_string(name: &ObjectName) -> String {
  name
    .0
    .iter()
    .map(|p| match p {
      ObjectNamePart::Identifier(ident) => ident.value.to_owned(),
      // TODO: handle function calls if needed; for now just use the function name
      ObjectNamePart::Function(func) => func.name.value.to_owned(),
    })
    .collect::<Vec<_>>()
    .join(".")
}

fn parse_single_statement(query: &str) -> Result<SQLStatement, TranslateError> {
  let dialect = GenericDialect {};
  let stmts = Parser::parse_sql(&dialect, query)
    .map_err(|e| TranslateError::Custom(format!("failed to parse SQL: {}", e)))?;

  if stmts.is_empty() {
    return Err(TranslateError::Custom("no SQL statement found".into()));
  }

  if stmts.len() > 1 {
    return Err(TranslateError::Custom(
      "only a single SQL statement is supported".into(),
    ));
  }

  Ok(stmts.into_iter().next().unwrap())
}

// Helper: find a column index by name in the table schema
fn find_column_index(
  table_schema: &db_engine::TableSchema,
  col_name: &str,
) -> Result<usize, TranslateError> {
  for (i, col) in table_schema.columns.iter().enumerate() {
    if &col.name == col_name {
      return Ok(i);
    }
  }
  Err(TranslateError::Custom(format!(
    "unknown column '{}' in table '{}'",
    col_name, table_schema.name
  )))
}

// Helper: resolve a compound identifier like [schema, table, column] or [table, column].
fn resolve_column_from_compound_identifier(
  idents: &[sqlparser::ast::Ident],
  table_schema: &db_engine::TableSchema,
  table_name: &str,
) -> Result<usize, TranslateError> {
  if idents.is_empty() {
    return Err(TranslateError::Custom("empty identifier".into()));
  }

  let col_name = &idents.last().unwrap().value;

  if idents.len() >= 2 {
    let qual_table = idents[..idents.len() - 1]
      .iter()
      .map(|id| id.value.clone())
      .collect::<Vec<_>>()
      .join(".");
    if qual_table != table_name {
      return Err(TranslateError::Custom(format!(
        "qualified column refers to unknown table: {}",
        qual_table
      )));
    }
  }

  find_column_index(table_schema, col_name)
}

fn resolve_projection_item(
  item: &SelectItem,
  table_schema: &db_engine::TableSchema,
  table_name: &str,
) -> Result<Vec<usize>, TranslateError> {
  // Use the string representation for robust handling across sqlparser versions.
  let s = item.to_string();
  let s_trim = s.trim();

  // wildcard
  if s_trim == "*" {
    return Ok((0..table_schema.columns.len()).collect());
  }

  // qualified wildcard like users.*
  if s_trim.ends_with(".*") {
    let qual = s_trim[..s_trim.len() - 2].trim().trim_matches('"');
    if qual == table_name {
      return Ok((0..table_schema.columns.len()).collect());
    } else {
      return Err(TranslateError::Custom(format!(
        "qualified wildcard refers to unknown table: {}",
        qual
      )));
    }
  }

  // remove AS alias if present (case-insensitive)
  let mut expr_part = s_trim;
  if let Some(pos) = s_trim.to_uppercase().find(" AS ") {
    expr_part = &s_trim[..pos];
  }

  // If it's a simple identifier or qualified identifier like table.col
  if expr_part.contains('.') {
    let parts: Vec<&str> = expr_part.split('.').collect();
    let last = parts.last().unwrap().trim().trim_matches('"');
    let qual = parts[..parts.len() - 1]
      .join(".")
      .trim()
      .trim_matches('"')
      .to_string();
    if qual != table_name {
      return Err(TranslateError::Custom(format!(
        "qualified column refers to unknown table: {}",
        qual
      )));
    }
    return Ok(vec![find_column_index(table_schema, last)?]);
  }

  // simple identifier
  let simple = expr_part.trim().trim_matches('"');
  // disallow expressions for now
  if simple.contains('(') || simple.contains(' ') || simple.contains('+') || simple.contains('*') {
    return Err(TranslateError::Custom(format!(
      "unsupported select expression: {}",
      item
    )));
  }

  Ok(vec![find_column_index(table_schema, simple)?])
}

// Translate a simple SELECT into our engine Query. Keep this focused and small so we can
// extend it later (joins, predicates, aggregates, etc.).
async fn translate_select<S>(
  select: &sqlparser::ast::Select,
  resolver: &S,
  params: &mut ParamState<'_>,
) -> Result<Query, TranslateError>
where
  S: DescribeSchema,
{
  if select.from.is_empty() {
    return Err(TranslateError::Custom(
      "SELECT must have a FROM clause (unsupported)".into(),
    ));
  }

  if select.from.len() != 1 {
    return Err(TranslateError::Custom(
      "SELECT with multiple FROM tables is not supported".into(),
    ));
  }

  let table_with_joins = &select.from[0];

  if !table_with_joins.joins.is_empty() {
    return Err(TranslateError::Custom("JOINs are not supported".into()));
  }

  let table_name = match &table_with_joins.relation {
    TableFactor::Table { name, .. } => object_name_to_string(name),
    _ => {
      return Err(TranslateError::Custom(
        "unsupported table factor in FROM clause".into(),
      ));
    }
  };

  let table_schema = resolver
    .describe_table(&table_name)
    .await
    .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", table_name)))?;

  // Translate WHERE if present
  let predicate = match &select.selection {
    Some(expr) => Some(translate_predicate(
      expr,
      &table_schema,
      &table_name,
      resolver,
      params,
    )?),
    None => None,
  };

  let mut projection_indices: Vec<usize> = Vec::new();

  for item in &select.projection {
    let mut resolved = resolve_projection_item(item, &table_schema, &table_name)?;
    projection_indices.append(&mut resolved);
  }

  Ok(Query::select_simple(
    table_name,
    projection_indices,
    predicate,
  ))
}

// Parse a SQL literal (as string) into a db_engine::Value based on the target column type.
fn parse_literal_for_type(
  raw: &str,
  target_type: &db_engine::ValueType,
) -> Result<db_engine::Value, TranslateError> {
  let s = raw.trim();

  // Remove cast suffixes like '::uuid' if present
  let s = if let Some(idx) = s.find("::") {
    &s[..idx]
  } else {
    s
  };
  let s = s.trim();

  // Null
  if s.eq_ignore_ascii_case("NULL") {
    return Ok(db_engine::Value::Null);
  }

  // Helper: strip single quotes around literals
  let strip_quotes = |v: &str| {
    let v = v.trim();
    if v.starts_with('\'') && v.ends_with('\'') && v.len() >= 2 {
      v[1..v.len() - 1].to_string()
    } else {
      v.to_string()
    }
  };

  match target_type {
    db_engine::ValueType::Uuid => {
      let inner = strip_quotes(s);
      match uuid::Uuid::parse_str(&inner) {
        Ok(u) => Ok(db_engine::Value::Uuid(u)),
        Err(_) => Err(TranslateError::Custom(format!(
          "failed to parse UUID literal: {}",
          inner
        ))),
      }
    }
    db_engine::ValueType::Integer => {
      // Accept quoted numbers as well
      let candidate = strip_quotes(s);
      candidate
        .parse::<i64>()
        .map(db_engine::Value::Integer)
        .map_err(|e| TranslateError::Custom(format!("failed to parse integer: {}", e)))
    }
    db_engine::ValueType::Float => {
      let candidate = strip_quotes(s);
      candidate
        .parse::<f64>()
        .map(db_engine::Value::Float)
        .map_err(|e| TranslateError::Custom(format!("failed to parse float: {}", e)))
    }
    db_engine::ValueType::Text => Ok(db_engine::Value::Text(strip_quotes(s))),
    db_engine::ValueType::Json => {
      let candidate = strip_quotes(s);
      serde_json::from_str::<serde_json::Value>(&candidate)
        .map(db_engine::Value::Json)
        .map_err(|e| TranslateError::Custom(format!("failed to parse json: {}", e)))
    }
    db_engine::ValueType::Blob => Err(TranslateError::Custom(
      "blob literals are not supported by the minimal translator".into(),
    )),
    db_engine::ValueType::Null => Ok(db_engine::Value::Null),
  }
}

// Convert an arbitrary SQL expression into a db_engine::Value by guessing the type. This
// is used for INSERTs when a target column schema is not available.
fn expr_to_value_guess(expr: &SQLExpr) -> Result<db_engine::Value, TranslateError> {
  let raw = expr.to_string();
  let s = raw.trim();

  // Remove cast suffixes like '::uuid' if present
  let s = if let Some(idx) = s.find("::") {
    &s[..idx]
  } else {
    s
  };
  let s = s.trim();

  if s.eq_ignore_ascii_case("NULL") {
    return Ok(db_engine::Value::Null);
  }

  // quoted string
  if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
    let inner = s[1..s.len() - 1].to_string();
    // try uuid first
    if let Ok(u) = uuid::Uuid::parse_str(&inner) {
      return Ok(db_engine::Value::Uuid(u));
    }
    // try json
    if let Ok(j) = serde_json::from_str::<serde_json::Value>(&inner) {
      return Ok(db_engine::Value::Json(j));
    }
    return Ok(db_engine::Value::Text(inner));
  }

  // try integer
  if let Ok(i) = s.parse::<i64>() {
    return Ok(db_engine::Value::Integer(i));
  }

  // try float
  if let Ok(f) = s.parse::<f64>() {
    return Ok(db_engine::Value::Float(f));
  }

  Err(TranslateError::Custom(format!(
    "unsupported literal expression for INSERT: {}",
    expr
  )))
}

// Convert a SQL expression into an engine ExprValue, using the table schema to resolve
// column references and to parse literals with a target type.
fn expr_to_expr_value<S>(
  expr: &SQLExpr,
  table_schema: &db_engine::TableSchema,
  table_name: &str,
  _resolver: &S,
  params: &mut ParamState<'_>,
) -> Result<db_engine::ExprValue, TranslateError>
where
  S: DescribeSchema,
{
  // Detect placeholders by inspecting the printed form of the expression. This keeps
  // behavior robust across sqlparser versions and handles casts like `$1::uuid`.
  let raw = expr.to_string();
  let s = raw.trim();
  let s = if let Some(idx) = s.find("::") {
    &s[..idx]
  } else {
    s
  };
  let s = s.trim();

  // Positional `?` consumes the next parameter
  if s == "?" {
    let v = params.take_next()?;
    return Ok(db_engine::ExprValue::Value(v));
  }

  // Postgres-style indexed parameters like $1, $2 (1-based)
  if s.starts_with('$') {
    let digits = &s[1..];
    if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
      let idx1 = digits
        .parse::<usize>()
        .map_err(|e| TranslateError::Custom(format!("invalid parameter index: {}", e)))?;
      if idx1 == 0 {
        return Err(TranslateError::Custom(
          "parameter index must be >= 1".into(),
        ));
      }
      let v = params.get_indexed(idx1 - 1)?;
      return Ok(db_engine::ExprValue::Value(v));
    }
  }

  match expr {
    SQLExpr::Identifier(ident) => {
      let idx = find_column_index(table_schema, &ident.value)?;
      Ok(db_engine::ExprValue::Column(db_engine::Column {
        table: table_name.to_string(),
        column_index: idx as u8,
      }))
    }
    SQLExpr::CompoundIdentifier(idents) => {
      let idx = resolve_column_from_compound_identifier(idents, table_schema, table_name)?;
      Ok(db_engine::ExprValue::Column(db_engine::Column {
        table: table_name.to_string(),
        column_index: idx as u8,
      }))
    }
    SQLExpr::Value(_) | SQLExpr::Nested(_) | SQLExpr::Cast { .. } | SQLExpr::TypedString { .. } => {
      // Try to parse literal without a target type
      Ok(db_engine::ExprValue::Value(expr_to_value_guess(expr)?))
    }
    other => Err(TranslateError::Custom(format!(
      "unsupported expression in assignment/predicate: {}",
      other
    ))),
  }
}

// Translate a simple boolean expression into a db_engine::Expr. Supports basic binary
// comparisons and &&/||/NOT composed expressions.
fn translate_predicate<S>(
  expr: &SQLExpr,
  table_schema: &db_engine::TableSchema,
  table_name: &str,
  resolver: &S,
  params: &mut ParamState<'_>,
) -> Result<db_engine::Expr, TranslateError>
where
  S: DescribeSchema,
{
  match expr {
    SQLExpr::BinaryOp { left, op, right } => {
      // Handle logical operators by recursion so we don't accidentally consume
      // parameters twice (expr_to_expr_value would otherwise be invoked twice).
      match op {
        BinaryOperator::And => Ok(db_engine::Expr::And(
          Box::new(translate_predicate(
            left.as_ref(),
            table_schema,
            table_name,
            resolver,
            params,
          )?),
          Box::new(translate_predicate(
            right.as_ref(),
            table_schema,
            table_name,
            resolver,
            params,
          )?),
        )),
        BinaryOperator::Or => Ok(db_engine::Expr::Or(
          Box::new(translate_predicate(
            left.as_ref(),
            table_schema,
            table_name,
            resolver,
            params,
          )?),
          Box::new(translate_predicate(
            right.as_ref(),
            table_schema,
            table_name,
            resolver,
            params,
          )?),
        )),
        // Comparisons: evaluate both sides to concrete ExprValue
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => {
          let left_val =
            expr_to_expr_value(left.as_ref(), table_schema, table_name, resolver, params)?;
          let right_val =
            expr_to_expr_value(right.as_ref(), table_schema, table_name, resolver, params)?;

          match op {
            BinaryOperator::Eq => Ok(db_engine::Expr::Equals(left_val, right_val)),
            BinaryOperator::NotEq => Ok(db_engine::Expr::NotEquals(left_val, right_val)),
            BinaryOperator::Lt => Ok(db_engine::Expr::LessThan(left_val, right_val)),
            BinaryOperator::LtEq => Ok(db_engine::Expr::LessThanOrEquals(left_val, right_val)),
            BinaryOperator::Gt => Ok(db_engine::Expr::GreaterThan(left_val, right_val)),
            BinaryOperator::GtEq => Ok(db_engine::Expr::GreaterThanOrEquals(left_val, right_val)),
            _ => unreachable!(),
          }
        }
        _ => Err(TranslateError::Custom(format!(
          "unsupported binary operator in WHERE: {}",
          op
        ))),
      }
    }
    SQLExpr::UnaryOp { op, expr } => {
      // Only support NOT for now
      let inner = translate_predicate(expr.as_ref(), table_schema, table_name, resolver, params)?;
      match op.to_string().as_str() {
        "NOT" => Ok(db_engine::Expr::Not(Box::new(inner))),
        _ => Err(TranslateError::Custom(format!(
          "unsupported unary operator in WHERE: {}",
          op
        ))),
      }
    }
    SQLExpr::IsNull(expr) => Ok(db_engine::Expr::IsNull(expr_to_expr_value(
      expr,
      table_schema,
      table_name,
      resolver,
      params,
    )?)),
    SQLExpr::IsNotNull(expr) => Ok(db_engine::Expr::IsNotNull(expr_to_expr_value(
      expr,
      table_schema,
      table_name,
      resolver,
      params,
    )?)),
    other => Err(TranslateError::Custom(format!(
      "unsupported WHERE expression: {}",
      other
    ))),
  }
}

// Translate a simple INSERT statement that uses VALUES. Supports optional column lists and
// single-row VALUES. Keeps behavior conservative and explicit.
async fn translate_insert<S>(
  stmt: &SQLStatement,
  resolver: &S,
  params: &mut ParamState<'_>,
) -> Result<Query, TranslateError>
where
  S: DescribeSchema,
{
  match stmt {
    SQLStatement::Insert(insert) => {
      let insert = insert; // insert: &sqlparser::ast::Insert
      // table is a TableObject; use its string form
      let table = insert.table.to_string();
      let table_schema = resolver
        .describe_table(&table)
        .await
        .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", table)))?;

      // Expect a SOURCE query containing VALUES
      if let Some(source) = &insert.source {
        match &*source.body {
          sqlparser::ast::SetExpr::Values(values) => {
            if values.rows.len() != 1 {
              return Err(TranslateError::Custom(
                "only single-row VALUES INSERT is supported".into(),
              ));
            }

            let row_exprs = &values.rows[0];

            // Build target row initialized with NULLs
            let mut row: Vec<db_engine::Value> =
              vec![db_engine::Value::Null; table_schema.columns.len()];

            if insert.columns.is_empty() {
              // Values must match table column count
              if row_exprs.len() != table_schema.columns.len() {
                return Err(TranslateError::Custom(format!(
                  "VALUES count ({}) does not match table column count ({})",
                  row_exprs.len(),
                  table_schema.columns.len()
                )));
              }

              for (i, expr) in row_exprs.iter().enumerate() {
                let col_schema = &table_schema.columns[i];
                // Handle parameter placeholders ($n or ?) first
                let raw = expr.to_string();
                let s = raw.trim();
                let s = if let Some(idx) = s.find("::") {
                  &s[..idx]
                } else {
                  s
                };
                let s = s.trim();

                if s == "?" {
                  row[i] = params.take_next()?;
                } else if s.starts_with('$') {
                  let digits = &s[1..];
                  if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
                    let idx1 = digits.parse::<usize>().map_err(|e| {
                      TranslateError::Custom(format!("invalid parameter index: {}", e))
                    })?;
                    if idx1 == 0 {
                      return Err(TranslateError::Custom(
                        "parameter index must be >= 1".into(),
                      ));
                    }
                    row[i] = params.get_indexed(idx1 - 1)?;
                  } else {
                    return Err(TranslateError::Custom(format!(
                      "unsupported VALUES expression: {}",
                      expr
                    )));
                  }
                } else {
                  // Prefer parsing by column type
                  let v = parse_literal_for_type(&expr.to_string(), &col_schema.r#type)?;
                  row[i] = v;
                }
              }
            } else {
              // Column list provided: map each value to its declared column
              if insert.columns.len() != row_exprs.len() {
                return Err(TranslateError::Custom(
                  "number of columns does not match number of VALUES expressions".into(),
                ));
              }

              for (col_ident, expr) in insert.columns.iter().zip(row_exprs.iter()) {
                let idx = find_column_index(&table_schema, &col_ident.to_string())?;
                let col_schema = &table_schema.columns[idx];

                let raw = expr.to_string();
                let s = raw.trim();
                let s = if let Some(idx) = s.find("::") {
                  &s[..idx]
                } else {
                  s
                };
                let s = s.trim();

                let v = if s == "?" {
                  params.take_next()?
                } else if s.starts_with('$') {
                  let digits = &s[1..];
                  if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
                    let idx1 = digits.parse::<usize>().map_err(|e| {
                      TranslateError::Custom(format!("invalid parameter index: {}", e))
                    })?;
                    if idx1 == 0 {
                      return Err(TranslateError::Custom(
                        "parameter index must be >= 1".into(),
                      ));
                    }
                    params.get_indexed(idx1 - 1)?
                  } else {
                    return Err(TranslateError::Custom(format!(
                      "unsupported VALUES expression: {}",
                      expr
                    )));
                  }
                } else {
                  parse_literal_for_type(&expr.to_string(), &col_schema.r#type)?
                };

                row[idx] = v;
              }
            }

            Ok(Query::Insert {
              table,
              row,
              returning: None,
            })
          }
          _ => Err(TranslateError::Custom(
            "only INSERT ... VALUES (...) is supported by the minimal translator".into(),
          )),
        }
      } else if !insert.assignments.is_empty() {
        // Support INSERT ... SET col = expr (MySQL style) for single-row
        // Not implementing fully; return not implemented
        Err(TranslateError::Custom(
          "INSERT ... SET not implemented".into(),
        ))
      } else {
        Err(TranslateError::Custom(
          "only INSERT with VALUES source is supported by the minimal translator".into(),
        ))
      }
    }
    other => Err(TranslateError::Custom(format!(
      "expected INSERT statement, got {}",
      other
    ))),
  }
}

// Translate a simple UPDATE statement with SET assignments and an optional WHERE.
// This implementation supports assignments to columns with literal or column expressions
// and a restricted subset of WHERE expressions (basic comparisons, AND/OR, NOT).
async fn translate_update<S>(
  stmt: &SQLStatement,
  resolver: &S,
  params: &mut ParamState<'_>,
) -> Result<Query, TranslateError>
where
  S: DescribeSchema,
{
  match stmt {
    SQLStatement::Update(update) => {
      let update = update; // &sqlparser::ast::Update
      let table = &update.table;
      if !table.joins.is_empty() {
        return Err(TranslateError::Custom(
          "UPDATE with JOINs is not supported".into(),
        ));
      }

      let table_name = match &table.relation {
        TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => {
          return Err(TranslateError::Custom(
            "unsupported table factor in UPDATE".into(),
          ));
        }
      };

      let table_schema = resolver
        .describe_table(&table_name)
        .await
        .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", table_name)))?;

      // Translate assignments
      let mut assigns: Vec<db_engine::UpdateAssignment> = Vec::new();
      for assign in update.assignments.iter() {
        // Assignment target
        match &assign.target {
          sqlparser::ast::AssignmentTarget::ColumnName(obj_name) => {
            // obj_name is an ObjectName; extract last part and optional qualifier
            let full = object_name_to_string(obj_name);
            let parts: Vec<&str> = full.split('.').collect();
            let (qual, col_name) = if parts.len() >= 2 {
              (
                Some(parts[..parts.len() - 1].join(".")),
                parts[parts.len() - 1].to_string(),
              )
            } else {
              (None, parts[0].to_string())
            };
            if let Some(q) = qual {
              if q != table_name {
                return Err(TranslateError::Custom(format!(
                  "qualified column refers to unknown table: {}",
                  q
                )));
              }
            }
            let idx = find_column_index(&table_schema, &col_name)?;
            let value =
              expr_to_expr_value(&assign.value, &table_schema, &table_name, resolver, params)?;
            assigns.push(db_engine::UpdateAssignment {
              column: db_engine::Column {
                table: table_name.clone(),
                column_index: idx as u8,
              },
              value,
            });
          }
          _ => {
            return Err(TranslateError::Custom(
              "unsupported assignment target in UPDATE".into(),
            ));
          }
        }
      }

      // Translate optional predicate
      let predicate = match &update.selection {
        Some(expr) => Some(translate_predicate(
          expr,
          &table_schema,
          &table_name,
          resolver,
          params,
        )?),
        None => None,
      };

      Ok(Query::Update {
        table: table_name,
        assignments: assigns,
        predicate,
        joins: Vec::new(),
        from_tables: Vec::new(),
        returning: None,
      })
    }
    other => Err(TranslateError::Custom(format!(
      "expected UPDATE statement, got {}",
      other
    ))),
  }
}

// Translate a simple DELETE statement. Supports an optional WHERE using the same
// predicate translator as UPDATE.
async fn translate_delete<S>(
  stmt: &SQLStatement,
  resolver: &S,
  params: &mut ParamState<'_>,
) -> Result<Query, TranslateError>
where
  S: DescribeSchema,
{
  match stmt {
    SQLStatement::Delete(del) => {
      // The Delete struct exposes a "from" and "selection". For simple cases where
      // FROM contains a single table, we'll use its table name.
      let table = match &del.from {
        sqlparser::ast::FromTable::WithoutKeyword(twj) => {
          // use first relation
          if twj.is_empty() {
            return Err(TranslateError::Custom("DELETE missing FROM table".into()));
          }
          match &twj[0].relation {
            TableFactor::Table { name, .. } => object_name_to_string(name),
            _ => {
              return Err(TranslateError::Custom(
                "unsupported table factor in DELETE".into(),
              ));
            }
          }
        }
        sqlparser::ast::FromTable::WithFromKeyword(twj) => {
          if twj.is_empty() {
            return Err(TranslateError::Custom("DELETE missing FROM table".into()));
          }
          match &twj[0].relation {
            TableFactor::Table { name, .. } => object_name_to_string(name),
            _ => {
              return Err(TranslateError::Custom(
                "unsupported table factor in DELETE".into(),
              ));
            }
          }
        }
      };

      let table_schema = resolver
        .describe_table(&table)
        .await
        .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", table)))?;

      let predicate = match &del.selection {
        Some(expr) => Some(translate_predicate(
          expr,
          &table_schema,
          &table,
          resolver,
          params,
        )?),
        None => None,
      };

      Ok(Query::Delete {
        table,
        predicate,
        returning: None,
      })
    }
    other => Err(TranslateError::Custom(format!(
      "expected DELETE statement, got {}",
      other
    ))),
  }
}

#[async_trait]
impl Translator for SqlTranslator {
  async fn translate_with_params<S>(
    &self,
    query: &str,
    params: Option<&[Value]>,
    resolver: &S,
  ) -> Result<Query, TranslateError>
  where
    S: DescribeSchema,
  {
    let mut pstate = ParamState::new(params);

    let stmt = parse_single_statement(query)?;

    match &stmt {
      SQLStatement::Query(q) => match &*q.body {
        sqlparser::ast::SetExpr::Select(select) => {
          translate_select(select, resolver, &mut pstate).await
        }
        _ => Err(TranslateError::Custom(
          "only simple SELECT statements are supported by the minimal translator".into(),
        )),
      },
      SQLStatement::Insert(_) => translate_insert(&stmt, resolver, &mut pstate).await,
      SQLStatement::Update(_) => translate_update(&stmt, resolver, &mut pstate).await,
      SQLStatement::Delete(_) => translate_delete(&stmt, resolver, &mut pstate).await,
      other => Err(TranslateError::Custom(format!(
        "unsupported SQL statement: {}",
        other
      ))),
    }
  }
}
