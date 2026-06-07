#[cfg(not(feature = "std"))]
use alloc::{
  borrow::ToOwned,
  boxed::Box,
  format,
  string::{String, ToString},
  vec,
  vec::Vec,
};

use db_query::{
  DataDefinition, Query, QueryColumn, QueryExpr, QueryExprValue, QueryJoin, QueryJoinKind,
  QueryParams, QuerySelectOptions, QueryTableIndex, QueryUpdateAssignment, Statement,
  TranslateError, Translator,
};
use db_schema::{ColumnSchema, ColumnSchemaIndex, DescribeSchema, TableSchema};
use sqlparser::ast::{
  BinaryOperator, ColumnOption, DataType, Expr as SQLExpr, JoinConstraint, JoinOperator,
  ObjectName, ObjectNamePart, SelectItem, Statement as SQLStatement, TableConstraint, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use db_value::{Value, ValueType};

#[derive(Debug, Clone, Copy)]
pub struct SqlTranslator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaceholderStyle {
  PositionalOrIndexed,
  Named,
}

enum ParsedPlaceholder {
  Positional,
  Indexed(usize),
  Named(String),
}

struct ParamState<'a> {
  params: Option<&'a QueryParams>,
  next: usize,
  style: Option<PlaceholderStyle>,
}

impl<'a> ParamState<'a> {
  fn new(params: Option<&'a QueryParams>) -> Self {
    Self {
      params,
      next: 0,
      style: None,
    }
  }

  fn ensure_style(&mut self, style: PlaceholderStyle) -> Result<(), TranslateError> {
    match self.style {
      Some(existing) if existing != style => Err(TranslateError::MixedPlaceholderStyles),
      _ => {
        self.style = Some(style);
        Ok(())
      }
    }
  }

  fn positional_params(&self) -> Result<&[Value], TranslateError> {
    match self.params {
      Some(QueryParams::Positional(values)) => Ok(values),
      Some(QueryParams::Named(_)) => Err(TranslateError::Custom(
        "named parameters provided for positional/indexed placeholder".into(),
      )),
      None => Err(TranslateError::Custom(
        "no parameters provided for positional/indexed placeholder".into(),
      )),
    }
  }

  fn take_next(&mut self) -> Result<Value, TranslateError> {
    self.ensure_style(PlaceholderStyle::PositionalOrIndexed)?;

    let values = self.positional_params()?;
    if self.next >= values.len() {
      return Err(TranslateError::Custom(format!(
        "missing parameter at position {}",
        self.next + 1
      )));
    }

    let v = values[self.next].clone();
    self.next += 1;
    Ok(v)
  }

  fn get_indexed(&mut self, idx0: usize) -> Result<Value, TranslateError> {
    self.ensure_style(PlaceholderStyle::PositionalOrIndexed)?;

    self
      .positional_params()?
      .get(idx0)
      .cloned()
      .ok_or_else(|| TranslateError::Custom(format!("missing parameter ${}", idx0 + 1)))
  }

  fn get_named(&mut self, name: &str) -> Result<Value, TranslateError> {
    self.ensure_style(PlaceholderStyle::Named)?;

    match self.params {
      Some(QueryParams::Named(values)) => values
        .get(name)
        .cloned()
        .ok_or_else(|| TranslateError::MissingNamedParameter(name.to_string())),
      Some(QueryParams::Positional(_)) => Err(TranslateError::Custom(
        "positional parameters provided for named placeholder".into(),
      )),
      None => Err(TranslateError::Custom(
        "no parameters provided for named placeholder".into(),
      )),
    }
  }

  fn resolve_placeholder(
    &mut self,
    placeholder: ParsedPlaceholder,
  ) -> Result<Value, TranslateError> {
    match placeholder {
      ParsedPlaceholder::Positional => self.take_next(),
      ParsedPlaceholder::Indexed(idx0) => self.get_indexed(idx0),
      ParsedPlaceholder::Named(name) => self.get_named(&name),
    }
  }
}

fn trim_cast_suffix(raw: &str) -> &str {
  let s = raw.trim();
  if let Some(idx) = s.find("::") {
    s[..idx].trim()
  } else {
    s
  }
}

fn is_valid_named_param(name: &str) -> bool {
  let mut chars = name.chars();
  match chars.next() {
    Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
    _ => return false,
  }

  chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn parse_placeholder(expr: &SQLExpr) -> Result<Option<ParsedPlaceholder>, TranslateError> {
  let raw = expr.to_string();
  let token = trim_cast_suffix(&raw);

  if token == "?" {
    return Ok(Some(ParsedPlaceholder::Positional));
  }

  if let Some(digits) = token.strip_prefix('$')
    && !digits.is_empty()
    && digits.chars().all(|c| c.is_ascii_digit())
  {
    let idx1 = digits
      .parse::<usize>()
      .map_err(|e| TranslateError::Custom(format!("invalid parameter index: {}", e)))?;
    if idx1 == 0 {
      return Err(TranslateError::Custom(
        "parameter index must be >= 1".into(),
      ));
    }

    return Ok(Some(ParsedPlaceholder::Indexed(idx1 - 1)));
  }

  if let Some(name) = token.strip_prefix(':')
    && is_valid_named_param(name)
  {
    return Ok(Some(ParsedPlaceholder::Named(name.to_string())));
  }

  Ok(None)
}

fn object_name_to_string(name: &ObjectName) -> String {
  name
    .0
    .iter()
    .map(|p| match p {
      ObjectNamePart::Identifier(ident) => ident.value.to_owned(),
      ObjectNamePart::Function(_func) => todo!("handle function calls if needed"),
    })
    .collect::<Vec<_>>()
    .join(".")
}

fn parse_table_factor_name_and_alias(
  table_factor: &TableFactor,
) -> Result<(String, Option<String>), TranslateError> {
  match table_factor {
    TableFactor::Table { name, alias, .. } => {
      let table_name = object_name_to_string(name);
      let alias_name = alias.as_ref().map(|alias| alias.name.value.clone());
      Ok((table_name, alias_name))
    }
    _ => Err(TranslateError::Custom(
      "unsupported table factor in FROM clause".into(),
    )),
  }
}

fn decode_join_operator(
  join_operator: &JoinOperator,
) -> Result<(QueryJoinKind, &JoinConstraint), TranslateError> {
  match join_operator {
    JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
      Ok((QueryJoinKind::Inner, constraint))
    }
    JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
      Ok((QueryJoinKind::Left, constraint))
    }
    JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
      Ok((QueryJoinKind::Right, constraint))
    }
    JoinOperator::FullOuter(constraint) => Ok((QueryJoinKind::Full, constraint)),
    _ => Err(TranslateError::Custom("unsupported JOIN type".into())),
  }
}

struct TableEntry {
  name: String,
  alias: Option<String>,
  schema: TableSchema,
}

impl TableEntry {
  fn matches_name(&self, name: &str) -> bool {
    self.name == name || self.alias.as_deref() == Some(name)
  }

  fn column_index(&self, col_name: &str) -> Option<usize> {
    self
      .schema
      .columns
      .iter()
      .position(|col| col.name == col_name)
  }
}

fn find_column_in_tables(
  tables: &[TableEntry],
  col_name: &str,
) -> Result<(usize, usize), TranslateError> {
  let mut matches = Vec::new();

  for (table_index, table) in tables.iter().enumerate() {
    if let Some(column_index) = table.column_index(col_name) {
      matches.push((table_index, column_index));
    }
  }

  match matches.len() {
    0 => Err(TranslateError::Custom(format!(
      "unknown column '{}' in any table",
      col_name
    ))),
    1 => Ok(matches.into_iter().next().unwrap()),
    _ => Err(TranslateError::Custom(format!(
      "ambiguous column reference: {}",
      col_name
    ))),
  }
}

fn resolve_column_from_compound_identifier(
  idents: &[sqlparser::ast::Ident],
  tables: &[TableEntry],
) -> Result<(usize, usize), TranslateError> {
  if idents.is_empty() {
    return Err(TranslateError::Custom("empty identifier".into()));
  }

  let col_name = &idents.last().unwrap().value;
  if idents.len() >= 2 {
    let qual = idents[..idents.len() - 1]
      .iter()
      .map(|id| id.value.clone())
      .collect::<Vec<_>>()
      .join(".");

    for (table_index, table) in tables.iter().enumerate() {
      if table.matches_name(&qual) {
        return table
          .column_index(col_name)
          .map(|column_index| (table_index, column_index))
          .ok_or_else(|| {
            TranslateError::Custom(format!(
              "unknown column '{}' in table '{}'",
              col_name, table.name
            ))
          });
      }
    }

    return Err(TranslateError::Custom(format!(
      "qualified column refers to unknown table: {}",
      qual
    )));
  }

  find_column_in_tables(tables, col_name)
}

fn resolve_column_reference(
  expr: &SQLExpr,
  tables: &[TableEntry],
) -> Result<QueryColumn, TranslateError> {
  let (table_index, column_index) = match expr {
    SQLExpr::Identifier(ident) => find_column_in_tables(tables, &ident.value)?,
    SQLExpr::CompoundIdentifier(idents) => resolve_column_from_compound_identifier(idents, tables)?,
    _ => {
      return Err(TranslateError::Custom(format!(
        "unsupported select expression: {}",
        expr
      )));
    }
  };

  Ok(QueryColumn {
    table_index: table_index as QueryTableIndex,
    column_index: column_index as ColumnSchemaIndex,
  })
}

fn resolve_projection_item(
  item: &SelectItem,
  tables: &[TableEntry],
) -> Result<Vec<QueryColumn>, TranslateError> {
  match item {
    SelectItem::Wildcard(_) => {
      let mut cols = Vec::new();
      for (table_index, table) in tables.iter().enumerate() {
        for column_index in 0..table.schema.columns.len() {
          cols.push(QueryColumn {
            table_index: table_index as QueryTableIndex,
            column_index: column_index as ColumnSchemaIndex,
          });
        }
      }
      Ok(cols)
    }
    SelectItem::QualifiedWildcard(kind, _) => {
      let qual = match kind {
        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
          object_name_to_string(name)
        }
        _ => {
          return Err(TranslateError::Custom(
            "unsupported qualified wildcard expression".into(),
          ));
        }
      };
      let table_index = tables
        .iter()
        .position(|table| table.matches_name(&qual))
        .ok_or_else(|| {
          TranslateError::Custom(format!(
            "qualified wildcard refers to unknown table: {}",
            qual
          ))
        })?;

      let mut cols = Vec::new();
      for column_index in 0..tables[table_index].schema.columns.len() {
        cols.push(QueryColumn {
          table_index: table_index as QueryTableIndex,
          column_index: column_index as ColumnSchemaIndex,
        });
      }
      Ok(cols)
    }
    SelectItem::UnnamedExpr(expr) => Ok(vec![resolve_column_reference(expr, tables)?]),
    SelectItem::ExprWithAlias { expr, .. } => Ok(vec![resolve_column_reference(expr, tables)?]),
  }
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

fn find_column_index(table_schema: &TableSchema, col_name: &str) -> Result<usize, TranslateError> {
  for (i, col) in table_schema.columns.iter().enumerate() {
    if col.name == col_name {
      return Ok(i);
    }
  }
  Err(TranslateError::Custom(format!(
    "unknown column '{}' in table '{}'",
    col_name, table_schema.name
  )))
}

fn translate_join_constraint(
  constraint: &sqlparser::ast::JoinConstraint,
  tables: &[TableEntry],
  params: &mut ParamState<'_>,
) -> Result<QueryExpr, TranslateError> {
  match constraint {
    JoinConstraint::On(expr) => translate_predicate(expr, tables, params),
    JoinConstraint::Using(_) => Err(TranslateError::Custom("JOIN USING is not supported".into())),
    sqlparser::ast::JoinConstraint::Natural => Err(TranslateError::Custom(
      "NATURAL JOIN is not supported".into(),
    )),
    sqlparser::ast::JoinConstraint::None => Err(TranslateError::Custom(
      "JOIN without ON is not supported".into(),
    )),
  }
}

fn translate_predicate(
  expr: &SQLExpr,
  tables: &[TableEntry],
  params: &mut ParamState<'_>,
) -> Result<QueryExpr, TranslateError> {
  match expr {
    SQLExpr::BinaryOp { left, op, right } => match op {
      BinaryOperator::And => Ok(QueryExpr::And(
        Box::new(translate_predicate(left.as_ref(), tables, params)?),
        Box::new(translate_predicate(right.as_ref(), tables, params)?),
      )),
      BinaryOperator::Or => Ok(QueryExpr::Or(
        Box::new(translate_predicate(left.as_ref(), tables, params)?),
        Box::new(translate_predicate(right.as_ref(), tables, params)?),
      )),
      BinaryOperator::Eq
      | BinaryOperator::NotEq
      | BinaryOperator::Lt
      | BinaryOperator::LtEq
      | BinaryOperator::Gt
      | BinaryOperator::GtEq => {
        let left_val = expr_to_expr_value(left.as_ref(), tables, params)?;
        let right_val = expr_to_expr_value(right.as_ref(), tables, params)?;

        match op {
          BinaryOperator::Eq => Ok(QueryExpr::Equals(left_val, right_val)),
          BinaryOperator::NotEq => Ok(QueryExpr::NotEquals(left_val, right_val)),
          BinaryOperator::Lt => Ok(QueryExpr::LessThan(left_val, right_val)),
          BinaryOperator::LtEq => Ok(QueryExpr::LessThanOrEquals(left_val, right_val)),
          BinaryOperator::Gt => Ok(QueryExpr::GreaterThan(left_val, right_val)),
          BinaryOperator::GtEq => Ok(QueryExpr::GreaterThanOrEquals(left_val, right_val)),
          _ => unreachable!(),
        }
      }
      _ => Err(TranslateError::Custom(format!(
        "unsupported binary operator in WHERE: {}",
        op
      ))),
    },
    SQLExpr::UnaryOp { op, expr } => {
      let inner = translate_predicate(expr.as_ref(), tables, params)?;
      match op.to_string().as_str() {
        "NOT" => Ok(QueryExpr::Not(Box::new(inner))),
        _ => Err(TranslateError::Custom(format!(
          "unsupported unary operator in WHERE: {}",
          op
        ))),
      }
    }
    SQLExpr::IsNull(expr) => Ok(QueryExpr::IsNull(expr_to_expr_value(expr, tables, params)?)),
    SQLExpr::IsNotNull(expr) => Ok(QueryExpr::IsNotNull(expr_to_expr_value(
      expr, tables, params,
    )?)),
    other => Err(TranslateError::Custom(format!(
      "unsupported WHERE expression: {}",
      other
    ))),
  }
}

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
  let mut table_entries: Vec<TableEntry> = Vec::new();
  let mut tables: Vec<String> = Vec::new();

  let (base_name, base_alias) = parse_table_factor_name_and_alias(&table_with_joins.relation)?;
  let base_schema = resolver
    .describe_table(&base_name)
    .await
    .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", base_name)))?;

  table_entries.push(TableEntry {
    name: base_name.clone(),
    alias: base_alias,
    schema: base_schema,
  });
  tables.push(base_name);

  let mut joins: Vec<QueryJoin> = Vec::new();
  for join in &table_with_joins.joins {
    let (join_name, join_alias) = parse_table_factor_name_and_alias(&join.relation)?;
    let join_schema = resolver
      .describe_table(&join_name)
      .await
      .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", join_name)))?;

    let table_index = table_entries.len() as QueryTableIndex;
    table_entries.push(TableEntry {
      name: join_name.clone(),
      alias: join_alias,
      schema: join_schema,
    });
    tables.push(join_name);

    let (join_kind, join_constraint) = decode_join_operator(&join.join_operator)?;
    let on = translate_join_constraint(join_constraint, &table_entries, params)?;

    joins.push(QueryJoin {
      kind: join_kind,
      table_index,
      on,
    });
  }

  let predicate = match &select.selection {
    Some(expr) => Some(translate_predicate(expr, &table_entries, params)?),
    None => None,
  };

  let mut projection: Vec<QueryColumn> = Vec::new();
  for item in &select.projection {
    let mut resolved = resolve_projection_item(item, &table_entries)?;
    projection.append(&mut resolved);
  }

  let options = QuerySelectOptions {
    joins,
    aggregates: Vec::new(),
    group_by: Vec::new(),
    order_by: Vec::new(),
    limit: None,
    offset: None,
    distinct: false,
    having: None,
  };

  Ok(Query::Select {
    tables,
    table_index: 0,
    projection,
    predicate,
    options: if options.is_simple() {
      None
    } else {
      Some(Box::new(options))
    },
  })
}

fn parse_literal_for_type(raw: &str, target_type: &ValueType) -> Result<Value, TranslateError> {
  let s = trim_cast_suffix(raw);

  if s.eq_ignore_ascii_case("NULL") {
    return Ok(Value::Null);
  }

  let strip_quotes = |v: &str| {
    let v = v.trim();
    if v.starts_with('\'') && v.ends_with('\'') && v.len() >= 2 {
      v[1..v.len() - 1].to_string()
    } else {
      v.to_string()
    }
  };

  match target_type {
    ValueType::Null => Ok(Value::Null),
    ValueType::Type => Err(TranslateError::Custom(
      "type literals are not supported by the minimal translator".into(),
    )),
    ValueType::Bool => {
      let s_lower = s.to_ascii_lowercase();
      match s_lower.as_str() {
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        _ => Err(TranslateError::Custom(format!(
          "failed to parse boolean literal: {}",
          s
        ))),
      }
    }
    ValueType::Uuid => {
      let inner = strip_quotes(s);
      match uuid::Uuid::parse_str(&inner) {
        Ok(u) => Ok(Value::Uuid(u)),
        Err(_) => Err(TranslateError::Custom(format!(
          "failed to parse UUID literal: {}",
          inner
        ))),
      }
    }
    ValueType::Integer => {
      let candidate = strip_quotes(s);
      candidate
        .parse::<i64>()
        .map(Value::Integer)
        .map_err(|e| TranslateError::Custom(format!("failed to parse integer: {}", e)))
    }
    ValueType::Float => {
      let candidate = strip_quotes(s);
      candidate
        .parse::<f64>()
        .map(Value::Float)
        .map_err(|e| TranslateError::Custom(format!("failed to parse float: {}", e)))
    }
    ValueType::Text => Ok(Value::Text(strip_quotes(s))),
    ValueType::Json => {
      let candidate = strip_quotes(s);
      serde_json::from_str::<serde_json::Value>(&candidate)
        .map(Value::from)
        .map_err(|e| TranslateError::Custom(format!("failed to parse json: {}", e)))
    }
    ValueType::Blob => Err(TranslateError::Custom(
      "blob literals are not supported by the minimal translator".into(),
    )),
  }
}

fn expr_to_value_guess(expr: &SQLExpr) -> Result<Value, TranslateError> {
  let raw = expr.to_string();
  let s = trim_cast_suffix(&raw);

  if s.eq_ignore_ascii_case("NULL") {
    return Ok(Value::Null);
  }

  if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
    let inner = s[1..s.len() - 1].to_string();

    if let Ok(u) = uuid::Uuid::parse_str(&inner) {
      return Ok(Value::Uuid(u));
    }

    if let Ok(j) = serde_json::from_str::<serde_json::Value>(&inner) {
      return Ok(Value::from(j));
    }
    return Ok(Value::Text(inner));
  }

  if let Ok(i) = s.parse::<i64>() {
    return Ok(Value::Integer(i));
  }

  if let Ok(f) = s.parse::<f64>() {
    return Ok(Value::Float(f));
  }

  Err(TranslateError::Custom(format!(
    "unsupported literal expression for INSERT: {}",
    expr
  )))
}

fn expr_to_expr_value(
  expr: &SQLExpr,
  tables: &[TableEntry],
  params: &mut ParamState<'_>,
) -> Result<QueryExprValue, TranslateError> {
  if let Some(placeholder) = parse_placeholder(expr)? {
    return Ok(QueryExprValue::Value(
      params.resolve_placeholder(placeholder)?,
    ));
  }

  match expr {
    SQLExpr::Identifier(_) | SQLExpr::CompoundIdentifier(_) => Ok(QueryExprValue::Column(
      resolve_column_reference(expr, tables)?,
    )),
    SQLExpr::Value(_) | SQLExpr::Nested(_) | SQLExpr::Cast { .. } | SQLExpr::TypedString { .. } => {
      Ok(QueryExprValue::Value(expr_to_value_guess(expr)?))
    }
    other => Err(TranslateError::Custom(format!(
      "unsupported expression in assignment/predicate: {}",
      other
    ))),
  }
}

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
      let table = insert.table.to_string();
      let table_index = 0;
      let table_schema = resolver
        .describe_table(&table)
        .await
        .ok_or_else(|| TranslateError::Custom(format!("unknown table: {}", table)))?;

      if let Some(source) = &insert.source {
        match &*source.body {
          sqlparser::ast::SetExpr::Values(values) => {
            if values.rows.len() != 1 {
              return Err(TranslateError::Custom(
                "only single-row VALUES INSERT is supported".into(),
              ));
            }

            let row_exprs = &values.rows[0];
            let mut row: Vec<Value> = vec![Value::Null; table_schema.columns.len()];

            if insert.columns.is_empty() {
              if row_exprs.len() != table_schema.columns.len() {
                return Err(TranslateError::Custom(format!(
                  "VALUES count ({}) does not match table column count ({})",
                  row_exprs.len(),
                  table_schema.columns.len()
                )));
              }

              for (i, expr) in row_exprs.iter().enumerate() {
                let col_schema = &table_schema.columns[i];
                row[i] = if let Some(placeholder) = parse_placeholder(expr)? {
                  params.resolve_placeholder(placeholder)?
                } else {
                  parse_literal_for_type(&expr.to_string(), &col_schema.r#type)?
                };
              }
            } else {
              if insert.columns.len() != row_exprs.len() {
                return Err(TranslateError::Custom(
                  "number of columns does not match number of VALUES expressions".into(),
                ));
              }

              for (col_ident, expr) in insert.columns.iter().zip(row_exprs.iter()) {
                let idx = find_column_index(&table_schema, &col_ident.to_string())?;
                let col_schema = &table_schema.columns[idx];

                let v = if let Some(placeholder) = parse_placeholder(expr)? {
                  params.resolve_placeholder(placeholder)?
                } else {
                  parse_literal_for_type(&expr.to_string(), &col_schema.r#type)?
                };

                row[idx] = v;
              }
            }

            Ok(Query::Insert {
              tables: vec![table],
              table_index,
              row,
              returning: None,
            })
          }
          _ => Err(TranslateError::Custom(
            "only INSERT ... VALUES (...) is supported by the minimal translator".into(),
          )),
        }
      } else if !insert.assignments.is_empty() {
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
      let table_index = 0;

      let mut assigns: Vec<QueryUpdateAssignment> = Vec::new();
      for assign in update.assignments.iter() {
        match &assign.target {
          sqlparser::ast::AssignmentTarget::ColumnName(obj_name) => {
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
            if let Some(q) = qual
              && q != table_name
            {
              return Err(TranslateError::Custom(format!(
                "qualified column refers to unknown table: {}",
                q
              )));
            }
            let idx = find_column_index(&table_schema, &col_name)?;
            let table_entries = vec![TableEntry {
              name: table_name.clone(),
              alias: None,
              schema: table_schema.clone(),
            }];
            let value = expr_to_expr_value(&assign.value, &table_entries, params)?;
            assigns.push(QueryUpdateAssignment {
              column: QueryColumn {
                table_index,
                column_index: idx as ColumnSchemaIndex,
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

      let table_entries = vec![TableEntry {
        name: table_name.clone(),
        alias: None,
        schema: table_schema.clone(),
      }];

      let predicate = match &update.selection {
        Some(expr) => Some(translate_predicate(expr, &table_entries, params)?),
        None => None,
      };

      Ok(Query::Update {
        tables: vec![table_name],
        table_index,
        assignments: assigns,
        predicate,
        joins: Vec::new(),
        from_table_indexes: Vec::new(),
        returning: None,
      })
    }
    other => Err(TranslateError::Custom(format!(
      "expected UPDATE statement, got {}",
      other
    ))),
  }
}

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
      let table = match &del.from {
        sqlparser::ast::FromTable::WithoutKeyword(twj) => {
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
      let table_index = 0;

      let table_entries = vec![TableEntry {
        name: table.clone(),
        alias: None,
        schema: table_schema.clone(),
      }];

      let predicate = match &del.selection {
        Some(expr) => Some(translate_predicate(expr, &table_entries, params)?),
        None => None,
      };

      Ok(Query::Delete {
        tables: vec![table],
        table_index,
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

fn parse_create_table_schema(
  create: &sqlparser::ast::CreateTable,
) -> Result<TableSchema, TranslateError> {
  let table_name = object_name_to_string(&create.name);

  let mut columns = Vec::new();
  let mut primary_key = Vec::new();

  for (column_index, col) in create.columns.iter().enumerate() {
    let column_type = match &col.data_type {
      DataType::Uuid => ValueType::Uuid,
      DataType::Text => ValueType::Text,
      DataType::Varchar(_)
      | DataType::Char(_)
      | DataType::Character(_)
      | DataType::CharacterVarying(_)
      | DataType::CharVarying(_)
      | DataType::Nvarchar(_) => ValueType::Text,
      DataType::Int(_)
      | DataType::Integer(_)
      | DataType::Int2(_)
      | DataType::Int4(_)
      | DataType::Int8(_)
      | DataType::Int16
      | DataType::Int32
      | DataType::Int64
      | DataType::Int128
      | DataType::Int256
      | DataType::IntUnsigned(_)
      | DataType::Int4Unsigned(_)
      | DataType::IntegerUnsigned(_)
      | DataType::Int2Unsigned(_)
      | DataType::Int8Unsigned(_) => ValueType::Integer,
      DataType::Float(_)
      | DataType::FloatUnsigned(_)
      | DataType::Float4
      | DataType::Float32
      | DataType::Float64
      | DataType::Real
      | DataType::RealUnsigned
      | DataType::Float8
      | DataType::Double(_)
      | DataType::DoubleUnsigned(_)
      | DataType::DoublePrecision
      | DataType::DoublePrecisionUnsigned => ValueType::Float,
      DataType::Boolean => ValueType::Bool,
      DataType::JSON | DataType::JSONB => ValueType::Json,
      _ => {
        return Err(TranslateError::Custom(format!(
          "unsupported column type in CREATE TABLE: {}",
          col.data_type
        )));
      }
    };

    if col
      .options
      .iter()
      .any(|opt| matches!(opt.option, ColumnOption::PrimaryKey(_)))
    {
      primary_key.push(column_index as u32);
    }

    columns.push(ColumnSchema {
      name: col.name.value.clone(),
      r#type: column_type,
    });
  }

  for constraint in create.constraints.iter() {
    if let TableConstraint::PrimaryKey(pk) = constraint {
      for ident in &pk.columns {
        let column_name = ident.column.to_string();
        let idx = columns
          .iter()
          .position(|c| c.name == column_name)
          .ok_or_else(|| {
            TranslateError::Custom(format!(
              "unknown column '{}' in PRIMARY KEY constraint",
              column_name
            ))
          })?;
        primary_key.push(idx as u32);
      }
    }
  }

  Ok(TableSchema {
    name: table_name,
    columns,
    primary_key,
  })
}

fn translate_create_table<S>(
  stmt: &SQLStatement,
  _resolver: &S,
  _params: &mut ParamState<'_>,
) -> Result<Statement, TranslateError>
where
  S: DescribeSchema,
{
  match stmt {
    SQLStatement::CreateTable(create) => {
      let schema = parse_create_table_schema(create)?;
      Ok(Statement::DataDefinition(DataDefinition::CreateTable {
        schema,
        if_not_exists: create.if_not_exists,
      }))
    }
    other => Err(TranslateError::Custom(format!(
      "expected CREATE TABLE statement, got {}",
      other
    ))),
  }
}

impl Translator for SqlTranslator {
  async fn translate_with_params<S>(
    &self,
    query: &str,
    params: Option<&QueryParams>,
    resolver: &S,
  ) -> Result<Statement, TranslateError>
  where
    S: DescribeSchema,
  {
    let mut pstate = ParamState::new(params);

    let stmt = parse_single_statement(query)?;

    match &stmt {
      SQLStatement::Query(q) => match &*q.body {
        sqlparser::ast::SetExpr::Select(select) => translate_select(select, resolver, &mut pstate)
          .await
          .map(Statement::Query),
        _ => Err(TranslateError::Custom(
          "only simple SELECT statements are supported by the minimal translator".into(),
        )),
      },
      SQLStatement::CreateTable(_) => translate_create_table(&stmt, resolver, &mut pstate),
      SQLStatement::Insert(_) => translate_insert(&stmt, resolver, &mut pstate)
        .await
        .map(Statement::Query),
      SQLStatement::Update(_) => translate_update(&stmt, resolver, &mut pstate)
        .await
        .map(Statement::Query),
      SQLStatement::Delete(_) => translate_delete(&stmt, resolver, &mut pstate)
        .await
        .map(Statement::Query),
      other => Err(TranslateError::Custom(format!(
        "unsupported SQL statement: {}",
        other
      ))),
    }
  }
}
