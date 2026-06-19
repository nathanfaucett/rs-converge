use db_query::{
  AlterIndexOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr, QueryExprValue,
  QueryFrom, QueryInsert, QueryJoin, QueryJoinKind, QueryParams, QuerySelect, QueryUpdate,
  Statement, TranslateError, TranslateResult, Translator,
};

use db_schema::{IndexSchema, TableSchema};
use db_value::{Value, ValueType};
use sqlparser::{
  ast::{
    self, Expr, Join, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, SelectItem,
    TableFactor, TableObject, TableWithJoins,
  },
  dialect::PostgreSqlDialect,
  parser::Parser,
};

#[derive(Debug, Clone, Copy)]
pub struct SqlTranslator;

impl Translator for SqlTranslator {
  async fn translate_with_params(
    &self,
    query: &str,
    params: Option<&QueryParams>,
  ) -> TranslateResult<Vec<Statement>> {
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, query).map_err(TranslateError::custom)?;

    let mut results = Vec::with_capacity(stmts.len());
    for stmt in stmts {
      results.push(translate_stmt(stmt, params)?);
    }
    Ok(results)
  }
}

fn table_name_to_string(name: &TableObject) -> TranslateResult<String> {
  match name {
    TableObject::TableName(object_name) => {
      if object_name.0.len() == 1 {
        object_name_part_to_string(&object_name.0[0])
      } else {
        Err(TranslateError::custom(format!(
          "Unsupported table name with {} parts: {:?}",
          object_name.0.len(),
          object_name
        )))
      }
    }
    _ => Err(TranslateError::custom(format!(
      "Unsupported table object: {:?}",
      name
    ))),
  }
}

fn object_name_to_string(name: &ObjectName) -> TranslateResult<String> {
  if name.0.len() == 1 {
    object_name_part_to_string(&name.0[0])
  } else {
    Err(TranslateError::custom(format!(
      "Unsupported object name with {} parts: {:?}",
      name.0.len(),
      name
    )))
  }
}

fn object_name_to_table_column_string(
  name: &ObjectName,
) -> TranslateResult<(Option<String>, String)> {
  if name.0.len() == 1 {
    let column_name = object_name_part_to_string(&name.0[0])?;
    Ok((None, column_name))
  } else if name.0.len() == 2 {
    let table_name = object_name_part_to_string(&name.0[0])?;
    let column_name = object_name_part_to_string(&name.0[1])?;
    Ok((Some(table_name), column_name))
  } else {
    Err(TranslateError::custom(format!(
      "Unsupported object name with {} parts: {:?}",
      name.0.len(),
      name
    )))
  }
}

fn object_name_part_to_string(part: &ObjectNamePart) -> TranslateResult<String> {
  match part {
    ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
    ObjectNamePart::Function(_) => Err(TranslateError::custom(
      "Function names not supported in object names",
    )),
  }
}

fn translate_stmt(
  stmt: ast::Statement,
  params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
  match stmt {
    ast::Statement::Query(query) => translate_query(*query, params),
    ast::Statement::Insert(insert) => translate_insert(insert, params),
    ast::Statement::Update(update) => translate_update(update, params),
    ast::Statement::Delete(delete) => translate_delete(delete, params),
    ast::Statement::CreateTable(create_table) => translate_create_table(create_table),
    ast::Statement::Drop {
      object_type,
      if_exists,
      names,
      ..
    } => translate_drop(object_type, if_exists, names),
    ast::Statement::AlterTable(alter_table) => translate_alter_table(alter_table),
    ast::Statement::CreateIndex(create_index) => translate_create_index(create_index),
    ast::Statement::AlterIndex { name, operation } => translate_alter_index(name, operation),
    _ => Err(TranslateError::custom(format!(
      "Unsupported SQL statement: {:?}",
      stmt
    ))),
  }
}

fn translate_query(query: ast::Query, _params: Option<&QueryParams>) -> TranslateResult<Statement> {
  if query.with.is_some() {
    return Err(TranslateError::custom("CTEs not yet supported"));
  }
  if !matches!(*query.body, ast::SetExpr::Select(_)) {
    return Err(TranslateError::custom(
      "Only plain SELECT queries supported",
    ));
  }

  let select = match *query.body {
    ast::SetExpr::Select(select) => select,
    _ => unreachable!(),
  };

  let from = translate_from(&select.from)?;
  let projection = translate_projection(&select.projection)?;
  let predicate = select.selection.map(translate_expr).transpose()?;
  let having = select.having.map(translate_expr).transpose()?;

  // TODO: Expand these as needed
  let aggregates = vec![];
  let group_by = vec![];
  let order_by = vec![];

  let limit = None; // TODO: implement properly
  let offset = None; // TODO: implement properly

  Ok(Statement::Query(Query::Select(QuerySelect {
    from,
    projection,
    predicate,
    aggregates,
    group_by,
    order_by,
    limit,
    offset,
    having,
  })))
}

fn translate_from(from: &[TableWithJoins]) -> TranslateResult<QueryFrom> {
  if from.is_empty() {
    return Err(TranslateError::custom("Missing FROM clause"));
  }

  let twj = &from[0];
  let (table, alias) = match &twj.relation {
    TableFactor::Table { name, alias, .. } => (
      object_name_to_string(name)?,
      alias.as_ref().map(|a| a.name.value.to_owned()),
    ),
    _ => {
      return Err(TranslateError::custom(
        "Only simple table references supported in FROM",
      ));
    }
  };

  let mut joins = Vec::new();
  for join in &twj.joins {
    joins.push(translate_join(join)?);
  }

  Ok(QueryFrom {
    table,
    alias,
    joins,
  })
}

fn translate_join(join: &Join) -> TranslateResult<QueryJoin> {
  let (table, alias) = match &join.relation {
    TableFactor::Table { name, alias, .. } => (
      object_name_to_string(name)?,
      alias.as_ref().map(|a| a.name.value.to_owned()),
    ),
    _ => {
      return Err(TranslateError::custom(
        "Complex table in JOIN not supported",
      ));
    }
  };

  let (kind, on) = match &join.join_operator {
    JoinOperator::Inner(join_constraint) => (
      QueryJoinKind::Inner,
      parse_join_constraint(join_constraint)?,
    ),
    JoinOperator::Left(join_constraint) => {
      (QueryJoinKind::Left, parse_join_constraint(join_constraint)?)
    }
    JoinOperator::Right(join_constraint) => (
      QueryJoinKind::Right,
      parse_join_constraint(join_constraint)?,
    ),
    JoinOperator::FullOuter(join_constraint) => {
      (QueryJoinKind::Full, parse_join_constraint(join_constraint)?)
    }
    _ => return Err(TranslateError::custom("Unsupported JOIN type")),
  };

  Ok(QueryJoin {
    kind,
    table,
    alias,
    on,
  })
}

fn parse_join_constraint(constraint: &JoinConstraint) -> TranslateResult<QueryExpr> {
  match constraint {
    JoinConstraint::On(expr) => translate_expr(expr.clone()),
    JoinConstraint::Using(_) => Err(TranslateError::custom("USING joins not yet supported")),
    JoinConstraint::Natural => Err(TranslateError::custom("NATURAL joins not yet supported")),
    JoinConstraint::None => Err(TranslateError::custom("JOIN without ON condition")),
  }
}

fn translate_projection(projection: &[SelectItem]) -> TranslateResult<Vec<QueryColumn>> {
  let mut cols = Vec::new();
  for item in projection {
    match item {
      SelectItem::UnnamedExpr(expr) => {
        if let Expr::Identifier(ident) = expr {
          cols.push(QueryColumn::new("".to_string(), ident.value.clone()));
        } else if let Expr::CompoundIdentifier(idents) = expr {
          if idents.len() == 2 {
            let table = idents[0].value.clone();
            let column = idents[1].value.clone();
            cols.push(QueryColumn::new(table, column));
          } else {
            cols.push(QueryColumn::new("".to_string(), format!("{:?}", expr)));
          }
        } else {
          cols.push(QueryColumn::new("".to_string(), format!("expr:{:?}", expr)));
        }
      }
      SelectItem::Wildcard(_) => {
        cols.push(QueryColumn::new("".to_string(), "*".to_string()));
      }
      _ => return Err(TranslateError::custom("Unsupported projection item")),
    }
  }
  Ok(cols)
}

fn translate_expr(expr: Expr) -> TranslateResult<QueryExpr> {
  match expr {
    Expr::BinaryOp { left, op, right } => {
      let left_val = Box::new(translate_expr(*left)?);
      let right_val = Box::new(translate_expr(*right)?);

      match op {
        ast::BinaryOperator::Eq => Ok(QueryExpr::Equals(left_val, right_val)),
        ast::BinaryOperator::NotEq => Ok(QueryExpr::NotEquals(left_val, right_val)),
        ast::BinaryOperator::Lt => Ok(QueryExpr::LessThan(left_val, right_val)),
        ast::BinaryOperator::LtEq => Ok(QueryExpr::LessThanOrEquals(left_val, right_val)),
        ast::BinaryOperator::Gt => Ok(QueryExpr::GreaterThan(left_val, right_val)),
        ast::BinaryOperator::GtEq => Ok(QueryExpr::GreaterThanOrEquals(left_val, right_val)),
        ast::BinaryOperator::And => Ok(QueryExpr::And(left_val, right_val)),
        ast::BinaryOperator::Or => Ok(QueryExpr::Or(left_val, right_val)),
        _ => Err(TranslateError::custom(format!(
          "Unsupported binary operator: {:?}",
          op
        ))),
      }
    }
    Expr::Identifier(ident) => {
      let col = QueryColumn::new("".into(), ident.value);
      Ok(QueryExpr::Value(QueryExprValue::Column(col)))
    }
    Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
      let col = QueryColumn::new(idents[0].value.clone(), idents[1].value.clone());
      Ok(QueryExpr::Value(QueryExprValue::Column(col)))
    }
    Expr::IsNull(inner) => Ok(QueryExpr::IsNull(Box::new(translate_expr(*inner)?))),
    Expr::IsNotNull(inner) => Ok(QueryExpr::IsNotNull(Box::new(translate_expr(*inner)?))),
    Expr::Value(ast::ValueWithSpan { value, .. }) => {
      let db_value = match value {
        ast::Value::Number(n, _) => Value::Text(n),
        ast::Value::SingleQuotedString(s) => Value::Text(s),
        ast::Value::Boolean(b) => Value::Bool(b),
        _ => Value::Text(format!("{:?}", value)),
      };
      Ok(QueryExpr::Value(QueryExprValue::Value(db_value)))
    }
    _ => Err(TranslateError::custom(format!(
      "Unsupported expression: {:?}",
      expr
    ))),
  }
}

fn translate_insert(
  insert: ast::Insert,
  _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
  let table = table_name_to_string(&insert.table)?;

  let row = if let Some(source) = insert.source {
    if let ast::SetExpr::Values(values) = *source.body {
      if let Some(row_exprs) = values.rows.first() {
        row_exprs
          .iter()
          .map(|e| {
            translate_expr_value(e.clone()).map(|v| match v {
              QueryExprValue::Value(val) => val,
              _ => Value::Text(format!("{:?}", v)),
            })
          })
          .collect::<Result<Vec<_>, _>>()?
      } else {
        vec![]
      }
    } else {
      vec![]
    }
  } else {
    vec![]
  };

  let returning = if let Some(returning_items) = insert.returning {
    let mut returning = Vec::new();

    for item in returning_items {
      match item {
        SelectItem::UnnamedExpr(expr) => {
          if let Expr::Identifier(ident) = expr {
            returning.push(ident.value.clone());
          } else if let Expr::CompoundIdentifier(idents) = expr {
            if idents.len() == 1 {
              let column = idents[0].value.clone();
              returning.push(column);
            } else {
              return Err(TranslateError::custom(format!(
                "Unsupported RETURNING identifier: {:?}",
                idents
              )));
            }
          } else {
            return Err(TranslateError::custom(format!(
              "Unsupported RETURNING expression: {:?}",
              expr
            )));
          }
        }
        SelectItem::Wildcard(_) => {
          returning.push("*".to_string());
        }
        _ => return Err(TranslateError::custom("Unsupported RETURNING item")),
      }
    }

    Some(returning)
  } else {
    None
  };

  Ok(Statement::Query(Query::Insert(QueryInsert {
    table,
    row,
    returning,
  })))
}

fn translate_update(
  update: ast::Update,
  _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
  let table = match &update.table.relation {
    TableFactor::Table { name, .. } => object_name_to_string(name)?,
    _ => {
      return Err(TranslateError::custom(
        "Only simple table references supported in UPDATE",
      ));
    }
  };

  let predicate = update.selection.map(translate_expr).transpose()?;

  // TODO: Implement proper assignment parsing from update.assignments
  let assignments = vec![];

  Ok(Statement::Query(Query::Update(QueryUpdate {
    from: QueryFrom {
      table,
      alias: None,
      joins: vec![], // TODO: handle joins in UPDATE
    },
    assignments,
    predicate,
    returning: None, // TODO: handle RETURNING clause in UPDATE
  })))
}

fn translate_delete(
  delete: ast::Delete,
  _params: Option<&QueryParams>,
) -> TranslateResult<Statement> {
  if delete.tables.is_empty() {
    return Err(TranslateError::custom("No tables in DELETE"));
  }
  if delete.tables.len() > 1 {
    return Err(TranslateError::custom(
      "Multiple tables in DELETE not supported",
    ));
  }
  let table = object_name_to_string(&delete.tables[0])?;

  let predicate = delete.selection.map(translate_expr).transpose()?;

  Ok(Statement::Query(Query::Delete(QueryDelete {
    from: QueryFrom {
      table,
      alias: None,
      joins: vec![], // TODO: handle joins in DELETE
    },
    predicate,
    returning: None, // TODO: handle RETURNING clause in DELETE
  })))
}

fn translate_column_data_type(data_type: &ast::DataType) -> TranslateResult<ValueType> {
  match data_type {
    ast::DataType::Char(_) | ast::DataType::Varchar(_) | ast::DataType::Text => Ok(ValueType::Text),
    ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::BigInt(_) => {
      Ok(ValueType::Integer)
    }
    ast::DataType::Float(_) | ast::DataType::Double(_) => Ok(ValueType::Float),
    ast::DataType::Boolean => Ok(ValueType::Bool),
    ast::DataType::Blob(_) => Ok(ValueType::Blob),
    ast::DataType::Uuid => Ok(ValueType::Uuid),
    ast::DataType::JSON | ast::DataType::JSONB => Ok(ValueType::Json),
    _ => Err(TranslateError::custom(format!(
      "Unsupported column data type: {:?}",
      data_type
    ))),
  }
}

fn translate_create_table(create_table: ast::CreateTable) -> TranslateResult<Statement> {
  let table_name = object_name_to_string(&create_table.name)?;

  let columns = {
    let mut columns = Vec::with_capacity(create_table.columns.len());
    for col_def in &create_table.columns {
      columns.push(db_schema::ColumnSchema {
        name: col_def.name.value.clone(),
        r#type: translate_column_data_type(&col_def.data_type)?,
        primary_key: col_def
          .options
          .iter()
          .any(|opt| matches!(opt.option, ast::ColumnOption::PrimaryKey(_))),
      });
    }
    columns
  };

  let schema = TableSchema {
    name: table_name,
    columns,
  };

  Ok(Statement::DataDefinition(DataDefinition::CreateTable {
    schema,
    if_not_exists: create_table.if_not_exists,
  }))
}

fn translate_drop(
  object_type: ast::ObjectType,
  if_exists: bool,
  names: Vec<ast::ObjectName>,
) -> TranslateResult<Statement> {
  if names.is_empty() {
    return Err(TranslateError::custom("No object names in DROP"));
  }
  let name = object_name_to_string(&names[0])?;

  match object_type {
    ast::ObjectType::Table => Ok(Statement::DataDefinition(DataDefinition::DropTable {
      table_name: name,
      if_exists,
    })),
    ast::ObjectType::Index => Ok(Statement::DataDefinition(DataDefinition::DropIndex {
      index_name: name,
      if_exists,
    })),
    _ => Err(TranslateError::custom(format!(
      "DROP {:?} not supported",
      object_type
    ))),
  }
}

fn translate_alter_table(alter_table: ast::AlterTable) -> TranslateResult<Statement> {
  let table_name = object_name_to_string(&alter_table.name)?;

  // TODO: proper mapping of operations
  let operations = vec![];

  Ok(Statement::DataDefinition(DataDefinition::AlterTable {
    table_name,
    operations,
    if_exists: alter_table.if_exists,
  }))
}

fn translate_create_index(create_index: ast::CreateIndex) -> TranslateResult<Statement> {
  let index_name = create_index
    .name
    .map(|n| object_name_to_string(&n))
    .unwrap_or(Ok(String::new()))?;

  let table_name = object_name_to_string(&create_index.table_name)?;

  let column_indices: Vec<u32> = create_index
    .columns
    .into_iter()
    .map(|_| 0u32) // TODO: proper mapping to actual column indices
    .collect();

  let schema = IndexSchema {
    name: index_name,
    table_name,
    column_indices,
    unique: create_index.unique,
  };

  Ok(Statement::DataDefinition(DataDefinition::CreateIndex {
    schema,
    if_not_exists: create_index.if_not_exists,
  }))
}

fn translate_alter_index(
  name: ast::ObjectName,
  operation: ast::AlterIndexOperation,
) -> TranslateResult<Statement> {
  let index_name = object_name_to_string(&name)?;

  let op = match operation {
    ast::AlterIndexOperation::RenameIndex { index_name } => AlterIndexOperation::Rename {
      new_name: object_name_to_string(&index_name)?,
    },
  };

  Ok(Statement::DataDefinition(DataDefinition::AlterIndex {
    index_name,
    operation: op,
    if_exists: false,
  }))
}

fn translate_expr_value(expr: Expr) -> TranslateResult<QueryExprValue> {
  match expr {
    Expr::Identifier(ident) => Ok(QueryExprValue::Column(QueryColumn::new(
      "".into(),
      ident.value,
    ))),
    Expr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(QueryExprValue::Column(
      QueryColumn::new(idents[0].value.clone(), idents[1].value.clone()),
    )),
    Expr::Value(ast::ValueWithSpan { value, .. }) => {
      let db_value = match value {
        ast::Value::Number(n, _) => Value::Text(n),
        ast::Value::SingleQuotedString(s) => Value::Text(s),
        ast::Value::Boolean(b) => Value::Bool(b),
        _ => Value::Text(format!("{:?}", value)),
      };
      Ok(QueryExprValue::Value(db_value))
    }
    _ => Err(TranslateError::custom(format!(
      "Unsupported expr value: {:?}",
      expr
    ))),
  }
}
