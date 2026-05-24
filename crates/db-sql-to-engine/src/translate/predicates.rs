#[cfg(not(feature = "std"))]
use alloc::{
  boxed::Box,
  format,
  string::{String, ToString},
  vec::Vec,
};
#[cfg(feature = "std")]
use std::string::ToString;

use hashbrown::HashMap;
use sqlparser::ast::{
  BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator,
};

use super::TranslateError;

fn extract_single_function_arg(
  func: &sqlparser::ast::Function,
) -> Result<&SqlExpr, TranslateError> {
  match &func.args {
    FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
      FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
      _ => Err(TranslateError::UnsupportedFeature(
        "function argument must be a simple expression".into(),
      )),
    },
    _ => Err(TranslateError::UnsupportedFeature(
      "function must have exactly one argument".into(),
    )),
  }
}

fn resolve_operand(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedOperand, TranslateError> {
  if matches!(
    expr,
    SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
  ) {
    return Ok(db_engine::QualifiedOperand::Column(
      crate::translate::helpers::resolve_column_local(expr, alias_map, table_schemas)?,
    ));
  }

  if matches!(expr, SqlExpr::Value(_)) {
    return Ok(db_engine::QualifiedOperand::Value(
      mapper.map_sql_value(expr)?,
    ));
  }

  if let SqlExpr::Cast { expr: inner, .. } = expr {
    return resolve_cast_operand(inner, expr, alias_map, table_schemas, mapper);
  }

  if let SqlExpr::Function(func) = expr {
    return resolve_function_operand(func, alias_map, table_schemas, mapper);
  }

  Err(TranslateError::UnsupportedFeature(
    "unsupported operand in comparison".into(),
  ))
}

fn resolve_cast_operand(
  inner: &SqlExpr,
  cast_expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedOperand, TranslateError> {
  // Resolve through CAST — treat as pass-through for column operands.
  // Literal-value casts (e.g. UUID::text) fall back to the mapper.
  if matches!(
    inner,
    SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
  ) {
    return resolve_operand(inner, alias_map, table_schemas, mapper);
  }

  Ok(db_engine::QualifiedOperand::Value(
    mapper.map_sql_value(cast_expr)?,
  ))
}

fn resolve_function_operand(
  func: &sqlparser::ast::Function,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedOperand, TranslateError> {
  let name = func.name.to_string().to_lowercase();
  if name != "lower" {
    return Err(TranslateError::UnsupportedFeature(format!(
      "unsupported function in operand: {}",
      name
    )));
  }

  let inner = extract_single_function_arg(func)?;
  let operand = resolve_operand(inner, alias_map, table_schemas, mapper)?;
  Ok(db_engine::QualifiedOperand::Lower(Box::new(operand)))
}

fn binary_predicate(
  op: &BinaryOperator,
  left: db_engine::QualifiedOperand,
  right: db_engine::QualifiedOperand,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  Ok(match op {
    BinaryOperator::Eq => db_engine::QualifiedPredicate::Equals(left, right),
    BinaryOperator::NotEq => db_engine::QualifiedPredicate::NotEquals(left, right),
    BinaryOperator::Lt => db_engine::QualifiedPredicate::LessThan(left, right),
    BinaryOperator::LtEq => db_engine::QualifiedPredicate::LessThanOrEquals(left, right),
    BinaryOperator::Gt => db_engine::QualifiedPredicate::GreaterThan(left, right),
    BinaryOperator::GtEq => db_engine::QualifiedPredicate::GreaterThanOrEquals(left, right),
    _ => {
      return Err(TranslateError::UnsupportedFeature(
        "unsupported binary operator in WHERE".into(),
      ));
    }
  })
}

fn translate_binary_expr(
  left: &SqlExpr,
  op: &BinaryOperator,
  right: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  match op {
    BinaryOperator::And => Ok(db_engine::QualifiedPredicate::And(
      Box::new(expr_to_qualified_predicate(
        left,
        alias_map,
        table_schemas,
        resolver,
        mapper,
      )?),
      Box::new(expr_to_qualified_predicate(
        right,
        alias_map,
        table_schemas,
        resolver,
        mapper,
      )?),
    )),
    BinaryOperator::Or => Ok(db_engine::QualifiedPredicate::Or(
      Box::new(expr_to_qualified_predicate(
        left,
        alias_map,
        table_schemas,
        resolver,
        mapper,
      )?),
      Box::new(expr_to_qualified_predicate(
        right,
        alias_map,
        table_schemas,
        resolver,
        mapper,
      )?),
    )),
    BinaryOperator::Eq
    | BinaryOperator::NotEq
    | BinaryOperator::Lt
    | BinaryOperator::LtEq
    | BinaryOperator::Gt
    | BinaryOperator::GtEq => {
      let left_op = resolve_operand(left, alias_map, table_schemas, mapper)?;
      let right_op = resolve_operand(right, alias_map, table_schemas, mapper)?;
      binary_predicate(op, left_op, right_op)
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported binary operator in WHERE".into(),
    )),
  }
}

fn translate_unary_expr(
  op: &UnaryOperator,
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  match op {
    UnaryOperator::Not => Ok(db_engine::QualifiedPredicate::Not(Box::new(
      expr_to_qualified_predicate(expr, alias_map, table_schemas, resolver, mapper)?,
    ))),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported unary operator in WHERE".into(),
    )),
  }
}

fn translate_in_list_expr(
  in_expr: &SqlExpr,
  list: &[SqlExpr],
  negated: bool,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  let qc = crate::translate::helpers::resolve_column_local(in_expr, alias_map, table_schemas)?;
  let mut values: Vec<db_engine::EngineValue> = Vec::new();
  for item in list {
    if matches!(item, SqlExpr::Value(_) | SqlExpr::Cast { .. }) {
      values.push(mapper.map_sql_value(item)?);
    } else {
      return Err(TranslateError::UnsupportedFeature(
        "IN list only supports literal values in v1".into(),
      ));
    }
  }
  Ok(db_engine::QualifiedPredicate::InList {
    expr: qc,
    list: values,
    negated,
  })
}

fn translate_in_subquery_expr(
  in_expr: &SqlExpr,
  subquery: &sqlparser::ast::Query,
  negated: bool,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  let qc = crate::translate::helpers::resolve_column_local(in_expr, alias_map, table_schemas)?;
  let sub_sql = format!("{}", subquery);
  let sub_q = crate::translate::parse_and_translate_with_mapper(&sub_sql, resolver, mapper)?;
  Ok(db_engine::QualifiedPredicate::InSubquery {
    expr: qc,
    subquery: Box::new(sub_q),
    negated,
  })
}

fn translate_like_expr(
  expr: &SqlExpr,
  pattern: &SqlExpr,
  negated: bool,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  let expr_op = resolve_operand(expr, alias_map, table_schemas, mapper)?;
  let pattern_op = resolve_operand(pattern, alias_map, table_schemas, mapper)?;
  Ok(db_engine::QualifiedPredicate::Like {
    expr: expr_op,
    pattern: pattern_op,
    negated,
  })
}

fn translate_ilike_expr(
  expr: &SqlExpr,
  pattern: &SqlExpr,
  negated: bool,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  let expr_op = resolve_operand(expr, alias_map, table_schemas, mapper)?;
  let pattern_op = resolve_operand(pattern, alias_map, table_schemas, mapper)?;
  Ok(db_engine::QualifiedPredicate::Like {
    expr: db_engine::QualifiedOperand::Lower(Box::new(expr_op)),
    pattern: db_engine::QualifiedOperand::Lower(Box::new(pattern_op)),
    negated,
  })
}

pub fn expr_to_qualified_predicate(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<db_engine::QualifiedPredicate, TranslateError> {
  type PredicateConverter = fn(
    &SqlExpr,
    &HashMap<String, String>,
    &HashMap<String, db_engine::TableSchema>,
    &dyn crate::translate::SchemaResolver,
    &dyn crate::translate::ValueMapper,
  ) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError>;

  let converters: &[PredicateConverter] = &[
    predicate_from_binary,
    predicate_from_unary,
    predicate_from_in_list,
    predicate_from_in_subquery,
    predicate_from_null_check,
    predicate_from_like,
    predicate_from_ilike,
  ];

  for converter in converters {
    if let Some(predicate) = converter(expr, alias_map, table_schemas, resolver, mapper)? {
      return Ok(predicate);
    }
  }

  Err(TranslateError::UnsupportedFeature(
    "unsupported WHERE expression".into(),
  ))
}

fn predicate_from_binary(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  if let SqlExpr::BinaryOp { left, op, right } = expr {
    return translate_binary_expr(left, op, right, alias_map, table_schemas, resolver, mapper)
      .map(Some);
  }
  Ok(None)
}

fn predicate_from_unary(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  if let SqlExpr::UnaryOp { op, expr } = expr {
    return translate_unary_expr(op, expr, alias_map, table_schemas, resolver, mapper).map(Some);
  }
  Ok(None)
}

fn predicate_from_in_list(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  _resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  if let SqlExpr::InList {
    expr: in_expr,
    list,
    negated,
  } = expr
  {
    return translate_in_list_expr(in_expr, list, *negated, alias_map, table_schemas, mapper)
      .map(Some);
  }
  Ok(None)
}

fn predicate_from_in_subquery(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  if let SqlExpr::InSubquery {
    expr: in_expr,
    subquery,
    negated,
  } = expr
  {
    return translate_in_subquery_expr(
      in_expr,
      subquery,
      *negated,
      resolver,
      mapper,
      alias_map,
      table_schemas,
    )
    .map(Some);
  }
  Ok(None)
}

fn predicate_from_null_check(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  _resolver: &dyn crate::translate::SchemaResolver,
  _mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  match expr {
    SqlExpr::IsNull(inner) => Ok(Some(db_engine::QualifiedPredicate::IsNull(
      crate::translate::helpers::resolve_column_local(inner, alias_map, table_schemas)?,
    ))),
    SqlExpr::IsNotNull(inner) => Ok(Some(db_engine::QualifiedPredicate::IsNotNull(
      crate::translate::helpers::resolve_column_local(inner, alias_map, table_schemas)?,
    ))),
    _ => Ok(None),
  }
}

fn predicate_from_like(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  _resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  if let SqlExpr::Like {
    negated,
    expr,
    pattern,
    ..
  } = expr
  {
    return translate_like_expr(expr, pattern, *negated, alias_map, table_schemas, mapper)
      .map(Some);
  }
  Ok(None)
}

fn predicate_from_ilike(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
  _resolver: &dyn crate::translate::SchemaResolver,
  mapper: &dyn crate::translate::ValueMapper,
) -> Result<Option<db_engine::QualifiedPredicate>, TranslateError> {
  if let SqlExpr::ILike {
    negated,
    expr,
    pattern,
    ..
  } = expr
  {
    return translate_ilike_expr(expr, pattern, *negated, alias_map, table_schemas, mapper)
      .map(Some);
  }
  Ok(None)
}

#[cfg(test)]
mod tests {
  use super::*;
  #[cfg(not(feature = "std"))]
  use alloc::vec;
  use db_engine::{ColumnSchema, EngineType, TableSchema};
  use sqlparser::ast::Ident;

  struct StubResolver;
  impl crate::translate::SchemaResolver for StubResolver {
    fn describe_table(&self, _name: &str) -> Option<TableSchema> {
      None
    }
  }

  fn sample_schemas() -> HashMap<String, db_engine::TableSchema> {
    let mut table_schemas = HashMap::new();
    table_schemas.insert(
      "users".into(),
      TableSchema {
        name: "users".into(),
        columns: vec![ColumnSchema {
          name: "name".into(),
          data_type: EngineType::Text,
        }],
        primary_key: vec![],
      },
    );
    table_schemas
  }

  #[test]
  fn predicate_from_null_check_handles_is_null() {
    let alias_map = HashMap::new();
    let table_schemas = sample_schemas();
    let resolver = StubResolver;
    let mapper = crate::translate::DefaultValueMapper;
    let expr = SqlExpr::IsNull(Box::new(SqlExpr::Identifier(Ident::new("name"))));

    let pred = predicate_from_null_check(&expr, &alias_map, &table_schemas, &resolver, &mapper)
      .expect("should parse")
      .expect("should convert");

    assert!(matches!(pred, db_engine::QualifiedPredicate::IsNull(_)));
  }

  #[test]
  fn predicate_from_null_check_handles_is_not_null() {
    let alias_map = HashMap::new();
    let table_schemas = sample_schemas();
    let resolver = StubResolver;
    let mapper = crate::translate::DefaultValueMapper;
    let expr = SqlExpr::IsNotNull(Box::new(SqlExpr::Identifier(Ident::new("name"))));

    let pred = predicate_from_null_check(&expr, &alias_map, &table_schemas, &resolver, &mapper)
      .expect("should parse")
      .expect("should convert");

    assert!(matches!(pred, db_engine::QualifiedPredicate::IsNotNull(_)));
  }
}
