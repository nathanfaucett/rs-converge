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
  match expr {
    SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
      Ok(db_engine::QualifiedOperand::Column(
        crate::translate::helpers::resolve_column_local(expr, alias_map, table_schemas)?,
      ))
    }
    SqlExpr::Value(_) => Ok(db_engine::QualifiedOperand::Value(
      mapper.map_sql_value(expr)?,
    )),
    SqlExpr::Cast { expr: inner, .. } => {
      // Resolve through CAST — treat as pass-through for column operands.
      // Literal-value casts (e.g. UUID::text) fall back to the mapper.
      if matches!(
        inner.as_ref(),
        SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_)
      ) {
        resolve_operand(inner, alias_map, table_schemas, mapper)
      } else {
        Ok(db_engine::QualifiedOperand::Value(
          mapper.map_sql_value(expr)?,
        ))
      }
    }
    SqlExpr::Function(func) => {
      let name = func.name.to_string().to_lowercase();
      if name == "lower" {
        let inner = extract_single_function_arg(func)?;
        let operand = resolve_operand(inner, alias_map, table_schemas, mapper)?;
        Ok(db_engine::QualifiedOperand::Lower(Box::new(operand)))
      } else {
        Err(TranslateError::UnsupportedFeature(format!(
          "unsupported function in operand: {}",
          name
        )))
      }
    }
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported operand in comparison".into(),
    )),
  }
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
  match expr {
    SqlExpr::BinaryOp { left, op, right } => {
      translate_binary_expr(left, op, right, alias_map, table_schemas, resolver, mapper)
    }
    SqlExpr::UnaryOp { op, expr } => {
      translate_unary_expr(op, expr, alias_map, table_schemas, resolver, mapper)
    }
    SqlExpr::InList {
      expr: in_expr,
      list,
      negated,
    } => translate_in_list_expr(in_expr, list, *negated, alias_map, table_schemas, mapper),
    SqlExpr::InSubquery {
      expr: in_expr,
      subquery,
      negated,
    } => translate_in_subquery_expr(
      in_expr,
      subquery,
      *negated,
      resolver,
      mapper,
      alias_map,
      table_schemas,
    ),
    SqlExpr::IsNull(inner) => Ok(db_engine::QualifiedPredicate::IsNull(
      crate::translate::helpers::resolve_column_local(inner, alias_map, table_schemas)?,
    )),
    SqlExpr::IsNotNull(inner) => Ok(db_engine::QualifiedPredicate::IsNotNull(
      crate::translate::helpers::resolve_column_local(inner, alias_map, table_schemas)?,
    )),
    SqlExpr::Like {
      negated,
      expr,
      pattern,
      ..
    } => translate_like_expr(expr, pattern, *negated, alias_map, table_schemas, mapper),
    SqlExpr::ILike {
      negated,
      expr,
      pattern,
      ..
    } => translate_ilike_expr(expr, pattern, *negated, alias_map, table_schemas, mapper),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported WHERE expression".into(),
    )),
  }
}
