#[cfg(not(feature = "std"))]
use alloc::{
  boxed::Box,
  string::{String, ToString},
  vec::Vec,
};

use hashbrown::HashMap;
use sqlparser::ast::{
  BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator,
};

use super::TranslateError;

#[allow(dead_code)]
pub struct HavingContext<'a> {
  pub group_by: &'a Vec<db_engine::QualifiedColumn>,
  pub aggregates: &'a Vec<db_engine::Aggregate>,
  pub proj_alias_map: &'a HashMap<String, db_engine::QualifiedColumn>,
  pub alias_map: &'a HashMap<String, String>,
  pub table_schemas: &'a HashMap<String, db_engine::TableSchema>,
  pub resolver: &'a dyn crate::translate::SchemaResolver,
  pub mapper: &'a dyn crate::translate::ValueMapper,
}

// helper to resolve qualified column for HAVING
fn resolve_qc_for_having(
  expr: &SqlExpr,
  alias_map: &HashMap<String, String>,
  table_schemas: &HashMap<String, db_engine::TableSchema>,
) -> Result<db_engine::QualifiedColumn, TranslateError> {
  crate::translate::helpers::resolve_column_local(expr, alias_map, table_schemas)
}

fn find_agg_index(
  ctx: &HavingContext<'_>,
  func_name: &str,
  arg_qc: Option<db_engine::QualifiedColumn>,
) -> Option<usize> {
  for (i, ag) in ctx.aggregates.iter().enumerate() {
    match (ag, func_name) {
      (db_engine::Aggregate::Count(opt), "count") => match (opt, &arg_qc) {
        (None, None) => return Some(i),
        (Some(a), Some(q)) if a == q => return Some(i),
        _ => {}
      },
      (db_engine::Aggregate::Sum(a), "sum") if Some(a) == arg_qc.as_ref() => return Some(i),
      (db_engine::Aggregate::Min(a), "min") if Some(a) == arg_qc.as_ref() => return Some(i),
      (db_engine::Aggregate::Max(a), "max") if Some(a) == arg_qc.as_ref() => return Some(i),
      (db_engine::Aggregate::Avg(a), "avg") if Some(a) == arg_qc.as_ref() => return Some(i),
      _ => {}
    }
  }
  None
}

fn resolve_aggregate_ref(
  func: &sqlparser::ast::Function,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::RefOrAgg, TranslateError> {
  let fname = func.name.to_string().to_lowercase();
  let first = fname.split('.').next().unwrap_or("");
  let args = match &func.args {
    FunctionArguments::List(list) => &list.args[..],
    FunctionArguments::None => &[],
    FunctionArguments::Subquery(_) => {
      return Err(TranslateError::UnsupportedFeature(
        "aggregate with subquery arg in HAVING unsupported".into(),
      ));
    }
  };
  if args.len() != 1 && !(first == "count" && args.len() == 1) {
    return Err(TranslateError::UnsupportedFeature(
      "aggregate in HAVING must take one argument".into(),
    ));
  }
  let arg_opt = match &args[0] {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) => Some(resolve_qc_for_having(
      arg_expr,
      ctx.alias_map,
      ctx.table_schemas,
    )?),
    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
    _ => {
      return Err(TranslateError::UnsupportedFeature(
        "unsupported aggregate arg in HAVING".into(),
      ));
    }
  };
  if let Some(idx) = find_agg_index(ctx, first, arg_opt.clone()) {
    Ok(db_engine::RefOrAgg::AggregateIndex(idx))
  } else {
    Err(TranslateError::UnsupportedFeature(
      "unknown aggregate in HAVING".into(),
    ))
  }
}

fn resolve_projection_alias_ref(
  ident: &sqlparser::ast::Ident,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::RefOrAgg, TranslateError> {
  if let Some(qc) = ctx.proj_alias_map.get(&ident.value) {
    for (i, ag) in ctx.aggregates.iter().enumerate() {
      match ag {
        db_engine::Aggregate::Count(opt) => {
          if let Some(arg) = opt
            && arg == qc
          {
            return Ok(db_engine::RefOrAgg::AggregateIndex(i));
          }
        }
        db_engine::Aggregate::Sum(a)
        | db_engine::Aggregate::Min(a)
        | db_engine::Aggregate::Max(a)
        | db_engine::Aggregate::Avg(a) => {
          if a == qc {
            return Ok(db_engine::RefOrAgg::AggregateIndex(i));
          }
        }
      }
    }
    Ok(db_engine::RefOrAgg::Column(qc.clone()))
  } else {
    let qc = resolve_qc_for_having(
      &SqlExpr::Identifier(ident.clone()),
      ctx.alias_map,
      ctx.table_schemas,
    )?;
    Ok(db_engine::RefOrAgg::Column(qc))
  }
}

fn resolve_column_ref(
  expr: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::RefOrAgg, TranslateError> {
  let qc = resolve_qc_for_having(expr, ctx.alias_map, ctx.table_schemas)?;
  Ok(db_engine::RefOrAgg::Column(qc))
}

fn resolve_ref(
  e: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::RefOrAgg, TranslateError> {
  match e {
    SqlExpr::Function(func) => resolve_aggregate_ref(func, ctx),
    SqlExpr::Identifier(ident) => resolve_projection_alias_ref(ident, ctx),
    SqlExpr::CompoundIdentifier(_) => resolve_column_ref(e, ctx),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported HAVING operand".into(),
    )),
  }
}

fn resolve_having_literal(
  expr: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::EngineValue, TranslateError> {
  match expr {
    SqlExpr::Value(_) | SqlExpr::Cast { .. } => ctx.mapper.map_sql_value(expr),
    _ => Err(TranslateError::UnsupportedFeature(
      "HAVING RHS must be literal value in v1".into(),
    )),
  }
}

fn translate_having_binary(
  left: &SqlExpr,
  op: &BinaryOperator,
  right: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::HavingPredicate, TranslateError> {
  match op {
    BinaryOperator::And => {
      translate_having_logical(db_engine::HavingPredicate::And, left, right, ctx)
    }
    BinaryOperator::Or => {
      translate_having_logical(db_engine::HavingPredicate::Or, left, right, ctx)
    }
    BinaryOperator::Eq
    | BinaryOperator::NotEq
    | BinaryOperator::Lt
    | BinaryOperator::LtEq
    | BinaryOperator::Gt
    | BinaryOperator::GtEq => translate_having_comparison(left, op, right, ctx),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported binary operator in HAVING".into(),
    )),
  }
}

fn translate_having_logical<F>(
  constructor: F,
  left: &SqlExpr,
  right: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::HavingPredicate, TranslateError>
where
  F: Fn(
    Box<db_engine::HavingPredicate>,
    Box<db_engine::HavingPredicate>,
  ) -> db_engine::HavingPredicate,
{
  Ok(constructor(
    Box::new(expr_to_having_predicate(left, ctx)?),
    Box::new(expr_to_having_predicate(right, ctx)?),
  ))
}

fn translate_having_comparison(
  left: &SqlExpr,
  op: &BinaryOperator,
  right: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::HavingPredicate, TranslateError> {
  let lref = resolve_ref(left, ctx)?;
  let rval = resolve_having_literal(right, ctx)?;

  Ok(match op {
    BinaryOperator::Eq => db_engine::HavingPredicate::Equals(lref, rval),
    BinaryOperator::NotEq => db_engine::HavingPredicate::NotEquals(lref, rval),
    BinaryOperator::Lt => db_engine::HavingPredicate::LessThan(lref, rval),
    BinaryOperator::LtEq => db_engine::HavingPredicate::LessThanOrEquals(lref, rval),
    BinaryOperator::Gt => db_engine::HavingPredicate::GreaterThan(lref, rval),
    BinaryOperator::GtEq => db_engine::HavingPredicate::GreaterThanOrEquals(lref, rval),
    _ => unreachable!(),
  })
}

fn translate_having_unary(
  op: &UnaryOperator,
  expr: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::HavingPredicate, TranslateError> {
  match op {
    UnaryOperator::Not => Ok(db_engine::HavingPredicate::Not(Box::new(
      expr_to_having_predicate(expr, ctx)?,
    ))),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported unary operator in HAVING".into(),
    )),
  }
}

pub fn expr_to_having_predicate(
  expr: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::HavingPredicate, TranslateError> {
  match expr {
    SqlExpr::BinaryOp { left, op, right } => translate_having_binary(left, op, right, ctx),
    SqlExpr::UnaryOp { op, expr } => translate_having_unary(op, expr, ctx),
    SqlExpr::IsNull(inner) => Ok(db_engine::HavingPredicate::IsNull(resolve_ref(inner, ctx)?)),
    SqlExpr::IsNotNull(inner) => Ok(db_engine::HavingPredicate::IsNotNull(resolve_ref(
      inner, ctx,
    )?)),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported HAVING expression".into(),
    )),
  }
}
