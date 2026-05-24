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

fn function_name(func: &sqlparser::ast::Function) -> String {
  func.name.to_string().to_lowercase()
}

fn resolve_having_arg(
  arg: &FunctionArg,
  ctx: &HavingContext<'_>,
) -> Result<Option<db_engine::QualifiedColumn>, TranslateError> {
  match arg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) => Ok(Some(resolve_qc_for_having(
      arg_expr,
      ctx.alias_map,
      ctx.table_schemas,
    )?)),
    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
    _ => Err(TranslateError::UnsupportedFeature(
      "unsupported aggregate arg in HAVING".into(),
    )),
  }
}

fn aggregate_name(ag: &db_engine::Aggregate) -> &'static str {
  match ag {
    db_engine::Aggregate::Count(_) => "count",
    db_engine::Aggregate::Sum(_) => "sum",
    db_engine::Aggregate::Min(_) => "min",
    db_engine::Aggregate::Max(_) => "max",
    db_engine::Aggregate::Avg(_) => "avg",
  }
}

fn aggregate_arg_matches(
  ag: &db_engine::Aggregate,
  arg_qc: &Option<db_engine::QualifiedColumn>,
) -> bool {
  match ag {
    db_engine::Aggregate::Count(opt) => match (opt, arg_qc) {
      (None, None) => true,
      (Some(a), Some(q)) => a == q,
      _ => false,
    },
    db_engine::Aggregate::Sum(a)
    | db_engine::Aggregate::Min(a)
    | db_engine::Aggregate::Max(a)
    | db_engine::Aggregate::Avg(a) => Some(a) == arg_qc.as_ref(),
  }
}

fn aggregate_matches(
  ag: &db_engine::Aggregate,
  func_name: &str,
  arg_qc: &Option<db_engine::QualifiedColumn>,
) -> bool {
  func_name == aggregate_name(ag) && aggregate_arg_matches(ag, arg_qc)
}

fn find_agg_index(
  ctx: &HavingContext<'_>,
  func_name: &str,
  arg_qc: Option<db_engine::QualifiedColumn>,
) -> Option<usize> {
  ctx.aggregates.iter().enumerate().find_map(|(i, ag)| {
    if aggregate_matches(ag, func_name, &arg_qc) {
      Some(i)
    } else {
      None
    }
  })
}

fn resolve_aggregate_ref(
  func: &sqlparser::ast::Function,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::RefOrAgg, TranslateError> {
  let fname = function_name(func);
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

  if args.len() != 1 {
    return Err(TranslateError::UnsupportedFeature(
      "aggregate in HAVING must take one argument".into(),
    ));
  }

  let arg_opt = resolve_having_arg(&args[0], ctx)?;
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
    if let Some(index) = ctx
      .aggregates
      .iter()
      .enumerate()
      .find_map(|(i, ag)| match ag {
        db_engine::Aggregate::Count(opt) => {
          if let Some(arg) = opt
            && arg == qc
          {
            Some(i)
          } else {
            None
          }
        }
        db_engine::Aggregate::Sum(a)
        | db_engine::Aggregate::Min(a)
        | db_engine::Aggregate::Max(a)
        | db_engine::Aggregate::Avg(a) => {
          if a == qc {
            Some(i)
          } else {
            None
          }
        }
      })
    {
      return Ok(db_engine::RefOrAgg::AggregateIndex(index));
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

fn having_predicate_from_comparison(
  op: &BinaryOperator,
  lref: db_engine::RefOrAgg,
  rval: db_engine::EngineValue,
) -> db_engine::HavingPredicate {
  match op {
    BinaryOperator::Eq => db_engine::HavingPredicate::Equals(lref, rval),
    BinaryOperator::NotEq => db_engine::HavingPredicate::NotEquals(lref, rval),
    BinaryOperator::Lt => db_engine::HavingPredicate::LessThan(lref, rval),
    BinaryOperator::LtEq => db_engine::HavingPredicate::LessThanOrEquals(lref, rval),
    BinaryOperator::Gt => db_engine::HavingPredicate::GreaterThan(lref, rval),
    BinaryOperator::GtEq => db_engine::HavingPredicate::GreaterThanOrEquals(lref, rval),
    _ => unreachable!(),
  }
}

fn translate_having_comparison(
  left: &SqlExpr,
  op: &BinaryOperator,
  right: &SqlExpr,
  ctx: &HavingContext<'_>,
) -> Result<db_engine::HavingPredicate, TranslateError> {
  let lref = resolve_ref(left, ctx)?;
  let rval = resolve_having_literal(right, ctx)?;
  Ok(having_predicate_from_comparison(op, lref, rval))
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

#[cfg(test)]
mod tests {
  use super::*;
  #[cfg(not(feature = "std"))]
  use alloc::vec;
  use db_engine::{ColumnSchema, EngineType, RefOrAgg, TableSchema};
  use sqlparser::ast::Ident;

  struct StubResolver;
  impl crate::translate::SchemaResolver for StubResolver {
    fn describe_table(&self, _name: &str) -> Option<TableSchema> {
      None
    }
  }

  #[test]
  fn having_predicate_from_comparison_builds_predicate() {
    let lref = RefOrAgg::Column(db_engine::QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    });
    let rval = db_engine::EngineValue::Integer(1);

    let pred = having_predicate_from_comparison(&BinaryOperator::Eq, lref, rval.clone());

    match pred {
      db_engine::HavingPredicate::Equals(r, v) => {
        match r {
          RefOrAgg::Column(qc) => assert_eq!(qc.column_index, 0),
          _ => panic!("expected column ref"),
        }
        assert_eq!(v, rval);
      }
      _ => panic!("expected Equals predicate"),
    }
  }

  #[test]
  fn expr_to_having_predicate_resolves_is_null() {
    let table_schema = TableSchema {
      name: "t".into(),
      columns: vec![ColumnSchema {
        name: "name".into(),
        data_type: EngineType::Text,
      }],
      primary_key: vec![],
    };
    let mut table_schemas = HashMap::new();
    table_schemas.insert("t".into(), table_schema);

    let group_by = Vec::new();
    let aggregates = Vec::new();
    let proj_alias_map = HashMap::new();
    let alias_map = HashMap::new();
    let mapper = crate::translate::DefaultValueMapper;
    let resolver = StubResolver;
    let ctx = HavingContext {
      group_by: &group_by,
      aggregates: &aggregates,
      proj_alias_map: &proj_alias_map,
      alias_map: &alias_map,
      table_schemas: &table_schemas,
      resolver: &resolver,
      mapper: &mapper,
    };

    let expr = SqlExpr::IsNull(Box::new(SqlExpr::Identifier(Ident::new("name"))));

    let predicate = expr_to_having_predicate(&expr, &ctx).expect("should translate HAVING expr");

    assert!(matches!(predicate, db_engine::HavingPredicate::IsNull(_)));
  }

  #[test]
  fn aggregate_name_maps_variants() {
    let col = db_engine::QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };

    assert_eq!(aggregate_name(&db_engine::Aggregate::Count(None)), "count");
    assert_eq!(
      aggregate_name(&db_engine::Aggregate::Count(Some(col.clone()))),
      "count"
    );
    assert_eq!(
      aggregate_name(&db_engine::Aggregate::Sum(col.clone())),
      "sum"
    );
    assert_eq!(
      aggregate_name(&db_engine::Aggregate::Min(col.clone())),
      "min"
    );
    assert_eq!(
      aggregate_name(&db_engine::Aggregate::Max(col.clone())),
      "max"
    );
    assert_eq!(aggregate_name(&db_engine::Aggregate::Avg(col)), "avg");
  }

  #[test]
  fn aggregate_arg_matches_covers_count_and_non_count() {
    let col = db_engine::QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };
    let other = db_engine::QualifiedColumn {
      table: "t".into(),
      column_index: 1,
    };

    assert!(aggregate_arg_matches(
      &db_engine::Aggregate::Count(None),
      &None
    ));
    assert!(aggregate_arg_matches(
      &db_engine::Aggregate::Count(Some(col.clone())),
      &Some(col.clone())
    ));
    assert!(!aggregate_arg_matches(
      &db_engine::Aggregate::Count(Some(col.clone())),
      &Some(other.clone())
    ));
    assert!(!aggregate_arg_matches(
      &db_engine::Aggregate::Count(None),
      &Some(col.clone())
    ));

    assert!(aggregate_arg_matches(
      &db_engine::Aggregate::Sum(col.clone()),
      &Some(col.clone())
    ));
    assert!(!aggregate_arg_matches(
      &db_engine::Aggregate::Sum(col),
      &Some(other)
    ));
  }

  #[test]
  fn resolve_aggregate_ref_resolves_known_and_rejects_unknown() {
    let col = db_engine::QualifiedColumn {
      table: "t".into(),
      column_index: 0,
    };
    let aggregates = vec![db_engine::Aggregate::Sum(col.clone())];
    let group_by = Vec::new();
    let proj_alias_map = HashMap::new();
    let alias_map = HashMap::new();
    let mut table_schemas = HashMap::new();
    table_schemas.insert(
      "t".into(),
      TableSchema {
        name: "t".into(),
        columns: vec![ColumnSchema {
          name: "value".into(),
          data_type: EngineType::Integer,
        }],
        primary_key: vec![],
      },
    );
    let mapper = crate::translate::DefaultValueMapper;
    let resolver = StubResolver;
    let ctx = HavingContext {
      group_by: &group_by,
      aggregates: &aggregates,
      proj_alias_map: &proj_alias_map,
      alias_map: &alias_map,
      table_schemas: &table_schemas,
      resolver: &resolver,
      mapper: &mapper,
    };

    let known = sqlparser::ast::Function {
      name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
        sqlparser::ast::Ident::new("sum"),
      )]),
      uses_odbc_syntax: false,
      parameters: sqlparser::ast::FunctionArguments::None,
      args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
        duplicate_treatment: None,
        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
          SqlExpr::Identifier(Ident::new("value")),
        ))],
        clauses: vec![],
      }),
      filter: None,
      null_treatment: None,
      over: None,
      within_group: vec![],
    };

    let resolved = resolve_aggregate_ref(&known, &ctx).expect("known aggregate should resolve");
    assert!(matches!(resolved, db_engine::RefOrAgg::AggregateIndex(0)));

    let unknown = sqlparser::ast::Function {
      name: sqlparser::ast::ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
        sqlparser::ast::Ident::new("count"),
      )]),
      uses_odbc_syntax: false,
      parameters: sqlparser::ast::FunctionArguments::None,
      args: sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
        duplicate_treatment: None,
        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
        clauses: vec![],
      }),
      filter: None,
      null_treatment: None,
      over: None,
      within_group: vec![],
    };

    let err = resolve_aggregate_ref(&unknown, &ctx).expect_err("unknown aggregate should fail");
    assert!(matches!(err, TranslateError::UnsupportedFeature(_)));
  }
}
