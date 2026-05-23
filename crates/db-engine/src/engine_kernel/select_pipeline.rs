use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;
use hashbrown::{HashMap, HashSet};

use crate::predicate::PredicateEvaluator;
use crate::store_adapter::EngineStore;
use crate::{
  EngineError, EngineRow, EngineValue,
  query::{EngineQuery, EngineResult, QualifiedColumn, QualifiedPredicate, SelectOptions},
};

use super::join_builder::{
  JoinedRowState, JoinedRowStates, apply_join_clauses, build_join_template, collect_table_rows_map,
  collect_tables, seed_joined_row_states,
};

pub(crate) async fn materialize_joined_rows<S>(
  tx: &mut S::Transaction,
  base_table: &str,
  options: &SelectOptions,
) -> Result<JoinedRowStates, EngineError>
where
  S: EngineStore,
{
  let tables = collect_tables(base_table, &[], &options.joins);
  let table_rows_map = collect_table_rows_map::<S>(tx, &tables).await?;
  let template = build_join_template(&tables);
  let partial_results = seed_joined_row_states(base_table, &table_rows_map, &template);
  apply_join_clauses(&options.joins, &table_rows_map, &template, partial_results)
}

pub(crate) fn build_sorted_projection_rows(
  partial_results: &[JoinedRowState],
  projection: &[QualifiedColumn],
  options: &SelectOptions,
) -> Result<Vec<(Vec<EngineValue>, EngineRow)>, EngineError> {
  let mut keyed: Vec<(Vec<EngineValue>, EngineRow)> = Vec::new();

  for partial in partial_results {
    let mut out_row: EngineRow = Vec::with_capacity(projection.len());
    for proj in projection {
      match partial.get(&proj.table) {
        Some(Some(row)) => out_row.push(
          row
            .get(proj.column_index)
            .cloned()
            .unwrap_or(EngineValue::Null),
        ),
        Some(None) => out_row.push(EngineValue::Null),
        None => {
          return Err(EngineError::SchemaMismatch(format!(
            "projection references unknown table {}",
            proj.table
          )));
        }
      }
    }

    let mut keys: Vec<EngineValue> = Vec::with_capacity(options.order_by.len());
    for ord in &options.order_by {
      let qc = &ord.expr;
      match partial.get(&qc.table) {
        Some(Some(row)) => keys.push(
          row
            .get(qc.column_index)
            .cloned()
            .unwrap_or(EngineValue::Null),
        ),
        Some(None) => keys.push(EngineValue::Null),
        None => {
          return Err(EngineError::SchemaMismatch(format!(
            "ORDER BY references unknown table {}",
            qc.table
          )));
        }
      }
    }

    keyed.push((keys, out_row));
  }

  Ok(keyed)
}

fn collect_subqueries(pred: &QualifiedPredicate, acc: &mut Vec<EngineQuery>) {
  match pred {
    QualifiedPredicate::InSubquery { subquery, .. } => acc.push((**subquery).clone()),
    QualifiedPredicate::And(l, r) | QualifiedPredicate::Or(l, r) => {
      collect_subqueries(l, acc);
      collect_subqueries(r, acc);
    }
    QualifiedPredicate::Not(p) => collect_subqueries(p, acc),
    _ => {}
  }
}

/// Filter `partial_results` by `predicate`, executing any subqueries via `run_subquery`.
///
/// Keeping this inside the select pipeline seam ensures all join-state mutation
/// lives behind one interface rather than leaking into planner orchestration.
pub(crate) async fn filter_joined_rows<F, Fut>(
  partial_results: &mut JoinedRowStates,
  predicate: &QualifiedPredicate,
  run_subquery: F,
) -> Result<(), EngineError>
where
  F: Fn(EngineQuery) -> Fut,
  Fut: Future<Output = Result<EngineResult, EngineError>>,
{
  let mut subquery_list: Vec<EngineQuery> = Vec::new();
  collect_subqueries(predicate, &mut subquery_list);

  let mut subquery_cache: HashMap<String, HashSet<EngineValue>> = HashMap::new();
  for query in subquery_list {
    let key = format!("{:?}", query);
    if subquery_cache.contains_key(&key) {
      continue;
    }
    let res = run_subquery(query).await?;
    let set: HashSet<EngineValue> = res
      .rows
      .into_iter()
      .filter_map(|row| row.into_iter().next())
      .collect();
    subquery_cache.insert(key, set);
  }

  let eval_ctx = crate::predicate::EvalContext::with_cache(subquery_cache);
  let evaluator = PredicateEvaluator::new(&eval_ctx);
  partial_results.retain(|partial| evaluator.matches_joined_row(predicate, partial));
  Ok(())
}
