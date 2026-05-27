use alloc::vec::Vec;
use core::future::Future;

use crate::store_backend::EngineStoreBackend;
use crate::{
  EngineError, EngineResult,
  query::{EngineQuery, QualifiedColumn, QualifiedPredicate, ResultColumn, SelectOptions},
};

use super::join_builder::JoinedRowStates;
use super::operators::{Aggregator, Sorter};
use super::select_pipeline::{
  build_sorted_projection_rows, filter_joined_rows, materialize_joined_rows,
};

pub(crate) enum SelectStageOutput {
  Joined(JoinedRowStates),
  Final(EngineResult),
}

pub(crate) async fn execute_select_pipeline<S, F, Fut>(
  tx: &mut super::NamedTreeEngineTransaction<S>,
  base_table: &str,
  projection: &[QualifiedColumn],
  predicate: Option<QualifiedPredicate>,
  options: &SelectOptions,
  output_columns: Vec<ResultColumn>,
  run_subquery: F,
) -> Result<SelectStageOutput, EngineError>
where
  S: EngineStoreBackend,
  F: Fn(EngineQuery) -> Fut,
  Fut: Future<Output = Result<EngineResult, EngineError>>,
{
  let mut partial_results = materialize_joined_rows::<S>(tx, base_table, options).await?;

  if let Some(qpred) = &predicate {
    filter_joined_rows(&mut partial_results, qpred, run_subquery).await?;
  }

  let needs_grouping = !options.group_by.is_empty() || !options.aggregates.is_empty();
  if !needs_grouping {
    let keyed = build_sorted_projection_rows(&partial_results, projection, options)?;
    let out_rows = Sorter::new(
      options.order_by.clone(),
      options.distinct,
      options.limit,
      options.offset,
      keyed,
    )
    .execute();

    return Ok(SelectStageOutput::Final(EngineResult::new_with_columns(
      out_rows,
      output_columns,
    )));
  }

  Ok(SelectStageOutput::Joined(partial_results))
}

pub(crate) fn finalize_grouped_result(
  partial_results: JoinedRowStates,
  options: &SelectOptions,
  output_columns: Vec<ResultColumn>,
) -> Result<EngineResult, EngineError> {
  let aggregator = Aggregator::new(
    options.group_by.clone(),
    options.aggregates.clone(),
    options.having.clone(),
    options.order_by.clone(),
    options.limit,
    options.offset,
    partial_results,
  );
  Ok(EngineResult::new_with_columns(
    aggregator.execute()?,
    output_columns,
  ))
}
