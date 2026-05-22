use futures::future::FutureExt;

use crate::store_adapter::EngineStore;
use crate::{
  EngineError, query::EngineResult, query::QualifiedColumn, query::QualifiedPredicate,
  query::SelectOptions,
};

use super::planner::EngineKernel;
use super::select_orchestrator::{
  SelectStageOutput, execute_select_pipeline, finalize_grouped_result,
};

impl<S> EngineKernel<S>
where
  S: EngineStore,
{
  pub(crate) async fn read_extended(
    &self,
    base_table: &str,
    projection: &[QualifiedColumn],
    predicate: Option<QualifiedPredicate>,
    options: &SelectOptions,
  ) -> Result<EngineResult, EngineError> {
    let output_columns = self.output_columns_for_select(projection, options)?;
    let mut tx = self.store().engine_read_transaction().await?;
    match execute_select_pipeline::<S, _, _>(
      &mut tx,
      base_table,
      projection,
      predicate,
      options,
      output_columns.clone(),
      |q| self.run(q).boxed_local(),
    )
    .await?
    {
      SelectStageOutput::Final(result) => Ok(result),
      SelectStageOutput::Joined(partial_results) => {
        finalize_grouped_result(partial_results, options, output_columns)
      }
    }
  }
}
