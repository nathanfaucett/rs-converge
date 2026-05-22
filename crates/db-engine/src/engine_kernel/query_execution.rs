use crate::store_adapter::EngineStore;
use crate::{EngineError, query::EngineQuery, query::EngineResult};

use super::planner::EngineKernel;

impl<S> EngineKernel<S>
where
  S: EngineStore,
{
  pub(crate) async fn run(&self, query: EngineQuery) -> Result<EngineResult, EngineError> {
    let (result, _events) = self.run_with_events(query).await?;
    Ok(result)
  }

  pub(crate) async fn run_with_events(
    &self,
    query: EngineQuery,
  ) -> Result<(EngineResult, Vec<crate::ChangeEvent>), EngineError> {
    match query {
      EngineQuery::Select {
        table,
        projection,
        predicate,
        options,
      } => {
        let result = self
          .read_extended(&table, &projection, predicate, &options)
          .await?;
        Ok((result, Vec::new()))
      }
      EngineQuery::Insert { .. } | EngineQuery::Update { .. } | EngineQuery::Delete { .. } => {
        self.execute_write_query(query).await
      }
    }
  }
}
