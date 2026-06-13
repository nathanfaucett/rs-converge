#[cfg(not(feature = "std"))]
use alloc::{
  string::{String, ToString},
  vec::Vec,
};

use db_query::{DataDefinition, Query, QueryResult, Statement};

use crate::{EngineResult, engine::Engine, kernel::EngineKernel};

pub async fn execute_statement<K>(
  engine: &Engine<K>,
  statements: Vec<Statement>,
) -> EngineResult<Vec<QueryResult>>
where
  K: EngineKernel,
{
  let mut results = Vec::with_capacity(statements.len());
  for statement in statements {
    match statement {
      Statement::Query(query) => results.push(execute_query(engine, query).await?),
      Statement::DataDefinition(ddl) => results.push(execute_ddl(engine, ddl).await?),
    }
  }
  Ok(results)
}

pub async fn execute_query<K>(_engine: &Engine<K>, query: Query) -> EngineResult<QueryResult>
where
  K: EngineKernel,
{
  match query {
    Query::Select { .. } => unimplemented!(),
    Query::Insert { .. } => unimplemented!(),
    Query::Update { .. } => unimplemented!(),
    Query::Delete { .. } => unimplemented!(),
  }
}

async fn execute_ddl<K>(_engine: &Engine<K>, ddl: DataDefinition) -> EngineResult<QueryResult>
where
  K: EngineKernel,
{
  match ddl {
    DataDefinition::CreateTable { .. } => unimplemented!(),
    DataDefinition::AlterTable { .. } => unimplemented!(),
    DataDefinition::CreateIndex { .. } => unimplemented!(),
    DataDefinition::AlterIndex { .. } => unimplemented!(),
    DataDefinition::DropIndex { .. } => unimplemented!(),
    DataDefinition::DropTable { .. } => unimplemented!(),
  }
}
