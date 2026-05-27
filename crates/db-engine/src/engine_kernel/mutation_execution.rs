use crate::store_backend::EngineStoreBackend;
use crate::{EngineError, query::EngineQuery, query::EngineResult, query::UpdateValueExpr};
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use super::{EngineWriteTxn, planner::EngineKernel};

impl<S> EngineKernel<S>
where
  S: EngineStoreBackend,
{
  fn output_columns_for_returning(
    &self,
    table_name: &str,
    returning: &[UpdateValueExpr],
  ) -> Result<Vec<crate::query::ResultColumn>, EngineError> {
    let schema = self.table(table_name)?;
    let mut columns = Vec::with_capacity(returning.len());

    for (index, expression) in returning.iter().enumerate() {
      match expression {
        UpdateValueExpr::Column(column_ref) => {
          if column_ref.table != table_name {
            return Err(EngineError::SchemaMismatch(format!(
              "RETURNING column {} must reference target table {}",
              column_ref.table, table_name
            )));
          }

          let column = schema.columns.get(column_ref.column_index).ok_or_else(|| {
            EngineError::SchemaMismatch(format!(
              "RETURNING index {} is out of bounds for table {}",
              column_ref.column_index, table_name
            ))
          })?;

          columns.push(crate::query::ResultColumn::new(
            column.name.clone(),
            Some(table_name.to_string()),
            Some(column_ref.column_index),
          ));
        }
        _ => {
          columns.push(crate::query::ResultColumn::new(
            format!("expr_{}", index + 1),
            None,
            None,
          ));
        }
      }
    }

    Self::dedupe_result_column_names(&mut columns);
    Ok(columns)
  }

  pub(crate) async fn execute_write_query(
    &self,
    query: EngineQuery,
  ) -> Result<(EngineResult, Vec<crate::ChangeEvent>), EngineError> {
    match query {
      EngineQuery::Insert {
        table,
        row,
        returning,
      } => self.execute_insert(table, row, returning).await,
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => {
        self
          .execute_update(table, assignments, predicate, joins, from_tables, returning)
          .await
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => self.execute_delete(table, predicate, returning).await,
      EngineQuery::Select { .. } => Err(EngineError::SchemaMismatch(
        "write query dispatcher received select query".into(),
      )),
    }
  }

  async fn execute_insert(
    &self,
    table: String,
    row: crate::EngineRow,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<(EngineResult, Vec<crate::ChangeEvent>), EngineError> {
    let mut writer = self.writer();
    let returning_columns = match &returning {
      Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
      None => None,
    };

    let rows = writer.insert_returning(&table, row, returning).await;
    self.commit_writer(writer, rows, returning_columns).await
  }

  async fn execute_update(
    &self,
    table: String,
    assignments: Vec<crate::query::UpdateAssignment>,
    predicate: Option<crate::query::QualifiedPredicate>,
    joins: Vec<crate::query::JoinClause>,
    from_tables: Vec<String>,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<(EngineResult, Vec<crate::ChangeEvent>), EngineError> {
    let mut writer = self.writer();
    let returning_columns = match &returning {
      Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
      None => None,
    };

    let rows = writer
      .update(
        &table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      )
      .await;
    self.commit_writer(writer, rows, returning_columns).await
  }

  async fn execute_delete(
    &self,
    table: String,
    predicate: Option<crate::query::QualifiedPredicate>,
    returning: Option<Vec<UpdateValueExpr>>,
  ) -> Result<(EngineResult, Vec<crate::ChangeEvent>), EngineError> {
    let mut writer = self.writer();
    let returning_columns = match &returning {
      Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
      None => None,
    };

    let rows = writer.delete(&table, predicate, returning).await;
    self.commit_writer(writer, rows, returning_columns).await
  }

  async fn commit_writer(
    &self,
    writer: EngineWriteTxn<'_, S>,
    result: Result<Vec<crate::EngineRow>, EngineError>,
    returning_columns: Option<Vec<crate::query::ResultColumn>>,
  ) -> Result<(EngineResult, Vec<crate::ChangeEvent>), EngineError> {
    match result {
      Ok(rows) => {
        let events = writer.commit().await?;
        Ok((self.build_result(rows, returning_columns), events))
      }
      Err(error) => {
        let _ = writer.rollback().await;
        Err(error)
      }
    }
  }

  fn build_result(
    &self,
    rows: Vec<crate::EngineRow>,
    returning_columns: Option<Vec<crate::query::ResultColumn>>,
  ) -> EngineResult {
    match returning_columns {
      Some(columns) => EngineResult::new_with_columns(rows, columns),
      None => EngineResult::new(rows),
    }
  }
}
