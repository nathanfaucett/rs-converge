use crate::store_adapter::EngineStore;
use crate::{EngineError, query::EngineQuery, query::EngineResult, query::UpdateValueExpr};
use alloc::string::ToString;
use alloc::vec::Vec;

use super::planner::EngineKernel;

impl<S> EngineKernel<S>
where
  S: EngineStore,
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
      } => {
        let mut writer = self.writer();
        let returning_columns = match &returning {
          Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
          None => None,
        };
        match writer.insert_returning(&table, row, returning).await {
          Ok(rows) => {
            let events = writer.commit().await?;
            Ok((
              match returning_columns {
                Some(columns) => EngineResult::new_with_columns(rows, columns),
                None => EngineResult::new(rows),
              },
              events,
            ))
          }
          Err(error) => {
            let _ = writer.rollback().await;
            Err(error)
          }
        }
      }
      EngineQuery::Update {
        table,
        assignments,
        predicate,
        joins,
        from_tables,
        returning,
      } => {
        let mut writer = self.writer();
        let returning_columns = match &returning {
          Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
          None => None,
        };
        match writer
          .update(
            &table,
            assignments,
            predicate,
            joins,
            from_tables,
            returning,
          )
          .await
        {
          Ok(rows) => {
            let events = writer.commit().await?;
            Ok((
              match returning_columns {
                Some(columns) => EngineResult::new_with_columns(rows, columns),
                None => EngineResult::new(rows),
              },
              events,
            ))
          }
          Err(error) => {
            let _ = writer.rollback().await;
            Err(error)
          }
        }
      }
      EngineQuery::Delete {
        table,
        predicate,
        returning,
      } => {
        let mut writer = self.writer();
        let returning_columns = match &returning {
          Some(columns) => Some(self.output_columns_for_returning(&table, columns)?),
          None => None,
        };
        match writer.delete(&table, predicate, returning).await {
          Ok(rows) => {
            let events = writer.commit().await?;
            Ok((
              match returning_columns {
                Some(columns) => EngineResult::new_with_columns(rows, columns),
                None => EngineResult::new(rows),
              },
              events,
            ))
          }
          Err(error) => {
            let _ = writer.rollback().await;
            Err(error)
          }
        }
      }
      EngineQuery::Select { .. } => Err(EngineError::SchemaMismatch(
        "write query dispatcher received select query".into(),
      )),
    }
  }
}
