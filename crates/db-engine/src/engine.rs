#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use db_schema::{ColumnSchema, ColumnSchemaIndex, IndexSchema, TableSchema};
use db_value::Row;
use futures::{StreamExt, pin_mut};
#[cfg(feature = "std")]
use std::sync::Arc;

use thiserror::Error;

use db_btree::{BTree, BTreeError, BTreeTransaction};
use db_query::{
    Query, QueryColumn, QueryExpr, QueryExprValue, QueryFrom, QueryJoin, QueryJoinKind,
    QueryOrderBy, QueryParams, QueryResult, QuerySelect, QuerySortDirection, Statement,
    TranslateError, Translator,
};

use crate::{
    catalog::{
        ENGINE_INDEX_FIELD_INDEX_NAME, ENGINE_INDEX_FIELD_TABLE_NAME, ENGINE_INDEX_FIELD_UNIQUE,
        ENGINE_INDEX_FIELDS, ENGINE_INDEX_FIELDS_FIELD_COLUMN_INDEX,
        ENGINE_INDEX_FIELDS_FIELD_FIELD_ORDER, ENGINE_INDEX_FIELDS_FIELD_INDEX_NAME,
        ENGINE_INDICES, ENGINE_TABLE_FIELD_TABLE_NAME, ENGINE_TABLE_FIELDS,
        ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX, ENGINE_TABLE_FIELDS_FIELD_COLUMN_NAME,
        ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY, ENGINE_TABLE_FIELDS_FIELD_TABLE_NAME,
        ENGINE_TABLE_FIELDS_FIELD_VALUE_TYPE, ENGINE_TABLES,
    },
    executor::execute_statement,
    kernel::{Kernel, KernelTransaction},
};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Translate error: {0}")]
    TranslateError(#[from] TranslateError),

    #[error("BTree error: {0}")]
    BTreeError(#[from] BTreeError),

    #[error("Unsupported query shape for MVP executor: {0}")]
    Unsupported(&'static str),

    #[error("Invalid query: {0}")]
    InvalidQuery(&'static str),

    #[error("Error: {0}")]
    Custom(String),
}

impl EngineError {
    pub fn custom<T>(error: T) -> Self
    where
        T: ToString,
    {
        Self::Custom(error.to_string())
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Clone)]
pub struct Engine<K> {
    pub(crate) kernel: Arc<K>,
}

impl<K> From<K> for Engine<K> {
    fn from(kernel: K) -> Self {
        Self {
            kernel: Arc::new(kernel),
        }
    }
}

impl<K> Engine<K> {
    pub fn new(kernel: K) -> Self {
        Self::from(kernel)
    }
}

impl<K> Engine<K>
where
    K: Kernel,
{
    pub async fn index_schema(&self, name: &str) -> EngineResult<IndexSchema> {
        let mut query_results = self
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: ENGINE_INDICES.to_string(),
                    joins: vec![QueryJoin {
                        kind: QueryJoinKind::Inner,
                        table: ENGINE_INDEX_FIELDS.to_string(),
                        on: QueryExpr::Equals(
                            Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                                ENGINE_INDEX_FIELDS.to_string(),
                                ENGINE_INDEX_FIELDS_FIELD_INDEX_NAME.to_string(),
                            )))),
                            Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                                ENGINE_INDICES.to_string(),
                                ENGINE_INDEX_FIELD_INDEX_NAME.to_string(),
                            )))),
                        ),
                    }],
                },
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        ENGINE_INDICES.to_string(),
                        ENGINE_INDEX_FIELD_TABLE_NAME.to_string(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(name.into()))),
                )),
                projection: vec![
                    QueryColumn::new(
                        ENGINE_INDICES.to_string(),
                        ENGINE_INDEX_FIELD_TABLE_NAME.to_string(),
                    ),
                    QueryColumn::new(
                        ENGINE_INDICES.to_string(),
                        ENGINE_INDEX_FIELD_UNIQUE.to_string(),
                    ),
                    QueryColumn::new(
                        ENGINE_INDEX_FIELDS.to_string(),
                        ENGINE_INDEX_FIELDS_FIELD_COLUMN_INDEX.to_string(),
                    ),
                ],
                order_by: vec![QueryOrderBy {
                    by: QueryColumn::new(
                        ENGINE_INDEX_FIELDS.to_string(),
                        ENGINE_INDEX_FIELDS_FIELD_FIELD_ORDER.to_string(),
                    ),
                    direction: QuerySortDirection::Asc,
                }],
                ..Default::default()
            }))])
            .await?;

        if query_results.len() != 1 {
            return Err(EngineError::InvalidQuery(
                "Expected exactly one query result for index schema",
            ));
        }

        let query_result = query_results.remove(0);

        let mut index_schema = IndexSchema {
            name: name.to_string(),
            table_name: String::new(),
            column_indices: Vec::with_capacity(query_result.rows.len()),
            unique: false,
        };

        for row in query_result.rows {
            let table_name = row.values[0].to_text().ok_or({
                EngineError::InvalidQuery(
                    "Expected table_name to be text in index schema query result",
                )
            })?;
            let unique = row.values[1].to_bool().ok_or({
                EngineError::InvalidQuery("Expected unique to be bool in index schema query result")
            })?;
            let column_index = row.values[3].to_integer().ok_or({
                EngineError::InvalidQuery(
                    "Expected field_order to be integer in index schema query result",
                )
            })?;

            if index_schema.table_name.is_empty() {
                index_schema.table_name = table_name;
                index_schema.unique = unique;
            }

            index_schema
                .column_indices
                .push(column_index as ColumnSchemaIndex);
        }

        Ok(index_schema)
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        let mut query_results = self
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: ENGINE_TABLES.to_string(),
                    joins: vec![QueryJoin {
                        kind: QueryJoinKind::Inner,
                        table: ENGINE_TABLE_FIELDS.to_string(),
                        on: QueryExpr::Equals(
                            Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                                ENGINE_TABLES.to_string(),
                                ENGINE_TABLE_FIELDS_FIELD_TABLE_NAME.to_string(),
                            )))),
                            Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                                ENGINE_TABLE_FIELDS.to_string(),
                                ENGINE_TABLE_FIELD_TABLE_NAME.to_string(),
                            )))),
                        ),
                    }],
                },
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        ENGINE_TABLES.to_string(),
                        ENGINE_TABLE_FIELD_TABLE_NAME.to_string(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(name.into()))),
                )),
                projection: vec![
                    QueryColumn::new(
                        ENGINE_TABLES.to_string(),
                        ENGINE_TABLE_FIELD_TABLE_NAME.to_string(),
                    ),
                    QueryColumn::new(
                        ENGINE_TABLE_FIELDS.to_string(),
                        ENGINE_TABLE_FIELDS_FIELD_COLUMN_NAME.to_string(),
                    ),
                    QueryColumn::new(
                        ENGINE_TABLE_FIELDS.to_string(),
                        ENGINE_TABLE_FIELDS_FIELD_VALUE_TYPE.to_string(),
                    ),
                    QueryColumn::new(
                        ENGINE_TABLE_FIELDS.to_string(),
                        ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY.to_string(),
                    ),
                ],
                order_by: vec![QueryOrderBy {
                    by: QueryColumn::new(
                        ENGINE_TABLE_FIELDS.to_string(),
                        ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX.to_string(),
                    ),
                    direction: QuerySortDirection::Asc,
                }],
                ..Default::default()
            }))])
            .await?;

        if query_results.len() != 1 {
            return Err(EngineError::InvalidQuery(
                "Expected exactly one query result for index schema",
            ));
        }

        let query_result = query_results.remove(0);

        let mut table_schema = TableSchema {
            name: name.to_string(),
            columns: Vec::with_capacity(query_result.rows.len()),
        };

        for row in query_result.rows {
            let table_name = row.values[0].to_text().ok_or({
                EngineError::InvalidQuery(
                    "Expected table_name to be text in index schema query result",
                )
            })?;
            let column_name = row.values[1].to_text().ok_or({
                EngineError::InvalidQuery(
                    "Expected column_name to be text in index schema query result",
                )
            })?;
            let value_type = row.values[2].to_type().ok_or({
                EngineError::InvalidQuery(
                    "Expected value_type to be ValueType in index schema query result",
                )
            })?;
            let primary_key = row.values[3].to_bool().ok_or({
                EngineError::InvalidQuery(
                    "Expected primary_key to be bool in index schema query result",
                )
            })?;

            if table_schema.name.is_empty() {
                table_schema.name = table_name;
            }

            table_schema.columns.push(ColumnSchema {
                name: column_name,
                r#type: value_type,
                primary_key,
            });
        }

        Ok(table_schema)
    }

    pub async fn create_table(&self, table_schema: TableSchema) -> EngineResult<()> {
        let etx = self.kernel.transaction().await?;
        {
            let tables = etx.write_table(ENGINE_TABLES).await?;
            let mut tx = tables.transaction().await?;

            tx.insert(
                Row::new(vec![table_schema.name.clone().into()]),
                Row::new(vec![table_schema.name.clone().into()]),
            )
            .await?;

            tx.commit().await?;
        }
        {
            let table_fields = etx.write_table(ENGINE_TABLE_FIELDS).await?;
            let mut tx = table_fields.transaction().await?;

            for (column_index, column_schema) in table_schema.columns.iter().enumerate() {
                tx.insert(
                    Row::new(vec![
                        table_schema.name.clone().into(),
                        column_schema.name.clone().into(),
                    ]),
                    Row::new(vec![
                        table_schema.name.clone().into(),
                        column_schema.name.clone().into(),
                        column_schema.r#type.into(),
                        (column_index as i64).into(),
                        column_schema.primary_key.into(),
                    ]),
                )
                .await?;
            }

            tx.commit().await?;
        }
        etx.commit().await?;
        Ok(())
    }

    pub async fn drop_table(&self, table_name: &str) -> EngineResult<()> {
        let etx = self.kernel.transaction().await?;
        {
            let tables = etx.write_table(ENGINE_TABLES).await?;
            let mut tx = tables.transaction().await?;

            tx.remove(&Row::new(vec![table_name.into()])).await?;

            tx.commit().await?;
        }
        {
            let table_fields = etx.write_table(ENGINE_TABLE_FIELDS).await?;
            let mut tx = table_fields.transaction().await?;
            {
                let range = tx.remove_range(Row::new(vec![table_name.into()])..);
                pin_mut!(range);

                while let Some(item) = range.next().await {
                    let _ = item?;
                }
            }
            tx.commit().await?;
        }
        etx.commit().await?;
        Ok(())
    }

    pub async fn translate_and_execute_with_params<T>(
        &self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate_with_params(query, params).await?;
        self.execute(statements).await
    }

    pub async fn translate_and_execute<T>(
        &self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate(query).await?;
        self.execute(statements).await
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
        execute_statement(self, statements).await
    }
}
