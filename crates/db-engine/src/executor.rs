use alloc::{string::String, vec, vec::Vec};

use db_query::{
    DataDefinition, Query, QueryColumn, QueryInsert, QueryResult, QueryResultColumn, QuerySelect,
};
use db_schema::{ColumnSchema, TableSchema};
use db_value::{Row, Value};
use futures::{StreamExt, pin_mut};

use crate::{
    EngineError, EngineResult,
    catalog::{
        ENGINE_INDEX_FIELDS, ENGINE_INDICES, ENGINE_TABLE_FIELDS,
        ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX, ENGINE_TABLE_FIELDS_FIELD_COLUMN_NAME,
        ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY, ENGINE_TABLE_FIELDS_FIELD_VALUE_TYPE, ENGINE_TABLES,
    },
    engine::Engine,
    kernel::{Kernel, KernelTransaction},
    reconciler::RowReconciler,
};

pub async fn execute_statement<K, R>(
    engine: &Engine<K, R>,
    statements: Vec<db_query::Statement>,
) -> EngineResult<Vec<QueryResult>>
where
    K: Kernel,
    R: RowReconciler<K::Transaction>,
{
    let mut transaction = engine.kernel.transaction().await?;
    if let Err(error) = ensure_catalog(&mut transaction).await {
        transaction.rollback().await?;
        return Err(error);
    }
    let mut results = Vec::with_capacity(statements.len());

    for statement in statements {
        let result = match statement {
            db_query::Statement::Query(query) => {
                execute_query(&mut transaction, engine.reconciler.as_ref(), query).await
            }
            db_query::Statement::DataDefinition(ddl) => {
                execute_ddl(&mut transaction, engine.reconciler.as_ref(), ddl).await
            }
        };
        match result {
            Ok(result) => results.push(result),
            Err(error) => {
                transaction.rollback().await?;
                return Err(error);
            }
        }
    }

    transaction.commit().await?;
    Ok(results)
}

async fn ensure_catalog<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    for table in [
        ENGINE_TABLES,
        ENGINE_TABLE_FIELDS,
        ENGINE_INDICES,
        ENGINE_INDEX_FIELDS,
    ] {
        transaction.ensure_table(table).await?;
    }
    Ok(())
}

async fn execute_query<T, R>(
    transaction: &mut T,
    reconciler: &R,
    query: Query,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowReconciler<T>,
{
    match query {
        Query::Insert(insert) => insert_row(transaction, reconciler, insert).await,
        Query::Select(select) => select_rows(transaction, reconciler, select).await,
        Query::Update(_) | Query::Delete(_) => Err(EngineError::Unsupported(
            "only INSERT and simple SELECT are supported",
        )),
    }
}

async fn execute_ddl<T, R>(
    transaction: &mut T,
    reconciler: &R,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowReconciler<T>,
{
    match ddl {
        DataDefinition::CreateTable {
            schema,
            if_not_exists,
        } => {
            let table_key = Row::new(vec![Value::from(schema.name.as_str())]);
            if transaction
                .get_entry(ENGINE_TABLES, &table_key)
                .await?
                .is_some()
            {
                if if_not_exists {
                    return Ok(QueryResult::default());
                }
                return Err(EngineError::InvalidQuery("Table already exists"));
            }
            reconciler.ensure_table(transaction, &schema.name).await?;
            create_table(transaction, schema).await?;
            Ok(QueryResult::default())
        }
        _ => Err(EngineError::Unsupported("only CREATE TABLE is supported")),
    }
}

async fn create_table<T>(transaction: &mut T, table_schema: TableSchema) -> EngineResult<()>
where
    T: KernelTransaction,
{
    transaction
        .put_entry(
            ENGINE_TABLES,
            Row::new(vec![table_schema.name.clone().into()]),
            Row::new(vec![table_schema.name.clone().into()]),
        )
        .await?;

    for (column_index, column_schema) in table_schema.columns.iter().enumerate() {
        transaction
            .put_entry(
                ENGINE_TABLE_FIELDS,
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

    Ok(())
}

async fn insert_row<T, R>(
    transaction: &mut T,
    reconciler: &R,
    insert: QueryInsert,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowReconciler<T>,
{
    if insert.returning.is_some() {
        return Err(EngineError::Unsupported("INSERT RETURNING"));
    }

    let schema = table_schema(transaction, &insert.table).await?;
    if insert.row.values.len() != schema.columns.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT row has the wrong column count",
        ));
    }

    let key = primary_key(&schema, &insert.row)?;
    reconciler
        .put_row(transaction, &insert.table, key, insert.row)
        .await?;

    Ok(QueryResult::default())
}

async fn select_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    select: QuerySelect,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowReconciler<T>,
{
    if !select.from.joins.is_empty()
        || select.predicate.is_some()
        || !select.aggregates.is_empty()
        || !select.group_by.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.having.is_some()
    {
        return Err(EngineError::Unsupported("complex SELECT"));
    }

    let schema = table_schema(transaction, &select.from.table).await?;
    let projection = projection(&schema, &select.from.table, &select.projection)?;
    let stream = reconciler.scan_rows(transaction, &select.from.table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, row) = item?;
        rows.push(Row::new(
            projection
                .iter()
                .map(|(index, _)| row.values[*index].clone())
                .collect(),
        ));
    }

    Ok(QueryResult::new_with_columns(
        rows,
        projection.into_iter().map(|(_, column)| column).collect(),
    ))
}

async fn table_schema<T>(transaction: &T, name: &str) -> EngineResult<TableSchema>
where
    T: KernelTransaction,
{
    let table_key = Row::new(vec![Value::from(name)]);
    if transaction
        .get_entry(ENGINE_TABLES, &table_key)
        .await?
        .is_none()
    {
        return Err(EngineError::InvalidQuery("Table not found"));
    }

    let stream = transaction.scan_entries(ENGINE_TABLE_FIELDS);
    pin_mut!(stream);
    let mut columns = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, row) = item?;
        if row.values.first().and_then(Value::as_text) != Some(name) {
            continue;
        }
        let column =
            row.values
                .get(1)
                .and_then(Value::to_text)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_TABLE_FIELDS_FIELD_COLUMN_NAME,
                ))?;
        let value_type =
            row.values
                .get(2)
                .and_then(Value::to_type)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_TABLE_FIELDS_FIELD_VALUE_TYPE,
                ))?;
        let index =
            row.values
                .get(3)
                .and_then(Value::to_integer)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX,
                ))?;
        let primary_key =
            row.values
                .get(4)
                .and_then(Value::to_bool)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY,
                ))?;
        columns.push((
            index,
            ColumnSchema {
                name: column,
                r#type: value_type,
                primary_key,
            },
        ));
    }

    columns.sort_by_key(|(index, _)| *index);
    Ok(TableSchema {
        name: String::from(name),
        columns: columns.into_iter().map(|(_, column)| column).collect(),
    })
}

fn primary_key(schema: &TableSchema, row: &Row) -> EngineResult<Row> {
    let values: Vec<_> = schema
        .columns
        .iter()
        .zip(&row.values)
        .filter(|(column, _)| column.primary_key)
        .map(|(_, value)| value.clone())
        .collect();
    if values.is_empty() || values.iter().any(|value| matches!(value, Value::Null)) {
        return Err(EngineError::InvalidQuery(
            "INSERT requires non-null primary-key values",
        ));
    }
    Ok(Row::new(values))
}

fn projection(
    schema: &TableSchema,
    table: &str,
    projection: &[QueryColumn],
) -> EngineResult<Vec<(usize, QueryResultColumn)>> {
    let mut result = Vec::new();
    for requested in projection {
        if requested.column == "*" {
            result.extend(schema.columns.iter().enumerate().map(|(index, column)| {
                (
                    index,
                    QueryResultColumn {
                        name: column.name.clone(),
                        source_table: Some(String::from(table)),
                        source_column: Some(column.name.clone()),
                    },
                )
            }));
            continue;
        }
        if !requested.table.is_empty() && requested.table != table {
            return Err(EngineError::InvalidQuery("Unknown projection table"));
        }
        let (index, column) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == requested.column)
            .ok_or(EngineError::InvalidQuery("Unknown projection column"))?;
        result.push((
            index,
            QueryResultColumn {
                name: column.name.clone(),
                source_table: Some(String::from(table)),
                source_column: Some(column.name.clone()),
            },
        ));
    }
    Ok(result)
}
