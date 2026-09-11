use alloc::{string::String, vec::Vec};

use db_btree::{BTree, BTreeRead, BTreeTransaction};
use db_query::{
    DataDefinition, Query, QueryColumn, QueryInsert, QueryResult, QueryResultColumn, QuerySelect,
};
use db_schema::{ColumnSchema, TableSchema};
use db_value::{Row, Value};
use futures::{StreamExt, pin_mut};

use crate::{
    EngineError, EngineResult,
    catalog::{
        ENGINE_TABLE_FIELDS, ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX,
        ENGINE_TABLE_FIELDS_FIELD_COLUMN_NAME, ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY,
        ENGINE_TABLE_FIELDS_FIELD_VALUE_TYPE, ENGINE_TABLES,
    },
    engine::Engine,
    kernel::{Kernel, KernelTransaction},
};

pub async fn execute_statement<K>(
    engine: &Engine<K>,
    statements: Vec<db_query::Statement>,
) -> EngineResult<Vec<QueryResult>>
where
    K: Kernel,
{
    let mut results = Vec::with_capacity(statements.len());
    for statement in statements {
        match statement {
            db_query::Statement::Query(query) => results.push(execute_query(engine, query).await?),
            db_query::Statement::DataDefinition(ddl) => {
                results.push(execute_ddl(engine, ddl).await?)
            }
        }
    }
    Ok(results)
}

async fn execute_query<K>(engine: &Engine<K>, query: Query) -> EngineResult<QueryResult>
where
    K: Kernel,
{
    match query {
        Query::Insert(insert) => insert_row(engine, insert).await,
        Query::Select(select) => select_rows(engine, select).await,
        Query::Update(_) | Query::Delete(_) => Err(EngineError::Unsupported(
            "only INSERT and simple SELECT are supported",
        )),
    }
}

async fn execute_ddl<K>(engine: &Engine<K>, ddl: DataDefinition) -> EngineResult<QueryResult>
where
    K: Kernel,
{
    match ddl {
        DataDefinition::CreateTable {
            schema,
            if_not_exists,
        } => {
            let table = engine.kernel.read_table(&schema.name).await;
            if table.is_ok() {
                if if_not_exists {
                    return Ok(QueryResult::default());
                }
                return Err(EngineError::InvalidQuery("Table already exists"));
            }

            let transaction = engine.kernel.transaction().await?;
            transaction.write_table(&schema.name).await?;
            transaction.commit().await?;
            engine.create_table(schema).await?;
            Ok(QueryResult::default())
        }
        _ => Err(EngineError::Unsupported("only CREATE TABLE is supported")),
    }
}

async fn insert_row<K>(engine: &Engine<K>, insert: QueryInsert) -> EngineResult<QueryResult>
where
    K: Kernel,
{
    if insert.returning.is_some() {
        return Err(EngineError::Unsupported("INSERT RETURNING"));
    }

    let schema = table_schema(engine, &insert.table).await?;
    if insert.row.values.len() != schema.columns.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT row has the wrong column count",
        ));
    }

    let key = primary_key(&schema, &insert.row)?;
    let transaction = engine.kernel.transaction().await?;
    let table = transaction.write_table(&insert.table).await?;
    let mut table_transaction = table.transaction().await?;
    table_transaction.insert(key, insert.row).await?;
    table_transaction.commit().await?;
    transaction.commit().await?;

    Ok(QueryResult::default())
}

async fn select_rows<K>(engine: &Engine<K>, select: QuerySelect) -> EngineResult<QueryResult>
where
    K: Kernel,
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

    let schema = table_schema(engine, &select.from.table).await?;
    let projection = projection(&schema, &select.from.table, &select.projection)?;
    let table = engine.kernel.read_table(&select.from.table).await?;
    let stream = table.range(..);
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

async fn table_schema<K>(engine: &Engine<K>, name: &str) -> EngineResult<TableSchema>
where
    K: Kernel,
{
    let table_key = Row::new(vec![Value::from(name)]);
    if engine
        .kernel
        .read_table(ENGINE_TABLES)
        .await?
        .get(&table_key)
        .await?
        .is_none()
    {
        return Err(EngineError::InvalidQuery("Table not found"));
    }

    let fields = engine.kernel.read_table(ENGINE_TABLE_FIELDS).await?;
    let stream = fields.range(..);
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
