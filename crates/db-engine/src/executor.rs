use alloc::{string::String, vec, vec::Vec};

use db_query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryInsert, QueryResult, QueryResultColumn, QuerySelect, QueryUpdate,
    QueryUpdateAssignment,
};
use db_schema::{ColumnSchema, TableSchema};
use db_value::{Row, Value};
use futures::{StreamExt, pin_mut};
use uuid::Uuid;

use crate::{
    Change, EngineError, EngineResult,
    catalog::{
        ENGINE_INDEX_FIELDS, ENGINE_INDICES, ENGINE_TABLE_FIELDS,
        ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID, ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX,
        ENGINE_TABLE_FIELDS_FIELD_COLUMN_NAME, ENGINE_TABLE_FIELDS_FIELD_DEFAULT,
        ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY, ENGINE_TABLE_FIELDS_FIELD_VALUE_TYPE, ENGINE_TABLES,
    },
    change::{apply_change, ensure_change_log},
    codec::RowCodec,
    engine::Engine,
    kernel::{Kernel, KernelTransaction},
};

pub async fn execute_statement<K, R>(
    engine: &Engine<K, R>,
    statements: Vec<db_query::Statement>,
) -> EngineResult<Vec<QueryResult>>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
{
    let mut transaction = engine.kernel.transaction().await?;
    if let Err(error) = ensure_catalog(&mut transaction).await {
        transaction.rollback().await?;
        return Err(error);
    }
    if let Err(error) = ensure_change_log(&mut transaction).await {
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
    R: RowCodec<T>,
{
    match query {
        Query::Insert(insert) => insert_row(transaction, reconciler, insert).await,
        Query::Select(select) => select_rows(transaction, reconciler, select).await,
        Query::Update(update) => update_rows(transaction, reconciler, update).await,
        Query::Delete(delete) => delete_rows(transaction, reconciler, delete).await,
    }
}

async fn execute_ddl<T, R>(
    transaction: &mut T,
    reconciler: &R,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
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
            create_table(transaction, reconciler, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::CreateIndex {
            schema,
            if_not_exists,
        } => {
            let index_key = Row::new(vec![Value::from(schema.name.as_str())]);
            if transaction
                .get_entry(ENGINE_INDICES, &index_key)
                .await?
                .is_some()
            {
                if if_not_exists {
                    return Ok(QueryResult::default());
                }
                return Err(EngineError::InvalidQuery("Index already exists"));
            }
            create_index(transaction, reconciler, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::CreateIndexUnresolved {
            index_name,
            table_name,
            column_names,
            unique,
            if_not_exists,
        } => {
            let index_key = Row::new(vec![Value::from(index_name.as_str())]);
            if transaction
                .get_entry(ENGINE_INDICES, &index_key)
                .await?
                .is_some()
            {
                if if_not_exists {
                    return Ok(QueryResult::default());
                }
                return Err(EngineError::InvalidQuery("Index already exists"));
            }
            let table_schema = table_schema(transaction, &table_name).await?;
            let column_indices = column_names
                .iter()
                .map(|column_name| {
                    table_schema
                        .columns
                        .iter()
                        .position(|column| column.name == *column_name)
                        .map(|index| index as u32)
                        .ok_or(EngineError::InvalidQuery("Index column not found"))
                })
                .collect::<EngineResult<Vec<_>>>()?;
            create_index(
                transaction,
                reconciler,
                db_schema::IndexSchema {
                    name: index_name,
                    table_name,
                    column_indices,
                    unique,
                },
            )
            .await?;
            Ok(QueryResult::default())
        }
        DataDefinition::DropIndex {
            index_name,
            if_exists,
        } => {
            drop_index(transaction, reconciler, &index_name, if_exists).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::AlterTable {
            table_name,
            operations,
            if_exists,
        } => {
            let table_key = Row::new(vec![Value::from(table_name.as_str())]);
            if transaction
                .get_entry(ENGINE_TABLES, &table_key)
                .await?
                .is_none()
            {
                if if_exists {
                    return Ok(QueryResult::default());
                }
                return Err(EngineError::InvalidQuery("Table not found"));
            }
            let schema = table_schema(transaction, &table_name).await?;
            let mut column_names: Vec<_> = schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
            for (column_index, operation) in (column_names.len()..).zip(operations) {
                let AlterTableOperation::AddColumn(column) = operation else {
                    return Err(EngineError::Unsupported(
                        "only ALTER TABLE ADD COLUMN is supported",
                    ));
                };
                if column_names.iter().any(|name| name == &column.name) {
                    return Err(EngineError::InvalidQuery("Column already exists"));
                }
                add_column(transaction, reconciler, &table_name, column_index, &column).await?;
                column_names.push(column.name);
            }
            Ok(QueryResult::default())
        }
        _ => Err(EngineError::Unsupported(
            "only CREATE TABLE and ALTER TABLE ADD COLUMN are supported",
        )),
    }
}

async fn drop_index<T, R>(
    transaction: &mut T,
    codec: &R,
    name: &str,
    if_exists: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let key = Row::new(vec![Value::from(name)]);
    if transaction.get_entry(ENGINE_INDICES, &key).await?.is_none() {
        return if if_exists {
            Ok(())
        } else {
            Err(EngineError::InvalidQuery("Index not found"))
        };
    }
    let keys = {
        let fields = transaction.scan_entries(ENGINE_INDEX_FIELDS);
        pin_mut!(fields);
        let mut keys = Vec::new();
        while let Some(field) = fields.next().await {
            let (key, value) = field?;
            if value.values.first().and_then(Value::as_text) == Some(name) {
                keys.push(key);
            }
        }
        keys
    };
    for key in keys {
        apply_change(
            transaction,
            codec,
            Change::entry(Uuid::now_v7(), String::from(ENGINE_INDEX_FIELDS), key, None),
        )
        .await?;
    }
    apply_change(
        transaction,
        codec,
        Change::entry(Uuid::now_v7(), String::from(ENGINE_INDICES), key, None),
    )
    .await
}

async fn create_index<T, R>(
    transaction: &mut T,
    codec: &R,
    schema: db_schema::IndexSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table_schema = table_schema(transaction, &schema.table_name).await?;
    if schema.name.is_empty() || schema.column_indices.is_empty() {
        return Err(EngineError::InvalidQuery(
            "Index requires a name and column",
        ));
    }
    if schema
        .column_indices
        .iter()
        .any(|index| *index as usize >= table_schema.columns.len())
    {
        return Err(EngineError::InvalidQuery("Index column is out of range"));
    }

    let index_key = Row::new(vec![Value::from(schema.name.as_str())]);
    let index_value = Row::new(vec![
        Value::from(schema.name.as_str()),
        Value::from(schema.table_name.as_str()),
        Value::Bool(schema.unique),
        Value::Integer(schema.column_indices.len() as i64),
    ]);
    apply_change(
        transaction,
        codec,
        Change::entry(
            Uuid::now_v7(),
            String::from(ENGINE_INDICES),
            index_key,
            Some(postcard::to_allocvec(&index_value).map_err(EngineError::custom)?),
        ),
    )
    .await?;

    for (order, column_index) in schema.column_indices.iter().enumerate() {
        let key = Row::new(vec![
            Value::from(schema.name.as_str()),
            Value::Integer(order as i64),
        ]);
        let value = Row::new(vec![
            Value::from(schema.name.as_str()),
            Value::Integer(order as i64),
            Value::Integer(i64::from(*column_index)),
        ]);
        apply_change(
            transaction,
            codec,
            Change::entry(
                Uuid::now_v7(),
                String::from(ENGINE_INDEX_FIELDS),
                key,
                Some(postcard::to_allocvec(&value).map_err(EngineError::custom)?),
            ),
        )
        .await?;
    }
    Ok(())
}

async fn create_table<T, R>(
    transaction: &mut T,
    codec: &R,
    table_schema: TableSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table_key = Row::new(vec![table_schema.name.clone().into()]);
    let table_value = Row::new(vec![table_schema.name.clone().into()]);
    apply_change(
        transaction,
        codec,
        Change::entry(
            Uuid::now_v7(),
            String::from(ENGINE_TABLES),
            table_key,
            Some(postcard::to_allocvec(&table_value).map_err(EngineError::custom)?),
        ),
    )
    .await?;

    for (column_index, column_schema) in table_schema.columns.iter().enumerate() {
        add_column(
            transaction,
            codec,
            &table_schema.name,
            column_index,
            column_schema,
        )
        .await?;
    }

    Ok(())
}

async fn add_column<T, R>(
    transaction: &mut T,
    codec: &R,
    table_name: &str,
    column_index: usize,
    column_schema: &ColumnSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let key = Row::new(vec![table_name.into(), column_schema.name.clone().into()]);
    let value = Row::new(vec![
        table_name.into(),
        column_schema.name.clone().into(),
        column_schema.r#type.into(),
        column_schema.default.clone(),
        (column_index as i64).into(),
        column_schema.primary_key.into(),
        Value::Uuid(Uuid::now_v7()),
    ]);
    apply_change(
        transaction,
        codec,
        Change::entry(
            Uuid::now_v7(),
            String::from(ENGINE_TABLE_FIELDS),
            key,
            Some(postcard::to_allocvec(&value).map_err(EngineError::custom)?),
        ),
    )
    .await
}

async fn insert_row<T, R>(
    transaction: &mut T,
    reconciler: &R,
    insert: QueryInsert,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
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
    let changed_columns: Vec<_> = (0..insert.row.values.len()).collect();
    let value = reconciler
        .encode_row(
            transaction,
            &insert.table,
            &key,
            &insert.row,
            &changed_columns,
        )
        .await?;
    apply_change(
        transaction,
        reconciler,
        Change::row(Uuid::now_v7(), insert.table, key, Some(value)),
    )
    .await?;

    Ok(QueryResult::default())
}

async fn update_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
    update: QueryUpdate,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if !update.from.joins.is_empty() {
        return Err(EngineError::Unsupported("UPDATE JOIN"));
    }
    if update.returning.is_some() {
        return Err(EngineError::Unsupported("UPDATE RETURNING"));
    }

    let schema = table_schema(transaction, &update.from.table).await?;
    let assignments = assignments(&schema, &update.from.table, &update.assignments)?;
    let predicate = predicate(&schema, &update.from.table, update.predicate.as_ref())?;
    let rows = matching_rows(
        transaction,
        reconciler,
        &update.from.table,
        &schema,
        predicate,
    )
    .await?;

    for (old_key, mut row) in rows {
        for (index, value) in &assignments {
            row.values[*index] = value.clone();
        }
        let key = primary_key(&schema, &row)?;
        let changed_columns: Vec<_> = assignments.iter().map(|(index, _)| *index).collect();
        let value = reconciler
            .encode_row(
                transaction,
                &update.from.table,
                &key,
                &row,
                &changed_columns,
            )
            .await?;
        if key != old_key {
            apply_change(
                transaction,
                reconciler,
                Change::row(Uuid::now_v7(), update.from.table.clone(), old_key, None),
            )
            .await?;
        }
        apply_change(
            transaction,
            reconciler,
            Change::row(Uuid::now_v7(), update.from.table.clone(), key, Some(value)),
        )
        .await?;
    }

    Ok(QueryResult::default())
}

async fn delete_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
    delete: QueryDelete,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if !delete.from.joins.is_empty() {
        return Err(EngineError::Unsupported("DELETE JOIN"));
    }
    if delete.returning.is_some() {
        return Err(EngineError::Unsupported("DELETE RETURNING"));
    }

    let schema = table_schema(transaction, &delete.from.table).await?;
    let predicate = predicate(&schema, &delete.from.table, delete.predicate.as_ref())?;
    let rows = matching_rows(
        transaction,
        reconciler,
        &delete.from.table,
        &schema,
        predicate,
    )
    .await?;

    for (key, _) in rows {
        apply_change(
            transaction,
            reconciler,
            Change::row(Uuid::now_v7(), delete.from.table.clone(), key, None),
        )
        .await?;
    }

    Ok(QueryResult::default())
}

async fn matching_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    table: &str,
    schema: &TableSchema,
    predicate: (usize, Value),
) -> EngineResult<Vec<(Row, Row)>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let stream = reconciler.scan_rows(transaction, table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (key, row) = item?;
        let row = materialize_defaults(schema, row);
        if row.values.get(predicate.0) == Some(&predicate.1) {
            rows.push((key, row));
        }
    }

    Ok(rows)
}

fn assignments(
    schema: &TableSchema,
    table: &str,
    assignments: &[QueryUpdateAssignment],
) -> EngineResult<Vec<(usize, Value)>> {
    if assignments.is_empty() {
        return Err(EngineError::InvalidQuery("UPDATE requires an assignment"));
    }

    assignments
        .iter()
        .map(|assignment| {
            let index = column_index(
                schema,
                table,
                &assignment.column,
                "Unknown assignment column",
            )?;
            let QueryExprValue::Value(value) = &assignment.value else {
                return Err(EngineError::Unsupported("UPDATE column assignments"));
            };
            Ok((index, value.clone()))
        })
        .collect()
}

fn predicate(
    schema: &TableSchema,
    table: &str,
    predicate: Option<&QueryExpr>,
) -> EngineResult<(usize, Value)> {
    let Some(QueryExpr::Equals(left, right)) = predicate else {
        return Err(EngineError::Unsupported(
            "UPDATE and DELETE require column equality predicates",
        ));
    };

    let (column, value) = match (left.as_ref(), right.as_ref()) {
        (
            QueryExpr::Value(QueryExprValue::Column(column)),
            QueryExpr::Value(QueryExprValue::Value(value)),
        )
        | (
            QueryExpr::Value(QueryExprValue::Value(value)),
            QueryExpr::Value(QueryExprValue::Column(column)),
        ) => (column, value),
        _ => {
            return Err(EngineError::Unsupported(
                "UPDATE and DELETE require column equality predicates",
            ));
        }
    };

    Ok((
        column_index(schema, table, column, "Unknown predicate column")?,
        value.clone(),
    ))
}

fn column_index(
    schema: &TableSchema,
    table: &str,
    column: &QueryColumn,
    unknown_column: &'static str,
) -> EngineResult<usize> {
    if !column.table.is_empty() && column.table != table {
        return Err(EngineError::InvalidQuery("Unknown column table"));
    }
    schema
        .columns
        .iter()
        .position(|schema_column| schema_column.name == column.column)
        .ok_or(EngineError::InvalidQuery(unknown_column))
}

async fn select_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    select: QuerySelect,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
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
        let row = materialize_defaults(&schema, row);
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

pub(crate) async fn table_schema<T>(transaction: &T, name: &str) -> EngineResult<TableSchema>
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
        let default = row
            .values
            .get(3)
            .cloned()
            .ok_or(EngineError::InvalidQuery(ENGINE_TABLE_FIELDS_FIELD_DEFAULT))?;
        let index =
            row.values
                .get(4)
                .and_then(Value::to_integer)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_TABLE_FIELDS_FIELD_COLUMN_INDEX,
                ))?;
        let primary_key =
            row.values
                .get(5)
                .and_then(Value::to_bool)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_TABLE_FIELDS_FIELD_PRIMARY_KEY,
                ))?;
        let id = row
            .values
            .get(6)
            .and_then(Value::to_uuid)
            .ok_or(EngineError::InvalidQuery(
                ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID,
            ))?;
        columns.push((
            index,
            id,
            ColumnSchema {
                name: column,
                r#type: value_type,
                default,
                primary_key,
            },
        ));
    }

    columns.sort_by_key(|(index, id, _)| (*index, *id));
    Ok(TableSchema {
        name: String::from(name),
        columns: columns.into_iter().map(|(_, _, column)| column).collect(),
    })
}

#[cfg(all(test, feature = "in-memory"))]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::{DirectRowCodec, Engine, InMemoryKernel};

    #[test]
    fn resolves_create_index_column_names() {
        block_on(async {
            let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
            engine
                .create_table(TableSchema {
                    name: "users".into(),
                    columns: vec![
                        ColumnSchema {
                            name: "id".into(),
                            r#type: db_value::ValueType::Integer,
                            default: Value::Null,
                            primary_key: true,
                        },
                        ColumnSchema {
                            name: "email".into(),
                            r#type: db_value::ValueType::Text,
                            default: Value::Null,
                            primary_key: false,
                        },
                    ],
                })
                .await
                .unwrap();
            engine
                .execute(vec![db_query::Statement::DataDefinition(
                    DataDefinition::CreateIndexUnresolved {
                        index_name: "users_email".into(),
                        table_name: "users".into(),
                        column_names: vec!["email".into()],
                        unique: true,
                        if_not_exists: false,
                    },
                )])
                .await
                .unwrap();

            assert_eq!(
                engine
                    .index_schema("users_email")
                    .await
                    .unwrap()
                    .column_indices,
                vec![1]
            );
        });
    }
}

pub(crate) fn materialize_defaults(schema: &TableSchema, mut row: Row) -> Row {
    row.values.extend(
        schema.columns[row.values.len()..]
            .iter()
            .map(|column| column.default.clone()),
    );
    row
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
