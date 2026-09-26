use alloc::{string::String, vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryInsert, QueryInsertValue, QueryInsertValues, QueryResult,
    QueryResultColumn, QuerySelect, QueryUpdate, QueryUpdateAssignment,
};
use schema::{ColumnSchema, TableSchema};
use uuid::Uuid;
use value::{Row, Value};

fn next_uuid(timestamp_provider: TimestampProvider) -> EngineResult<Uuid> {
    let timestamp = timestamp_provider().ok_or(EngineError::MissingTimestampProvider)?;
    Ok(Uuid::new_v7(timestamp))
}

use crate::{
    Change, EngineError, EngineResult, SchemaChange,
    change::apply_local_change,
    codec::RowCodec,
    engine::{Engine, TimestampProvider},
    kernel::{Kernel, KernelTransaction},
    schema::{
        columns as schema_columns, ensure as ensure_schema, lookup_table_name,
        table_schema as schema_table_schema,
    },
};

pub async fn execute_statement<K, R>(
    engine: &Engine<K, R>,
    statements: Vec<query::Statement>,
) -> EngineResult<Vec<QueryResult>>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    let mut transaction = engine.kernel.transaction().await?;
    if let Err(error) = ensure_catalog(&mut transaction).await {
        transaction.rollback().await?;
        return Err(error);
    }
    let mut changes = Vec::new();

    let mut results = Vec::with_capacity(statements.len());

    for statement in statements {
        let result = match statement {
            query::Statement::Query(query) => {
                execute_query(
                    &mut transaction,
                    engine.reconciler.as_ref(),
                    engine.timestamp_provider,
                    &mut changes,
                    query,
                )
                .await
            }
            query::Statement::DataDefinition(ddl) => {
                execute_ddl(
                    &mut transaction,
                    engine.reconciler.as_ref(),
                    engine.timestamp_provider,
                    &mut changes,
                    ddl,
                )
                .await
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
    ensure_schema(transaction).await
}

async fn execute_query<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    query: Query,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match query {
        Query::Insert(insert) => {
            insert_row(transaction, reconciler, timestamp_provider, changes, insert).await
        }
        Query::InsertValues(insert) => {
            insert_values(transaction, reconciler, timestamp_provider, changes, insert).await
        }
        Query::Select(select) => select_rows(transaction, reconciler, select).await,
        Query::Update(update) => {
            update_rows(transaction, reconciler, timestamp_provider, changes, update).await
        }
        Query::Delete(delete) => {
            delete_rows(transaction, reconciler, timestamp_provider, changes, delete).await
        }
    }
}

async fn execute_ddl<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match &ddl {
        DataDefinition::CreateTable { .. }
        | DataDefinition::CreateTableWithIndexes { .. }
        | DataDefinition::DropTable { .. }
        | DataDefinition::AlterTable { .. } => {
            execute_table_ddl(transaction, reconciler, timestamp_provider, changes, ddl).await
        }
        DataDefinition::CreateIndex { .. }
        | DataDefinition::CreateIndexUnresolved { .. }
        | DataDefinition::DropIndex { .. } => {
            execute_index_ddl(transaction, reconciler, timestamp_provider, changes, ddl).await
        }
        _ => Err(EngineError::Unsupported(
            "only CREATE TABLE and ALTER TABLE ADD COLUMN are supported",
        )),
    }
}

async fn execute_table_ddl<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
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
            if lookup_table_name(transaction, &schema.name).await.is_ok() {
                return if if_not_exists {
                    Ok(QueryResult::default())
                } else {
                    Err(EngineError::InvalidQuery("Table already exists"))
                };
            }
            create_table(transaction, codec, timestamp_provider, changes, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::CreateTableWithIndexes {
            schema,
            indexes,
            if_not_exists,
        } => {
            if lookup_table_name(transaction, &schema.name).await.is_ok() {
                return if if_not_exists {
                    Ok(QueryResult::default())
                } else {
                    Err(EngineError::InvalidQuery("Table already exists"))
                };
            }
            create_table(transaction, codec, timestamp_provider, changes, schema).await?;
            for index in indexes {
                create_index(transaction, codec, timestamp_provider, changes, index).await?;
            }
            Ok(QueryResult::default())
        }
        DataDefinition::DropTable {
            table_name,
            if_exists,
        } => {
            let table = match lookup_table_name(transaction, &table_name).await {
                Ok(table) => table,
                Err(_) if if_exists => return Ok(QueryResult::default()),
                Err(error) => return Err(error),
            };
            apply_local_change(
                transaction,
                codec,
                changes,
                Change::schema(
                    next_uuid(timestamp_provider)?,
                    SchemaChange::TombstoneTable(table),
                ),
            )
            .await?;
            Ok(QueryResult::default())
        }
        DataDefinition::AlterTable {
            table_name,
            operations,
            if_exists,
        } => {
            alter_table(
                transaction,
                codec,
                timestamp_provider,
                changes,
                table_name,
                operations,
                if_exists,
            )
            .await
        }
        _ => unreachable!(),
    }
}

async fn execute_index_ddl<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match ddl {
        DataDefinition::CreateIndex {
            schema,
            if_not_exists,
        } => {
            ensure_index_absent(transaction, &schema.name, if_not_exists).await?;
            create_index(transaction, codec, timestamp_provider, changes, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::CreateIndexUnresolved {
            index_name,
            table_name,
            column_names,
            unique,
            if_not_exists,
        } => {
            create_unresolved_index(
                transaction,
                codec,
                timestamp_provider,
                changes,
                index_name,
                table_name,
                column_names,
                unique,
                if_not_exists,
            )
            .await
        }
        DataDefinition::DropIndex {
            index_name,
            if_exists,
        } => {
            drop_index(
                transaction,
                codec,
                timestamp_provider,
                changes,
                &index_name,
                if_exists,
            )
            .await?;
            Ok(QueryResult::default())
        }
        _ => unreachable!(),
    }
}

async fn ensure_index_absent<T>(
    transaction: &T,
    name: &str,
    if_not_exists: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    if crate::index::index_schema(transaction, name)
        .await?
        .is_some()
        && !if_not_exists
    {
        return Err(EngineError::InvalidQuery("Index already exists"));
    }
    Ok(())
}

async fn create_unresolved_index<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    index_name: String,
    table_name: String,
    column_names: Vec<String>,
    unique: bool,
    if_not_exists: bool,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if crate::index::index_schema(transaction, &index_name)
        .await?
        .is_some()
    {
        return if if_not_exists {
            Ok(QueryResult::default())
        } else {
            Err(EngineError::InvalidQuery("Index already exists"))
        };
    }
    let column_indices = unresolved_index_columns(transaction, &table_name, &column_names).await?;
    create_index(
        transaction,
        codec,
        timestamp_provider,
        changes,
        schema::IndexSchema {
            name: index_name,
            table_name,
            column_indices,
            unique,
        },
    )
    .await?;
    Ok(QueryResult::default())
}

async fn unresolved_index_columns<T>(
    transaction: &T,
    table_name: &str,
    names: &[String],
) -> EngineResult<Vec<u32>>
where
    T: KernelTransaction,
{
    let schema = table_schema(transaction, table_name).await?;
    names
        .iter()
        .map(|name| {
            schema
                .columns
                .iter()
                .position(|column| column.name == *name)
                .map(|index| index as u32)
                .ok_or(EngineError::InvalidQuery("Index column not found"))
        })
        .collect()
}

async fn alter_table<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table_name: String,
    operations: Vec<AlterTableOperation>,
    if_exists: bool,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = match lookup_table_name(transaction, &table_name).await {
        Ok(table) => table,
        Err(_) if if_exists => return Ok(QueryResult::default()),
        Err(error) => return Err(error),
    };
    let mut names: Vec<_> = table_schema(transaction, &table_name)
        .await?
        .columns
        .into_iter()
        .map(|column| column.name)
        .collect();
    for (position, operation) in (names.len()..).zip(operations) {
        let AlterTableOperation::AddColumn(column) = operation else {
            return Err(EngineError::Unsupported(
                "only ALTER TABLE ADD COLUMN is supported",
            ));
        };
        if names.iter().any(|name| name == &column.name) {
            return Err(EngineError::InvalidQuery("Column already exists"));
        }
        if column.primary_key {
            return Err(EngineError::InvalidQuery("Cannot add a primary-key column"));
        }
        add_column(
            transaction,
            codec,
            timestamp_provider,
            changes,
            table.clone(),
            position,
            &column,
        )
        .await?;
        names.push(column.name);
    }
    Ok(QueryResult::default())
}

async fn drop_index<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    name: &str,
    if_exists: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if crate::index::index_schema(transaction, name)
        .await?
        .is_none()
    {
        return if if_exists {
            Ok(())
        } else {
            Err(EngineError::InvalidQuery("Index not found"))
        };
    }
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(
            next_uuid(timestamp_provider)?,
            SchemaChange::TombstoneIndex(name.into()),
        ),
    )
    .await
}

async fn create_index<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    schema: schema::IndexSchema,
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

    let table = lookup_table_name(transaction, &schema.table_name).await?;
    let columns = schema_columns(transaction, &table).await?;
    let index_columns = schema
        .column_indices
        .iter()
        .map(|position| {
            columns
                .get(*position as usize)
                .map(|(name, _)| name.clone())
                .ok_or(EngineError::InvalidQuery("Index column is out of range"))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(
            next_uuid(timestamp_provider)?,
            SchemaChange::CreateIndex {
                index: schema.name,
                table: table.clone(),
                unique: schema.unique,
                columns: index_columns,
            },
        ),
    )
    .await
}

async fn create_table<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table_schema: TableSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    table_schema
        .validate_uuid_primary_key()
        .map_err(EngineError::InvalidQuery)?;
    let table = table_schema.name.clone();
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(
            next_uuid(timestamp_provider)?,
            SchemaChange::CreateTable {
                table: table.clone(),
            },
        ),
    )
    .await?;
    for (position, column) in table_schema.columns.iter().enumerate() {
        add_column(
            transaction,
            codec,
            timestamp_provider,
            changes,
            table.clone(),
            position,
            column,
        )
        .await?;
    }
    Ok(())
}

async fn add_column<T, R>(
    transaction: &mut T,
    codec: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    table: String,
    position: usize,
    column: &ColumnSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let position =
        u32::try_from(position).map_err(|_| EngineError::InvalidQuery("Too many columns"))?;
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(
            next_uuid(timestamp_provider)?,
            SchemaChange::AddColumn {
                table,
                column: column.name.clone(),
                value_type: column.r#type,
                default: column.default.clone(),
                position,
                primary_key: column.primary_key,
            },
        ),
    )
    .await
}

async fn insert_row<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    insert: QueryInsert,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let schema = table_schema(transaction, &insert.table).await?;
    let row_value = materialize_insert(&schema, insert.row, timestamp_provider)?;
    let row = row_id(&schema, &row_value)?;
    let table = lookup_table_name(transaction, &insert.table).await?;
    if row_is_deleted(reconciler, transaction, &table, row).await? {
        return Err(EngineError::InvalidQuery("Primary key was deleted"));
    }
    if reconciler
        .get_row(transaction, &table, &row)
        .await?
        .is_some()
    {
        return Err(EngineError::InvalidQuery("Primary key already exists"));
    }
    let changed_columns: Vec<_> = (0..row_value.values.len()).collect();
    let value = reconciler
        .encode_row(transaction, &table, &row, &row_value, &changed_columns)
        .await?;
    apply_local_change(
        transaction,
        reconciler,
        changes,
        Change::row(next_uuid(timestamp_provider)?, table, row, Some(value)),
    )
    .await?;

    let Some(returning) = insert.returning else {
        return Ok(QueryResult::default());
    };
    let mut result = Vec::with_capacity(returning.len());
    let mut columns = Vec::with_capacity(returning.len());
    for name in returning {
        if name == "*" {
            result.extend(row_value.values.iter().cloned());
            columns.extend(schema.columns.iter().map(|column| QueryResultColumn {
                name: column.name.clone(),
                source_table: Some(insert.table.clone()),
                source_column: Some(column.name.clone()),
            }));
            continue;
        }
        let index = schema
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or(EngineError::InvalidQuery("Unknown RETURNING column"))?;
        result.push(row_value.values[index].clone());
        columns.push(QueryResultColumn {
            name: name.clone(),
            source_table: Some(insert.table.clone()),
            source_column: Some(name),
        });
    }
    Ok(QueryResult::new_with_columns(
        vec![Row::new(result)],
        columns,
    ))
}

async fn row_is_deleted<T, R>(
    reconciler: &R,
    transaction: &T,
    table: &str,
    row: Uuid,
) -> EngineResult<bool>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    reconciler.row_is_deleted(transaction, table, &row).await
}

async fn insert_values<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
    insert: QueryInsertValues,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let schema = table_schema(transaction, &insert.table).await?;
    let mut values = vec![None; schema.columns.len()];
    let columns = if insert.columns.is_empty() {
        schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    } else {
        insert.columns
    };
    if columns.len() != insert.values.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT column/value count mismatch",
        ));
    }
    for (column, value) in columns.iter().zip(insert.values) {
        let index = schema
            .columns
            .iter()
            .position(|candidate| candidate.name == *column)
            .ok_or(EngineError::InvalidQuery("Unknown INSERT column"))?;
        if values[index].is_some() {
            return Err(EngineError::InvalidQuery("Duplicate INSERT column"));
        }
        values[index] = Some(value);
    }
    let primary_key = schema
        .columns
        .iter()
        .position(|column| column.primary_key)
        .ok_or(EngineError::InvalidQuery(
            "Table requires exactly one UUID primary key",
        ))?;
    let row = Row::new(
        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| match value {
                Some(QueryInsertValue::Value(value)) => Ok(value),
                Some(QueryInsertValue::Default) | None if index == primary_key => {
                    next_uuid(timestamp_provider).map(Value::Uuid)
                }
                Some(QueryInsertValue::Default) | None => Ok(schema.columns[index].default.clone()),
            })
            .collect::<EngineResult<Vec<_>>>()?,
    );
    insert_row(
        transaction,
        reconciler,
        timestamp_provider,
        changes,
        QueryInsert {
            table: insert.table,
            row,
            returning: insert.returning,
        },
    )
    .await
}

async fn update_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
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

    let table = lookup_table_name(transaction, &update.from.table).await?;
    for (id, mut row) in rows {
        for (index, value) in &assignments {
            row.values[*index] = value.clone();
        }
        if row_id(&schema, &row)? != id {
            return Err(EngineError::InvalidQuery("Cannot update the primary key"));
        }
        let changed_columns: Vec<_> = assignments.iter().map(|(index, _)| *index).collect();
        let value = reconciler
            .encode_row(transaction, &table, &id, &row, &changed_columns)
            .await?;
        apply_local_change(
            transaction,
            reconciler,
            changes,
            Change::row(
                next_uuid(timestamp_provider)?,
                table.clone(),
                id,
                Some(value),
            ),
        )
        .await?;
    }

    Ok(QueryResult::default())
}

async fn delete_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    changes: &mut Vec<Change>,
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

    let table = lookup_table_name(transaction, &delete.from.table).await?;
    for (row, _) in rows {
        apply_local_change(
            transaction,
            reconciler,
            changes,
            Change::row(next_uuid(timestamp_provider)?, table.clone(), row, None),
        )
        .await?;
    }

    Ok(QueryResult::default())
}

pub(crate) async fn row_conflicts<T, R>(
    transaction: &T,
    reconciler: &R,
    table_name: &str,
    key: &Row,
) -> EngineResult<Vec<String>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = lookup_table_name(transaction, table_name).await?;
    let row = key_row_id(key)?;
    let schema = table_schema(transaction, table_name).await?;
    reconciler
        .conflicted_columns(transaction, &table, &row)
        .await?
        .into_iter()
        .map(|index| {
            schema
                .columns
                .get(index)
                .map(|column| column.name.clone())
                .ok_or(EngineError::custom(
                    "Conflicted column is missing from schema",
                ))
        })
        .collect()
}

pub(crate) async fn resolve_row<T, R>(
    transaction: &mut T,
    reconciler: &R,
    timestamp_provider: TimestampProvider,
    table_name: &str,
    key: &Row,
    values: Vec<(String, Value)>,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    if values.is_empty() {
        return Err(EngineError::InvalidQuery(
            "Resolution requires an assignment",
        ));
    }
    let table = lookup_table_name(transaction, table_name).await?;
    let row_id = key_row_id(key)?;
    let schema = table_schema(transaction, table_name).await?;
    let conflicts = reconciler
        .conflicted_columns(transaction, &table, &row_id)
        .await?;
    let mut row = reconciler
        .get_row(transaction, &table, &row_id)
        .await?
        .ok_or(EngineError::InvalidQuery("Row not found"))?;
    let mut changed_columns = Vec::with_capacity(values.len());
    for (name, value) in values {
        let index = schema
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or(EngineError::InvalidQuery("Unknown resolution column"))?;
        if !conflicts.contains(&index) {
            return Err(EngineError::InvalidQuery(
                "Resolution column is not conflicted",
            ));
        }
        if schema.columns[index].primary_key {
            return Err(EngineError::InvalidQuery("Cannot resolve the primary key"));
        }
        if changed_columns.contains(&index) {
            return Err(EngineError::InvalidQuery("Resolution column is repeated"));
        }
        row.values[index] = value;
        changed_columns.push(index);
    }
    let value = reconciler
        .encode_resolution(transaction, &table, &row_id, &row, &changed_columns)
        .await?;
    let mut changes = Vec::new();
    apply_local_change(
        transaction,
        reconciler,
        &mut changes,
        Change::row(next_uuid(timestamp_provider)?, table, row_id, Some(value)),
    )
    .await?;
    Ok(())
}

async fn matching_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    table_name: &str,
    schema: &TableSchema,
    predicate: (usize, Value),
) -> EngineResult<Vec<(Uuid, Row)>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = lookup_table_name(transaction, table_name).await?;
    let stream = reconciler.scan_rows(transaction, &table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (row_id, row) = item?;
        let row = materialize_defaults(schema, row);
        if row.values.get(predicate.0) == Some(&predicate.1) {
            rows.push((row_id, row));
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
            if schema.columns[index].primary_key {
                return Err(EngineError::InvalidQuery("Cannot update the primary key"));
            }
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
    let table = lookup_table_name(transaction, &select.from.table).await?;
    let stream = reconciler.scan_rows(transaction, &table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (_, row) = item?;
        let row = materialize_defaults(&schema, row);
        if !predicate_matches(&schema, &select.from.table, &row, select.predicate.as_ref())? {
            continue;
        }
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

fn predicate_matches(
    schema: &TableSchema,
    table: &str,
    row: &Row,
    predicate: Option<&QueryExpr>,
) -> EngineResult<bool> {
    match predicate {
        Some(predicate) => Ok(matches!(
            evaluate_expr(schema, table, row, predicate)?,
            Value::Bool(true)
        )),
        None => Ok(true),
    }
}

fn evaluate_expr(
    schema: &TableSchema,
    table: &str,
    row: &Row,
    expr: &QueryExpr,
) -> EngineResult<Value> {
    match expr {
        QueryExpr::Value(QueryExprValue::Value(value)) => Ok(value.clone()),
        QueryExpr::Value(QueryExprValue::Column(column)) => Ok(row.values
            [column_index(schema, table, column, "Unknown predicate column")?]
        .clone()),
        QueryExpr::IsNull(expr) => Ok(Value::Bool(matches!(
            evaluate_expr(schema, table, row, expr)?,
            Value::Null
        ))),
        QueryExpr::IsNotNull(expr) => Ok(Value::Bool(!matches!(
            evaluate_expr(schema, table, row, expr)?,
            Value::Null
        ))),
        QueryExpr::Equals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left == right)
        }
        QueryExpr::NotEquals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left != right)
        }
        QueryExpr::LessThan(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left < right)
        }
        QueryExpr::LessThanOrEquals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left <= right)
        }
        QueryExpr::GreaterThan(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left > right)
        }
        QueryExpr::GreaterThanOrEquals(left, right) => {
            compare_expr(schema, table, row, left, right, |left, right| left >= right)
        }
        QueryExpr::And(left, right) => Ok(Value::Bool(
            matches!(evaluate_expr(schema, table, row, left)?, Value::Bool(true))
                && matches!(evaluate_expr(schema, table, row, right)?, Value::Bool(true)),
        )),
        QueryExpr::Or(left, right) => Ok(Value::Bool(
            matches!(evaluate_expr(schema, table, row, left)?, Value::Bool(true))
                || matches!(evaluate_expr(schema, table, row, right)?, Value::Bool(true)),
        )),
        _ => Err(EngineError::Unsupported("SELECT predicate")),
    }
}

fn compare_expr(
    schema: &TableSchema,
    table: &str,
    row: &Row,
    left: &QueryExpr,
    right: &QueryExpr,
    compare: impl FnOnce(&Value, &Value) -> bool,
) -> EngineResult<Value> {
    let left = evaluate_expr(schema, table, row, left)?;
    let right = evaluate_expr(schema, table, row, right)?;
    Ok(Value::Bool(
        !matches!(left, Value::Null) && !matches!(right, Value::Null) && compare(&left, &right),
    ))
}

pub(crate) async fn table_schema<T>(transaction: &T, name: &str) -> EngineResult<TableSchema>
where
    T: KernelTransaction,
{
    schema_table_schema(transaction, name).await
}

pub(crate) fn materialize_defaults(schema: &TableSchema, mut row: Row) -> Row {
    row.values.extend(
        schema.columns[row.values.len()..]
            .iter()
            .map(|column| column.default.clone()),
    );
    row
}

fn materialize_insert(
    schema: &TableSchema,
    row: Row,
    timestamp_provider: TimestampProvider,
) -> EngineResult<Row> {
    if row.values.len() > schema.columns.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT row has the wrong column count",
        ));
    }
    let mut values = row.values;
    let primary_key = schema
        .columns
        .iter()
        .position(|column| column.primary_key)
        .ok_or(EngineError::InvalidQuery(
            "Table requires exactly one UUID primary key",
        ))?;
    if values.len() < schema.columns.len() && primary_key == 0 {
        let timestamp = timestamp_provider().ok_or(EngineError::MissingTimestampProvider)?;
        values.insert(0, Value::Uuid(Uuid::new_v7(timestamp)));
    }
    let mut row = Row::new(values);
    if row.values.len() < schema.columns.len() {
        row = materialize_defaults(schema, row);
    }
    if row.values.len() != schema.columns.len() {
        return Err(EngineError::InvalidQuery(
            "INSERT row has the wrong column count",
        ));
    }
    Ok(row)
}

pub(crate) fn row_id(schema: &TableSchema, row: &Row) -> EngineResult<Uuid> {
    let primary_keys: Vec<_> = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect();
    let [index] = primary_keys.as_slice() else {
        return Err(EngineError::InvalidQuery(
            "Table requires exactly one UUID primary key",
        ));
    };
    row.values
        .get(*index)
        .and_then(Value::as_uuid)
        .copied()
        .ok_or(EngineError::InvalidQuery("Primary key must be a UUID"))
}

fn key_row_id(key: &Row) -> EngineResult<Uuid> {
    match key.values.as_slice() {
        [Value::Uuid(id)] => Ok(*id),
        _ => Err(EngineError::InvalidQuery("Primary key must be a UUID")),
    }
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
