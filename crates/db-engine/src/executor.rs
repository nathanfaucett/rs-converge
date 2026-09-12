use alloc::{string::String, vec::Vec};

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
    Change, ColumnGenerationId, EngineError, EngineResult, IndexGenerationId, RowGenerationId,
    SchemaChange, TableGenerationId,
    catalog::ENGINE_ROW_MAPPINGS,
    change::{apply_local_change, ensure_change_log, row_generation_id},
    codec::RowCodec,
    engine::Engine,
    envelope::record_local,
    kernel::{Kernel, KernelTransaction},
    schema::{
        column_id, columns as schema_columns, ensure as ensure_schema, table_id,
        table_schema as schema_table_schema,
    },
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
    let mut changes = Vec::new();
    let mut results = Vec::with_capacity(statements.len());

    for statement in statements {
        let result = match statement {
            db_query::Statement::Query(query) => {
                execute_query(
                    &mut transaction,
                    engine.reconciler.as_ref(),
                    &mut changes,
                    query,
                )
                .await
            }
            db_query::Statement::DataDefinition(ddl) => {
                execute_ddl(
                    &mut transaction,
                    engine.reconciler.as_ref(),
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

    record_local(&mut transaction, changes).await?;
    transaction.commit().await?;
    Ok(results)
}

async fn ensure_catalog<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    ensure_schema(transaction).await?;
    transaction.ensure_table(ENGINE_ROW_MAPPINGS).await
}

async fn execute_query<T, R>(
    transaction: &mut T,
    reconciler: &R,
    changes: &mut Vec<Change>,
    query: Query,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match query {
        Query::Insert(insert) => insert_row(transaction, reconciler, changes, insert).await,
        Query::Select(select) => select_rows(transaction, reconciler, select).await,
        Query::Update(update) => update_rows(transaction, reconciler, changes, update).await,
        Query::Delete(delete) => delete_rows(transaction, reconciler, changes, delete).await,
    }
}

async fn execute_ddl<T, R>(
    transaction: &mut T,
    reconciler: &R,
    changes: &mut Vec<Change>,
    ddl: DataDefinition,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match &ddl {
        DataDefinition::CreateTable { .. }
        | DataDefinition::DropTable { .. }
        | DataDefinition::AlterTable { .. } => {
            execute_table_ddl(transaction, reconciler, changes, ddl).await
        }
        DataDefinition::CreateIndex { .. }
        | DataDefinition::CreateIndexUnresolved { .. }
        | DataDefinition::DropIndex { .. } => {
            execute_index_ddl(transaction, reconciler, changes, ddl).await
        }
        _ => Err(EngineError::Unsupported(
            "only CREATE TABLE and ALTER TABLE ADD COLUMN are supported",
        )),
    }
}

async fn execute_table_ddl<T, R>(
    transaction: &mut T,
    codec: &R,
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
            if table_generation_id(transaction, &schema.name).await.is_ok() {
                return if if_not_exists {
                    Ok(QueryResult::default())
                } else {
                    Err(EngineError::InvalidQuery("Table already exists"))
                };
            }
            create_table(transaction, codec, changes, schema).await?;
            Ok(QueryResult::default())
        }
        DataDefinition::DropTable {
            table_name,
            if_exists,
        } => {
            let table = match table_generation_id(transaction, &table_name).await {
                Ok(table) => table,
                Err(_) if if_exists => return Ok(QueryResult::default()),
                Err(error) => return Err(error),
            };
            apply_local_change(
                transaction,
                codec,
                changes,
                Change::schema(Uuid::now_v7(), SchemaChange::TombstoneTable(table)),
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
            create_index(transaction, codec, changes, schema).await?;
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
            drop_index(transaction, codec, changes, &index_name, if_exists).await?;
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
    if crate::index::index_generation_id(transaction, name)
        .await
        .is_ok()
        && !if_not_exists
    {
        return Err(EngineError::InvalidQuery("Index already exists"));
    }
    Ok(())
}

async fn create_unresolved_index<T, R>(
    transaction: &mut T,
    codec: &R,
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
    if crate::index::index_generation_id(transaction, &index_name)
        .await
        .is_ok()
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
        changes,
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
    changes: &mut Vec<Change>,
    table_name: String,
    operations: Vec<AlterTableOperation>,
    if_exists: bool,
) -> EngineResult<QueryResult>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = match table_generation_id(transaction, &table_name).await {
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
        add_column(transaction, codec, changes, table, position, &column).await?;
        names.push(column.name);
    }
    Ok(QueryResult::default())
}

async fn drop_index<T, R>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
    name: &str,
    if_exists: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let index = match crate::index::index_generation_id(transaction, name).await {
        Ok(index) => index,
        Err(_) if if_exists => return Ok(()),
        Err(error) => return Err(error),
    };
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(Uuid::now_v7(), SchemaChange::TombstoneIndex(index)),
    )
    .await
}

async fn create_index<T, R>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
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

    let table = table_generation_id(transaction, &schema.table_name).await?;
    let columns = schema_columns(transaction, table).await?;
    let index_columns = schema
        .column_indices
        .iter()
        .map(|position| {
            columns
                .get(*position as usize)
                .map(|(id, _)| *id)
                .ok_or(EngineError::InvalidQuery("Index column is out of range"))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(
            Uuid::now_v7(),
            SchemaChange::CreateIndex {
                index: IndexGenerationId::fresh(),
                label: schema.name,
                table,
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
    changes: &mut Vec<Change>,
    table_schema: TableSchema,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = TableGenerationId::fresh();
    apply_local_change(
        transaction,
        codec,
        changes,
        Change::schema(
            Uuid::now_v7(),
            SchemaChange::CreateTable {
                table,
                label: table_schema.name.clone(),
            },
        ),
    )
    .await?;
    for (position, column) in table_schema.columns.iter().enumerate() {
        add_column(transaction, codec, changes, table, position, column).await?;
    }
    Ok(())
}

async fn add_column<T, R>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
    table: TableGenerationId,
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
            Uuid::now_v7(),
            SchemaChange::AddColumn {
                table,
                column: ColumnGenerationId::fresh(),
                label: column.name.clone(),
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
    changes: &mut Vec<Change>,
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
    let table = table_generation_id(transaction, &insert.table).await?;
    if row_generation_id(transaction, table, &key).await?.is_some() {
        return Err(EngineError::InvalidQuery("Primary key already exists"));
    }
    let row = RowGenerationId::fresh();
    let changed_columns: Vec<_> = (0..insert.row.values.len()).collect();
    let value = reconciler
        .encode_row(transaction, &table, &row, &insert.row, &changed_columns)
        .await?;
    apply_local_change(
        transaction,
        reconciler,
        changes,
        Change::row(Uuid::now_v7(), table, row, key, None, Some(value)),
    )
    .await?;

    Ok(QueryResult::default())
}

async fn update_rows<T, R>(
    transaction: &mut T,
    reconciler: &R,
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

    let table = table_generation_id(transaction, &update.from.table).await?;
    for (old_key, row_id, mut row) in rows {
        for (index, value) in &assignments {
            row.values[*index] = value.clone();
        }
        let key = primary_key(&schema, &row)?;
        let changed_columns: Vec<_> = assignments.iter().map(|(index, _)| *index).collect();
        let value = reconciler
            .encode_row(transaction, &table, &row_id, &row, &changed_columns)
            .await?;
        let previous_key = (key != old_key).then_some(old_key);
        apply_local_change(
            transaction,
            reconciler,
            changes,
            Change::row(
                Uuid::now_v7(),
                table,
                row_id,
                key,
                previous_key,
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

    let table = table_generation_id(transaction, &delete.from.table).await?;
    for (key, row, _) in rows {
        apply_local_change(
            transaction,
            reconciler,
            changes,
            Change::row(Uuid::now_v7(), table, row, key, None, None),
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
    let table = table_generation_id(transaction, table_name).await?;
    let row = row_generation_id(transaction, table, key)
        .await?
        .ok_or(EngineError::InvalidQuery("Row not found"))?;
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
    let table = table_generation_id(transaction, table_name).await?;
    let row_id = row_generation_id(transaction, table, key)
        .await?
        .ok_or(EngineError::InvalidQuery("Row not found"))?;
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
        if changed_columns.contains(&index) {
            return Err(EngineError::InvalidQuery("Resolution column is repeated"));
        }
        row.values[index] = value;
        changed_columns.push(index);
    }
    let new_key = primary_key(&schema, &row)?;
    let previous_key = (new_key != *key).then(|| key.clone());
    let value = reconciler
        .encode_resolution(transaction, &table, &row_id, &row, &changed_columns)
        .await?;
    let mut changes = Vec::new();
    apply_local_change(
        transaction,
        reconciler,
        &mut changes,
        Change::row(
            Uuid::now_v7(),
            table,
            row_id,
            new_key,
            previous_key,
            Some(value),
        ),
    )
    .await?;
    record_local(transaction, changes).await?;
    Ok(())
}

async fn matching_rows<T, R>(
    transaction: &T,
    reconciler: &R,
    table_name: &str,
    schema: &TableSchema,
    predicate: (usize, Value),
) -> EngineResult<Vec<(Row, RowGenerationId, Row)>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let table = table_generation_id(transaction, table_name).await?;
    let stream = reconciler.scan_rows(transaction, &table);
    pin_mut!(stream);
    let mut rows = Vec::new();

    while let Some(item) = stream.next().await {
        let (row_id, row) = item?;
        let row = materialize_defaults(schema, row);
        if row.values.get(predicate.0) == Some(&predicate.1) {
            let key = row_key_for_generation(transaction, table, row_id).await?;
            rows.push((key, row_id, row));
        }
    }

    Ok(rows)
}

pub(crate) async fn row_key_for_generation<T>(
    transaction: &T,
    table: TableGenerationId,
    row: RowGenerationId,
) -> EngineResult<Row>
where
    T: KernelTransaction,
{
    let mappings = transaction.scan_entries(ENGINE_ROW_MAPPINGS);
    pin_mut!(mappings);
    while let Some(entry) = mappings.next().await {
        let (key, value) = entry?;
        if value.values.first().and_then(Value::as_uuid) != Some(&row.0)
            || key.values.first().and_then(Value::as_uuid) != Some(&table.0)
        {
            continue;
        }
        return Ok(Row::new(key.values[1..].to_vec()));
    }
    Err(EngineError::custom(
        "Row generation has no primary-key mapping",
    ))
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
    let table = table_generation_id(transaction, &select.from.table).await?;
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

pub(crate) async fn table_generation_id<T>(
    transaction: &T,
    name: &str,
) -> EngineResult<TableGenerationId>
where
    T: KernelTransaction,
{
    table_id(transaction, name).await
}

pub(crate) async fn column_generation_id<T>(
    transaction: &T,
    table_name: &str,
    column_name: &str,
) -> EngineResult<ColumnGenerationId>
where
    T: KernelTransaction,
{
    column_id(
        transaction,
        table_generation_id(transaction, table_name).await?,
        column_name,
    )
    .await
}

pub(crate) async fn table_schema<T>(transaction: &T, name: &str) -> EngineResult<TableSchema>
where
    T: KernelTransaction,
{
    schema_table_schema(transaction, name).await
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

pub(crate) fn primary_key(schema: &TableSchema, row: &Row) -> EngineResult<Row> {
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
