use alloc::{string::String, vec, vec::Vec};

use db_schema::{ColumnSchema, TableSchema};
use db_value::{Row, Value, ValueType};
use futures::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};

use crate::{
    ColumnGenerationId, EngineError, EngineResult, IndexGenerationId, KernelTransaction,
    TableGenerationId,
    catalog::{ENGINE_SCHEMA_TOMBSTONES, ENGINE_TABLE_FIELDS, ENGINE_TABLES},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SchemaChange {
    CreateTable {
        table: TableGenerationId,
        label: String,
    },
    AddColumn {
        table: TableGenerationId,
        column: ColumnGenerationId,
        label: String,
        value_type: ValueType,
        default: Value,
        position: u32,
        primary_key: bool,
    },
    CreateIndex {
        index: IndexGenerationId,
        label: String,
        table: TableGenerationId,
        unique: bool,
        columns: Vec<ColumnGenerationId>,
    },
    TombstoneTable(TableGenerationId),
    TombstoneColumn(ColumnGenerationId),
    TombstoneIndex(IndexGenerationId),
}

pub(crate) async fn ensure<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    for table in [
        ENGINE_TABLES,
        ENGINE_TABLE_FIELDS,
        "indices",
        ENGINE_SCHEMA_TOMBSTONES,
    ] {
        transaction.ensure_table(table).await?;
    }
    Ok(())
}

fn key(id: uuid::Uuid) -> Row {
    Row::new(vec![Value::Uuid(id)])
}

fn tombstone_key(kind: &str, id: uuid::Uuid) -> Row {
    Row::new(vec![Value::from(kind), Value::Uuid(id)])
}

async fn tombstoned<T>(transaction: &T, kind: &str, id: uuid::Uuid) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    Ok(transaction
        .get_entry(ENGINE_SCHEMA_TOMBSTONES, &tombstone_key(kind, id))
        .await?
        .is_some())
}

pub(crate) async fn materialize<T>(transaction: &mut T, change: &SchemaChange) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    match change {
        SchemaChange::CreateTable { table, label } => {
            let key = key(table.0);
            let value = Row::new(vec![Value::from(label.as_str())]);
            match transaction.get_entry(ENGINE_TABLES, &key).await? {
                Some(existing) if existing != value => {
                    Err(EngineError::custom("Conflicting table generation fact"))
                }
                Some(_) => Ok(false),
                None => transaction
                    .put_entry(ENGINE_TABLES, key, value)
                    .await
                    .map(|_| false),
            }
        }
        SchemaChange::AddColumn {
            table,
            column,
            label,
            value_type,
            default,
            position,
            primary_key,
        } => {
            let key = key(column.0);
            let value = Row::new(vec![
                Value::Uuid(table.0),
                Value::from(label.as_str()),
                (*value_type).into(),
                default.clone(),
                Value::Integer(i64::from(*position)),
                Value::Bool(*primary_key),
            ]);
            match transaction.get_entry(ENGINE_TABLE_FIELDS, &key).await? {
                Some(existing) if existing != value => {
                    Err(EngineError::custom("Conflicting column generation fact"))
                }
                Some(_) => Ok(false),
                None => transaction
                    .put_entry(ENGINE_TABLE_FIELDS, key, value)
                    .await
                    .map(|_| false),
            }
        }
        SchemaChange::CreateIndex {
            index,
            label,
            table,
            unique,
            columns,
        } => {
            let key = key(index.0);
            let mut value = vec![
                Value::from(label.as_str()),
                Value::Uuid(table.0),
                Value::Bool(*unique),
            ];
            value.extend(columns.iter().map(|column| Value::Uuid(column.0)));
            let value = Row::new(value);
            match transaction.get_entry("indices", &key).await? {
                Some(existing) if existing != value => {
                    Err(EngineError::custom("Conflicting index generation fact"))
                }
                Some(_) => Ok(false),
                None => transaction
                    .put_entry("indices", key, value)
                    .await
                    .map(|_| false),
            }
        }
        SchemaChange::TombstoneTable(table) => tombstone(transaction, "table", table.0).await,
        SchemaChange::TombstoneColumn(column) => tombstone(transaction, "column", column.0).await,
        SchemaChange::TombstoneIndex(index) => tombstone(transaction, "index", index.0).await,
    }
}

async fn tombstone<T>(transaction: &mut T, kind: &str, id: uuid::Uuid) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let key = tombstone_key(kind, id);
    if transaction
        .get_entry(ENGINE_SCHEMA_TOMBSTONES, &key)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    transaction
        .put_entry(ENGINE_SCHEMA_TOMBSTONES, key, Row::default())
        .await?;
    Ok(false)
}

pub(crate) async fn table_id<T>(transaction: &T, label: &str) -> EngineResult<TableGenerationId>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_TABLES);
    pin_mut!(entries);
    let mut winner = None;
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key.values.first().and_then(Value::as_uuid).copied() else {
            return Err(EngineError::custom("Invalid table generation"));
        };
        if value.values.first().and_then(Value::as_text) == Some(label)
            && !tombstoned(transaction, "table", id).await?
        {
            winner = Some(winner.map_or(id, |current: uuid::Uuid| current.min(id)));
        }
    }
    winner
        .map(TableGenerationId)
        .ok_or(EngineError::InvalidQuery("Table not found"))
}

pub(crate) async fn columns<T>(
    transaction: &T,
    table: TableGenerationId,
) -> EngineResult<Vec<(ColumnGenerationId, ColumnSchema)>>
where
    T: KernelTransaction,
{
    if tombstoned(transaction, "table", table.0).await? {
        return Err(EngineError::InvalidQuery("Table not found"));
    }
    let entries = transaction.scan_entries(ENGINE_TABLE_FIELDS);
    pin_mut!(entries);
    let mut all = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key.values.first().and_then(Value::as_uuid).copied() else {
            return Err(EngineError::custom("Invalid column generation"));
        };
        if value.values.first().and_then(Value::as_uuid) != Some(&table.0)
            || tombstoned(transaction, "column", id).await?
        {
            continue;
        }
        let label = value
            .values
            .get(1)
            .and_then(Value::to_text)
            .ok_or(EngineError::custom("Invalid column label"))?;
        let value_type = value
            .values
            .get(2)
            .and_then(Value::to_type)
            .ok_or(EngineError::custom("Invalid column type"))?;
        let default = value
            .values
            .get(3)
            .cloned()
            .ok_or(EngineError::custom("Invalid column default"))?;
        let position = value
            .values
            .get(4)
            .and_then(Value::to_integer)
            .ok_or(EngineError::custom("Invalid column position"))?;
        let primary_key = value
            .values
            .get(5)
            .and_then(Value::to_bool)
            .ok_or(EngineError::custom("Invalid column primary key"))?;
        all.push((
            position,
            id,
            label,
            ColumnSchema {
                name: String::new(),
                r#type: value_type,
                default,
                primary_key,
            },
        ));
    }
    all.sort_by_key(|(_, id, _, _)| *id);
    let mut winners = Vec::new();
    for field in all {
        if winners.iter().any(|(_, _, label, _)| *label == field.2) {
            continue;
        }
        winners.push(field);
    }
    winners.sort_by_key(|(position, id, _, _)| (*position, *id));
    Ok(winners
        .into_iter()
        .map(|(_, id, label, mut column)| {
            column.name = label;
            (ColumnGenerationId(id), column)
        })
        .collect())
}

pub(crate) async fn table_schema<T>(transaction: &T, label: &str) -> EngineResult<TableSchema>
where
    T: KernelTransaction,
{
    let table = table_id(transaction, label).await?;
    table_schema_for(transaction, table, String::from(label)).await
}

pub(crate) async fn table_label<T>(
    transaction: &T,
    table: TableGenerationId,
) -> EngineResult<String>
where
    T: KernelTransaction,
{
    let value = transaction
        .get_entry(ENGINE_TABLES, &key(table.0))
        .await?
        .ok_or(EngineError::InvalidQuery("Table not found"))?;
    if tombstoned(transaction, "table", table.0).await? {
        return Err(EngineError::InvalidQuery("Table not found"));
    }
    value
        .values
        .first()
        .and_then(Value::to_text)
        .ok_or(EngineError::custom("Invalid table label"))
}

pub(crate) async fn table_schema_for<T>(
    transaction: &T,
    table: TableGenerationId,
    name: String,
) -> EngineResult<TableSchema>
where
    T: KernelTransaction,
{
    Ok(TableSchema {
        name,
        columns: columns(transaction, table)
            .await?
            .into_iter()
            .map(|(_, column)| column)
            .collect(),
    })
}

pub(crate) async fn column_id<T>(
    transaction: &T,
    table: TableGenerationId,
    label: &str,
) -> EngineResult<ColumnGenerationId>
where
    T: KernelTransaction,
{
    columns(transaction, table)
        .await?
        .into_iter()
        .find_map(|(id, column)| (column.name == label).then_some(id))
        .ok_or(EngineError::InvalidQuery("Column not found"))
}

pub(crate) async fn checkpoint_changes<T>(transaction: &T) -> EngineResult<Vec<SchemaChange>>
where
    T: KernelTransaction,
{
    let mut changes = checkpoint_tables(transaction).await?;
    changes.extend(checkpoint_columns(transaction).await?);
    changes.extend(checkpoint_indexes(transaction).await?);
    changes.extend(checkpoint_tombstones(transaction).await?);
    Ok(changes)
}

async fn checkpoint_tables<T>(transaction: &T) -> EngineResult<Vec<SchemaChange>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_TABLES);
    pin_mut!(entries);
    let mut changes = Vec::new();
    while let Some(entry) = entries.next().await {
        changes.push(table_change(entry?)?);
    }
    Ok(changes)
}

fn table_change((key, value): (Row, Row)) -> EngineResult<SchemaChange> {
    let id = key
        .values
        .first()
        .and_then(Value::as_uuid)
        .copied()
        .ok_or(EngineError::custom("Invalid table generation"))?;
    let label = value
        .values
        .first()
        .and_then(Value::to_text)
        .ok_or(EngineError::custom("Invalid table label"))?;
    Ok(SchemaChange::CreateTable {
        table: TableGenerationId(id),
        label,
    })
}

async fn checkpoint_columns<T>(transaction: &T) -> EngineResult<Vec<SchemaChange>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_TABLE_FIELDS);
    pin_mut!(entries);
    let mut changes = Vec::new();
    while let Some(entry) = entries.next().await {
        changes.push(column_change(entry?)?);
    }
    Ok(changes)
}

fn column_change((key, value): (Row, Row)) -> EngineResult<SchemaChange> {
    let column = key
        .values
        .first()
        .and_then(Value::as_uuid)
        .copied()
        .ok_or(EngineError::custom("Invalid column generation"))?;
    let (
        Some(table),
        Some(label),
        Some(value_type),
        Some(default),
        Some(position),
        Some(primary_key),
    ) = (
        value.values.first().and_then(Value::as_uuid).copied(),
        value.values.get(1).and_then(Value::to_text),
        value.values.get(2).and_then(Value::to_type),
        value.values.get(3).cloned(),
        value.values.get(4).and_then(Value::to_integer),
        value.values.get(5).and_then(Value::to_bool),
    )
    else {
        return Err(EngineError::custom("Invalid column generation fact"));
    };
    Ok(SchemaChange::AddColumn {
        table: TableGenerationId(table),
        column: ColumnGenerationId(column),
        label,
        value_type,
        default,
        position: u32::try_from(position)
            .map_err(|_| EngineError::custom("Invalid column position"))?,
        primary_key,
    })
}

async fn checkpoint_indexes<T>(transaction: &T) -> EngineResult<Vec<SchemaChange>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries("indices");
    pin_mut!(entries);
    let mut changes = Vec::new();
    while let Some(entry) = entries.next().await {
        changes.push(index_change(entry?)?);
    }
    Ok(changes)
}

fn index_change((key, value): (Row, Row)) -> EngineResult<SchemaChange> {
    let index = key
        .values
        .first()
        .and_then(Value::as_uuid)
        .copied()
        .ok_or(EngineError::custom("Invalid index generation"))?;
    let (Some(label), Some(table), Some(unique)) = (
        value.values.first().and_then(Value::to_text),
        value.values.get(1).and_then(Value::as_uuid).copied(),
        value.values.get(2).and_then(Value::to_bool),
    ) else {
        return Err(EngineError::custom("Invalid index generation fact"));
    };
    let columns = value.values[3..]
        .iter()
        .map(|value| {
            value
                .as_uuid()
                .copied()
                .map(ColumnGenerationId)
                .ok_or(EngineError::custom("Invalid index column generation"))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    Ok(SchemaChange::CreateIndex {
        index: IndexGenerationId(index),
        label,
        table: TableGenerationId(table),
        unique,
        columns,
    })
}

async fn checkpoint_tombstones<T>(transaction: &T) -> EngineResult<Vec<SchemaChange>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_SCHEMA_TOMBSTONES);
    pin_mut!(entries);
    let mut changes = Vec::new();
    while let Some(entry) = entries.next().await {
        changes.push(tombstone_change(entry?.0)?);
    }
    Ok(changes)
}

fn tombstone_change(key: Row) -> EngineResult<SchemaChange> {
    let (Some(kind), Some(id)) = (
        key.values.first().and_then(Value::as_text),
        key.values.get(1).and_then(Value::as_uuid).copied(),
    ) else {
        return Err(EngineError::custom("Invalid schema tombstone"));
    };
    match kind {
        "table" => Ok(SchemaChange::TombstoneTable(TableGenerationId(id))),
        "column" => Ok(SchemaChange::TombstoneColumn(ColumnGenerationId(id))),
        "index" => Ok(SchemaChange::TombstoneIndex(IndexGenerationId(id))),
        _ => Err(EngineError::custom("Invalid schema tombstone")),
    }
}

pub(crate) async fn active_table_ids<T>(transaction: &T) -> EngineResult<Vec<TableGenerationId>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_TABLES);
    pin_mut!(entries);
    let mut tables = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, _) = entry?;
        let id = key
            .values
            .first()
            .and_then(Value::as_uuid)
            .copied()
            .ok_or(EngineError::custom("Invalid table generation"))?;
        if !tombstoned(transaction, "table", id).await? {
            tables.push(TableGenerationId(id));
        }
    }
    Ok(tables)
}
