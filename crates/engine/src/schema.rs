use alloc::{string::String, vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use schema::{ColumnSchema, TableSchema};
use serde::{Deserialize, Serialize};
use value::{Row, Value, ValueType};

use crate::{
    ColumnGenerationId, EngineError, EngineResult, IndexGenerationId, KernelTransaction, RowTable,
    TableGenerationId,
    catalog::{
        ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE, ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_TABLES_STORAGE,
    },
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
        ENGINE_TABLES_STORAGE,
        ENGINE_TABLE_FIELDS_STORAGE,
        ENGINE_INDICES_STORAGE,
        ENGINE_INDEX_FIELDS_STORAGE,
    ] {
        transaction.ensure_table(table).await?;
    }
    Ok(())
}

fn key(id: uuid::Uuid) -> Row {
    Row::new(vec![Value::Uuid(id)])
}

fn deleted(value: &Row, position: usize) -> EngineResult<bool> {
    match value.values.get(position) {
        None => Ok(false),
        Some(value) => value
            .to_bool()
            .ok_or(EngineError::custom("Invalid schema deletion state")),
    }
}

async fn table_deleted<T>(transaction: &T, id: uuid::Uuid) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let Some(value) = transaction
        .get_entry(ENGINE_TABLES_STORAGE, &key(id))
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 1)
}

async fn column_deleted<T>(transaction: &T, id: uuid::Uuid) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let Some(value) = transaction
        .get_entry(ENGINE_TABLE_FIELDS_STORAGE, &key(id))
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 6)
}

pub(crate) async fn index_field_deleted<T>(
    transaction: &T,
    index: uuid::Uuid,
    position: i64,
) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let key = Row::new(vec![Value::Uuid(index), Value::Integer(position)]);
    let Some(value) = transaction
        .get_entry(ENGINE_INDEX_FIELDS_STORAGE, &key)
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 1)
}

pub(crate) async fn index_deleted<T>(transaction: &T, id: uuid::Uuid) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let Some(value) = transaction
        .get_entry(ENGINE_INDICES_STORAGE, &key(id))
        .await?
    else {
        return Ok(false);
    };
    deleted(&value, 3)
}

pub(crate) async fn materialize<T>(transaction: &mut T, change: &SchemaChange) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    match change {
        SchemaChange::CreateTable { table, label } => {
            let key = key(table.0);
            let value = Row::new(vec![Value::from(label.as_str()), Value::Bool(false)]);
            match transaction.get_entry(ENGINE_TABLES_STORAGE, &key).await? {
                Some(existing)
                    if existing.values.first() != value.values.first()
                        || table_deleted(transaction, table.0).await? =>
                {
                    if existing.values.first() != value.values.first() {
                        Err(EngineError::custom("Conflicting table generation fact"))
                    } else {
                        Ok(false)
                    }
                }
                Some(_) => Ok(false),
                None => transaction
                    .put_entry(ENGINE_TABLES_STORAGE, key, value)
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
                Value::Bool(false),
            ]);
            match transaction
                .get_entry(ENGINE_TABLE_FIELDS_STORAGE, &key)
                .await?
            {
                Some(existing)
                    if existing.values.len() < 6
                        || existing.values[..6] != value.values[..6]
                        || column_deleted(transaction, column.0).await? =>
                {
                    if existing.values.len() < 6 || existing.values[..6] != value.values[..6] {
                        Err(EngineError::custom("Conflicting column generation fact"))
                    } else {
                        Ok(false)
                    }
                }
                Some(_) => Ok(false),
                None => transaction
                    .put_entry(ENGINE_TABLE_FIELDS_STORAGE, key, value)
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
            let value = Row::new(vec![
                Value::from(label.as_str()),
                Value::Uuid(table.0),
                Value::Bool(*unique),
                Value::Bool(false),
            ]);
            match transaction.get_entry(ENGINE_INDICES_STORAGE, &key).await? {
                Some(existing) if existing != value => {
                    Err(EngineError::custom("Conflicting index generation fact"))
                }
                Some(_) => Ok(false),
                None => {
                    transaction.ensure_table(index.0).await?;
                    transaction
                        .put_entry(ENGINE_INDICES_STORAGE, key, value)
                        .await?;
                    for (position, column) in columns.iter().enumerate() {
                        transaction
                            .put_entry(
                                ENGINE_INDEX_FIELDS_STORAGE,
                                Row::new(vec![
                                    Value::Uuid(index.0),
                                    Value::Integer(i64::try_from(position).map_err(|_| {
                                        EngineError::custom("Index column position overflow")
                                    })?),
                                ]),
                                Row::new(vec![Value::Uuid(column.0), Value::Bool(false)]),
                            )
                            .await?;
                    }
                    Ok(false)
                }
            }
        }
        SchemaChange::TombstoneTable(table) => {
            update_deleted(transaction, ENGINE_TABLES_STORAGE, table.0, 1).await
        }
        SchemaChange::TombstoneColumn(column) => {
            update_deleted(transaction, ENGINE_TABLE_FIELDS_STORAGE, column.0, 6).await
        }
        SchemaChange::TombstoneIndex(index) => {
            update_deleted(transaction, ENGINE_INDICES_STORAGE, index.0, 3).await?;
            let keys = {
                let fields = transaction.scan_entries_owned(ENGINE_INDEX_FIELDS_STORAGE);
                pin_mut!(fields);
                let mut keys = Vec::new();
                while let Some(entry) = fields.next().await {
                    let (key, _) = entry?;
                    if key.values.first().and_then(Value::as_uuid) == Some(&index.0) {
                        keys.push(key);
                    }
                }
                keys
            };
            for key in keys {
                let mut value = transaction
                    .get_entry(ENGINE_INDEX_FIELDS_STORAGE, &key)
                    .await?
                    .ok_or(EngineError::InvalidQuery("Index field not found"))?;
                if value.values.len() <= 1 {
                    value.values.resize(2, Value::Bool(false));
                }
                value.values[1] = Value::Bool(true);
                transaction
                    .put_entry(ENGINE_INDEX_FIELDS_STORAGE, key, value)
                    .await?;
            }
            transaction.drop_table(index.0).await?;
            Ok(false)
        }
    }
}

async fn update_deleted<T>(
    transaction: &mut T,
    table: uuid::Uuid,
    id: uuid::Uuid,
    position: usize,
) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let key = key(id);
    let mut value = transaction
        .get_entry(table, &key)
        .await?
        .ok_or(EngineError::InvalidQuery("Schema generation not found"))?;
    if deleted(&value, position)? {
        return Ok(false);
    }
    if value.values.len() <= position {
        value.values.resize(position + 1, Value::Bool(false));
    }
    value.values[position] = Value::Bool(true);
    transaction
        .put_entry(table, key, value)
        .await
        .map(|_| false)
}

pub(crate) async fn table_id<T>(transaction: &T, label: &str) -> EngineResult<TableGenerationId>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_TABLES_STORAGE);
    pin_mut!(entries);
    let mut winner = None;
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key.values.first().and_then(Value::as_uuid).copied() else {
            return Err(EngineError::custom("Invalid table generation"));
        };
        if value.values.first().and_then(Value::as_text) == Some(label)
            && !table_deleted(transaction, id).await?
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
    if table_deleted(transaction, table.0).await? {
        return Err(EngineError::InvalidQuery("Table not found"));
    }
    let entries = transaction.scan_entries(ENGINE_TABLE_FIELDS_STORAGE);
    pin_mut!(entries);
    let mut all = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key.values.first().and_then(Value::as_uuid).copied() else {
            return Err(EngineError::custom("Invalid column generation"));
        };
        if value.values.first().and_then(Value::as_uuid) != Some(&table.0)
            || column_deleted(transaction, id).await?
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
        .get_entry(ENGINE_TABLES_STORAGE, &key(table.0))
        .await?
        .ok_or(EngineError::InvalidQuery("Table not found"))?;
    if table_deleted(transaction, table.0).await? {
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

pub(crate) async fn active_table_ids<T>(transaction: &T) -> EngineResult<Vec<TableGenerationId>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_TABLES_STORAGE);
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
        if !table_deleted(transaction, id).await? {
            tables.push(TableGenerationId(id));
        }
    }
    Ok(tables)
}
