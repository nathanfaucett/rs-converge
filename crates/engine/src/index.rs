use alloc::{vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use schema::{ColumnSchemaIndex, IndexSchema};
use uuid::Uuid;
use value::{Row, Value};

use crate::{
    ColumnGenerationId, EngineError, EngineResult, IndexGenerationId, KernelTransaction, RowCodec,
    RowTable, TableGenerationId,
    catalog::INTERNAL_INDICES,
    catalog::internal_table_storage_uuid,
    executor::{materialize_defaults, row_id},
    schema::{columns, index_deleted, table_label, table_schema_for},
};

async fn index<T>(transaction: &T, name: &str) -> EngineResult<Option<(IndexGenerationId, Row)>>
where
    T: KernelTransaction,
{
    let storage = internal_table_storage_uuid(INTERNAL_INDICES).ok_or(EngineError::custom(
        "Internal index table is not configured",
    ))?;
    let entries = transaction.scan_entries_owned(storage);
    pin_mut!(entries);
    let mut winner = None;
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key.values.first().and_then(Value::as_uuid).copied() else {
            return Err(EngineError::custom("Invalid index generation"));
        };
        if value.values.first().and_then(Value::as_text) != Some(name) {
            continue;
        }
        if index_deleted(transaction, id).await? {
            continue;
        }
        if winner
            .as_ref()
            .is_none_or(|(current, _): &(uuid::Uuid, Row)| id < *current)
        {
            winner = Some((id, value));
        }
    }
    Ok(winner.map(|(id, value)| (IndexGenerationId(id), value)))
}

pub(crate) async fn index_generation_id<T>(
    transaction: &T,
    name: &str,
) -> EngineResult<IndexGenerationId>
where
    T: KernelTransaction,
{
    index(transaction, name)
        .await?
        .map(|(id, _)| id)
        .ok_or(EngineError::InvalidQuery("Index not found"))
}

async fn schema_for<T>(
    transaction: &T,
    id: IndexGenerationId,
    value: Row,
) -> EngineResult<IndexSchema>
where
    T: KernelTransaction,
{
    let label = value
        .values
        .first()
        .and_then(Value::to_text)
        .ok_or(EngineError::custom("Invalid index label"))?;
    let table = value
        .values
        .get(1)
        .and_then(Value::as_uuid)
        .copied()
        .map(TableGenerationId)
        .ok_or(EngineError::custom("Invalid index table"))?;
    let unique = value
        .values
        .get(2)
        .and_then(Value::to_bool)
        .ok_or(EngineError::custom("Invalid index uniqueness"))?;
    let columns = columns(transaction, table).await?;
    let mut positions = Vec::new();
    let end = if value.values.last().and_then(Value::to_bool).is_some() {
        value.values.len() - 1
    } else {
        value.values.len()
    };
    for column in value.values[3..end].iter() {
        let column = column
            .as_uuid()
            .copied()
            .map(ColumnGenerationId)
            .ok_or(EngineError::custom("Invalid index column"))?;
        positions.push(
            columns
                .iter()
                .position(|(id, _)| *id == column)
                .ok_or(EngineError::InvalidQuery("Index column not found"))?
                as ColumnSchemaIndex,
        );
    }
    let _ = id;
    Ok(IndexSchema {
        name: label,
        table_name: table_label(transaction, table).await?,
        column_indices: positions,
        unique,
    })
}

pub(crate) async fn index_schema<T>(
    transaction: &T,
    name: &str,
) -> EngineResult<Option<IndexSchema>>
where
    T: KernelTransaction,
{
    let Some((id, value)) = index(transaction, name).await? else {
        return Ok(None);
    };
    schema_for(transaction, id, value).await.map(Some)
}

async fn indexes_for_table<T>(
    transaction: &T,
    table: TableGenerationId,
) -> EngineResult<Vec<(IndexGenerationId, IndexSchema)>>
where
    T: KernelTransaction,
{
    let storage = internal_table_storage_uuid(INTERNAL_INDICES).ok_or(EngineError::custom(
        "Internal index table is not configured",
    ))?;
    let entries = transaction.scan_entries_owned(storage);
    pin_mut!(entries);
    let mut facts = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        let Some(id) = key.values.first().and_then(Value::as_uuid).copied() else {
            continue;
        };
        facts.push((IndexGenerationId(id), value));
    }

    let mut visible = Vec::new();
    for (id, value) in facts {
        if !index_deleted(transaction, id.0).await? {
            visible.push((id, value));
        }
    }

    let mut result = Vec::new();
    for (id, value) in &visible {
        if value.values.get(1).and_then(Value::as_uuid) != Some(&table.0) {
            continue;
        }
        let label = value
            .values
            .first()
            .and_then(Value::as_text)
            .ok_or(EngineError::custom("Invalid index label"))?;
        let winner = visible
            .iter()
            .filter(|(_, candidate)| {
                candidate.values.first().and_then(Value::as_text) == Some(label)
            })
            .map(|(candidate, _)| *candidate)
            .min()
            .ok_or(EngineError::custom("Missing index generation"))?;
        if *id != winner {
            continue;
        }
        result.push((*id, schema_for(transaction, *id, value.clone()).await?));
    }
    Ok(result)
}

fn record_key(
    id: IndexGenerationId,
    schema: &IndexSchema,
    row_id: uuid::Uuid,
    row: &Row,
) -> EngineResult<Row> {
    let mut values = Vec::with_capacity(schema.column_indices.len() + 2);
    values.push(Value::Uuid(id.0));
    for column in &schema.column_indices {
        values.push(
            row.values
                .get(*column as usize)
                .cloned()
                .ok_or(EngineError::InvalidQuery(
                    "Index column is missing from row",
                ))?,
        );
    }
    values.push(Value::Uuid(row_id));
    Ok(Row::new(values))
}

pub(crate) async fn lookup<T, R>(
    transaction: &T,
    codec: &R,
    name: &str,
    values: &Row,
) -> EngineResult<Option<Row>>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let Some((id, value)) = index(transaction, name).await? else {
        return Err(EngineError::InvalidQuery("Index not found"));
    };
    let schema = schema_for(transaction, id, value).await?;
    if values.values.len() != schema.column_indices.len() {
        return Err(EngineError::InvalidQuery(
            "Index key has the wrong column count",
        ));
    }
    let storage = id.0;
    let entries = transaction.scan_entries_owned(storage);
    pin_mut!(entries);
    let mut winner = None;
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        if key.values.get(1..=values.values.len()) != Some(values.values.as_slice()) {
            continue;
        }
        winner = Some(winner.map_or(value.clone(), |current: Row| current.min(value)));
    }
    let Some(row) = winner.and_then(|row| row.values.first().and_then(Value::as_uuid).copied())
    else {
        return Ok(None);
    };
    let table = table_id_from_index(&schema, transaction).await?;
    codec.get_row(transaction, table.0, &row).await
}

async fn table_id_from_index<T>(
    schema: &IndexSchema,
    transaction: &T,
) -> EngineResult<TableGenerationId>
where
    T: KernelTransaction,
{
    crate::schema::table_id(transaction, &schema.table_name).await
}

pub(crate) async fn rebuild_table<T, R>(
    transaction: &mut T,
    codec: &R,
    table: TableGenerationId,
    enforce_unique: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let indexes = indexes_for_table(transaction, table).await?;
    if indexes.is_empty() {
        return Ok(());
    }
    for (id, _) in &indexes {
        let storage = id.0;
        transaction.ensure_table(storage).await?;
        let stale = {
            let entries = transaction.scan_entries_owned(storage);
            pin_mut!(entries);
            let mut stale = Vec::new();
            while let Some(entry) = entries.next().await {
                let (key, _) = entry?;
                stale.push(key);
            }
            stale
        };
        for key in stale {
            transaction.remove_entry(storage, &key).await?;
        }
    }

    let schema =
        table_schema_for(transaction, table, table_label(transaction, table).await?).await?;
    let rows_to_index = {
        let rows = codec.scan_rows(transaction, table.0);
        pin_mut!(rows);
        let mut rows_to_index = Vec::new();
        while let Some(row) = rows.next().await {
            rows_to_index.push(row?);
        }
        rows_to_index
    };
    for (_, row) in rows_to_index {
        let row = materialize_defaults(&schema, row);
        update_row(transaction, table, None, Some(&row), enforce_unique).await?;
    }
    Ok(())
}

pub(crate) async fn update_row<T>(
    transaction: &mut T,
    table: TableGenerationId,
    old: Option<&Row>,
    new: Option<&Row>,
    enforce_unique: bool,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    let schema =
        table_schema_for(transaction, table, table_label(transaction, table).await?).await?;
    for (id, index) in indexes_for_table(transaction, table).await? {
        let storage = id.0;
        transaction.ensure_table(storage).await?;
        if let Some(row) = old {
            transaction
                .remove_entry(
                    storage,
                    &record_key(
                        id,
                        &index,
                        row_id(&schema, row)?,
                        &materialize_defaults(&schema, row.clone()),
                    )?,
                )
                .await?;
        }
        if let Some(row) = new {
            let row = materialize_defaults(&schema, row.clone());
            let row_id = row_id(&schema, &row)?;
            let key = record_key(id, &index, row_id, &row)?;
            if enforce_unique
                && index.unique
                && !key.values[1..=index.column_indices.len()]
                    .iter()
                    .any(|value| matches!(value, Value::Null))
                && unique_key_exists(transaction, storage, &key).await?
            {
                return Err(EngineError::InvalidQuery("Unique index violation"));
            }
            transaction
                .put_entry(storage, key, Row::new(vec![Value::Uuid(row_id)]))
                .await?;
        }
    }
    Ok(())
}

async fn unique_key_exists<T>(transaction: &T, storage: Uuid, key: &Row) -> EngineResult<bool>
where
    T: KernelTransaction,
{
    let values = &key.values[0..key.values.len() - 1];
    let entries = transaction.scan_entries(storage);
    pin_mut!(entries);
    while let Some(entry) = entries.next().await {
        let (candidate, _) = entry?;
        if candidate.values.len() == key.values.len()
            && candidate.values[..candidate.values.len() - 1] == *values
            && candidate.values.last() != key.values.last()
        {
            return Ok(true);
        }
    }
    Ok(false)
}
