use alloc::{string::String, vec, vec::Vec};

use futures::{StreamExt, pin_mut};
use schema::{ColumnSchemaIndex, IndexSchema};
use value::{Row, Value};

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec, RowTable,
    catalog::{ENGINE_INDEX_FIELDS_STORAGE, ENGINE_INDICES_STORAGE},
    executor::{materialize_defaults, row_id},
    schema::{columns, index_deleted, index_field_deleted, table_schema_for},
};

async fn index<T: KernelTransaction>(
    transaction: &T,
    name: &str,
) -> EngineResult<Option<(String, Row)>> {
    let value = transaction
        .get_entry(ENGINE_INDICES_STORAGE, &Row::new(vec![Value::from(name)]))
        .await?;
    let Some(value) = value else { return Ok(None) };
    if index_deleted(transaction, name).await? {
        return Ok(None);
    }
    Ok(Some((name.into(), value)))
}

pub(crate) async fn index_schema<T: KernelTransaction>(
    transaction: &T,
    name: &str,
) -> EngineResult<Option<IndexSchema>> {
    let Some((_, value)) = index(transaction, name).await? else {
        return Ok(None);
    };
    let table = value
        .values
        .first()
        .and_then(Value::to_text)
        .ok_or(EngineError::custom("Invalid index table"))?;
    let unique = value
        .values
        .get(1)
        .and_then(Value::to_bool)
        .ok_or(EngineError::custom("Invalid index uniqueness"))?;
    let mut fields = Vec::new();
    let entries = transaction.scan_entries_owned(ENGINE_INDEX_FIELDS_STORAGE);
    pin_mut!(entries);
    while let Some(entry) = entries.next().await {
        let (key, field) = entry?;
        if key.values.first().and_then(Value::as_text) != Some(name) {
            continue;
        }
        let position = key
            .values
            .get(1)
            .and_then(Value::to_integer)
            .ok_or(EngineError::custom("Invalid index field position"))?;
        if index_field_deleted(transaction, name, position).await? {
            continue;
        }
        let column = field
            .values
            .first()
            .and_then(Value::to_text)
            .ok_or(EngineError::custom("Invalid index column"))?;
        fields.push((position, column));
    }
    fields.sort_by_key(|(position, _)| *position);
    let schema_columns = columns(transaction, &table).await?;
    let column_indices = fields
        .into_iter()
        .map(|(_, column)| {
            schema_columns
                .iter()
                .position(|(name, _)| *name == column)
                .map(|position| position as ColumnSchemaIndex)
                .ok_or(EngineError::InvalidQuery("Index column not found"))
        })
        .collect::<EngineResult<Vec<_>>>()?;
    Ok(Some(IndexSchema {
        name: name.into(),
        table_name: table.into(),
        column_indices,
        unique,
    }))
}

async fn indexes_for_table<T: KernelTransaction>(
    transaction: &T,
    table: &str,
) -> EngineResult<Vec<(String, IndexSchema)>> {
    let entries = transaction.scan_entries_owned(ENGINE_INDICES_STORAGE);
    pin_mut!(entries);
    let mut names: Vec<String> = Vec::new();
    while let Some(entry) = entries.next().await {
        let (key, value) = entry?;
        if value.values.first().and_then(Value::as_text) == Some(table) {
            if let Some(name) = key.values.first().and_then(Value::to_text) {
                if !index_deleted(transaction, &name).await? {
                    names.push(name.into());
                }
            }
        }
    }
    let mut result = Vec::new();
    for name in names {
        if let Some(schema) = index_schema(transaction, &name).await? {
            result.push((name, schema));
        }
    }
    Ok(result)
}

fn record_key(schema: &IndexSchema, row_id: uuid::Uuid, row: &Row) -> EngineResult<Row> {
    let mut values = Vec::with_capacity(schema.column_indices.len() + 2);
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

pub(crate) async fn lookup<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &T,
    codec: &R,
    name: &str,
    values: &Row,
) -> EngineResult<Option<Row>> {
    let schema = index_schema(transaction, name)
        .await?
        .ok_or(EngineError::InvalidQuery("Index not found"))?;
    if values.values.len() != schema.column_indices.len() {
        return Err(EngineError::InvalidQuery(
            "Index key has the wrong column count",
        ));
    }
    let entries = transaction.scan_entries_owned(name);
    pin_mut!(entries);
    let mut winner = None;
    while let Some(entry) = entries.next().await {
        let (key, _) = entry?;
        if key.values.get(..values.values.len()) == Some(values.values.as_slice()) {
            winner = key.values.last().and_then(Value::as_uuid).copied();
            break;
        }
    }
    match winner {
        Some(row) => codec.get_row(transaction, &schema.table_name, &row).await,
        None => Ok(None),
    }
}

pub(crate) async fn rebuild_table<T: KernelTransaction, R: RowCodec<T>>(
    transaction: &mut T,
    codec: &R,
    table: &str,
    enforce_unique: bool,
) -> EngineResult<()> {
    let indexes = indexes_for_table(transaction, table).await?;
    for (name, _) in &indexes {
        transaction.ensure_table(name).await?;
        let stale = {
            let entries = transaction.scan_entries_owned(name);
            pin_mut!(entries);
            let mut keys = Vec::new();
            while let Some(entry) = entries.next().await {
                keys.push(entry?.0);
            }
            keys
        };
        for key in stale {
            transaction.remove_entry(name, &key).await?;
        }
    }
    let schema = table_schema_for(transaction, table, table.into()).await?;
    let rows = {
        let stream = codec.scan_rows(transaction, table);
        pin_mut!(stream);
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row?);
        }
        rows
    };
    for (_, row) in rows {
        update_row(
            transaction,
            table,
            None,
            Some(&materialize_defaults(&schema, row)),
            enforce_unique,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn update_row<T: KernelTransaction>(
    transaction: &mut T,
    table: &str,
    old: Option<&Row>,
    new: Option<&Row>,
    enforce_unique: bool,
) -> EngineResult<()> {
    let schema = table_schema_for(transaction, table, table.into()).await?;
    for (name, index) in indexes_for_table(transaction, table).await? {
        transaction.ensure_table(&name).await?;
        if let Some(row) = old {
            transaction
                .remove_entry(
                    &name,
                    &record_key(
                        &index,
                        row_id(&schema, row)?,
                        &materialize_defaults(&schema, row.clone()),
                    )?,
                )
                .await?;
        }
        if let Some(row) = new {
            let row = materialize_defaults(&schema, row.clone());
            let key = record_key(&index, row_id(&schema, &row)?, &row)?;
            if enforce_unique
                && index.unique
                && !key.values.iter().any(|value| matches!(value, Value::Null))
                && unique_key_exists(transaction, &name, &key).await?
            {
                return Err(EngineError::InvalidQuery("Unique index violation"));
            }
            transaction.put_entry(&name, key, Row::new(vec![])).await?;
        }
    }
    Ok(())
}

async fn unique_key_exists<T: KernelTransaction>(
    transaction: &T,
    table: &str,
    key: &Row,
) -> EngineResult<bool> {
    let entries = transaction.scan_entries(table);
    pin_mut!(entries);
    let unique_values = &key.values[..key.values.len() - 1];
    while let Some(entry) = entries.next().await {
        let (candidate, _) = entry?;
        if candidate.values.get(..unique_values.len()) == Some(unique_values) {
            return Ok(true);
        }
    }
    Ok(false)
}
