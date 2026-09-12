use alloc::{string::String, vec, vec::Vec};

use db_schema::{ColumnSchemaIndex, IndexSchema};
use db_value::{Row, Value};
use futures::{StreamExt, pin_mut};

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec,
    catalog::{
        ENGINE_INDEX_FIELD_COLUMN_COUNT, ENGINE_INDEX_FIELD_INDEX_NAME,
        ENGINE_INDEX_FIELD_TABLE_NAME, ENGINE_INDEX_FIELD_UNIQUE, ENGINE_INDEX_FIELDS,
        ENGINE_INDEX_FIELDS_FIELD_COLUMN_INDEX, ENGINE_INDEX_FIELDS_FIELD_FIELD_ORDER,
        ENGINE_INDICES,
    },
    executor::{materialize_defaults, table_schema},
};

pub(crate) const ENGINE_INDEX_RECORDS: &str = "__db_index_records";

pub(crate) async fn ensure_index_records<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    transaction.ensure_table(ENGINE_INDEX_RECORDS).await
}

pub(crate) async fn index_schema<T>(
    transaction: &T,
    name: &str,
) -> EngineResult<Option<IndexSchema>>
where
    T: KernelTransaction,
{
    let index_key = Row::new(vec![Value::from(name)]);
    let Some(index) = transaction.get_entry(ENGINE_INDICES, &index_key).await? else {
        return Ok(None);
    };
    let table_name = index
        .values
        .get(1)
        .and_then(Value::to_text)
        .ok_or(EngineError::InvalidQuery(ENGINE_INDEX_FIELD_TABLE_NAME))?;
    let unique = index
        .values
        .get(2)
        .and_then(Value::to_bool)
        .ok_or(EngineError::InvalidQuery(ENGINE_INDEX_FIELD_UNIQUE))?;
    let column_count = index
        .values
        .get(3)
        .and_then(Value::to_integer)
        .ok_or(EngineError::InvalidQuery(ENGINE_INDEX_FIELD_COLUMN_COUNT))?;
    let column_count = usize::try_from(column_count)
        .map_err(|_| EngineError::InvalidQuery(ENGINE_INDEX_FIELD_COLUMN_COUNT))?;

    let entries = transaction.scan_entries(ENGINE_INDEX_FIELDS);
    pin_mut!(entries);
    let mut fields = Vec::new();
    while let Some(entry) = entries.next().await {
        let (_, field) = entry?;
        if field.values.first().and_then(Value::as_text) != Some(name) {
            continue;
        }
        let order =
            field
                .values
                .get(1)
                .and_then(Value::to_integer)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_INDEX_FIELDS_FIELD_FIELD_ORDER,
                ))?;
        let column =
            field
                .values
                .get(2)
                .and_then(Value::to_integer)
                .ok_or(EngineError::InvalidQuery(
                    ENGINE_INDEX_FIELDS_FIELD_COLUMN_INDEX,
                ))?;
        fields.push((order, column));
    }
    fields.sort_by_key(|(order, _)| *order);
    if fields.len() != column_count {
        return Ok(None);
    }

    Ok(Some(IndexSchema {
        name: String::from(name),
        table_name,
        column_indices: fields
            .into_iter()
            .map(|(_, column)| {
                ColumnSchemaIndex::try_from(column)
                    .map_err(|_| EngineError::InvalidQuery("Invalid index column"))
            })
            .collect::<EngineResult<Vec<_>>>()?,
        unique,
    }))
}

async fn indexes_for_table<T>(transaction: &T, table: &str) -> EngineResult<Vec<IndexSchema>>
where
    T: KernelTransaction,
{
    let entries = transaction.scan_entries(ENGINE_INDICES);
    pin_mut!(entries);
    let mut names = Vec::new();
    while let Some(entry) = entries.next().await {
        let (_, index) = entry?;
        if index.values.get(1).and_then(Value::as_text) == Some(table) {
            let name = index
                .values
                .first()
                .and_then(Value::to_text)
                .ok_or(EngineError::InvalidQuery(ENGINE_INDEX_FIELD_INDEX_NAME))?;
            names.push(name);
        }
    }
    let mut schemas = Vec::with_capacity(names.len());
    for name in names {
        if let Some(schema) = index_schema(transaction, &name).await? {
            schemas.push(schema);
        }
    }
    Ok(schemas)
}

fn record_key(schema: &IndexSchema, primary_key: &Row, row: &Row) -> EngineResult<Row> {
    let mut values = Vec::with_capacity(schema.column_indices.len() + primary_key.values.len() + 1);
    values.push(Value::from(schema.name.as_str()));
    for column in &schema.column_indices {
        let value = row
            .values
            .get(*column as usize)
            .ok_or(EngineError::InvalidQuery(
                "Index column is missing from row",
            ))?;
        values.push(value.clone());
    }
    values.extend(primary_key.values.clone());
    Ok(Row::new(values))
}

async fn insert_record<T>(
    transaction: &mut T,
    schema: &IndexSchema,
    key: &Row,
    row: &Row,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    let record_key = record_key(schema, key, row)?;
    if schema.unique {
        let prefix_len = schema.column_indices.len() + 1;
        let records = transaction.scan_entries(ENGINE_INDEX_RECORDS);
        pin_mut!(records);
        while let Some(entry) = records.next().await {
            let (existing_key, existing_value) = entry?;
            if existing_key.values.get(..prefix_len) == record_key.values.get(..prefix_len)
                && existing_value != *key
            {
                return Err(EngineError::InvalidQuery(
                    "Unique index constraint violated",
                ));
            }
        }
    }
    transaction
        .put_entry(ENGINE_INDEX_RECORDS, record_key, key.clone())
        .await
}

pub(crate) async fn update_row<T>(
    transaction: &mut T,
    table: &str,
    key: &Row,
    old: Option<&Row>,
    new: Option<&Row>,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    ensure_index_records(transaction).await?;
    let schemas = indexes_for_table(transaction, table).await?;
    if schemas.is_empty() {
        return Ok(());
    }
    let table_schema = table_schema(transaction, table).await?;
    let old = old.map(|row| materialize_defaults(&table_schema, row.clone()));
    let new = new.map(|row| materialize_defaults(&table_schema, row.clone()));
    for schema in schemas {
        if let Some(row) = &old {
            transaction
                .remove_entry(ENGINE_INDEX_RECORDS, &record_key(&schema, key, row)?)
                .await?;
        }
        if let Some(row) = &new {
            insert_record(transaction, &schema, key, row).await?;
        }
    }
    Ok(())
}

pub(crate) async fn remove<T>(transaction: &mut T, name: &str) -> EngineResult<()>
where
    T: KernelTransaction,
{
    let keys = {
        let records = transaction.scan_entries(ENGINE_INDEX_RECORDS);
        pin_mut!(records);
        let mut keys = Vec::new();
        while let Some(entry) = records.next().await {
            let (key, _) = entry?;
            if key.values.first().and_then(Value::as_text) == Some(name) {
                keys.push(key);
            }
        }
        keys
    };
    for key in keys {
        transaction.remove_entry(ENGINE_INDEX_RECORDS, &key).await?;
    }
    Ok(())
}

pub(crate) async fn rebuild<T, R>(transaction: &mut T, codec: &R, name: &str) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    ensure_index_records(transaction).await?;
    let Some(schema) = index_schema(transaction, name).await? else {
        return Ok(());
    };
    let keys = {
        let records = transaction.scan_entries(ENGINE_INDEX_RECORDS);
        pin_mut!(records);
        let mut keys = Vec::new();
        while let Some(entry) = records.next().await {
            let (key, _) = entry?;
            if key.values.first().and_then(Value::as_text) == Some(name) {
                keys.push(key);
            }
        }
        keys
    };
    for key in keys {
        transaction.remove_entry(ENGINE_INDEX_RECORDS, &key).await?;
    }

    let rows = {
        let rows = codec.scan_rows(transaction, &schema.table_name);
        pin_mut!(rows);
        let mut result = Vec::new();
        while let Some(row) = rows.next().await {
            result.push(row?);
        }
        result
    };
    let table_schema = table_schema(transaction, &schema.table_name).await?;
    for (key, row) in rows {
        let row = materialize_defaults(&table_schema, row);
        insert_record(transaction, &schema, &key, &row).await?;
    }
    Ok(())
}
