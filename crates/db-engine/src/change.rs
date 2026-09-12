use alloc::{vec, vec::Vec};

use db_value::{Row, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec, RowGenerationId, TableGenerationId,
    catalog::ENGINE_ROW_MAPPINGS,
    envelope::ensure_envelope_log,
    index::{rebuild_table, update_row},
    schema::{SchemaChange, columns, materialize as materialize_schema},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub id: Uuid,
    pub key: ChangeKey,
    pub value: Option<Vec<u8>>,
}

impl Change {
    pub fn schema(id: Uuid, schema: SchemaChange) -> Self {
        Self {
            id,
            key: ChangeKey::Schema(schema),
            value: None,
        }
    }

    pub fn row(
        id: Uuid,
        table: TableGenerationId,
        row: RowGenerationId,
        key: Row,
        previous_key: Option<Row>,
        value: Option<Vec<u8>>,
    ) -> Self {
        Self {
            id,
            key: ChangeKey::Row {
                table,
                row,
                key,
                previous_key,
            },
            value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChangeKey {
    Schema(SchemaChange),
    Row {
        table: TableGenerationId,
        row: RowGenerationId,
        key: Row,
        previous_key: Option<Row>,
    },
}

pub(crate) async fn ensure_change_log<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    transaction.ensure_table(ENGINE_ROW_MAPPINGS).await?;
    ensure_envelope_log(transaction).await
}

pub(crate) fn row_mapping_key(table: TableGenerationId, key: &Row) -> Row {
    let mut values = Vec::with_capacity(key.values.len() + 1);
    values.push(Value::Uuid(table.0));
    values.extend(key.values.clone());
    Row::new(values)
}

pub(crate) async fn row_generation_id<T>(
    transaction: &T,
    table: TableGenerationId,
    key: &Row,
) -> EngineResult<Option<RowGenerationId>>
where
    T: KernelTransaction,
{
    let Some(mapping) = transaction
        .get_entry(ENGINE_ROW_MAPPINGS, &row_mapping_key(table, key))
        .await?
    else {
        return Ok(None);
    };
    mapping
        .values
        .first()
        .and_then(Value::as_uuid)
        .copied()
        .map(RowGenerationId)
        .map(Some)
        .ok_or(EngineError::custom("Invalid row-generation mapping"))
}

pub(crate) async fn put_row_mapping<T>(
    transaction: &mut T,
    table: TableGenerationId,
    key: &Row,
    row: RowGenerationId,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    transaction
        .put_entry(
            ENGINE_ROW_MAPPINGS,
            row_mapping_key(table, key),
            Row::new(vec![Value::Uuid(row.0)]),
        )
        .await
}

pub(crate) async fn remove_row_mapping<T>(
    transaction: &mut T,
    table: TableGenerationId,
    key: &Row,
    row: RowGenerationId,
) -> EngineResult<()>
where
    T: KernelTransaction,
{
    let mapping_key = row_mapping_key(table, key);
    if row_generation_id(transaction, table, key).await? == Some(row) {
        transaction
            .remove_entry(ENGINE_ROW_MAPPINGS, &mapping_key)
            .await?;
    }
    Ok(())
}

pub(crate) async fn apply_local_change<T, R>(
    transaction: &mut T,
    codec: &R,
    changes: &mut Vec<Change>,
    change: Change,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let _ = materialize_change(transaction, codec, &change).await?;
    changes.push(change);
    Ok(())
}

pub(crate) async fn materialize_change<T, R>(
    transaction: &mut T,
    codec: &R,
    change: &Change,
) -> EngineResult<bool>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match (&change.key, &change.value) {
        (ChangeKey::Schema(schema), None) => {
            let superseded = materialize_schema(transaction, schema).await?;
            if let SchemaChange::CreateTable { table, .. } = schema {
                codec.ensure_table(transaction, *table).await?;
            }
            match schema {
                SchemaChange::CreateTable { table, .. }
                | SchemaChange::AddColumn { table, .. }
                | SchemaChange::CreateIndex { table, .. } => {
                    rebuild_table(transaction, codec, *table).await?;
                }
                SchemaChange::TombstoneTable(_)
                | SchemaChange::TombstoneColumn(_)
                | SchemaChange::TombstoneIndex(_) => {}
            }
            if superseded {
                return Ok(true);
            }
        }
        (
            ChangeKey::Row {
                table,
                row,
                key,
                previous_key,
            },
            Some(value),
        ) => {
            if columns(transaction, *table).await.is_err() {
                return Ok(true);
            }
            let old = codec.get_row(transaction, table, row).await?;
            let Some(value) = codec.merge_row(transaction, table, *row, value).await? else {
                return Ok(true);
            };
            if let Some(previous_key) = previous_key {
                remove_row_mapping(transaction, *table, previous_key, *row).await?;
            }
            put_row_mapping(transaction, *table, key, *row).await?;
            update_row(transaction, *table, key, old.as_ref(), Some(&value)).await?;
        }
        (
            ChangeKey::Row {
                table, row, key, ..
            },
            None,
        ) => {
            if columns(transaction, *table).await.is_err() {
                return Ok(true);
            }
            let old = codec.remove_row(transaction, table, row).await?;
            remove_row_mapping(transaction, *table, key, *row).await?;
            update_row(transaction, *table, key, old.as_ref(), None).await?;
        }
        (_, Some(_)) => return Err(EngineError::custom("Invalid schema change value")),
    }
    Ok(false)
}
