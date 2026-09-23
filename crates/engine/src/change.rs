use alloc::vec::Vec;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec, TableGenerationId,
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

    pub fn row(id: Uuid, table: TableGenerationId, row: Uuid, value: Option<Vec<u8>>) -> Self {
        Self {
            id,
            key: ChangeKey::Row { table, row },
            value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChangeKey {
    Schema(SchemaChange),
    Row { table: TableGenerationId, row: Uuid },
}

pub(crate) async fn ensure_change_log<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    ensure_envelope_log(transaction).await
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
    let _ = materialize_change(transaction, codec, &change, true).await?;
    changes.push(change);
    Ok(())
}

pub(crate) async fn materialize_change<T, R>(
    transaction: &mut T,
    codec: &R,
    change: &Change,
    enforce_unique: bool,
) -> EngineResult<bool>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    match (&change.key, &change.value) {
        (ChangeKey::Schema(schema), None) => {
            let superseded = materialize_schema(transaction, schema).await?;
            if let SchemaChange::CreateTable { table, .. } = schema {
                codec.ensure_table(transaction, table.0).await?;
            }
            match schema {
                SchemaChange::CreateTable { table, .. }
                | SchemaChange::AddColumn { table, .. }
                | SchemaChange::CreateIndex { table, .. } => {
                    rebuild_table(transaction, codec, *table, enforce_unique).await?;
                }
                SchemaChange::TombstoneTable(_)
                | SchemaChange::TombstoneColumn(_)
                | SchemaChange::TombstoneIndex(_) => {}
            }
            if superseded {
                return Ok(true);
            }
        }
        (ChangeKey::Row { table, row }, Some(value)) => {
            if columns(transaction, *table).await.is_err() {
                return Ok(true);
            }
            let old = codec.get_row(transaction, table.0, row).await?;
            let Some(value) = codec.merge_row(transaction, table.0, *row, value).await? else {
                return Ok(true);
            };
            update_row(
                transaction,
                *table,
                old.as_ref(),
                Some(&value),
                enforce_unique,
            )
            .await?;
        }
        (ChangeKey::Row { table, row }, None) => {
            if columns(transaction, *table).await.is_err() {
                return Ok(true);
            }
            let old = codec.remove_row(transaction, table.0, row).await?;
            update_row(transaction, *table, old.as_ref(), None, enforce_unique).await?;
        }
        (_, Some(_)) => return Err(EngineError::custom("Invalid schema change value")),
    }
    Ok(false)
}
