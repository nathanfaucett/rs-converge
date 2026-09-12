use alloc::{string::String, vec::Vec};

use db_value::{Row, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    EngineError, EngineResult, KernelTransaction, RowCodec,
    catalog::{
        ENGINE_APPLIED_CHANGES, ENGINE_CHANGE_SEQUENCE, ENGINE_CHANGES, ENGINE_INDEX_FIELDS,
        ENGINE_INDICES, ENGINE_TABLES,
    },
    index::{rebuild, remove as remove_index, update_row},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Change {
    pub id: Uuid,
    pub key: ChangeKey,
    pub value: Option<Vec<u8>>,
}

impl Change {
    pub fn entry(id: Uuid, table: String, key: Row, value: Option<Vec<u8>>) -> Self {
        Self {
            id,
            key: ChangeKey::Entry { table, key },
            value,
        }
    }

    pub fn row(id: Uuid, table: String, key: Row, value: Option<Vec<u8>>) -> Self {
        Self {
            id,
            key: ChangeKey::Row { table, key },
            value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ChangeKey {
    Entry { table: String, key: Row },
    Row { table: String, key: Row },
}

pub trait ChangeReplication {
    type Cursor;

    fn changes_since(
        &self,
        cursor: Option<&Self::Cursor>,
    ) -> impl Future<Output = EngineResult<(Self::Cursor, Vec<Change>)>>;

    fn apply_changes(&self, changes: Vec<Change>) -> impl Future<Output = EngineResult<()>>;
}

pub(crate) async fn ensure_change_log<T>(transaction: &mut T) -> EngineResult<()>
where
    T: KernelTransaction,
{
    for table in [
        ENGINE_APPLIED_CHANGES,
        ENGINE_CHANGE_SEQUENCE,
        ENGINE_CHANGES,
    ] {
        transaction.ensure_table(table).await?;
    }
    Ok(())
}

pub(crate) async fn apply_change<T, R>(
    transaction: &mut T,
    codec: &R,
    change: Change,
) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    let change_key = Row::new(vec![Value::Uuid(change.id)]);
    if transaction
        .get_entry(ENGINE_APPLIED_CHANGES, &change_key)
        .await?
        .is_some()
    {
        return Ok(());
    }

    match (&change.key, &change.value) {
        (ChangeKey::Entry { table, key }, Some(value)) => {
            let value: Row = postcard::from_bytes(value).map_err(EngineError::custom)?;
            if table == ENGINE_TABLES {
                let Some(table_name) = key.values.first().and_then(Value::as_text) else {
                    return Err(EngineError::custom("Invalid table catalog change"));
                };
                codec.ensure_table(transaction, table_name).await?;
            }
            transaction.put_entry(table, key.clone(), value).await?;
            let index_name = (table == ENGINE_INDICES || table == ENGINE_INDEX_FIELDS)
                .then(|| key.values.first().and_then(Value::as_text))
                .flatten();
            if let Some(index_name) = index_name {
                rebuild(transaction, codec, index_name).await?;
            }
        }
        (ChangeKey::Entry { table, key }, None) => {
            if table == ENGINE_INDICES
                && let Some(index_name) = key.values.first().and_then(Value::as_text)
            {
                remove_index(transaction, index_name).await?;
            }
            transaction.remove_entry(table, key).await?;
        }
        (ChangeKey::Row { table, key }, Some(value)) => {
            let old = codec.get_row(transaction, table, key).await?;
            let row = codec
                .merge_row(transaction, table, key.clone(), value)
                .await?;
            update_row(transaction, table, key, old.as_ref(), Some(&row)).await?;
        }
        (ChangeKey::Row { table, key }, None) => {
            let old = codec.remove_row(transaction, table, key).await?;
            update_row(transaction, table, key, old.as_ref(), None).await?;
        }
    }

    let sequence_key = Row::default();
    let sequence = match transaction
        .get_entry(ENGINE_CHANGE_SEQUENCE, &sequence_key)
        .await?
    {
        Some(value) => match value.values.as_slice() {
            [Value::Integer(sequence)] => sequence
                .checked_add(1)
                .ok_or_else(|| EngineError::custom("Change sequence overflow"))?,
            _ => return Err(EngineError::custom("Invalid change sequence")),
        },
        None => 1,
    };
    let encoded = postcard::to_allocvec(&change).map_err(EngineError::custom)?;
    transaction
        .put_entry(ENGINE_APPLIED_CHANGES, change_key, Row::default())
        .await?;
    transaction
        .put_entry(
            ENGINE_CHANGE_SEQUENCE,
            sequence_key,
            Row::new(vec![Value::Integer(sequence)]),
        )
        .await?;
    transaction
        .put_entry(
            ENGINE_CHANGES,
            Row::new(vec![Value::Integer(sequence)]),
            Row::new(vec![Value::Blob(encoded)]),
        )
        .await
}
