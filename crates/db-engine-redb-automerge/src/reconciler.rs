use std::{string::String, vec::Vec};

use async_stream::stream;
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AutomergeValue};
use db_btree::{BTreeRead, BTreeTransaction};
use db_btree_automerge::{AutomergeBTreeTransaction, DocumentId, ThresholdPolicy};
use db_engine::{
    ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID, EngineError, EngineResult, KernelTransaction, RowCodec,
};
use db_value::{Row, Value};
use futures::{Stream, StreamExt, pin_mut};

use crate::{
    change_log::{ChangeKey, RedbChangeLogTransaction},
    kernel::RedbKernelTransaction,
};

const REPLICA_METADATA: &str = "__db_engine_replica_metadata";
const REPLICA_ACTOR: &str = "actor";
const ROW_MAPPINGS: &str = "__db_engine_row_mappings";
const TOMBSTONES: &str = "__db_engine_tombstones";
const COLUMN_COUNT_BYTES: usize = size_of::<u32>();
const ENGINE_TABLE_FIELDS: &str = "table_fields";

#[derive(Clone, Copy, Debug, Default)]
pub struct AutomergeRowCodec;

struct Column {
    id: String,
    default: Value,
}

impl AutomergeRowCodec {
    fn change_table(table: &str) -> String {
        format!("__db_engine_changes_{table}")
    }

    fn row_key(table: &str, key: &Row) -> Row {
        let mut values = Vec::with_capacity(key.values.len() + 1);
        values.push(Value::from(table));
        values.extend(key.values.clone());
        Row::new(values)
    }

    fn document_id(table: &str, key: &Row) -> EngineResult<DocumentId> {
        let key = postcard::to_allocvec(key).map_err(EngineError::custom)?;
        let mut id = Vec::with_capacity(table.len() + key.len() + 4);
        id.extend_from_slice(&(table.len() as u32).to_be_bytes());
        id.extend_from_slice(table.as_bytes());
        id.extend_from_slice(&key);
        Ok(id)
    }

    fn replica_metadata(
        transaction: &RedbKernelTransaction,
    ) -> db_btree_redb::RedbBTreeScopedTransaction<'_, Row, Row> {
        transaction.entries(REPLICA_METADATA)
    }

    async fn actor(transaction: &RedbKernelTransaction) -> EngineResult<ActorId> {
        let key = Row::new(vec![Value::from(REPLICA_ACTOR)]);
        let Some(row) = Self::replica_metadata(transaction)
            .get(&key)
            .await
            .map_err(EngineError::custom)?
        else {
            return Err(EngineError::custom("Missing replica actor"));
        };
        let Some(Value::Blob(actor)) = row.values.first() else {
            return Err(EngineError::custom("Invalid replica actor"));
        };
        Ok(ActorId::from(actor.clone()))
    }

    fn mapping(
        transaction: &RedbKernelTransaction,
    ) -> db_btree_redb::RedbBTreeScopedTransaction<'_, Row, Row> {
        transaction.entries(ROW_MAPPINGS)
    }

    async fn row_document_id(
        transaction: &RedbKernelTransaction,
        table: &str,
        key: &Row,
    ) -> EngineResult<Option<(DocumentId, usize)>> {
        let mapping_key = Self::row_key(table, key);
        let Some(mapping) = Self::mapping(transaction)
            .get(&mapping_key)
            .await
            .map_err(EngineError::custom)?
        else {
            return Ok(None);
        };
        let (Some(Value::Blob(id)), Some(Value::Integer(column_count))) =
            (mapping.values.first(), mapping.values.get(1))
        else {
            return Err(EngineError::custom("Invalid primary-key mapping"));
        };
        let column_count = usize::try_from(*column_count)
            .map_err(|_| EngineError::custom("Invalid primary-key mapping"))?;
        Ok(Some((id.clone(), column_count)))
    }

    async fn columns(
        transaction: &RedbKernelTransaction,
        table: &str,
        fallback_count: usize,
    ) -> EngineResult<Vec<Column>> {
        let entries = transaction.entries(ENGINE_TABLE_FIELDS);
        let fields = entries.range(..);
        pin_mut!(fields);
        let mut columns = Vec::new();
        while let Some(field) = fields.next().await {
            let (_, field) = field.map_err(EngineError::custom)?;
            if field.values.first().and_then(Value::as_text) != Some(table) {
                continue;
            }
            let index = field
                .values
                .get(4)
                .and_then(Value::to_integer)
                .ok_or(EngineError::custom("Invalid table field column index"))?;
            let id = field
                .values
                .get(6)
                .and_then(Value::as_uuid)
                .ok_or(EngineError::custom(ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID))?
                .to_string();
            let default = field
                .values
                .get(3)
                .cloned()
                .ok_or(EngineError::custom("Invalid table field default"))?;
            columns.push((index, Column { id, default }));
        }
        columns.sort_by(|(left_index, left), (right_index, right)| {
            left_index
                .cmp(right_index)
                .then_with(|| left.id.cmp(&right.id))
        });
        if columns.is_empty() {
            return Ok((0..fallback_count)
                .map(|index| Column {
                    id: index.to_string(),
                    default: Value::Null,
                })
                .collect());
        }
        Ok(columns.into_iter().map(|(_, column)| column).collect())
    }

    fn changes<'a>(
        transaction: &'a RedbKernelTransaction,
        table: &str,
    ) -> AutomergeBTreeTransaction<RedbChangeLogTransaction<'a>> {
        let changes = transaction.database.table(Self::change_table(table));
        AutomergeBTreeTransaction::new(
            RedbChangeLogTransaction::new(changes),
            ThresholdPolicy::default(),
        )
    }

    fn decode_row(document: &AutoCommit, columns: &[Column]) -> EngineResult<Row> {
        let mut values = Vec::with_capacity(columns.len());
        for column in columns {
            let values_for_column = document
                .get_all(ROOT, &column.id)
                .map_err(EngineError::custom)?;
            if values_for_column.is_empty() {
                values.push(column.default.clone());
                continue;
            }
            if values_for_column.len() != 1 {
                return Err(EngineError::custom(
                    "Logical row has a same-column conflict",
                ));
            }
            let Some((AutomergeValue::Scalar(value), _)) = values_for_column.first() else {
                return Err(EngineError::custom(
                    "Logical row column is not a scalar value",
                ));
            };
            let ScalarValue::Bytes(bytes) = value.as_ref() else {
                return Err(EngineError::custom(
                    "Logical row column is not encoded bytes",
                ));
            };
            values.push(postcard::from_bytes(bytes).map_err(EngineError::custom)?);
        }
        Ok(Row::new(values))
    }

    fn encode_row(row: &Row, columns: &[Column]) -> EngineResult<AutoCommit> {
        let mut document = AutoCommit::new();
        Self::update_document(&mut document, row, columns)?;
        Ok(document)
    }

    fn update_document(
        document: &mut AutoCommit,
        row: &Row,
        columns: &[Column],
    ) -> EngineResult<()> {
        for (value, column) in row.values.iter().zip(columns) {
            let bytes = postcard::to_allocvec(value).map_err(EngineError::custom)?;
            document
                .put(ROOT, &column.id, ScalarValue::Bytes(bytes))
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }

    fn update_document_columns(
        document: &mut AutoCommit,
        row: &Row,
        columns: &[Column],
        changed_columns: &[usize],
    ) -> EngineResult<()> {
        for &index in changed_columns {
            let (value, column) =
                row.values
                    .get(index)
                    .zip(columns.get(index))
                    .ok_or(EngineError::InvalidQuery(
                        "Logical row has an invalid column index",
                    ))?;
            let bytes = postcard::to_allocvec(value).map_err(EngineError::custom)?;
            document
                .put(ROOT, &column.id, ScalarValue::Bytes(bytes))
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }

    fn encode_incremental(column_count: usize, bytes: Vec<u8>) -> EngineResult<Vec<u8>> {
        let column_count = u32::try_from(column_count)
            .map_err(|_| EngineError::InvalidQuery("Logical row has too many columns"))?;
        let mut value = Vec::with_capacity(COLUMN_COUNT_BYTES + bytes.len());
        value.extend_from_slice(&column_count.to_be_bytes());
        value.extend_from_slice(&bytes);
        Ok(value)
    }

    fn decode_incremental(value: &[u8]) -> EngineResult<(usize, &[u8])> {
        let Some((column_count, bytes)) = value.split_at_checked(COLUMN_COUNT_BYTES) else {
            return Err(EngineError::custom("Invalid Automerge row change"));
        };
        let column_count = u32::from_be_bytes(
            column_count
                .try_into()
                .map_err(|_| EngineError::custom("Invalid Automerge row change"))?,
        );
        Ok((column_count as usize, bytes))
    }
}

impl RowCodec<RedbKernelTransaction> for AutomergeRowCodec {
    async fn ensure_table(
        &self,
        transaction: &mut RedbKernelTransaction,
        table: &str,
    ) -> EngineResult<()> {
        transaction
            .database
            .create_table::<ChangeKey, Vec<u8>>(&Self::change_table(table))
            .map_err(EngineError::custom)?;
        transaction.ensure_table(REPLICA_METADATA).await?;
        let actor_key = Row::new(vec![Value::from(REPLICA_ACTOR)]);
        if Self::replica_metadata(transaction)
            .get(&actor_key)
            .await
            .map_err(EngineError::custom)?
            .is_none()
        {
            Self::replica_metadata(transaction)
                .insert(
                    actor_key,
                    Row::new(vec![Value::Blob(uuid::Uuid::now_v7().as_bytes().to_vec())]),
                )
                .await
                .map_err(EngineError::custom)?;
        }
        transaction.ensure_table(ROW_MAPPINGS).await?;
        transaction.ensure_table(TOMBSTONES).await?;
        transaction.ensure_table(ENGINE_TABLE_FIELDS).await
    }

    async fn drop_table(
        &self,
        transaction: &mut RedbKernelTransaction,
        table: &str,
    ) -> EngineResult<()> {
        for entries_table in [ROW_MAPPINGS, TOMBSTONES] {
            let keys = {
                let entries = transaction.entries(entries_table);
                let entries = entries.range(..);
                pin_mut!(entries);
                let mut keys = Vec::new();
                while let Some(entry) = entries.next().await {
                    let (key, _) = entry.map_err(EngineError::custom)?;
                    if key.values.first().and_then(Value::as_text) == Some(table) {
                        keys.push(key);
                    }
                }
                keys
            };
            for key in keys {
                transaction
                    .entries(entries_table)
                    .remove(&key)
                    .await
                    .map_err(EngineError::custom)?;
            }
        }
        transaction
            .database
            .drop_table::<ChangeKey, Vec<u8>>(&Self::change_table(table))
            .map(|_| ())
            .map_err(EngineError::custom)
    }

    async fn get_row(
        &self,
        transaction: &RedbKernelTransaction,
        table: &str,
        key: &Row,
    ) -> EngineResult<Option<Row>> {
        if transaction
            .entries(TOMBSTONES)
            .get(&Self::row_key(table, key))
            .await
            .map_err(EngineError::custom)?
            .is_some()
        {
            return Ok(None);
        }
        let Some((id, column_count)) = Self::row_document_id(transaction, table, key).await? else {
            return Ok(None);
        };
        let Some(document) = Self::changes(transaction, table)
            .get(&id)
            .await
            .map_err(EngineError::custom)?
        else {
            return Err(EngineError::custom(
                "Primary-key mapping has no logical row",
            ));
        };
        let columns = Self::columns(transaction, table, column_count).await?;
        Self::decode_row(&document, &columns).map(Some)
    }

    fn scan_rows(
        &self,
        transaction: &RedbKernelTransaction,
        table: &str,
    ) -> impl Stream<Item = EngineResult<(Row, Row)>> {
        let table = table.to_owned();
        stream! {
            let entries = transaction.entries(ROW_MAPPINGS);
            let mappings = entries.range(..);
            for await entry in mappings {
                let (mapping_key, mapping) = entry.map_err(EngineError::custom)?;
                if mapping_key.values.first().and_then(Value::as_text) != Some(table.as_str()) {
                    continue;
                }
                let key = Row::new(mapping_key.values[1..].to_vec());
                if transaction.entries(TOMBSTONES)
                    .get(&Self::row_key(&table, &key))
                    .await
                    .map_err(EngineError::custom)?
                    .is_some()
                {
                    continue;
                }
                let (Some(Value::Blob(id)), Some(Value::Integer(column_count))) =
                    (mapping.values.first(), mapping.values.get(1))
                else {
                    yield Err(EngineError::custom("Invalid primary-key mapping"));
                    continue;
                };
                let Ok(column_count) = usize::try_from(*column_count) else {
                    yield Err(EngineError::custom("Invalid primary-key mapping"));
                    continue;
                };
                let Some(document) = Self::changes(transaction, &table)
                    .get(id)
                    .await
                    .map_err(EngineError::custom)?
                else {
                    yield Err(EngineError::custom("Primary-key mapping has no logical row"));
                    continue;
                };
                let columns = match Self::columns(transaction, &table, column_count).await {
                    Ok(columns) => columns,
                    Err(error) => {
                        yield Err(error);
                        continue;
                    }
                };
                yield Self::decode_row(&document, &columns).map(|row| (key, row));
            }
        }
    }

    async fn encode_row(
        &self,
        transaction: &RedbKernelTransaction,
        table: &str,
        key: &Row,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let id = Self::document_id(table, key)?;
        let mut document = Self::changes(transaction, table)
            .get(&id)
            .await
            .map_err(EngineError::custom)?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(Self::actor(transaction).await?);
        let columns = Self::columns(transaction, table, row.values.len()).await?;
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_incremental())
    }

    async fn merge_row(
        &self,
        transaction: &mut RedbKernelTransaction,
        table: &str,
        key: Row,
        value: &[u8],
    ) -> EngineResult<Row> {
        if transaction
            .entries(TOMBSTONES)
            .get(&Self::row_key(table, &key))
            .await
            .map_err(EngineError::custom)?
            .is_some()
        {
            return Err(EngineError::InvalidQuery("Logical row is tombstoned"));
        }

        let (column_count, incremental) = Self::decode_incremental(value)?;
        let id = match Self::row_document_id(transaction, table, &key).await? {
            Some((id, existing_column_count)) => {
                if column_count > existing_column_count {
                    Self::mapping(transaction)
                        .insert(
                            Self::row_key(table, &key),
                            Row::new(vec![
                                Value::Blob(id.clone()),
                                Value::Integer(column_count as i64),
                            ]),
                        )
                        .await
                        .map_err(EngineError::custom)?;
                }
                id
            }
            None => {
                let id = Self::document_id(table, &key)?;
                Self::mapping(transaction)
                    .insert(
                        Self::row_key(table, &key),
                        Row::new(vec![
                            Value::Blob(id.clone()),
                            Value::Integer(column_count as i64),
                        ]),
                    )
                    .await
                    .map_err(EngineError::custom)?;
                id
            }
        };

        let mut changes = Self::changes(transaction, table);
        let existing = changes.get(&id).await.map_err(EngineError::custom)?;
        let mut document = existing.unwrap_or_else(AutoCommit::new);
        document
            .load_incremental(incremental)
            .map_err(EngineError::custom)?;
        let columns = Self::columns(transaction, table, column_count).await?;
        let row = Self::decode_row(&document, &columns)?;

        if changes
            .get(&id)
            .await
            .map_err(EngineError::custom)?
            .is_some()
        {
            changes.remove(&id).await.map_err(EngineError::custom)?;
        }
        changes
            .insert(id, document)
            .await
            .map_err(EngineError::custom)?;
        Ok(row)
    }

    async fn put_row(
        &self,
        transaction: &mut RedbKernelTransaction,
        table: &str,
        key: Row,
        value: Row,
    ) -> EngineResult<()> {
        if transaction
            .entries(TOMBSTONES)
            .get(&Self::row_key(table, &key))
            .await
            .map_err(EngineError::custom)?
            .is_some()
        {
            return Err(EngineError::InvalidQuery("Logical row is tombstoned"));
        }
        let columns = Self::columns(transaction, table, value.values.len()).await?;
        let id = match Self::row_document_id(transaction, table, &key).await? {
            Some((id, column_count)) => {
                if columns.len() > column_count {
                    Self::mapping(transaction)
                        .insert(
                            Self::row_key(table, &key),
                            Row::new(vec![
                                Value::Blob(id.clone()),
                                Value::Integer(columns.len() as i64),
                            ]),
                        )
                        .await
                        .map_err(EngineError::custom)?;
                }
                id
            }
            None => {
                let id = Self::document_id(table, &key)?;
                Self::mapping(transaction)
                    .insert(
                        Self::row_key(table, &key),
                        Row::new(vec![
                            Value::Blob(id.clone()),
                            Value::Integer(columns.len() as i64),
                        ]),
                    )
                    .await
                    .map_err(EngineError::custom)?;
                id
            }
        };
        let mut changes = Self::changes(transaction, table);
        if let Some(document) = changes.get(&id).await.map_err(EngineError::custom)? {
            Self::decode_row(&document, &columns)?;
            let values: EngineResult<Vec<_>> = value
                .values
                .iter()
                .zip(&columns)
                .map(|(value, column)| {
                    postcard::to_allocvec(value)
                        .map(|bytes| (column.id.clone(), bytes))
                        .map_err(EngineError::custom)
                })
                .collect();
            let values = values?;
            changes
                .update(id, |document| {
                    for (id, bytes) in &values {
                        document
                            .put(ROOT, id, ScalarValue::Bytes(bytes.clone()))
                            .map_err(db_btree::BTreeError::custom)?;
                    }
                    Ok(())
                })
                .await
                .map_err(EngineError::custom)?;
        } else {
            changes
                .insert(id, Self::encode_row(&value, &columns)?)
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }

    async fn remove_row(
        &self,
        transaction: &mut RedbKernelTransaction,
        table: &str,
        key: &Row,
    ) -> EngineResult<Option<Row>> {
        let row = self.get_row(transaction, table, key).await?;
        if row.is_some() {
            let mapping_key = Self::row_key(table, key);
            Self::mapping(transaction)
                .remove(&mapping_key)
                .await
                .map_err(EngineError::custom)?;
            transaction
                .entries(TOMBSTONES)
                .insert(mapping_key, Row::default())
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(row)
    }
}
