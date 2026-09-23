use std::{string::String, vec::Vec};

use async_stream::stream;
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AutomergeValue};
use btree::{BTreeRead, BTreeTransaction};
use btree_automerge::{
    AutomergeBTreeTransaction, AutomergeChangeStore, DocumentChangeKey, DocumentId,
    ThresholdPolicy, get_document, reconstruct_document_values,
};
use serde::{Deserialize, Serialize};

use engine::{
    BytesTable, BytesTableTransaction, ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID,
    ENGINE_TABLE_FIELDS_STORAGE, EngineError, EngineResult, KernelTransaction, RowCodec, RowTable,
};
use futures::{Stream, StreamExt, pin_mut};
use value::{Row, Value};

const COLUMN_COUNT_BYTES: usize = size_of::<u32>();

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RowMetadata {
    pub version: u8,
    pub deleted: bool,
}

#[derive(Debug)]
pub struct AutomergeRowCodec {
    actor: ActorId,
}

impl AutomergeRowCodec {
    pub fn new() -> Self {
        Self {
            actor: ActorId::from(uuid::Uuid::now_v7().as_bytes().to_vec()),
        }
    }
}

impl Default for AutomergeRowCodec {
    fn default() -> Self {
        Self::new()
    }
}

struct Column {
    id: String,
    default: Value,
}

impl AutomergeRowCodec {
    fn document_id(row: &uuid::Uuid) -> DocumentId {
        row.as_bytes().to_vec()
    }

    fn row_id(id: &[u8]) -> EngineResult<uuid::Uuid> {
        uuid::Uuid::from_slice(id).map_err(EngineError::custom)
    }

    async fn columns<T>(
        transaction: &T,
        table: uuid::Uuid,
        fallback_count: usize,
    ) -> EngineResult<Vec<Column>>
    where
        T: KernelTransaction,
    {
        let table_id = table;
        let fields = transaction.scan_entries(ENGINE_TABLE_FIELDS_STORAGE);
        pin_mut!(fields);
        let mut columns = Vec::new();
        while let Some(field) = fields.next().await {
            let (key, field) = field.map_err(EngineError::custom)?;
            if field.values.first().and_then(Value::as_uuid) != Some(&table_id) {
                continue;
            }
            let index = field
                .values
                .get(4)
                .and_then(Value::to_integer)
                .ok_or(EngineError::custom("Invalid table field column index"))?;
            let id = key
                .values
                .first()
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

    async fn document<T>(
        transaction: &T,
        table: uuid::Uuid,
        id: &DocumentId,
    ) -> EngineResult<Option<AutoCommit>>
    where
        T: KernelTransaction,
    {
        get_document(
            &AutomergeChangeStore::new(BytesTable::new(transaction, table)),
            id,
        )
        .await
        .map_err(EngineError::custom)
    }

    fn changes<'a, T>(
        transaction: &'a mut T,
        table: uuid::Uuid,
    ) -> AutomergeBTreeTransaction<AutomergeChangeStore<BytesTableTransaction<'a, T>>>
    where
        T: KernelTransaction + Send,
    {
        AutomergeBTreeTransaction::new(
            AutomergeChangeStore::new(BytesTableTransaction::new(transaction, table)),
            ThresholdPolicy::default(),
        )
    }

    async fn metadata_deleted<T>(
        transaction: &T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<bool>
    where
        T: KernelTransaction,
    {
        let storage = table;
        let changes = AutomergeChangeStore::new(BytesTable::new(transaction, storage));
        let Some(value) = changes
            .get(&DocumentChangeKey::new_metadata(Self::document_id(row)))
            .await
            .map_err(EngineError::custom)?
        else {
            return Ok(false);
        };
        let metadata: RowMetadata = postcard::from_bytes(&value).map_err(EngineError::custom)?;
        Ok(metadata.deleted)
    }

    fn decode_row(document: &AutoCommit, columns: &[Column]) -> EngineResult<Row> {
        let mut values = Vec::with_capacity(columns.len());
        for column in columns {
            let Some((AutomergeValue::Scalar(value), _)) = document
                .get(ROOT, &column.id)
                .map_err(EngineError::custom)?
            else {
                values.push(column.default.clone());
                continue;
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

    fn conflicted_columns(document: &AutoCommit, columns: &[Column]) -> EngineResult<Vec<usize>> {
        let mut result = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            if document
                .get_all(ROOT, &column.id)
                .map_err(EngineError::custom)?
                .len()
                > 1
            {
                result.push(index);
            }
        }
        Ok(result)
    }

    fn has_conflicts(
        document: &AutoCommit,
        columns: &[Column],
        changed_columns: &[usize],
    ) -> EngineResult<bool> {
        for &index in changed_columns {
            let column = columns.get(index).ok_or(EngineError::InvalidQuery(
                "Logical row has an invalid column index",
            ))?;
            if document
                .get_all(ROOT, &column.id)
                .map_err(EngineError::custom)?
                .len()
                > 1
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn encode_row(&self, row: &Row, columns: &[Column]) -> EngineResult<AutoCommit> {
        let mut document = AutoCommit::new().with_actor(self.actor.clone());
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

    async fn document_columns<T>(
        transaction: &T,
        table: uuid::Uuid,
        document: &AutoCommit,
    ) -> EngineResult<Vec<Column>>
    where
        T: KernelTransaction,
    {
        let columns = Self::columns(transaction, table, 0).await?;
        if columns.is_empty() {
            return Ok((0..document.keys(ROOT).count())
                .map(|index| Column {
                    id: index.to_string(),
                    default: Value::Null,
                })
                .collect());
        }
        Ok(columns)
    }

    async fn store_row<T>(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        id: DocumentId,
        columns: &[Column],
        value: &Row,
    ) -> EngineResult<()>
    where
        T: KernelTransaction + Send,
    {
        let mut changes = Self::changes(transaction, table);
        if let Some(document) = changes.get(&id).await.map_err(EngineError::custom)? {
            let changed_columns: Vec<_> = (0..columns.len()).collect();
            if Self::has_conflicts(&document, columns, &changed_columns)? {
                return Err(EngineError::InvalidQuery(
                    "Logical row column is conflicted",
                ));
            }
            changes
                .update(id, |document| {
                    Self::update_document(document, value, columns)
                        .map_err(btree::BTreeError::custom)
                })
                .await
                .map_err(EngineError::custom)?;
        } else {
            changes
                .insert(id, self.encode_row(value, columns)?)
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }
}

impl<T> RowCodec<T> for AutomergeRowCodec
where
    T: KernelTransaction + Send,
{
    async fn ensure_table(&self, transaction: &mut T, table: uuid::Uuid) -> EngineResult<()> {
        transaction.ensure_table(table).await
    }

    async fn drop_table(&self, transaction: &mut T, table: uuid::Uuid) -> EngineResult<()> {
        transaction.drop_table(table).await
    }

    async fn get_row(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<Option<Row>> {
        if Self::metadata_deleted(transaction, table, row).await? {
            return Ok(None);
        }
        let id = Self::document_id(row);
        let Some(document) = Self::document(transaction, table, &id).await? else {
            return Ok(None);
        };
        let table_name = table;
        let columns = Self::document_columns(transaction, table_name, &document).await?;
        Self::decode_row(&document, &columns).map(Some)
    }

    fn scan_rows(
        &self,
        transaction: &T,
        table: uuid::Uuid,
    ) -> impl Stream<Item = EngineResult<(uuid::Uuid, Row)>> {
        let table_id = table;
        stream! {
            let storage = table_id;
            let changes = AutomergeChangeStore::new(BytesTable::new(transaction, storage));
            let documents = reconstruct_document_values(changes.range(..));
            pin_mut!(documents);

            while let Some(document) = documents.next().await {
                let (id, document) = document.map_err(EngineError::custom)?;
                let row = Self::row_id(&id)?;
                let table = table_id;
                let columns = Self::document_columns(transaction, table, &document).await?;
                yield Self::decode_row(&document, &columns).map(|value| (row, value));
            }
        }
    }

    async fn encode_row(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row_id: &uuid::Uuid,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table_name = table;
        let id = Self::document_id(row_id);
        let mut document = Self::document(transaction, table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, table_name, row.values.len()).await?;
        if Self::has_conflicts(&document, &columns, changed_columns)? {
            return Err(EngineError::InvalidQuery(
                "Logical row column is conflicted",
            ));
        }
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_incremental())
    }

    async fn conflicted_columns(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<Vec<usize>> {
        let table_name = table;
        let id = Self::document_id(row);
        let Some(document) = Self::document(transaction, table, &id).await? else {
            return Ok(Vec::new());
        };
        Self::conflicted_columns(
            &document,
            &Self::document_columns(transaction, table_name, &document).await?,
        )
    }

    async fn encode_resolution(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row_id: &uuid::Uuid,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table_name = table;
        let id = Self::document_id(row_id);
        let mut document = Self::document(transaction, table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, table_name, row.values.len()).await?;
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_incremental())
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        if Self::metadata_deleted(transaction, table, &row).await? {
            return Ok(None);
        }
        let table_name = table;
        let (column_count, incremental) = Self::decode_incremental(value)?;
        let id = Self::document_id(&row);

        let existing = Self::document(transaction, table, &id).await?;
        let has_existing = existing.is_some();
        let mut document = existing.unwrap_or_else(AutoCommit::new);
        document
            .load_incremental(incremental)
            .map_err(EngineError::custom)?;
        let columns = Self::columns(transaction, table_name, column_count).await?;
        let row = Self::decode_row(&document, &columns)?;

        let mut changes = Self::changes(transaction, table);
        if has_existing {
            changes.remove(&id).await.map_err(EngineError::custom)?;
        }
        changes
            .insert(id, document)
            .await
            .map_err(EngineError::custom)?;
        Ok(Some(row))
    }

    async fn export_row_state(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<Option<Vec<u8>>> {
        let id = Self::document_id(row);
        Self::document(transaction, table, &id)
            .await?
            .map(|mut document| Ok(document.save()))
            .transpose()
    }

    async fn merge_row_state(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        state: &[u8],
    ) -> EngineResult<Option<Row>> {
        if Self::metadata_deleted(transaction, table, &row).await? {
            return Ok(None);
        }
        let mut incoming = AutoCommit::load(state).map_err(EngineError::custom)?;
        let id = Self::document_id(&row);
        let existing = Self::document(transaction, table, &id).await?;
        let mut document = existing.clone().unwrap_or_else(AutoCommit::new);
        if existing.is_some() {
            document.merge(&mut incoming).map_err(EngineError::custom)?;
        } else {
            document = incoming;
        }
        let table_name = table;
        let columns = Self::document_columns(transaction, table_name, &document).await?;
        let row_value = Self::decode_row(&document, &columns)?;
        let mut changes = Self::changes(transaction, table);
        if existing.is_some() {
            changes.remove(&id).await.map_err(EngineError::custom)?;
        }
        changes
            .insert(id, document)
            .await
            .map_err(EngineError::custom)?;
        Ok(Some(row_value))
    }

    async fn delete_row(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<Option<Row>> {
        let value = self.get_row(transaction, table, row).await?;
        let key = DocumentChangeKey::new_metadata(Self::document_id(row)).encode_ordered();
        let metadata = RowMetadata {
            version: 1,
            deleted: true,
        };
        transaction
            .put_bytes(
                table,
                key,
                postcard::to_allocvec(&metadata).map_err(EngineError::custom)?,
            )
            .await?;
        Ok(value)
    }

    async fn row_is_deleted(
        &self,
        transaction: &T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<bool> {
        Self::metadata_deleted(transaction, table, row).await
    }

    fn export_row_metadata(
        &self,
        transaction: &T,
        table: uuid::Uuid,
    ) -> impl Stream<Item = EngineResult<(uuid::Uuid, Vec<u8>)>> {
        stream! {
            let entries = transaction.scan_bytes(table);
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                let key = DocumentChangeKey::decode_ordered(&key).map_err(EngineError::custom)?;
                if key.r#type().is_metadata() {
                    yield Ok((Self::row_id(key.id())?, value));
                }
            }
        }
    }

    async fn merge_row_metadata(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        metadata: &[u8],
    ) -> EngineResult<()> {
        let key = DocumentChangeKey::new_metadata(Self::document_id(&row)).encode_ordered();
        transaction.put_bytes(table, key, metadata.to_vec()).await
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: uuid::Uuid,
        value: Row,
    ) -> EngineResult<()> {
        if Self::metadata_deleted(transaction, table, &row).await? {
            return Err(EngineError::InvalidQuery("Logical row is metadatad"));
        }
        let table_name = table;
        let columns = Self::columns(transaction, table_name, value.values.len()).await?;
        let id = Self::document_id(&row);
        self.store_row(transaction, table, id, &columns, &value)
            .await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: uuid::Uuid,
        row: &uuid::Uuid,
    ) -> EngineResult<Option<Row>> {
        self.delete_row(transaction, table, row).await
    }
}
