use std::{string::String, vec::Vec};

use async_stream::stream;
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AutomergeValue};
use db_btree::{BTreeRead, BTreeTransaction};
use db_btree_automerge::{AutomergeBTreeTransaction, DocumentId, ThresholdPolicy, get_document};
use db_engine::{
    ENGINE_TABLE_FIELDS_FIELD_COLUMN_ID, EngineError, EngineResult, KernelTransaction, RowCodec,
    RowGenerationId, TableGenerationId,
};
use db_value::{Row, Value};
use futures::{Stream, StreamExt, pin_mut};

use crate::change_log::{ChangeLogRead, ChangeLogTransaction};

const ROW_MAPPINGS: &str = "__db_engine_row_mappings";
const TOMBSTONES: &str = "__db_engine_tombstones";
const COLUMN_COUNT_BYTES: usize = size_of::<u32>();
const ENGINE_TABLE_FIELDS: &str = "table_fields";

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
    fn change_table(table: impl AsRef<str>) -> String {
        format!("__db_engine_changes_{}", table.as_ref())
    }

    fn row_key(table: impl AsRef<str>, key: &Row) -> Row {
        let table = table.as_ref();
        let mut values = Vec::with_capacity(key.values.len() + 1);
        values.push(Value::from(table));
        values.extend(key.values.clone());
        Row::new(values)
    }

    fn document_id(table: impl AsRef<str>, key: &Row) -> EngineResult<DocumentId> {
        let table = table.as_ref();
        let key = postcard::to_allocvec(key).map_err(EngineError::custom)?;
        let mut id = Vec::with_capacity(table.len() + key.len() + 4);
        id.extend_from_slice(&(table.len() as u32).to_be_bytes());
        id.extend_from_slice(table.as_bytes());
        id.extend_from_slice(&key);
        Ok(id)
    }

    async fn row_document_id<T>(
        transaction: &T,
        table: impl AsRef<str>,
        key: &Row,
    ) -> EngineResult<Option<(DocumentId, usize)>>
    where
        T: KernelTransaction,
    {
        let mapping_key = Self::row_key(table, key);
        let Some(mapping) = transaction.get_entry(ROW_MAPPINGS, &mapping_key).await? else {
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

    async fn columns<T>(
        transaction: &T,
        table: impl AsRef<str>,
        fallback_count: usize,
    ) -> EngineResult<Vec<Column>>
    where
        T: KernelTransaction,
    {
        let table_id = uuid::Uuid::parse_str(table.as_ref()).map_err(EngineError::custom)?;
        let fields = transaction.scan_entries(ENGINE_TABLE_FIELDS);
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
        table: impl AsRef<str>,
        id: &DocumentId,
    ) -> EngineResult<Option<AutoCommit>>
    where
        T: KernelTransaction,
    {
        get_document(
            &ChangeLogRead::new(transaction, Self::change_table(table)),
            id,
        )
        .await
        .map_err(EngineError::custom)
    }

    fn changes<'a, T>(
        transaction: &'a mut T,
        table: &'a str,
    ) -> AutomergeBTreeTransaction<ChangeLogTransaction<'a, T>>
    where
        T: KernelTransaction + Send,
    {
        AutomergeBTreeTransaction::new(
            ChangeLogTransaction::new(transaction, Self::change_table(table)),
            ThresholdPolicy::default(),
        )
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

    fn table_entry_key(entry: (Row, Row), table: &str) -> Option<Row> {
        (entry.0.values.first().and_then(Value::as_text) == Some(table)).then_some(entry.0)
    }

    async fn put_row_mapping<T>(
        transaction: &mut T,
        table: &str,
        key: &Row,
        id: DocumentId,
        column_count: usize,
    ) -> EngineResult<()>
    where
        T: KernelTransaction,
    {
        transaction
            .put_entry(
                ROW_MAPPINGS,
                Self::row_key(table, key),
                Row::new(vec![Value::Blob(id), Value::Integer(column_count as i64)]),
            )
            .await
    }

    async fn ensure_row_mapping<T>(
        transaction: &mut T,
        table: &str,
        key: &Row,
        column_count: usize,
    ) -> EngineResult<DocumentId>
    where
        T: KernelTransaction,
    {
        match Self::row_document_id(transaction, table, key).await? {
            Some((id, existing_count)) if existing_count >= column_count => Ok(id),
            Some((id, _)) => {
                Self::put_row_mapping(transaction, table, key, id.clone(), column_count).await?;
                Ok(id)
            }
            None => {
                let id = Self::document_id(table, key)?;
                Self::put_row_mapping(transaction, table, key, id.clone(), column_count).await?;
                Ok(id)
            }
        }
    }

    async fn table_entry_keys<T>(
        transaction: &T,
        entries_table: &str,
        table: &str,
    ) -> EngineResult<Vec<Row>>
    where
        T: KernelTransaction,
    {
        let entries = transaction.scan_entries(entries_table);
        pin_mut!(entries);
        let mut keys = Vec::new();
        while let Some(entry) = entries.next().await {
            if let Some(key) = Self::table_entry_key(entry.map_err(EngineError::custom)?, table) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    async fn remove_table_entries<T>(
        transaction: &mut T,
        entries_table: &str,
        table: &str,
    ) -> EngineResult<()>
    where
        T: KernelTransaction,
    {
        for key in Self::table_entry_keys(transaction, entries_table, table).await? {
            transaction.remove_entry(entries_table, &key).await?;
        }
        Ok(())
    }

    async fn store_row<T>(
        &self,
        transaction: &mut T,
        table: &str,
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
            let values: EngineResult<Vec<_>> = value
                .values
                .iter()
                .zip(columns)
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
    async fn ensure_table(
        &self,
        transaction: &mut T,
        table: TableGenerationId,
    ) -> EngineResult<()> {
        transaction
            .ensure_table(&Self::change_table(table.0.to_string()))
            .await?;

        transaction.ensure_table(ROW_MAPPINGS).await?;
        transaction.ensure_table(TOMBSTONES).await?;
        transaction.ensure_table(ENGINE_TABLE_FIELDS).await
    }

    async fn drop_table(&self, transaction: &mut T, table: TableGenerationId) -> EngineResult<()> {
        let table = table.0.to_string();
        Self::remove_table_entries(transaction, ROW_MAPPINGS, &table).await?;
        Self::remove_table_entries(transaction, TOMBSTONES, &table).await?;
        transaction.drop_table(&Self::change_table(table)).await
    }

    async fn get_row(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Row>> {
        let table = &table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        if transaction
            .get_entry(TOMBSTONES, &Self::row_key(table, &key))
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let Some((id, column_count)) = Self::row_document_id(transaction, &table, &key).await?
        else {
            return Ok(None);
        };
        let Some(document) = Self::document(transaction, &table, &id).await? else {
            return Err(EngineError::custom(
                "Primary-key mapping has no logical row",
            ));
        };
        let columns = Self::columns(transaction, &table, column_count).await?;
        Self::decode_row(&document, &columns).map(Some)
    }

    fn scan_rows(
        &self,
        transaction: &T,
        table: &TableGenerationId,
    ) -> impl Stream<Item = EngineResult<(RowGenerationId, Row)>> {
        let table = table.0.to_string();
        stream! {
            let mappings = transaction.scan_entries(ROW_MAPPINGS);
            for await entry in mappings {
                let (mapping_key, mapping) = entry.map_err(EngineError::custom)?;
                if mapping_key.values.first().and_then(Value::as_text) != Some(table.as_str()) {
                    continue;
                }
                let key = Row::new(mapping_key.values[1..].to_vec());
                let Some(row) = key.values.first().and_then(Value::as_uuid).copied() else {
                    yield Err(EngineError::custom("Invalid row-generation mapping"));
                    continue;
                };
                if transaction
                    .get_entry(TOMBSTONES, &Self::row_key(&table, &key))
                    .await?
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
                let Some(document) = Self::document(transaction, &table, id).await?
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
                yield Self::decode_row(&document, &columns).map(|value| (RowGenerationId(row), value));
            }
        }
    }

    async fn encode_row(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row_id: &RowGenerationId,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table = table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row_id.0)]);
        let id = Self::document_id(&table, &key)?;
        let mut document = Self::document(transaction, &table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, &table, row.values.len()).await?;
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
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Vec<usize>> {
        let table = table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        let Some((id, column_count)) = Self::row_document_id(transaction, &table, &key).await?
        else {
            return Ok(Vec::new());
        };
        let Some(document) = Self::document(transaction, &table, &id).await? else {
            return Ok(Vec::new());
        };
        Self::conflicted_columns(
            &document,
            &Self::columns(transaction, &table, column_count).await?,
        )
    }

    async fn encode_resolution(
        &self,
        transaction: &T,
        table: &TableGenerationId,
        row_id: &RowGenerationId,
        row: &Row,
        changed_columns: &[usize],
    ) -> EngineResult<Vec<u8>> {
        let table = table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row_id.0)]);
        let id = Self::document_id(&table, &key)?;
        let mut document = Self::document(transaction, &table, &id)
            .await?
            .unwrap_or_else(AutoCommit::new)
            .with_actor(self.actor.clone());
        let columns = Self::columns(transaction, &table, row.values.len()).await?;
        Self::update_document_columns(&mut document, row, &columns, changed_columns)?;
        Self::encode_incremental(columns.len(), document.save_incremental())
    }

    async fn merge_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: RowGenerationId,
        value: &[u8],
    ) -> EngineResult<Option<Row>> {
        let table = &table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        if transaction
            .get_entry(TOMBSTONES, &Self::row_key(table, &key))
            .await?
            .is_some()
        {
            return Ok(None);
        }

        let (column_count, incremental) = Self::decode_incremental(value)?;
        let id = match Self::row_document_id(transaction, table, &key).await? {
            Some((id, existing_column_count)) => {
                if column_count > existing_column_count {
                    transaction
                        .put_entry(
                            ROW_MAPPINGS,
                            Self::row_key(table, &key),
                            Row::new(vec![
                                Value::Blob(id.clone()),
                                Value::Integer(column_count as i64),
                            ]),
                        )
                        .await?;
                }
                id
            }
            None => {
                let id = Self::document_id(table, &key)?;
                transaction
                    .put_entry(
                        ROW_MAPPINGS,
                        Self::row_key(table, &key),
                        Row::new(vec![
                            Value::Blob(id.clone()),
                            Value::Integer(column_count as i64),
                        ]),
                    )
                    .await?;
                id
            }
        };

        let existing = Self::document(transaction, table, &id).await?;
        let has_existing = existing.is_some();
        let mut document = existing.unwrap_or_else(AutoCommit::new);
        document
            .load_incremental(incremental)
            .map_err(EngineError::custom)?;
        let columns = Self::columns(transaction, table, column_count).await?;
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
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Vec<u8>>> {
        let table = table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        let Some((id, _)) = Self::row_document_id(transaction, &table, &key).await? else {
            return Ok(None);
        };
        Self::document(transaction, &table, &id)
            .await?
            .map(|mut document| Ok(document.save()))
            .transpose()
    }

    async fn merge_row_state(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: RowGenerationId,
        state: &[u8],
    ) -> EngineResult<Option<Row>> {
        let table = table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        if transaction
            .get_entry(TOMBSTONES, &Self::row_key(&table, &key))
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let mut incoming = AutoCommit::load(state).map_err(EngineError::custom)?;
        let id = Self::document_id(&table, &key)?;
        let existing = Self::document(transaction, &table, &id).await?;
        let mut document = existing.clone().unwrap_or_else(AutoCommit::new);
        if existing.is_some() {
            document.merge(&mut incoming).map_err(EngineError::custom)?;
        } else {
            document = incoming;
        }
        let columns = Self::columns(transaction, &table, 0).await?;
        let row_value = Self::decode_row(&document, &columns)?;
        transaction
            .put_entry(
                ROW_MAPPINGS,
                Self::row_key(&table, &key),
                Row::new(vec![
                    Value::Blob(id.clone()),
                    Value::Integer(columns.len() as i64),
                ]),
            )
            .await?;
        let mut changes = Self::changes(transaction, &table);
        if existing.is_some() {
            changes.remove(&id).await.map_err(EngineError::custom)?;
        }
        changes
            .insert(id, document)
            .await
            .map_err(EngineError::custom)?;
        Ok(Some(row_value))
    }

    async fn tombstone_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Row>> {
        let value = self.get_row(transaction, table, row).await?;
        let table = table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        let mapping_key = Self::row_key(&table, &key);
        transaction.remove_entry(ROW_MAPPINGS, &mapping_key).await?;
        transaction
            .put_entry(TOMBSTONES, mapping_key, Row::default())
            .await?;
        Ok(value)
    }

    fn row_tombstones(
        &self,
        transaction: &T,
        table: &TableGenerationId,
    ) -> impl Stream<Item = EngineResult<RowGenerationId>> {
        let table = table.0.to_string();
        futures::StreamExt::filter_map(transaction.scan_entries(TOMBSTONES), move |entry| {
            let table = table.clone();
            async move {
                match entry {
                    Ok((key, _))
                        if key.values.first().and_then(Value::as_text) == Some(table.as_str()) =>
                    {
                        key.values
                            .get(1)
                            .and_then(Value::as_uuid)
                            .copied()
                            .map(RowGenerationId)
                            .map(Ok)
                    }
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            }
        })
    }

    async fn put_row(
        &self,
        transaction: &mut T,
        table: TableGenerationId,
        row: RowGenerationId,
        value: Row,
    ) -> EngineResult<()> {
        let table = &table.0.to_string();
        let key = Row::new(vec![Value::Uuid(row.0)]);
        if transaction
            .get_entry(TOMBSTONES, &Self::row_key(table, &key))
            .await?
            .is_some()
        {
            return Err(EngineError::InvalidQuery("Logical row is tombstoned"));
        }
        let columns = Self::columns(transaction, table, value.values.len()).await?;
        let id = Self::ensure_row_mapping(transaction, table, &key, columns.len()).await?;
        self.store_row(transaction, table, id, &columns, &value)
            .await
    }

    async fn remove_row(
        &self,
        transaction: &mut T,
        table: &TableGenerationId,
        row: &RowGenerationId,
    ) -> EngineResult<Option<Row>> {
        self.tombstone_row(transaction, table, row).await
    }
}
