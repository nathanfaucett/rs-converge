use std::{sync::Arc, vec::Vec};

use async_stream::stream;
use automerge::transaction::Transactable;
use automerge::{AutoCommit, ROOT, ReadDoc, ScalarValue, Value as AutomergeValue};
use db_btree::{BTreeRead, BTreeTransaction};
use db_btree_automerge::{AutomergeBTreeTransaction, DocumentId, ThresholdPolicy};
use db_btree_redb::{RedbDatabase, RedbDatabaseTransaction};
use db_engine::{EngineError, EngineResult, Kernel, KernelTransaction};
use db_value::{Row, Value};
use futures::{Stream, StreamExt, pin_mut};

use crate::change_log::{ChangeKey, RedbChangeLogTransaction};

const ROW_MAPPINGS: &str = "__db_engine_row_mappings";
const TOMBSTONES: &str = "__db_engine_tombstones";
const CHANGE_LOG_PREFIX: &str = "__db_engine_changes_";
const CATALOG_TABLES: [&str; 4] = ["tables", "table_fields", "indices", "index_fields"];

#[derive(Clone)]
pub struct RedbAutomergeKernel {
    database: RedbDatabase,
}

impl RedbAutomergeKernel {
    pub fn new(database: Arc<redb::Database>) -> Self {
        Self {
            database: RedbDatabase::new(database),
        }
    }

    pub fn from_database(database: RedbDatabase) -> Self {
        Self { database }
    }
}

pub struct RedbAutomergeKernelTransaction {
    database: RedbDatabaseTransaction,
}

impl Kernel for RedbAutomergeKernel {
    type Transaction = RedbAutomergeKernelTransaction;

    async fn transaction(&self) -> EngineResult<Self::Transaction> {
        let database = self.database.transaction().map_err(EngineError::custom)?;
        for table in CATALOG_TABLES.into_iter().chain([ROW_MAPPINGS, TOMBSTONES]) {
            database
                .create_table::<Row, Row>(table)
                .map_err(EngineError::custom)?;
        }
        Ok(RedbAutomergeKernelTransaction { database })
    }
}

impl RedbAutomergeKernelTransaction {
    fn records(&self, table: &str) -> db_btree_redb::RedbBTreeScopedTransaction<'_, Row, Row> {
        self.database.table(table)
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

    fn mapping(&self) -> db_btree_redb::RedbBTreeScopedTransaction<'_, Row, Row> {
        self.records(ROW_MAPPINGS)
    }

    async fn row_document_id(
        &self,
        table: &str,
        key: &Row,
    ) -> EngineResult<Option<(DocumentId, usize)>> {
        let mapping_key = Self::row_key(table, key);
        let Some(mapping) = self
            .mapping()
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

    fn changes(&self, table: &str) -> AutomergeBTreeTransaction<RedbChangeLogTransaction<'_>> {
        let changes = self
            .database
            .table::<ChangeKey, Vec<u8>>(format!("{CHANGE_LOG_PREFIX}{table}"));
        AutomergeBTreeTransaction::new(
            RedbChangeLogTransaction::new(changes),
            ThresholdPolicy::default(),
        )
    }

    fn decode_row(document: &AutoCommit, columns: usize) -> EngineResult<Row> {
        let mut values = Vec::with_capacity(columns);
        for index in 0..columns {
            let values_for_column = document
                .get_all(ROOT, index.to_string())
                .map_err(EngineError::custom)?;
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

    fn encode_row(row: &Row) -> EngineResult<AutoCommit> {
        let mut document = AutoCommit::new();
        for (index, value) in row.values.iter().enumerate() {
            let bytes = postcard::to_allocvec(value).map_err(EngineError::custom)?;
            document
                .put(ROOT, index.to_string(), ScalarValue::Bytes(bytes))
                .map_err(EngineError::custom)?;
        }
        Ok(document)
    }
}

impl KernelTransaction for RedbAutomergeKernelTransaction {
    async fn create_table(&mut self, name: &str) -> EngineResult<()> {
        self.database
            .create_table::<ChangeKey, Vec<u8>>(&format!("{CHANGE_LOG_PREFIX}{name}"))
            .map_err(EngineError::custom)
    }

    async fn drop_table(&mut self, name: &str) -> EngineResult<()> {
        for table in [ROW_MAPPINGS, TOMBSTONES] {
            let keys = {
                let records = self.records(table);
                let entries = records.range(..);
                pin_mut!(entries);
                let mut keys = Vec::new();
                while let Some(entry) = entries.next().await {
                    let (key, _) = entry.map_err(EngineError::custom)?;
                    if key.values.first().and_then(Value::as_text) == Some(name) {
                        keys.push(key);
                    }
                }
                keys
            };
            for key in keys {
                self.records(table)
                    .remove(&key)
                    .await
                    .map_err(EngineError::custom)?;
            }
        }
        self.database
            .drop_table::<ChangeKey, Vec<u8>>(&format!("{CHANGE_LOG_PREFIX}{name}"))
            .map_err(EngineError::custom)?;
        Ok(())
    }

    async fn get_record(&self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        self.records(table)
            .get(key)
            .await
            .map_err(EngineError::custom)
    }

    fn scan_records(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>> {
        stream! {
            let records = self.records(table);
            let entries = records.range(..);
            for await entry in entries {
                yield entry.map_err(EngineError::custom);
            }
        }
    }

    async fn put_record(&mut self, table: &str, key: Row, value: Row) -> EngineResult<()> {
        self.records(table)
            .insert(key, value)
            .await
            .map_err(EngineError::custom)
    }

    async fn remove_record(&mut self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        self.records(table)
            .remove(key)
            .await
            .map_err(EngineError::custom)
    }

    async fn get_row(&self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        if self
            .records(TOMBSTONES)
            .get(&Self::row_key(table, key))
            .await
            .map_err(EngineError::custom)?
            .is_some()
        {
            return Ok(None);
        }
        let Some((id, column_count)) = self.row_document_id(table, key).await? else {
            return Ok(None);
        };
        let Some(document) = self
            .changes(table)
            .get(&id)
            .await
            .map_err(EngineError::custom)?
        else {
            return Err(EngineError::custom(
                "Primary-key mapping has no logical row",
            ));
        };
        Self::decode_row(&document, column_count).map(Some)
    }

    fn scan_rows(&self, table: &str) -> impl Stream<Item = EngineResult<(Row, Row)>> {
        let table = table.to_owned();
        stream! {
            let records = self.records(ROW_MAPPINGS);
            let mappings = records.range(..);
            for await entry in mappings {
                let (mapping_key, mapping) = entry.map_err(EngineError::custom)?;
                if mapping_key.values.first().and_then(Value::as_text) != Some(table.as_str()) {
                    continue;
                }
                let key = Row::new(mapping_key.values[1..].to_vec());
                if self
                    .records(TOMBSTONES)
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
                let changes = self.changes(&table);
                let Some(document) = changes.get(id).await.map_err(EngineError::custom)? else {
                    yield Err(EngineError::custom("Primary-key mapping has no logical row"));
                    continue;
                };
                yield Self::decode_row(&document, column_count).map(|row| (key, row));
            }
        }
    }

    async fn put_row(&mut self, table: &str, key: Row, value: Row) -> EngineResult<()> {
        if self
            .records(TOMBSTONES)
            .get(&Self::row_key(table, &key))
            .await
            .map_err(EngineError::custom)?
            .is_some()
        {
            return Err(EngineError::InvalidQuery("Logical row is tombstoned"));
        }
        let id = match self.row_document_id(table, &key).await? {
            Some((id, column_count)) => {
                if column_count != value.values.len() {
                    return Err(EngineError::InvalidQuery(
                        "Logical row has the wrong column count",
                    ));
                }
                id
            }
            None => {
                let id = Self::document_id(table, &key)?;
                self.mapping()
                    .insert(
                        Self::row_key(table, &key),
                        Row::new(vec![
                            Value::Blob(id.clone()),
                            Value::Integer(value.values.len() as i64),
                        ]),
                    )
                    .await
                    .map_err(EngineError::custom)?;
                id
            }
        };
        let mut changes = self.changes(table);
        if let Some(document) = changes.get(&id).await.map_err(EngineError::custom)? {
            Self::decode_row(&document, value.values.len())?;
            changes
                .update(id, |document| {
                    for (index, value) in value.values.iter().enumerate() {
                        let bytes =
                            postcard::to_allocvec(value).map_err(db_btree::BTreeError::custom)?;
                        document
                            .put(ROOT, index.to_string(), ScalarValue::Bytes(bytes))
                            .map_err(db_btree::BTreeError::custom)?;
                    }
                    Ok(())
                })
                .await
                .map_err(EngineError::custom)?;
        } else {
            changes
                .insert(id, Self::encode_row(&value)?)
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(())
    }

    async fn remove_row(&mut self, table: &str, key: &Row) -> EngineResult<Option<Row>> {
        let row = self.get_row(table, key).await?;
        if row.is_some() {
            self.records(TOMBSTONES)
                .insert(Self::row_key(table, key), Row::default())
                .await
                .map_err(EngineError::custom)?;
        }
        Ok(row)
    }

    async fn commit(self) -> EngineResult<()> {
        self.database.commit().map_err(EngineError::custom)
    }

    async fn rollback(self) -> EngineResult<()> {
        self.database.rollback().map_err(EngineError::custom)
    }
}
