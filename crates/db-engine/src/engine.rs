#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};
use db_schema::{IndexSchema, TableSchema};
use futures::{StreamExt, pin_mut};
#[cfg(feature = "std")]
use std::sync::Arc;

use thiserror::Error;

use db_query::{QueryParams, QueryResult, Statement, TranslateError, Translator};

use crate::{
    catalog::{
        ENGINE_CHANGES, ENGINE_INDEX_FIELDS, ENGINE_INDICES, ENGINE_TABLE_FIELDS, ENGINE_TABLES,
    },
    change::{Change, ChangeReplication, apply_change, ensure_change_log},
    codec::RowCodec,
    executor::execute_statement,
    index::{ENGINE_INDEX_RECORDS, index_schema},
    kernel::{Kernel, KernelTransaction},
};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Translate error: {0}")]
    TranslateError(#[from] TranslateError),

    #[error("Unsupported query shape for MVP executor: {0}")]
    Unsupported(&'static str),

    #[error("Invalid query: {0}")]
    InvalidQuery(&'static str),

    #[error("Error: {0}")]
    Custom(String),
}

impl EngineError {
    pub fn custom<T>(error: T) -> Self
    where
        T: ToString,
    {
        Self::Custom(error.to_string())
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Clone)]
pub struct Engine<K, R> {
    pub(crate) kernel: Arc<K>,
    pub(crate) reconciler: Arc<R>,
}

impl<K, R> From<(K, R)> for Engine<K, R> {
    fn from((kernel, reconciler): (K, R)) -> Self {
        Self {
            kernel: Arc::new(kernel),
            reconciler: Arc::new(reconciler),
        }
    }
}

impl<K, R> Engine<K, R> {
    pub fn new(kernel: K, reconciler: R) -> Self {
        Self::from((kernel, reconciler))
    }
}

impl<K, R> Engine<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
{
    pub async fn index_schema(&self, name: &str) -> EngineResult<IndexSchema> {
        let transaction = self.kernel.transaction().await?;
        let schema = index_schema(&transaction, name).await?;
        transaction.rollback().await?;
        schema.ok_or(EngineError::InvalidQuery("Index not found"))
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        let transaction = self.kernel.transaction().await?;
        let schema = crate::executor::table_schema(&transaction, name).await?;
        transaction.rollback().await?;
        Ok(schema)
    }

    pub async fn create_table(&self, table_schema: TableSchema) -> EngineResult<()> {
        self.execute(vec![Statement::DataDefinition(
            db_query::DataDefinition::CreateTable {
                schema: table_schema,
                if_not_exists: false,
            },
        )])
        .await?;
        Ok(())
    }

    pub async fn drop_table(&self, _table_name: &str) -> EngineResult<()> {
        Err(EngineError::Unsupported("DROP TABLE is not supported"))
    }

    pub async fn translate_and_execute_with_params<T>(
        &self,
        query: &str,
        params: Option<&QueryParams>,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate_with_params(query, params).await?;
        self.execute(statements).await
    }

    pub async fn translate_and_execute<T>(
        &self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        let statements = translator.translate(query).await?;
        self.execute(statements).await
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
        execute_statement(self, statements).await
    }

    pub async fn apply_changes(&self, changes: Vec<Change>) -> EngineResult<()> {
        let mut transaction = self.kernel.transaction().await?;
        for table in [
            ENGINE_TABLES,
            ENGINE_TABLE_FIELDS,
            ENGINE_INDICES,
            ENGINE_INDEX_FIELDS,
            ENGINE_INDEX_RECORDS,
        ] {
            transaction.ensure_table(table).await?;
        }
        ensure_change_log(&mut transaction).await?;
        for change in changes {
            apply_change(&mut transaction, self.reconciler.as_ref(), change).await?;
        }
        transaction.commit().await
    }
}

impl<K, R> ChangeReplication for Engine<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
{
    type Cursor = i64;

    async fn changes_since(
        &self,
        cursor: Option<&Self::Cursor>,
    ) -> EngineResult<(Self::Cursor, Vec<Change>)> {
        let transaction = self.kernel.transaction().await?;
        let mut changes = Vec::new();
        let mut next = cursor.copied().unwrap_or_default();
        {
            let entries = transaction.scan_entries(ENGINE_CHANGES);
            pin_mut!(entries);
            while let Some(entry) = entries.next().await {
                let (key, value) = entry?;
                let Some(sequence) = key.values.first().and_then(db_value::Value::to_integer)
                else {
                    return Err(EngineError::custom("Invalid canonical change sequence"));
                };
                if cursor.is_some_and(|cursor| sequence <= *cursor) {
                    continue;
                }
                let Some(bytes) = value.values.first().and_then(db_value::Value::as_blob) else {
                    return Err(EngineError::custom("Invalid canonical change"));
                };
                let change: Change = postcard::from_bytes(bytes).map_err(EngineError::custom)?;
                next = next.max(sequence);
                changes.push((sequence, change));
            }
        }
        changes.sort_by_key(|(sequence, _)| *sequence);
        transaction.rollback().await?;
        Ok((
            next,
            changes.into_iter().map(|(_, change)| change).collect(),
        ))
    }

    async fn apply_changes(&self, changes: Vec<Change>) -> EngineResult<()> {
        Engine::apply_changes(self, changes).await
    }
}
