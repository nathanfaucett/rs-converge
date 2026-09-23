#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};
use schema::{IndexSchema, TableSchema};
use value::{FromRow, Row, Value};

#[cfg(feature = "std")]
use std::sync::Arc;

use thiserror::Error;

use query::{QueryParams, QueryResult, Statement, TranslateError, Translator};

use crate::{
    Checkpoint, ColumnGenerationId, EnvelopeId, EnvelopeOutcome, Frontier, IndexGenerationId,
    TableGenerationId, TransactionEnvelope,
    codec::RowCodec,
    envelope::{
        checkpoint, ensure_envelope_log, envelopes_missing, frontier, import as import_envelope,
        import_checkpoint, outcome, outcomes, quarantine,
    },
    executor::{
        bootstrap_internal_tables, execute_statement, resolve_row as resolve_conflicted_row,
        row_conflicts as conflicted_row_columns,
    },
    index::{index_generation_id, index_schema, lookup as index_lookup},
    kernel::{Kernel, KernelTransaction},
    schema::ensure,
};

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Translate error: {0}")]
    TranslateError(#[from] TranslateError),

    #[error("Unsupported query shape for MVP executor: {0}")]
    Unsupported(&'static str),

    #[error("Invalid query: {0}")]
    InvalidQuery(&'static str),

    #[error("A timestamp provider is required to generate a UUID")]
    MissingTimestampProvider,

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

pub type TimestampProvider = fn() -> Option<uuid::Timestamp>;

#[cfg(feature = "std")]
fn default_timestamp_provider() -> Option<uuid::Timestamp> {
    Some(uuid::Timestamp::now(uuid::NoContext))
}

#[cfg(not(feature = "std"))]
fn default_timestamp_provider() -> Option<uuid::Timestamp> {
    None
}

#[derive(Clone)]
pub struct Engine<K, R> {
    pub(crate) kernel: Arc<K>,
    pub(crate) reconciler: Arc<R>,
    pub(crate) timestamp_provider: TimestampProvider,
}

impl<K, R> From<(K, R)> for Engine<K, R> {
    fn from((kernel, reconciler): (K, R)) -> Self {
        Self {
            kernel: Arc::new(kernel),
            reconciler: Arc::new(reconciler),
            timestamp_provider: default_timestamp_provider,
        }
    }
}

impl<K, R> Engine<K, R> {
    pub fn new(kernel: K, reconciler: R) -> Self {
        Self::from((kernel, reconciler))
    }

    pub fn with_timestamp_provider(
        kernel: K,
        reconciler: R,
        timestamp_provider: TimestampProvider,
    ) -> Self {
        Self {
            kernel: Arc::new(kernel),
            reconciler: Arc::new(reconciler),
            timestamp_provider,
        }
    }
}

impl<K, R> Engine<K, R>
where
    K: Kernel,
    R: RowCodec<K::Transaction> + Send + Sync,
{
    pub async fn index_schema(&self, name: &str) -> EngineResult<IndexSchema> {
        let transaction = self.kernel.transaction().await?;
        let schema = index_schema(&transaction, name).await?;
        transaction.rollback().await?;
        schema.ok_or(EngineError::InvalidQuery("Index not found"))
    }

    pub async fn index_lookup(&self, name: &str, values: &Row) -> EngineResult<Option<Row>> {
        let transaction = self.kernel.transaction().await?;
        let row = index_lookup(&transaction, self.reconciler.as_ref(), name, values).await?;
        transaction.rollback().await?;
        Ok(row)
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        let transaction = self.kernel.transaction().await?;
        let schema = crate::executor::table_schema(&transaction, name).await?;
        transaction.rollback().await?;
        Ok(schema)
    }

    pub async fn table_generation_id(&self, name: &str) -> EngineResult<TableGenerationId> {
        let transaction = self.kernel.transaction().await?;
        let id = crate::executor::table_generation_id(&transaction, name).await?;
        transaction.rollback().await?;
        Ok(id)
    }

    pub async fn column_generation_id(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> EngineResult<ColumnGenerationId> {
        let transaction = self.kernel.transaction().await?;
        let id =
            crate::executor::column_generation_id(&transaction, table_name, column_name).await?;
        transaction.rollback().await?;
        Ok(id)
    }

    pub async fn index_generation_id(&self, name: &str) -> EngineResult<IndexGenerationId> {
        let transaction = self.kernel.transaction().await?;
        let id = index_generation_id(&transaction, name).await?;
        transaction.rollback().await?;
        Ok(id)
    }

    pub async fn create_table(&self, table_schema: TableSchema) -> EngineResult<()> {
        self.execute(vec![Statement::DataDefinition(
            query::DataDefinition::CreateTable {
                schema: table_schema,
                if_not_exists: false,
            },
        )])
        .await?;
        Ok(())
    }

    pub async fn drop_table(&self, table_name: &str) -> EngineResult<()> {
        self.execute(vec![Statement::DataDefinition(
            query::DataDefinition::DropTable {
                table_name: String::from(table_name),
                if_exists: false,
            },
        )])
        .await?;
        Ok(())
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

    pub async fn translate_and_select<T, U>(
        &self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<U>>
    where
        T: Translator,
        U: FromRow,
    {
        let mut results = self.translate_and_execute(query, translator).await?;
        if results.len() != 1 {
            return Err(EngineError::InvalidQuery("Expected one query result"));
        }
        results
            .pop()
            .expect("result length was checked")
            .rows_as()
            .map_err(EngineError::custom)
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
        execute_statement(self, statements).await
    }

    pub async fn frontier(&self) -> EngineResult<Frontier> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let result = frontier(&transaction).await?;
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn export_checkpoint(&self) -> EngineResult<Checkpoint> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let result = checkpoint(&transaction, self.reconciler.as_ref()).await?;
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn import_checkpoint(&self, checkpoint: Checkpoint) -> EngineResult<()> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let result =
            import_checkpoint(&mut transaction, self.reconciler.as_ref(), checkpoint).await;
        match result {
            Ok(()) => transaction.commit().await,
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn missing_envelopes(
        &self,
        frontier: &Frontier,
    ) -> EngineResult<Vec<TransactionEnvelope>> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let result = envelopes_missing(&transaction, frontier).await?;
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn envelope_outcome(&self, id: EnvelopeId) -> EngineResult<Option<EnvelopeOutcome>> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let result = outcome(&transaction, id).await?;
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn envelope_outcomes(&self) -> EngineResult<Vec<(EnvelopeId, EnvelopeOutcome)>> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let outcomes = outcomes(&transaction).await?;
        transaction.commit().await?;
        Ok(outcomes)
    }

    pub async fn row_conflicts(&self, table_name: &str, key: &Row) -> EngineResult<Vec<String>> {
        let transaction = self.kernel.transaction().await?;
        let result =
            conflicted_row_columns(&transaction, self.reconciler.as_ref(), table_name, key).await?;
        transaction.rollback().await?;
        Ok(result)
    }

    pub async fn resolve_row(
        &self,
        table_name: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> EngineResult<()> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let result = resolve_conflicted_row(
            &mut transaction,
            self.reconciler.as_ref(),
            self.timestamp_provider,
            table_name,
            key,
            values,
        )
        .await;
        match result {
            Ok(()) => transaction.commit().await,
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    pub async fn import_envelope(
        &self,
        envelope: TransactionEnvelope,
    ) -> EngineResult<EnvelopeOutcome> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let outcome = import_envelope(&mut transaction, self.reconciler.as_ref(), envelope).await?;
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn import_envelope_bytes(&self, bytes: Vec<u8>) -> EngineResult<EnvelopeOutcome> {
        let envelope = match TransactionEnvelope::decode(&bytes) {
            Ok(envelope) => envelope,
            Err(error) => return self.quarantine_envelope(bytes, error.to_string()).await,
        };
        self.import_envelope(envelope).await
    }

    async fn quarantine_envelope(
        &self,
        bytes: Vec<u8>,
        reason: String,
    ) -> EngineResult<EnvelopeOutcome> {
        let mut transaction = self.kernel.transaction().await?;
        ensure_replication_tables(&mut transaction, self.reconciler.as_ref()).await?;
        let outcome = quarantine(&mut transaction, bytes, reason).await?;
        transaction.commit().await?;
        Ok(outcome)
    }
}

async fn ensure_replication_tables<T, R>(transaction: &mut T, codec: &R) -> EngineResult<()>
where
    T: KernelTransaction,
    R: RowCodec<T>,
{
    ensure(transaction).await?;
    bootstrap_internal_tables(transaction, codec, &mut Vec::new()).await?;
    ensure_envelope_log(transaction).await
}
