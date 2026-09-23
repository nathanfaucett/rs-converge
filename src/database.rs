use alloc::format;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

#[cfg(feature = "in-memory")]
use engine::InMemoryKernel;
use engine::{
    Checkpoint, ColumnGenerationId, Engine, EngineError, EngineResult, EnvelopeId, EnvelopeOutcome,
    Frontier, IndexGenerationId, TableGenerationId, TransactionEnvelope,
};
use query::{QueryParams, QueryResult, Statement, Translator};
use schema::{IndexSchema, TableSchema};
use value::{FromRow, Row, Value};

#[cfg(feature = "automerge")]
use engine_automerge::AutomergeRowCodec;
#[cfg(feature = "redb")]
use engine_redb::{RedbKernel, redb};

/// An application-facing database using the Automerge row codec.
pub enum Database {
    #[cfg(all(feature = "automerge", feature = "redb"))]
    File(Engine<RedbKernel, AutomergeRowCodec>),
    #[cfg(all(feature = "automerge", feature = "in-memory"))]
    InMemory(Engine<InMemoryKernel, AutomergeRowCodec>),
}

macro_rules! database_call {
    ($database:expr, |$engine:ident| $body:expr) => {{
        match $database {
            #[cfg(all(feature = "automerge", feature = "redb"))]
            Database::File($engine) => $body,
            #[cfg(all(feature = "automerge", feature = "in-memory"))]
            Database::InMemory($engine) => $body,
            #[cfg(not(any(
                all(feature = "automerge", feature = "redb"),
                all(feature = "automerge", feature = "in-memory"),
            )))]
            _ => Err(EngineError::custom("no database backend is enabled")),
        }
    }};
}

impl Database {
    /// Open a database from a URI string.
    ///
    /// Supports `:in_memory:` and `ofdb://<path>` schemes. The path is
    /// interpreted as a filesystem path relative to the current working
    /// directory when it does not begin with `/`.
    ///
    /// Returns `EngineError::Custom` when the URI scheme is unsupported
    /// or when the required kernel features are not enabled.
    pub fn open_uri(uri: &str) -> Result<Self, EngineError> {
        match crate::uri::parse_uri(uri) {
            Ok(crate::uri::Uri {
                scheme: crate::uri::UriScheme::InMemory,
                ..
            }) => {
                #[cfg(all(feature = "automerge", feature = "in-memory"))]
                {
                    Ok(Self::in_memory())
                }
                #[cfg(not(all(feature = "automerge", feature = "in-memory")))]
                {
                    Err(EngineError::custom(
                        "in-memory kernel not available: enable features `automerge` and `in-memory`",
                    ))
                }
            }
            Ok(crate::uri::Uri {
                scheme: crate::uri::UriScheme::File,
                path,
            }) => {
                let path = path.ok_or_else(|| EngineError::custom("file URI missing path"))?;
                #[cfg(all(feature = "automerge", feature = "redb"))]
                {
                    Self::open(path)
                }
                #[cfg(not(all(feature = "automerge", feature = "redb")))]
                {
                    Err(EngineError::custom(
                        "redb kernel not available: enable features `automerge` and `redb`",
                    ))
                }
            }
            Err(crate::uri::UriError::UnsupportedScheme) => Err(EngineError::custom(format!(
                "unsupported URI scheme: {uri}"
            ))),
            Err(crate::uri::UriError::MissingPath) => {
                Err(EngineError::custom("file URI missing path"))
            }
        }
    }

    #[cfg(all(feature = "automerge", feature = "redb"))]
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, EngineError> {
        let database = redb::Database::create(path).map_err(EngineError::custom)?;
        Ok(Self::File(Engine::new(
            RedbKernel::new(std::sync::Arc::new(database)),
            AutomergeRowCodec::new(),
        )))
    }

    #[cfg(all(feature = "automerge", feature = "in-memory"))]
    pub fn in_memory() -> Self {
        Self::InMemory(Engine::new(InMemoryKernel::new(), AutomergeRowCodec::new()))
    }

    pub async fn index_schema(&self, name: &str) -> EngineResult<IndexSchema> {
        database_call!(self, |engine| engine.index_schema(name).await)
    }

    pub async fn index_lookup(&self, name: &str, values: &Row) -> EngineResult<Option<Row>> {
        database_call!(self, |engine| engine.index_lookup(name, values).await)
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        database_call!(self, |engine| engine.table_schema(name).await)
    }

    pub async fn table_generation_id(&self, name: &str) -> EngineResult<TableGenerationId> {
        database_call!(self, |engine| engine.table_generation_id(name).await)
    }

    pub async fn column_generation_id(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> EngineResult<ColumnGenerationId> {
        database_call!(self, |engine| engine
            .column_generation_id(table_name, column_name)
            .await)
    }

    pub async fn index_generation_id(&self, name: &str) -> EngineResult<IndexGenerationId> {
        database_call!(self, |engine| engine.index_generation_id(name).await)
    }

    pub async fn create_table(&self, table_schema: TableSchema) -> EngineResult<()> {
        database_call!(self, |engine| engine.create_table(table_schema).await)
    }

    pub async fn drop_table(&self, table_name: &str) -> EngineResult<()> {
        database_call!(self, |engine| engine.drop_table(table_name).await)
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
        database_call!(self, |engine| {
            engine
                .translate_and_execute_with_params(query, params, translator)
                .await
        })
    }

    pub async fn translate_and_execute<T>(
        &self,
        query: &str,
        translator: &T,
    ) -> EngineResult<Vec<QueryResult>>
    where
        T: Translator,
    {
        database_call!(self, |engine| engine
            .translate_and_execute(query, translator)
            .await)
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
        database_call!(self, |engine| engine
            .translate_and_select(query, translator)
            .await)
    }

    pub async fn execute(&self, statements: Vec<Statement>) -> EngineResult<Vec<QueryResult>> {
        database_call!(self, |engine| engine.execute(statements).await)
    }

    pub async fn frontier(&self) -> EngineResult<Frontier> {
        database_call!(self, |engine| engine.frontier().await)
    }

    pub async fn export_checkpoint(&self) -> EngineResult<Checkpoint> {
        database_call!(self, |engine| engine.export_checkpoint().await)
    }

    pub async fn import_checkpoint(&self, checkpoint: Checkpoint) -> EngineResult<()> {
        database_call!(self, |engine| engine.import_checkpoint(checkpoint).await)
    }

    pub async fn missing_envelopes(
        &self,
        frontier: &Frontier,
    ) -> EngineResult<Vec<TransactionEnvelope>> {
        database_call!(self, |engine| engine.missing_envelopes(frontier).await)
    }

    pub async fn envelope_outcome(&self, id: EnvelopeId) -> EngineResult<Option<EnvelopeOutcome>> {
        database_call!(self, |engine| engine.envelope_outcome(id).await)
    }

    pub async fn envelope_outcomes(&self) -> EngineResult<Vec<(EnvelopeId, EnvelopeOutcome)>> {
        database_call!(self, |engine| engine.envelope_outcomes().await)
    }

    pub async fn row_conflicts(&self, table_name: &str, key: &Row) -> EngineResult<Vec<String>> {
        database_call!(self, |engine| engine.row_conflicts(table_name, key).await)
    }

    pub async fn resolve_row(
        &self,
        table_name: &str,
        key: &Row,
        values: Vec<(String, Value)>,
    ) -> EngineResult<()> {
        database_call!(self, |engine| engine
            .resolve_row(table_name, key, values)
            .await)
    }

    pub async fn import_envelope(
        &self,
        envelope: TransactionEnvelope,
    ) -> EngineResult<EnvelopeOutcome> {
        database_call!(self, |engine| engine.import_envelope(envelope).await)
    }

    pub async fn import_envelope_bytes(&self, bytes: Vec<u8>) -> EngineResult<EnvelopeOutcome> {
        database_call!(self, |engine| engine.import_envelope_bytes(bytes).await)
    }
}
