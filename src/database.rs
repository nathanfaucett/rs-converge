use alloc::format;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

#[cfg(feature = "in-memory")]
use engine::InMemoryKernel;
use engine::{Engine, EngineError, EngineResult};
use query::{QueryParams, QueryResult, Statement, Translator};
use schema::{IndexSchema, TableSchema};
#[cfg(feature = "sync")]
use sync::{
    SyncManifest, SyncStateUnit, apply_sync_state_for, export_sync_state_for, sync_manifest_for,
};
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

    #[cfg(feature = "sync")]
    pub async fn sync_manifest(&self) -> EngineResult<SyncManifest> {
        database_call!(self, |engine| sync_manifest_for(engine).await)
    }

    #[cfg(feature = "sync")]
    pub async fn export_sync_state(&self) -> EngineResult<Vec<SyncStateUnit>> {
        database_call!(self, |engine| export_sync_state_for(engine).await)
    }

    #[cfg(feature = "sync")]
    pub async fn apply_sync_state(&self, unit: SyncStateUnit) -> EngineResult<()> {
        database_call!(self, |engine| apply_sync_state_for(engine, unit).await)
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
}

#[cfg(all(test, feature = "automerge", feature = "in-memory", feature = "redb"))]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn in_memory_database_uses_the_engine_api() {
        block_on(async {
            let database = Database::in_memory();
            database
                .translate_and_execute(
                    "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                    &crate::SqlTranslator,
                )
                .await
                .unwrap();
            assert_eq!(database.table_schema("users").await.unwrap().name, "users");
        });
    }

    #[test]
    fn file_database_reopens_persisted_schema() {
        block_on(async {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let database_path = std::env::temp_dir()
                .join(format!("ofdb-database-{}-{nanos}.redb", std::process::id()));

            {
                let database = Database::open(&database_path).unwrap();
                database
                    .translate_and_execute(
                        "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                        &crate::SqlTranslator,
                    )
                    .await
                    .unwrap();
            }

            let database = Database::open(&database_path).unwrap();
            assert_eq!(database.table_schema("users").await.unwrap().name, "users");
            let _ = std::fs::remove_file(&database_path);
        });
    }
}
