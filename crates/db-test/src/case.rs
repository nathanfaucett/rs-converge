use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use db_engine::{DirectRowCodec, Engine, Kernel, RowCodec};
use db_engine_automerge::AutomergeRowCodec;
use db_engine_redb::RedbKernel;
use db_sql_translator::SqlTranslator;
use db_value::Row;
use futures::executor::block_on;

static DATABASE_ID: AtomicU64 = AtomicU64::new(0);

pub struct Case {
    pub name: &'static str,
    pub setup: &'static [&'static str],
    pub query: &'static str,
    pub expected: Vec<Row>,
}

pub fn assert_case(case: Case) {
    run_case("direct/in-memory", &case, || {
        Engine::new(db_engine::InMemoryKernel::new(), DirectRowCodec)
    });
    run_redb_case("direct/Redb", &case, || DirectRowCodec);
    run_redb_case("Automerge/Redb", &case, AutomergeRowCodec::new);
}

fn run_case<K, R>(backend: &str, case: &Case, new_engine: impl FnOnce() -> Engine<K, R>)
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
{
    let engine = new_engine();
    assert_rows(backend, case, block_on(execute(&engine, case)));
}

fn run_redb_case<R>(backend: &str, case: &Case, new_codec: impl FnOnce() -> R)
where
    R: RowCodec<db_engine_redb::RedbKernelTransaction>,
{
    let path = database_path();
    let engine = Engine::new(redb_kernel(&path), new_codec());
    let actual = block_on(execute(&engine, case));
    drop(engine);
    std::fs::remove_file(path).unwrap();
    assert_rows(backend, case, actual);
}

async fn execute<K, R>(engine: &Engine<K, R>, case: &Case) -> Vec<Row>
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
{
    for statement in case.setup {
        engine
            .translate_and_execute(statement, &SqlTranslator)
            .await
            .unwrap();
    }

    engine
        .translate_and_execute(case.query, &SqlTranslator)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .rows
}

fn assert_rows(backend: &str, case: &Case, actual: Vec<Row>) {
    assert_eq!(
        actual, case.expected,
        "{backend}: {}: {}",
        case.name, case.query
    );
}

fn redb_kernel(path: &PathBuf) -> RedbKernel {
    RedbKernel::new(Arc::new(redb::Database::create(path).unwrap()))
}

fn database_path() -> PathBuf {
    let id = DATABASE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("db-test-{}-{id}.redb", std::process::id()))
}
