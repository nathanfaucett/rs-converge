use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use db::{
    engine::Engine,
    redb_automerge::{AutomergeRowReconciler, RedbKernel},
    sql_translator::SqlTranslator,
};

fn database_path() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("db-redb-automerge-example-{nanos}.redb"))
}

#[tokio::main]
async fn main() {
    let path = database_path();
    let database = Arc::new(redb::Database::create(&path).expect("open Redb database"));
    let engine = Engine::new(RedbKernel::new(database), AutomergeRowReconciler);

    db_examples_util::run(engine, SqlTranslator).await;
    std::fs::remove_file(path).expect("remove Redb database");
}
