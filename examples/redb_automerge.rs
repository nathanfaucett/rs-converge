use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use converge::{AutomergeRowCodec, Engine, RedbKernel, SqlTranslator, redb};

fn database_path() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("db-redb-automerge-example-{nanos}.redb"))
}

fn main() {
    futures::executor::block_on(async {
        let path = database_path();
        let database = Arc::new(reconverge::Database::create(&path).expect("open Redb database"));
        let engine = Engine::new(RedbKernel::new(database), AutomergeRowCodec::new());

        examples_util::run(engine, SqlTranslator).await;
        std::fs::remove_file(path).expect("remove Redb database");
    });
}
