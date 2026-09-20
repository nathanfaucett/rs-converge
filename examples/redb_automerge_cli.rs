use std::time::{SystemTime, UNIX_EPOCH};

use converge::{Database, SqlTranslator};

fn database_path() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("db-redb-automerge-cli-{nanos}.redb"))
}

fn main() {
    futures::executor::block_on(async {
        let path = database_path();
        let database = Database::open(&path).expect("open Redb database");

        examples_util::cli(database.into_inner(), SqlTranslator).await;
        std::fs::remove_file(path).expect("remove Redb database");
    });
}
