use std::time::{SystemTime, UNIX_EPOCH};

use ofdb::{Database, SqlTranslator};

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
        let database = Database::open(&path).expect("open Redb database");

        database
            .translate_and_execute(
                "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                &SqlTranslator,
            )
            .await
            .expect("create users");
        std::fs::remove_file(path).expect("remove Redb database");
    });
}
