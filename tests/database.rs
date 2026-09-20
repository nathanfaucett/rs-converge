#![cfg(all(feature = "automerge", feature = "in-memory", feature = "redb"))]

use std::time::{SystemTime, UNIX_EPOCH};

use converge::{Database, FileDatabase, InMemoryDatabase, SqlTranslator};

fn path() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("converge-database-{nanos}.redb"))
}

#[test]
fn in_memory_database_uses_the_engine_api() {
    futures::executor::block_on(async {
        let database: InMemoryDatabase = Database::in_memory();
        database
            .translate_and_execute(
                "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                &SqlTranslator,
            )
            .await
            .unwrap();
        assert_eq!(database.table_schema("users").await.unwrap().name, "users");
    });
}

#[test]
fn file_database_reopens_persisted_schema() {
    futures::executor::block_on(async {
        let database_path = path();
        {
            let database: FileDatabase = Database::open(&database_path).unwrap();
            database
                .translate_and_execute(
                    "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
                    &SqlTranslator,
                )
                .await
                .unwrap();
        }

        let database = FileDatabase::open(&database_path).unwrap();
        assert_eq!(database.table_schema("users").await.unwrap().name, "users");
        drop(database);
        std::fs::remove_file(database_path).unwrap();
    });
}
