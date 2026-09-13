use db_engine::{Engine, Kernel, RowCodec};
use db_query::Translator;

pub async fn run<K, R, T>(engine: Engine<K, R>, translator: T)
where
    K: Kernel,
    R: RowCodec<K::Transaction>,
    T: Translator,
{
    engine
        .translate_and_execute(
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);",
            &translator,
        )
        .await
        .expect("create users");
    engine
        .translate_and_execute(
            "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Alice');",
            &translator,
        )
        .await
        .expect("insert user 1");
    engine
        .translate_and_execute(
            "INSERT INTO users (id, name) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Bob');",
            &translator,
        )
        .await
        .expect("insert user 2");

    let results = engine
        .translate_and_execute("SELECT name FROM users;", &translator)
        .await
        .expect("select users");

    for result in results {
        println!("Rows: {}", result.rows.len());
        for row in result.rows {
            println!("row: {:?}", row);
        }
    }
}
