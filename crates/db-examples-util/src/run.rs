use db_engine::{Engine, Kernel};
use db_query::Translator;

pub async fn run<K, T>(engine: Engine<K>, translator: T)
where
    K: Kernel,
    T: Translator,
{
    // Create tables via SQL using the facade.
    engine
        .translate_and_execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
            &translator,
        )
        .await
        .expect("create users");
    engine
        .translate_and_execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
            &translator,
        )
        .await
        .expect("insert user 1");
    engine
        .translate_and_execute(
            "INSERT INTO users (id, name) VALUES (2, 'Bob');",
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
