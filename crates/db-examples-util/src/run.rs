use db_engine::{Engine, EngineKernel};
use db_query::{QueryParams, Translator};
use db_value::Value;
use uuid::Uuid;

pub async fn run<K, T>(engine: Engine<K>, translator: T)
where
    K: EngineKernel,
    T: Translator,
{
    // Create tables via SQL using the facade.
    engine
        .translate_and_execute(
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);",
            &translator,
        )
        .await
        .expect("create users");
    engine
        .translate_and_execute(
            "CREATE TABLE orders (id UUID PRIMARY KEY, user_id UUID, amount INT);",
            &translator,
        )
        .await
        .expect("create orders");

    let alice_id = Uuid::now_v7();
    let bob_id = Uuid::now_v7();

    // Insert some users via SQL using the facade
    engine
        .translate_and_execute_with_params(
            "INSERT INTO users (id, name) VALUES ($1, 'Alice');",
            Some(&QueryParams::Positional(vec![Value::Uuid(alice_id)])),
            &translator,
        )
        .await
        .expect("insert user 1");
    engine
        .translate_and_execute_with_params(
            "INSERT INTO users (id, name) VALUES ($1, 'Bob');",
            Some(&QueryParams::Positional(vec![Value::Uuid(bob_id)])),
            &translator,
        )
        .await
        .expect("insert user 2");

    // Insert some orders via SQL using the facade
    engine
        .translate_and_execute_with_params(
            "INSERT INTO orders (id, user_id, amount) VALUES ($1,$2,100);",
            Some(&QueryParams::Positional(vec![
                Value::Uuid(Uuid::now_v7()),
                Value::Uuid(alice_id),
            ])),
            &translator,
        )
        .await
        .expect("insert order 1");
    engine
        .translate_and_execute_with_params(
            "INSERT INTO orders (id, user_id, amount) VALUES ($1,$2,200);",
            Some(&QueryParams::Positional(vec![
                Value::Uuid(Uuid::now_v7()),
                Value::Uuid(bob_id),
            ])),
            &translator,
        )
        .await
        .expect("insert order 2");

    let results = engine
        .translate_and_execute(
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id;",
            &translator,
        )
        .await
        .expect("translate_and_execute select");

    for result in results {
        println!("Joined rows: {}", result.rows.len());
        for row in result.rows {
            println!("row: {:?}", row);
        }
    }
}
