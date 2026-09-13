use db_test::{Case, Row, Value, assert_case};

#[test]
fn wildcard_projection() {
    assert_case(Case {
        name: "wildcard_projection",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        ],
        query: "SELECT * FROM users",
        expected: vec![Row::new(vec![
            Value::Uuid(uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap()),
            Value::from("Ada"),
        ])],
    });
}

#[test]
fn projection_and_comparison() {
    assert_case(Case {
        name: "projection_and_comparison",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        ],
        query: "SELECT id, name FROM users WHERE id >= CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)",
        expected: vec![Row::new(vec![
            Value::Uuid(uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abd").unwrap()),
            Value::from("Lin"),
        ])],
    });
}

#[test]
fn null_predicate() {
    assert_case(Case {
        name: "null_predicate",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)",
        ],
        query: "SELECT id FROM users WHERE name IS NULL",
        expected: vec![Row::new(vec![Value::Uuid(
            uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abd").unwrap(),
        )])],
    });
}

#[test]
fn boolean_predicate() {
    assert_case(Case {
        name: "boolean_predicate",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY, active BOOLEAN)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), TRUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), FALSE)",
        ],
        query: "SELECT id FROM users WHERE active = TRUE",
        expected: vec![Row::new(vec![Value::Uuid(
            uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap(),
        )])],
    });
}

#[test]
fn boolean_composition() {
    assert_case(Case {
        name: "boolean_composition",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')",
        ],
        query: "SELECT id FROM users WHERE name IS NOT NULL AND (id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID) OR id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID))",
        expected: vec![
            Row::new(vec![Value::Uuid(
                uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap(),
            )]),
            Row::new(vec![Value::Uuid(
                uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abe").unwrap(),
            )]),
        ],
    });
}

#[test]
fn insert_columns_defaults_and_returning() {
    assert_case(Case {
        name: "insert_columns_defaults_and_returning",
        setup: &["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'Ada')"],
        query: "INSERT INTO users (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)) RETURNING id, name",
        expected: vec![Row::new(vec![
            Value::Uuid(uuid::Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap()),
            Value::from("Ada"),
        ])],
    });
}

#[tokio::test]
async fn unique_constraints_are_enforced() {
    let engine =
        db_engine::Engine::new(db_engine::InMemoryKernel::new(), db_engine::DirectRowCodec);
    engine
        .translate_and_execute(
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE)",
            &db_sql_translator::SqlTranslator,
        )
        .await
        .unwrap();
    engine
            .translate_and_execute(
                "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
                &db_sql_translator::SqlTranslator,
            )
            .await
            .unwrap();
    let error = engine
            .translate_and_execute(
                "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'ada@example.com')",
                &db_sql_translator::SqlTranslator,
            )
            .await
            .unwrap_err();
    assert!(error.to_string().contains("Unique index violation"));
}
