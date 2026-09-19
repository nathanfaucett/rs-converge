use db::{FromRow, Row, Uuid, Value};

#[derive(db::FromRow, Debug, PartialEq)]
struct User {
    id: Uuid,
    name: String,
}
use db_test::{Case, assert_case, direct_in_memory_cluster, run};

#[test]
fn derive_from_row_uses_the_db_facade() {
    let id = Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap();
    let user = User::from_row(
        &Row::new(vec![Value::Uuid(id), Value::from("Ada")]),
        &["id", "name"],
    )
    .unwrap();
    assert_eq!(
        user,
        User {
            id,
            name: "Ada".into()
        }
    );
}

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
            Value::Uuid(Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap()),
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
            Value::Uuid(Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abd").unwrap()),
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
            Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abd").unwrap(),
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
            Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap(),
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
                Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap(),
            )]),
            Row::new(vec![Value::Uuid(
                Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abe").unwrap(),
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
            Value::Uuid(Uuid::parse_str("018f0f8e-7b6d-7c4a-8f12-123456789abc").unwrap()),
            Value::from("Ada"),
        ])],
    });
}

#[test]
fn generated_uuid_crud_and_predicates_work_on_every_backend() {
    assert_case(Case {
        name: "generated_uuid_crud_and_predicates",
        setup: &["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'unknown')"],
        query: "INSERT INTO users (name) VALUES ('Ada'); UPDATE users SET name = 'Lin' WHERE name = 'Ada'; DELETE FROM users WHERE name = 'unknown'; SELECT name FROM users WHERE id IS NOT NULL",
        expected: vec![Row::new(vec![Value::from("Lin")])],
    });
}

#[test]
fn indexes_after_rows_can_be_recreated_on_every_backend() {
    assert_case(Case {
        name: "indexes_after_rows_can_be_recreated",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
            "CREATE INDEX users_email ON users (email)",
            "DROP INDEX users_email",
            "CREATE INDEX users_email ON users (email)",
        ],
        query: "SELECT email FROM users",
        expected: vec![Row::new(vec![Value::from("ada@example.com")])],
    });
}

#[test]
fn failed_statement_batch_is_atomic_through_sql() {
    run(async {
        let cluster = direct_in_memory_cluster(1);
        assert!(cluster
        .try_exec(
            0,
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT); INSERT INTO users VALUES (NULL, 'Ada')",
        )
        .await
        .is_err());
        assert!(cluster.try_exec(0, "SELECT * FROM users").await.is_err());
    });
}

#[test]
fn alter_table_default_materializes_existing_rows_on_every_backend() {
    assert_case(Case {
        name: "alter_table_default_materializes_existing_rows",
        setup: &[
            "CREATE TABLE users (id UUID PRIMARY KEY)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'",
        ],
        query: "SELECT role FROM users",
        expected: vec![Row::new(vec![Value::from("member")])],
    });
}

#[test]
fn rejected_sql_does_not_mutate_state() {
    run(async {
        let cluster = direct_in_memory_cluster(1);
        let _ = cluster
            .exec(0, "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)")
            .await;

        for sql in [
            "THIS IS NOT SQL",
            "INSERT INTO missing VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            "INSERT INTO users VALUES (CAST('not-a-uuid' AS UUID), 'Ada')",
            "INSERT INTO users VALUES ('not-a-uuid', 'Ada')",
            "INSERT INTO users VALUES (NULL, 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            "DROP TABLE users",
            "SELECT missing FROM users",
            "SELECT * FROM users ORDER BY name",
            "SELECT * FROM users LIMIT 1",
            "SELECT COUNT(*) FROM users",
            "SELECT DISTINCT name FROM users",
            "SELECT name FROM users GROUP BY name",
            "SELECT * FROM users, users AS other",
            "SELECT * FROM users JOIN users AS other ON users.id = other.id",
        ] {
            assert!(
                cluster.try_exec(0, sql).await.is_err(),
                "expected error for {sql}"
            );
        }

        let _ = cluster
        .exec(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        )
        .await;
        assert!(
        cluster
            .try_exec(
                0,
                "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Lin')",
            )
            .await
            .is_err()
    );
        assert_eq!(cluster.exec(0, "SELECT * FROM users").await.len(), 1);
    });
}

#[test]
fn unique_constraints_are_enforced() {
    run(async {
        let cluster = direct_in_memory_cluster(1);
        cluster
            .exec(
                0,
                "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE)",
            )
            .await;
        cluster
            .exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')")
            .await;
        let error = cluster
            .try_exec(0, "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'ada@example.com')")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Unique index violation"));
    });
}
