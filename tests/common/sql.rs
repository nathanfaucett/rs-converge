use ofdb::Uuid;
use ofdb_test::{ExpectedError, Row, TestCase, TestSuite, Value};

fn uuid(s: &str) -> Value {
    Value::Uuid(Uuid::parse_str(s).unwrap())
}

pub fn case_wildcard_projection() -> TestCase {
    TestCase::builder("wildcard_projection")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        ])
        .expect_query(
            "SELECT * FROM users",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc"),
                Value::from("Ada"),
            ])],
        )
        .build()
}

pub fn case_projection_and_comparison() -> TestCase {
    TestCase::builder("projection_and_comparison")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        ])
        .expect_query(
            "SELECT id, name FROM users WHERE id >= CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abd"),
                Value::from("Lin"),
            ])],
        )
        .build()
}

pub fn case_null_predicate() -> TestCase {
    TestCase::builder("null_predicate")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)",
        ])
        .expect_query(
            "SELECT id FROM users WHERE name IS NULL",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abd")])],
        )
        .build()
}

pub fn case_boolean_predicate() -> TestCase {
    TestCase::builder("boolean_predicate")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, active BOOLEAN)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), TRUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), FALSE)",
        ])
        .expect_query(
            "SELECT id FROM users WHERE active = TRUE",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc")])],
        )
        .build()
}

pub fn case_boolean_composition() -> TestCase {
    TestCase::builder("boolean_composition")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin')",
        ])
        .expect_query(
            "SELECT id FROM users WHERE name = 'Ada'",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name = 'Lin'",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abe")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NOT NULL AND id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NOT NULL AND id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID)",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abe")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NOT NULL AND id = CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID)",
            vec![],
        )
        .build()
}

pub fn case_insert_columns_defaults_and_returning() -> TestCase {
    TestCase::builder("insert_columns_defaults_and_returning")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'Ada')"])
        .step(
            0,
            "INSERT INTO users (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)) RETURNING id, name",
        )
        .expect_query(
            "SELECT id, name FROM users",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc"),
                Value::from("Ada"),
            ])],
        )
        .build()
}

pub fn case_generated_uuid_crud_and_predicates() -> TestCase {
    TestCase::builder("generated_uuid_crud_and_predicates")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'unknown')"])
        .step(0, "INSERT INTO users (name) VALUES ('Ada')")
        .step(0, "UPDATE users SET name = 'Lin' WHERE name = 'Ada'")
        .step(0, "DELETE FROM users WHERE name = 'unknown'")
        .expect_query(
            "SELECT name FROM users WHERE id IS NOT NULL",
            vec![Row::from(["Lin"])],
        )
        .build()
}

pub fn case_indexes_after_rows_can_be_recreated() -> TestCase {
    TestCase::builder("indexes_after_rows_can_be_recreated")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
            "CREATE INDEX users_email ON users (email)",
            "DROP INDEX users_email",
            "CREATE INDEX users_email ON users (email)",
        ])
        .expect_query(
            "SELECT email FROM users",
            vec![Row::from(["ada@example.com"])],
        )
        .build()
}

pub fn case_alter_table_default_materializes_existing_rows() -> TestCase {
    TestCase::builder("alter_table_default_materializes_existing_rows")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'",
        ])
        .expect_query("SELECT role FROM users", vec![Row::from(["member"])])
        .build()
}

pub fn case_failed_statement_batch_is_atomic() -> TestCase {
    TestCase::builder("failed_statement_batch_is_atomic")
        .step_failing(
            0,
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT); INSERT INTO users VALUES (NULL, 'Ada')",
            ExpectedError::TypeMismatch,
        )
        .step_failing(0, "SELECT * FROM users", ExpectedError::TableNotFound)
        .build()
}

pub fn case_rejected_sql_does_not_mutate_state() -> TestCase {
    TestCase::builder("rejected_sql_does_not_mutate_state")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step_failing(0, "THIS IS NOT SQL", ExpectedError::SyntaxError)
        .step_failing(
            0,
            "INSERT INTO missing VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            ExpectedError::TableNotFound,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('not-a-uuid' AS UUID), 'Ada')",
            ExpectedError::TypeMismatch,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES ('not-a-uuid', 'Ada')",
            ExpectedError::TypeMismatch,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (NULL, 'Ada')",
            ExpectedError::TypeMismatch,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            ExpectedError::TypeMismatch,
        )
        .step_failing(0, "DROP TABLE users", ExpectedError::SyntaxError)
        .step_failing(0, "SELECT missing FROM users", ExpectedError::ColumnNotFound)
        .step_failing(0, "SELECT * FROM users ORDER BY name", ExpectedError::SyntaxError)
        .step_failing(0, "SELECT * FROM users LIMIT 1", ExpectedError::SyntaxError)
        .step_failing(0, "SELECT COUNT(*) FROM users", ExpectedError::ColumnNotFound)
        .step_failing(0, "SELECT DISTINCT name FROM users", ExpectedError::SyntaxError)
        .step_failing(0, "SELECT name FROM users GROUP BY name", ExpectedError::SyntaxError)
        .step_failing(
            0,
            "SELECT * FROM users, users AS other",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT * FROM users JOIN users AS other ON users.id = other.id",
            ExpectedError::SyntaxError,
        )
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Lin')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query("SELECT name FROM users", vec![Row::from(["Ada"])])
        .build()
}

pub fn case_inline_unique_constraint() -> TestCase {
    TestCase::builder("inline_unique_constraint")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE)"])
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'ada@example.com')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query(
            "SELECT email FROM users",
            vec![Row::from(["ada@example.com"])],
        )
        .build()
}

pub fn sql_suite() -> TestSuite {
    let mut suite = TestSuite::new("sql_surface_suite");
    suite.add(case_wildcard_projection());
    suite.add(case_projection_and_comparison());
    suite.add(case_null_predicate());
    suite.add(case_boolean_predicate());
    suite.add(case_boolean_composition());
    suite.add(case_insert_columns_defaults_and_returning());
    suite.add(case_generated_uuid_crud_and_predicates());
    suite.add(case_indexes_after_rows_can_be_recreated());
    suite.add(case_alter_table_default_materializes_existing_rows());
    suite.add(case_failed_statement_batch_is_atomic());
    suite.add(case_rejected_sql_does_not_mutate_state());
    suite.add(case_inline_unique_constraint());
    suite
}
