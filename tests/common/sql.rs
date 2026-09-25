use ofdb::Uuid;
use ofdb_test::{ExpectedError, Row, TestCase, TestSuite, Value};

fn uuid(s: &str) -> Value {
    Value::Uuid(Uuid::parse_str(s).unwrap())
}

/// Full CRUD lifecycle: column-list insert with defaults, RETURNING,
/// generated UUID keys, UPDATE, and DELETE.
pub fn case_crud_lifecycle() -> TestCase {
    TestCase::builder("crud_lifecycle")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT DEFAULT 'unknown')"])
        .step(
            0,
            "INSERT INTO users (id) VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID)) RETURNING id, name",
        )
        .step(0, "INSERT INTO users (name) VALUES ('Ada')")
        .step(0, "UPDATE users SET name = 'Lin' WHERE name = 'Ada'")
        .step(0, "DELETE FROM users WHERE name = 'unknown'")
        .expect_query(
            "SELECT name FROM users WHERE id IS NOT NULL",
            vec![Row::from(["Lin"])],
        )
        .build()
}

/// Filtering and projection: wildcard and column projection, range
/// comparison, NULL predicates, and boolean composition.
pub fn case_filtering_and_projection() -> TestCase {
    TestCase::builder("filtering_and_projection")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, active BOOLEAN)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada', TRUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), NULL, FALSE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID), 'Lin', TRUE)",
        ])
        .expect_query(
            "SELECT * FROM users WHERE name = 'Ada'",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abc"),
                Value::from("Ada"),
                Value::Bool(true),
            ])],
        )
        .expect_query(
            "SELECT id, name FROM users WHERE id >= CAST('018f0f8e-7b6d-7c4a-8f12-123456789abe' AS UUID)",
            vec![Row::new(vec![
                uuid("018f0f8e-7b6d-7c4a-8f12-123456789abe"),
                Value::from("Lin"),
            ])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NULL",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abd")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE active = TRUE AND name = 'Lin'",
            vec![Row::new(vec![uuid("018f0f8e-7b6d-7c4a-8f12-123456789abe")])],
        )
        .expect_query(
            "SELECT id FROM users WHERE name IS NOT NULL AND active = FALSE",
            vec![],
        )
        .build()
}

/// Constraints and indexes: inline UNIQUE enforcement plus index
/// create/drop/recreate over existing rows.
pub fn case_constraints_and_indexes() -> TestCase {
    TestCase::builder("constraints_and_indexes")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY, email TEXT UNIQUE)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'ada@example.com')",
            "CREATE INDEX users_email ON users (email)",
            "DROP INDEX users_email",
            "CREATE INDEX users_email ON users (email)",
        ])
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

/// Schema evolution: ALTER TABLE ADD COLUMN with a default materializes
/// the value on existing rows.
pub fn case_schema_evolution() -> TestCase {
    TestCase::builder("schema_evolution")
        .setup([
            "CREATE TABLE users (id UUID PRIMARY KEY)",
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID))",
            "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'member'",
        ])
        .expect_query("SELECT role FROM users", vec![Row::from(["member"])])
        .build()
}

/// A failed statement batch is atomic: nothing in the batch is applied.
pub fn case_atomic_batch() -> TestCase {
    TestCase::builder("atomic_batch")
        .step_failing(
            0,
            "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT); INSERT INTO users VALUES (NULL, 'Ada')",
            ExpectedError::TypeMismatch,
        )
        .step_failing(0, "SELECT * FROM users", ExpectedError::TableNotFound)
        .build()
}

/// The error categories real users hit: syntax, missing table, missing
/// column, type mismatch, and constraint violation — none mutate state.
pub fn case_common_errors() -> TestCase {
    TestCase::builder("common_errors")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step_failing(0, "NOT A VALID SQL STATEMENT", ExpectedError::SyntaxError)
        .step_failing(
            0,
            "INSERT INTO nonexistent_table VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-000000000001' AS UUID), 'A')",
            ExpectedError::TableNotFound,
        )
        .step_failing(
            0,
            "SELECT nonexistent_column FROM users",
            ExpectedError::ColumnNotFound,
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES ('not-a-valid-uuid', 'B')",
            ExpectedError::TypeMismatch,
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

/// Contract: SQL the engine does not support yet is rejected without
/// mutating state. Expected to change as support lands.
pub fn case_unsupported_sql_is_rejected() -> TestCase {
    TestCase::builder("unsupported_sql_is_rejected")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        )
        .step_failing(
            0,
            "SELECT * FROM users ORDER BY name",
            ExpectedError::SyntaxError,
        )
        .step_failing(0, "SELECT * FROM users LIMIT 1", ExpectedError::SyntaxError)
        .step_failing(0, "SELECT COUNT(*) FROM users", ExpectedError::ColumnNotFound)
        .step_failing(
            0,
            "SELECT DISTINCT name FROM users",
            ExpectedError::SyntaxError,
        )
        .step_failing(
            0,
            "SELECT name FROM users GROUP BY name",
            ExpectedError::SyntaxError,
        )
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
        .step_failing(0, "DROP TABLE users", ExpectedError::SyntaxError)
        .expect_query("SELECT name FROM users", vec![Row::from(["Ada"])])
        .build()
}

/// The 90% single-node SQL surface.
pub fn sql_suite() -> TestSuite {
    TestSuite::new("sql_surface_suite")
        .with_case(case_crud_lifecycle())
        .with_case(case_filtering_and_projection())
        .with_case(case_constraints_and_indexes())
        .with_case(case_schema_evolution())
        .with_case(case_atomic_batch())
        .with_case(case_common_errors())
}

/// Contract tests for SQL the engine does not support yet (non-default).
pub fn sql_limits_suite() -> TestSuite {
    TestSuite::new("sql_limits_suite").with_case(case_unsupported_sql_is_rejected())
}
