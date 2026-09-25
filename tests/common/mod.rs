use ofdb_test::{ExpectedError, Row, TestCase, TestSuite, Value};

/// Canonical concurrent insert scenario from ADR 0001.
pub fn case_concurrent_user_inserts() -> TestCase {
    TestCase::builder("concurrent_user_inserts")
        .setup(["CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)"])
        .step(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada')",
        )
        .step(
            1,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abd' AS UUID), 'Lin')",
        )
        .step_failing(
            0,
            "INSERT INTO users VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-123456789abc' AS UUID), 'Ada Duplicate')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query(
            "SELECT name FROM users WHERE name = 'Ada'",
            vec![Row::from(["Ada"])],
        )
        .expect_query(
            "SELECT name FROM users WHERE name = 'Lin'",
            vec![Row::from(["Lin"])],
        )
        .build()
}

/// Basic multi-node CRUD lifecycle across separate logical actors.
pub fn case_basic_crud() -> TestCase {
    TestCase::builder("basic_crud")
        .setup(["CREATE TABLE inventory (id UUID PRIMARY KEY, name TEXT, quantity INTEGER)"])
        .step(
            0,
            "INSERT INTO inventory VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-111111111111' AS UUID), 'Widget', 10)",
        )
        .step(
            1,
            "INSERT INTO inventory VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-222222222222' AS UUID), 'Gadget', 20)",
        )
        .step(
            0,
            "UPDATE inventory SET quantity = 15 WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-111111111111' AS UUID)",
        )
        .step(
            1,
            "DELETE FROM inventory WHERE id = CAST('018f0f8e-7b6d-7c4a-8f12-222222222222' AS UUID)",
        )
        .expect_query(
            "SELECT name, quantity FROM inventory WHERE name = 'Widget'",
            vec![Row::new(vec![Value::from("Widget"), Value::Integer(15)])],
        )
        .expect_query("SELECT name FROM inventory WHERE name = 'Gadget'", vec![])
        .build()
}

/// Enforces unique constraint validation and convergence under duplicate attempts.
pub fn case_unique_constraints() -> TestCase {
    TestCase::builder("unique_constraints")
        .setup([
            "CREATE TABLE accounts (id UUID PRIMARY KEY, email TEXT)",
            "CREATE UNIQUE INDEX accounts_email ON accounts (email)",
        ])
        .step(
            0,
            "INSERT INTO accounts VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-333333333333' AS UUID), 'user@example.com')",
        )
        .step_failing(
            0,
            "INSERT INTO accounts VALUES (CAST('018f0f8e-7b6d-7c4a-8f12-444444444444' AS UUID), 'user@example.com')",
            ExpectedError::ConstraintViolation,
        )
        .expect_query(
            "SELECT email FROM accounts",
            vec![Row::from(["user@example.com"])],
        )
        .build()
}

/// Multi-node / convergence scenarios shared by all runners.
pub fn standard_suite() -> TestSuite {
    let mut suite = TestSuite::new("standard_integration_suite");
    suite.add(case_concurrent_user_inserts());
    suite.add(case_basic_crud());
    suite.add(case_unique_constraints());
    suite
}
