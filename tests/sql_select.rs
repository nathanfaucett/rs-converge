use db_test::{Case, Row, Value, assert_case};

#[test]
fn wildcard_projection() {
    assert_case(Case {
        name: "wildcard_projection",
        setup: &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (1, 'Ada')",
        ],
        query: "SELECT * FROM users",
        expected: vec![Row::new(vec![Value::Integer(1), Value::from("Ada")])],
    });
}

#[test]
fn projection_and_comparison() {
    assert_case(Case {
        name: "projection_and_comparison",
        setup: &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (1, 'Ada')",
            "INSERT INTO users VALUES (2, 'Lin')",
        ],
        query: "SELECT id, name FROM users WHERE id >= 2",
        expected: vec![Row::new(vec![Value::Integer(2), Value::from("Lin")])],
    });
}

#[test]
fn null_predicate() {
    assert_case(Case {
        name: "null_predicate",
        setup: &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (1, 'Ada')",
            "INSERT INTO users VALUES (2, NULL)",
        ],
        query: "SELECT id FROM users WHERE name IS NULL",
        expected: vec![Row::new(vec![Value::Integer(2)])],
    });
}

#[test]
fn boolean_predicate() {
    assert_case(Case {
        name: "boolean_predicate",
        setup: &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, active BOOLEAN)",
            "INSERT INTO users VALUES (1, TRUE)",
            "INSERT INTO users VALUES (2, FALSE)",
        ],
        query: "SELECT id FROM users WHERE active = TRUE",
        expected: vec![Row::new(vec![Value::Integer(1)])],
    });
}

#[test]
fn boolean_composition() {
    assert_case(Case {
        name: "boolean_composition",
        setup: &[
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO users VALUES (1, 'Ada')",
            "INSERT INTO users VALUES (2, NULL)",
            "INSERT INTO users VALUES (3, 'Lin')",
        ],
        query: "SELECT id FROM users WHERE name IS NOT NULL AND (id = 1 OR id = 3)",
        expected: vec![
            Row::new(vec![Value::Integer(1)]),
            Row::new(vec![Value::Integer(3)]),
        ],
    });
}
