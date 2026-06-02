use db_engine::{
  Column, ColumnSchema, DescribeSchema, Engine, Expr, ExprValue, InMemoryBTreeManager, Join,
  JoinKind, Query, QueryParams, SelectOptions, Statement, TableSchema, Translator, Value,
  ValueType,
};
use db_sql_translator::SqlTranslator;
use futures::executor::block_on;
use hashbrown::HashMap;
use std::collections::BTreeMap;

struct MockResolver {
  tables: BTreeMap<String, TableSchema>,
}

impl MockResolver {
  fn new() -> Self {
    Self {
      tables: BTreeMap::new(),
    }
  }

  fn add_table(&mut self, name: &str, columns: Vec<(&str, ValueType)>) {
    let cols = columns
      .into_iter()
      .map(|(n, t)| ColumnSchema {
        name: n.to_string(),
        r#type: t,
      })
      .collect();
    let table = TableSchema {
      name: name.to_string(),
      columns: cols,
      primary_key_index: Vec::new(),
    };
    self.tables.insert(name.to_string(), table);
  }
}

impl DescribeSchema for MockResolver {
  fn describe_table(
    &self,
    table_name: &str,
  ) -> impl db_engine::MaybeSendFuture<Output = Option<TableSchema>> {
    // return an immediate future with the cloned table schema
    let table = self.tables.get(table_name).cloned();
    async move { table }
  }
}

#[test]
fn select_star() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let q = block_on(translator.translate("SELECT * FROM users", &resolver)).expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::select_simple("users".to_string(), vec![0, 1], None))
  );
}

#[test]
fn select_columns() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![
      ("id", ValueType::Integer),
      ("name", ValueType::Text),
      ("email", ValueType::Text),
    ],
  );

  let translator = SqlTranslator;
  let q =
    block_on(translator.translate("SELECT name, email FROM users", &resolver)).expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::select_simple("users".to_string(), vec![1, 2], None))
  );
}

#[test]
fn select_inner_join_translation() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );
  resolver.add_table(
    "orders",
    vec![("id", ValueType::Integer), ("user_id", ValueType::Integer)],
  );

  let translator = SqlTranslator;
  let q = block_on(translator.translate(
    "SELECT users.id, orders.id FROM users INNER JOIN orders ON users.id = orders.user_id",
    &resolver,
  ))
  .expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::Select {
      tables: vec!["users".to_string(), "orders".to_string()],
      table_index: 0,
      projection: vec![
        Column {
          table_index: 0,
          column_index: 0,
        },
        Column {
          table_index: 1,
          column_index: 0,
        },
      ],
      predicate: None,
      options: Box::new(SelectOptions {
        joins: vec![Join {
          kind: JoinKind::Inner,
          table_index: 1,
          on: Expr::Equals(
            ExprValue::Column(Column {
              table_index: 0,
              column_index: 0,
            }),
            ExprValue::Column(Column {
              table_index: 1,
              column_index: 1,
            }),
          ),
        }],
        aggregates: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      }),
    })
  );
}

#[test]
fn select_plain_join_translation() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );
  resolver.add_table(
    "orders",
    vec![("id", ValueType::Integer), ("user_id", ValueType::Integer)],
  );

  let translator = SqlTranslator;
  let q = block_on(translator.translate(
    "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id",
    &resolver,
  ))
  .expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::Select {
      tables: vec!["users".to_string(), "orders".to_string()],
      table_index: 0,
      projection: vec![
        Column {
          table_index: 0,
          column_index: 0,
        },
        Column {
          table_index: 1,
          column_index: 0,
        },
      ],
      predicate: None,
      options: Box::new(SelectOptions {
        joins: vec![Join {
          kind: JoinKind::Inner,
          table_index: 1,
          on: Expr::Equals(
            ExprValue::Column(Column {
              table_index: 0,
              column_index: 0,
            }),
            ExprValue::Column(Column {
              table_index: 1,
              column_index: 1,
            }),
          ),
        }],
        aggregates: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        distinct: false,
        having: None,
      }),
    })
  );
}

#[test]
fn select_inner_join_roundtrip() {
  let engine = Engine::new(InMemoryBTreeManager::new());
  let translator = SqlTranslator;

  block_on(async {
    engine
      .translate_and_execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        &translator,
      )
      .await
      .expect("create users");

    engine
      .translate_and_execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);",
        &translator,
      )
      .await
      .expect("create orders");

    engine
      .translate_and_execute(
        "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        &translator,
      )
      .await
      .expect("insert user");

    engine
      .translate_and_execute(
        "INSERT INTO orders (id, user_id) VALUES (10, 1);",
        &translator,
      )
      .await
      .expect("insert order");

    let result = engine
      .translate_and_execute(
        "SELECT users.id, orders.id FROM users INNER JOIN orders ON users.id = orders.user_id;",
        &translator,
      )
      .await
      .expect("select join");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![Value::Integer(1), Value::Integer(10)],);
  });
}

#[test]
fn select_plain_join_roundtrip() {
  let engine = Engine::new(InMemoryBTreeManager::new());
  let translator = SqlTranslator;

  block_on(async {
    engine
      .translate_and_execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        &translator,
      )
      .await
      .expect("create users");

    engine
      .translate_and_execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);",
        &translator,
      )
      .await
      .expect("create orders");

    engine
      .translate_and_execute(
        "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        &translator,
      )
      .await
      .expect("insert user");

    engine
      .translate_and_execute(
        "INSERT INTO orders (id, user_id) VALUES (10, 1);",
        &translator,
      )
      .await
      .expect("insert order");

    let result = engine
      .translate_and_execute(
        "SELECT users.id, orders.id FROM users JOIN orders ON users.id = orders.user_id;",
        &translator,
      )
      .await
      .expect("select join");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![Value::Integer(1), Value::Integer(10)],);
  });
}

#[test]
fn create_table_insert_select_roundtrip() {
  let engine = Engine::new(InMemoryBTreeManager::new());
  let translator = SqlTranslator;

  block_on(async {
    engine
      .translate_and_execute(
        "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT);",
        &translator,
      )
      .await
      .expect("create table");

    engine
      .translate_and_execute(
        "INSERT INTO users (id, name) VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 'Alice');",
        &SqlTranslator,
      )
      .await
      .expect("insert row");

    let result = engine
      .translate_and_execute("SELECT id, name FROM users;", &SqlTranslator)
      .await
      .expect("select rows");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
  });
}

#[test]
fn select_qualified_wildcard() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let q =
    block_on(translator.translate("SELECT users.* FROM users", &resolver)).expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::select_simple("users".to_string(), vec![0, 1], None))
  );
}

#[test]
fn unknown_table_error() {
  let resolver = MockResolver::new();
  let translator = SqlTranslator;
  let res = block_on(translator.translate("SELECT * FROM missing", &resolver));
  assert!(res.is_err());
  let err = res.err().unwrap();
  let msg = format!("{}", err);
  assert!(msg.contains("unknown table"));
}

// Parameterized query tests (positional ? and indexed $n)
#[test]
fn select_with_positional_param() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let params = QueryParams::Positional(vec![Value::Integer(42)]);
  let q = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = ?",
    Some(&params),
    &resolver,
  ))
  .expect("translate");

  let expected_pred = Expr::Equals(
    ExprValue::Column(Column {
      table_index: 0,
      column_index: 0u8,
    }),
    ExprValue::Value(Value::Integer(42)),
  );

  assert_eq!(
    q,
    Statement::Query(Query::select_simple(
      "users".to_string(),
      vec![0],
      Some(expected_pred)
    ))
  );
}

#[test]
fn select_with_indexed_param() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let params = QueryParams::Positional(vec![Value::Integer(7)]);
  let q = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = $1",
    Some(&params),
    &resolver,
  ))
  .expect("translate");

  let expected_pred = Expr::Equals(
    ExprValue::Column(Column {
      table_index: 0,
      column_index: 0u8,
    }),
    ExprValue::Value(Value::Integer(7)),
  );

  assert_eq!(
    q,
    Statement::Query(Query::select_simple(
      "users".to_string(),
      vec![0],
      Some(expected_pred)
    ))
  );
}

#[test]
fn insert_with_positional_params() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let params = QueryParams::Positional(vec![Value::Integer(1), Value::Text("alice".to_string())]);
  let q = block_on(translator.translate_with_params(
    "INSERT INTO users (id, name) VALUES (?, ?)",
    Some(&params),
    &resolver,
  ))
  .expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::Insert {
      tables: vec!["users".to_string()],
      table_index: 0,
      row: vec![Value::Integer(1), Value::Text("alice".to_string())],
      returning: None,
    })
  );
}

#[test]
fn missing_param_errors() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  // no params provided for $1
  let res = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = $1",
    None,
    &resolver,
  ));
  assert!(res.is_err());
  let msg = format!("{}", res.err().unwrap());
  assert!(msg.contains("no parameters provided") || msg.contains("missing parameter"));
}

#[test]
fn select_with_named_param() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let mut named = HashMap::new();
  named.insert("user_id".to_string(), Value::Integer(42));
  let params = QueryParams::Named(named);

  let q = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = :user_id",
    Some(&params),
    &resolver,
  ))
  .expect("translate");

  let expected_pred = Expr::Equals(
    ExprValue::Column(Column {
      table_index: 0,
      column_index: 0u8,
    }),
    ExprValue::Value(Value::Integer(42)),
  );

  assert_eq!(
    q,
    Statement::Query(Query::select_simple(
      "users".to_string(),
      vec![0],
      Some(expected_pred)
    ))
  );
}

#[test]
fn insert_with_named_params() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let mut named = HashMap::new();
  named.insert("id".to_string(), Value::Integer(1));
  named.insert("name".to_string(), Value::Text("alice".to_string()));
  let params = QueryParams::Named(named);

  let q = block_on(translator.translate_with_params(
    "INSERT INTO users (id, name) VALUES (:id, :name)",
    Some(&params),
    &resolver,
  ))
  .expect("translate");

  assert_eq!(
    q,
    Statement::Query(Query::Insert {
      tables: vec!["users".to_string()],
      table_index: 0,
      row: vec![Value::Integer(1), Value::Text("alice".to_string())],
      returning: None,
    })
  );
}

#[test]
fn missing_named_param_errors() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let params = QueryParams::Named(HashMap::new());
  let res = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = :user_id",
    Some(&params),
    &resolver,
  ));

  assert!(res.is_err());
  let msg = format!("{}", res.err().unwrap());
  assert!(msg.contains("Missing named parameter"));
}

#[test]
fn mixed_placeholder_styles_error() {
  let mut resolver = MockResolver::new();
  resolver.add_table(
    "users",
    vec![("id", ValueType::Integer), ("name", ValueType::Text)],
  );

  let translator = SqlTranslator;
  let mut named = HashMap::new();
  named.insert("user_id".to_string(), Value::Integer(42));
  let params = QueryParams::Named(named);
  let res = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = :user_id OR id = $1",
    Some(&params),
    &resolver,
  ));

  assert!(res.is_err());
  let msg = format!("{}", res.err().unwrap());
  assert!(msg.contains("cannot mix named and positional/indexed"));
}
