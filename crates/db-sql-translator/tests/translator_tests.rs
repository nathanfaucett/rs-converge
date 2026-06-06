use futures::executor::block_on;
use std::collections::BTreeMap;

use db_query::{
  Query, QueryColumn, QueryExpr, QueryExprValue, QueryJoin, QueryJoinKind, QueryParams,
  QuerySelectOptions, Statement, Translator,
};
use db_schema::{ColumnSchema, DescribeSchema, TableSchema};
use db_sql_translator::SqlTranslator;
use db_value::{Value, ValueType};

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
      primary_key: Vec::new(),
    };
    self.tables.insert(name.to_string(), table);
  }
}

impl DescribeSchema for MockResolver {
  async fn describe_table(&self, table_name: &str) -> Option<TableSchema> {
    self.tables.get(table_name).cloned()
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
    Statement::Query(Query::Select {
      tables: vec!["users".to_string()],
      table_index: 0,
      projection: vec![
        QueryColumn {
          table_index: 0,
          column_index: 0,
        },
        QueryColumn {
          table_index: 0,
          column_index: 1,
        },
      ],
      predicate: None,
      options: None,
    })
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
    Statement::Query(Query::Select {
      tables: vec!["users".to_string()],
      table_index: 0,
      projection: vec![
        QueryColumn {
          table_index: 0,
          column_index: 1,
        },
        QueryColumn {
          table_index: 0,
          column_index: 2,
        },
      ],
      predicate: None,
      options: None,
    })
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
        QueryColumn {
          table_index: 0,
          column_index: 0,
        },
        QueryColumn {
          table_index: 1,
          column_index: 0,
        },
      ],
      predicate: None,
      options: Some(Box::new(QuerySelectOptions {
        joins: vec![QueryJoin {
          kind: QueryJoinKind::Inner,
          table_index: 1,
          on: QueryExpr::Equals(
            QueryExprValue::Column(QueryColumn {
              table_index: 0,
              column_index: 0,
            }),
            QueryExprValue::Column(QueryColumn {
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
      })),
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
        QueryColumn {
          table_index: 0,
          column_index: 0,
        },
        QueryColumn {
          table_index: 1,
          column_index: 0,
        },
      ],
      predicate: None,
      options: Some(Box::new(QuerySelectOptions {
        joins: vec![QueryJoin {
          kind: QueryJoinKind::Inner,
          table_index: 1,
          on: QueryExpr::Equals(
            QueryExprValue::Column(QueryColumn {
              table_index: 0,
              column_index: 0,
            }),
            QueryExprValue::Column(QueryColumn {
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
      })),
    })
  );
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
    Statement::Query(Query::Select {
      tables: vec!["users".to_string()],
      table_index: 0,
      projection: vec![
        QueryColumn {
          table_index: 0,
          column_index: 0,
        },
        QueryColumn {
          table_index: 0,
          column_index: 1,
        },
      ],
      predicate: None,
      options: None,
    })
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

  let expected_pred = QueryExpr::Equals(
    QueryExprValue::Column(QueryColumn {
      table_index: 0,
      column_index: 0,
    }),
    QueryExprValue::Value(Value::Integer(42)),
  );

  assert_eq!(
    q,
    Statement::Query(Query::Select {
      tables: vec!["users".to_string()],
      table_index: 0,
      projection: vec![QueryColumn {
        table_index: 0,
        column_index: 0,
      }],
      predicate: Some(expected_pred),
      options: None,
    })
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

  let expected_pred = QueryExpr::Equals(
    QueryExprValue::Column(QueryColumn {
      table_index: 0,
      column_index: 0,
    }),
    QueryExprValue::Value(Value::Integer(7)),
  );

  assert_eq!(
    q,
    Statement::Query(Query::Select {
      tables: vec!["users".to_string()],
      table_index: 0,
      projection: vec![QueryColumn {
        table_index: 0,
        column_index: 0,
      }],
      predicate: Some(expected_pred),
      options: None,
    })
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
  let mut named = BTreeMap::new();
  named.insert("user_id".to_string(), Value::Integer(42));
  let params = QueryParams::Named(named);

  let q = block_on(translator.translate_with_params(
    "SELECT id FROM users WHERE id = :user_id",
    Some(&params),
    &resolver,
  ))
  .expect("translate");

  let expected_pred = QueryExpr::Equals(
    QueryExprValue::Column(QueryColumn {
      table_index: 0,
      column_index: 0,
    }),
    QueryExprValue::Value(Value::Integer(42)),
  );

  assert_eq!(
    q,
    Statement::Query(Query::Select {
      tables: vec!["users".to_string()],
      table_index: 0,
      projection: vec![QueryColumn {
        table_index: 0,
        column_index: 0,
      }],
      predicate: Some(expected_pred),
      options: None,
    })
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
  let mut named = BTreeMap::new();
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
  let params = QueryParams::Named(BTreeMap::new());
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
  let mut named = BTreeMap::new();
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
