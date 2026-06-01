use db_engine::{
  Column, ColumnSchema, DescribeSchema, Expr, ExprValue, Query, QueryParams, TableSchema,
  Translator, Value, ValueType,
};
use db_sql_to_engine::SqlTranslator;
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
    Query::select_simple("users".to_string(), vec![0, 1], None)
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
    Query::select_simple("users".to_string(), vec![1, 2], None)
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
    Query::select_simple("users".to_string(), vec![0, 1], None)
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
      table: "users".to_string(),
      column_index: 0u8,
    }),
    ExprValue::Value(Value::Integer(42)),
  );

  assert_eq!(
    q,
    Query::select_simple("users".to_string(), vec![0], Some(expected_pred))
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
      table: "users".to_string(),
      column_index: 0u8,
    }),
    ExprValue::Value(Value::Integer(7)),
  );

  assert_eq!(
    q,
    Query::select_simple("users".to_string(), vec![0], Some(expected_pred))
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
    Query::Insert {
      table: "users".to_string(),
      row: vec![Value::Integer(1), Value::Text("alice".to_string())],
      returning: None,
    }
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
      table: "users".to_string(),
      column_index: 0u8,
    }),
    ExprValue::Value(Value::Integer(42)),
  );

  assert_eq!(
    q,
    Query::select_simple("users".to_string(), vec![0], Some(expected_pred))
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
    Query::Insert {
      table: "users".to_string(),
      row: vec![Value::Integer(1), Value::Text("alice".to_string())],
      returning: None,
    }
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
