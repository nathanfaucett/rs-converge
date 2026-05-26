use db_engine::{ColumnSchema, EngineType, EngineValue, TableSchema};
use serde::Deserialize;

use db_engine::{EngineResult, FromRow, ResultColumn, RowDeserializeError};

#[derive(Debug, Deserialize, PartialEq)]
struct TestUser {
  id: i64,
  name: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct TestUserOptional {
  id: i64,
  name: String,
  #[serde(default)]
  email: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct TestProduct {
  price: f64,
  quantity: i64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct TestBlob {
  data: Vec<u8>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct TestUuid {
  id: Vec<u8>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
struct TestRename {
  #[serde(rename = "user_id")]
  id: i64,
  user_name: String,
}

fn create_user_schema() -> TableSchema {
  TableSchema {
    name: "users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "id".to_string(),
        data_type: EngineType::Integer,
      },
      ColumnSchema {
        name: "name".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  }
}

fn create_user_with_email_schema() -> TableSchema {
  TableSchema {
    name: "users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "id".to_string(),
        data_type: EngineType::Integer,
      },
      ColumnSchema {
        name: "name".to_string(),
        data_type: EngineType::Text,
      },
      ColumnSchema {
        name: "email".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  }
}

fn create_product_schema() -> TableSchema {
  TableSchema {
    name: "products".to_string(),
    columns: vec![
      ColumnSchema {
        name: "price".to_string(),
        data_type: EngineType::Float,
      },
      ColumnSchema {
        name: "quantity".to_string(),
        data_type: EngineType::Integer,
      },
    ],
    primary_key: vec![],
  }
}

fn create_blob_schema() -> TableSchema {
  TableSchema {
    name: "files".to_string(),
    columns: vec![ColumnSchema {
      name: "data".to_string(),
      data_type: EngineType::Blob,
    }],
    primary_key: vec![],
  }
}

fn create_uuid_schema() -> TableSchema {
  TableSchema {
    name: "items".to_string(),
    columns: vec![ColumnSchema {
      name: "id".to_string(),
      data_type: EngineType::Uuid,
    }],
    primary_key: vec![0],
  }
}

fn create_rename_schema() -> TableSchema {
  TableSchema {
    name: "users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "user_id".to_string(),
        data_type: EngineType::Integer,
      },
      ColumnSchema {
        name: "user_name".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  }
}

#[test]
fn test_basic_deserialization() {
  let schema = create_user_schema();
  let row = vec![
    EngineValue::Integer(1),
    EngineValue::Text("Alice".to_string()),
  ];

  let user: TestUser = TestUser::from_row(&schema, &row).unwrap();
  assert_eq!(
    user,
    TestUser {
      id: 1,
      name: "Alice".to_string()
    }
  );
}

#[test]
fn test_case_insensitive_column_matching() {
  let schema = TableSchema {
    name: "users".to_string(),
    columns: vec![
      ColumnSchema {
        name: "ID".to_string(),
        data_type: EngineType::Integer,
      },
      ColumnSchema {
        name: "NAME".to_string(),
        data_type: EngineType::Text,
      },
    ],
    primary_key: vec![0],
  };

  let row = vec![
    EngineValue::Integer(1),
    EngineValue::Text("Bob".to_string()),
  ];

  let user: TestUser = TestUser::from_row(&schema, &row).unwrap();
  assert_eq!(
    user,
    TestUser {
      id: 1,
      name: "Bob".to_string()
    }
  );
}

#[test]
fn test_type_mismatch_error() {
  let schema = create_user_schema();
  let row = vec![
    EngineValue::Text("not_an_int".to_string()),
    EngineValue::Text("Alice".to_string()),
  ];

  let result: Result<TestUser, _> = TestUser::from_row(&schema, &row);
  assert!(result.is_err());
}

#[test]
fn test_column_not_found() {
  let schema = TableSchema {
    name: "users".to_string(),
    columns: vec![ColumnSchema {
      name: "id".to_string(),
      data_type: EngineType::Integer,
    }],
    primary_key: vec![0],
  };

  let row = vec![EngineValue::Integer(1)];

  let result: Result<TestUser, _> = TestUser::from_row(&schema, &row);
  assert!(result.is_err());
  if let Err(RowDeserializeError::SerdeError { message }) = result {
    // Column 'name' should not be found
    assert!(message.contains("name") || message.contains("not found"));
  }
}

#[test]
fn test_float_deserialization() {
  let schema = create_product_schema();
  let row = vec![EngineValue::Float(19.99), EngineValue::Integer(100)];

  let product: TestProduct = TestProduct::from_row(&schema, &row).unwrap();
  assert_eq!(
    product,
    TestProduct {
      price: 19.99,
      quantity: 100
    }
  );
}

#[test]
fn test_integer_to_float_coercion() {
  let schema = create_product_schema();
  let row = vec![EngineValue::Integer(20), EngineValue::Integer(100)];

  let product: TestProduct = TestProduct::from_row(&schema, &row).unwrap();
  assert_eq!(
    product,
    TestProduct {
      price: 20.0,
      quantity: 100
    }
  );
}

#[test]
fn test_null_with_option() {
  let schema = create_user_with_email_schema();
  let row = vec![
    EngineValue::Integer(1),
    EngineValue::Text("Alice".to_string()),
    EngineValue::Null,
  ];

  let user: TestUserOptional = TestUserOptional::from_row(&schema, &row).unwrap();
  assert_eq!(
    user,
    TestUserOptional {
      id: 1,
      name: "Alice".to_string(),
      email: None
    }
  );
}

#[test]
fn test_blob_deserialization() {
  let schema = create_blob_schema();
  let data = vec![1, 2, 3, 4, 5];
  let row = vec![EngineValue::Blob(data.clone())];

  let blob: TestBlob = TestBlob::from_row(&schema, &row).unwrap();
  assert_eq!(blob.data, data);
}

#[test]
fn test_uuid_deserialization() {
  let schema = create_uuid_schema();
  let uuid_bytes = [1u8; 16];
  let row = vec![EngineValue::Uuid(uuid_bytes)];

  let item: TestUuid = TestUuid::from_row(&schema, &row).unwrap();
  assert_eq!(item.id, uuid_bytes.to_vec());
}

#[test]
fn test_serde_rename_attributes() {
  let schema = create_rename_schema();
  let row = vec![
    EngineValue::Integer(1),
    EngineValue::Text("Alice".to_string()),
  ];

  let user: TestRename = TestRename::from_row(&schema, &row).unwrap();
  assert_eq!(
    user,
    TestRename {
      id: 1,
      user_name: "Alice".to_string()
    }
  );
}

#[test]
fn test_engine_result_into_typed() {
  let schema = create_user_schema();
  let rows = vec![
    vec![
      EngineValue::Integer(1),
      EngineValue::Text("Alice".to_string()),
    ],
    vec![
      EngineValue::Integer(2),
      EngineValue::Text("Bob".to_string()),
    ],
  ];

  let result = EngineResult::new(rows);
  let users: Vec<TestUser> = result.into_typed::<TestUser>(&schema).unwrap();

  assert_eq!(users.len(), 2);
  assert_eq!(
    users[0],
    TestUser {
      id: 1,
      name: "Alice".to_string()
    }
  );
  assert_eq!(
    users[1],
    TestUser {
      id: 2,
      name: "Bob".to_string()
    }
  );
}

#[test]
fn test_engine_result_typed_rows() {
  let schema = create_user_schema();
  let rows = vec![
    vec![
      EngineValue::Integer(1),
      EngineValue::Text("Alice".to_string()),
    ],
    vec![
      EngineValue::Integer(2),
      EngineValue::Text("Bob".to_string()),
    ],
  ];

  let result = EngineResult::new(rows);
  let users: Vec<TestUser> = result.typed_rows::<TestUser>(&schema).unwrap();

  assert_eq!(users.len(), 2);
}

#[test]
fn test_empty_result() {
  let schema = create_user_schema();
  let result = EngineResult::new(vec![]);
  let users: Vec<TestUser> = result.into_typed::<TestUser>(&schema).unwrap();
  assert!(users.is_empty());
}

#[test]
fn test_named_rows() {
  let rows = vec![vec![
    EngineValue::Integer(1),
    EngineValue::Text("Alice".to_string()),
  ]];
  let columns = vec![
    ResultColumn::new("id", Some("users".to_string()), Some(0)),
    ResultColumn::new("name", Some("users".to_string()), Some(1)),
  ];

  let result = EngineResult::new_with_columns(rows, columns);
  let named = result.named_rows().unwrap();

  assert_eq!(named.len(), 1);
  assert_eq!(named[0].get("id"), Some(&EngineValue::Integer(1)));
  assert_eq!(
    named[0].get("name"),
    Some(&EngineValue::Text("Alice".to_string()))
  );
}

#[test]
fn test_into_typed_named() {
  let rows = vec![
    vec![
      EngineValue::Integer(1),
      EngineValue::Text("Alice".to_string()),
    ],
    vec![
      EngineValue::Integer(2),
      EngineValue::Text("Bob".to_string()),
    ],
  ];
  let columns = vec![
    ResultColumn::new("id", Some("users".to_string()), Some(0)),
    ResultColumn::new("name", Some("users".to_string()), Some(1)),
  ];

  let result = EngineResult::new_with_columns(rows, columns);
  let users: Vec<TestUser> = result.into_typed_named::<TestUser>().unwrap();

  assert_eq!(users.len(), 2);
  assert_eq!(users[0].id, 1);
  assert_eq!(users[0].name, "Alice");
  assert_eq!(users[1].id, 2);
  assert_eq!(users[1].name, "Bob");
}
