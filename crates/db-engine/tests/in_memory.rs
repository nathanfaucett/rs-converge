#![cfg(feature = "in-memory")]

use db_engine::{Change, ChangeReplication, DirectRowCodec, Engine, InMemoryKernel};
use db_query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryFrom, QueryInsert, QuerySelect, QueryUpdate, QueryUpdateAssignment,
    Statement,
};
use db_schema::{ColumnSchema, IndexSchema, TableSchema};
use db_value::{Row, Value, ValueType};
use futures::executor::block_on;
use uuid::Uuid;

#[test]
fn creates_inserts_and_selects_rows() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "name".into(),
                    r#type: ValueType::Text,
                    default: Value::Null,
                    primary_key: false,
                },
            ],
        };

        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateTable {
                    schema,
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let results = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();

        assert_eq!(results[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
}

#[test]
fn updates_and_deletes_rows_through_changes() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        r#type: ValueType::Integer,
                        default: Value::Null,
                        primary_key: true,
                    },
                    ColumnSchema {
                        name: "name".into(),
                        r#type: ValueType::Text,
                        default: Value::Null,
                        primary_key: false,
                    },
                ],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let id = |value| {
            Some(QueryExpr::Equals(
                Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                    "users".into(),
                    "id".into(),
                )))),
                Box::new(QueryExpr::Value(QueryExprValue::Value(Value::Integer(
                    value,
                )))),
            ))
        };
        engine
            .execute(vec![Statement::Query(Query::Update(QueryUpdate {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                assignments: vec![QueryUpdateAssignment {
                    column: QueryColumn::new("users".into(), "name".into()),
                    value: QueryExprValue::Value(Value::from("Grace")),
                }],
                predicate: id(1),
                returning: None,
            }))])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Update(QueryUpdate {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                assignments: vec![QueryUpdateAssignment {
                    column: QueryColumn::new("users".into(), "id".into()),
                    value: QueryExprValue::Value(Value::Integer(2)),
                }],
                predicate: id(1),
                returning: None,
            }))])
            .await
            .unwrap();

        let results = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "*".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(
            results[0].rows,
            vec![Row::new(vec![Value::Integer(2), Value::from("Grace")])]
        );

        engine
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                predicate: id(2),
                returning: None,
            }))])
            .await
            .unwrap();
        let results = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "*".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert!(results[0].rows.is_empty());

        let (_, changes) = engine.changes_since(None).await.unwrap();
        assert_eq!(changes.len(), 8);
    });
}

#[test]
fn alter_table_add_column_updates_the_catalog() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(0)]),
                returning: None,
            }))])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::AlterTable {
                    table_name: "users".into(),
                    operations: vec![AlterTableOperation::AddColumn(ColumnSchema {
                        name: "role".into(),
                        r#type: ValueType::Text,
                        default: Value::from("member"),
                        primary_key: false,
                    })],
                    if_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("member")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let results = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "role".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(
            results[0].rows,
            vec![
                Row::new(vec![Value::from("member")]),
                Row::new(vec![Value::from("member")]),
            ]
        );
    });
}

#[test]
fn derives_unique_indexes_and_rolls_back_collisions() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        r#type: ValueType::Integer,
                        default: Value::Null,
                        primary_key: true,
                    },
                    ColumnSchema {
                        name: "email".into(),
                        r#type: ValueType::Text,
                        default: Value::Null,
                        primary_key: false,
                    },
                ],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1), Value::from("ada@example.com")]),
                returning: None,
            }))])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "users_email".into(),
                        table_name: "users".into(),
                        column_indices: vec![1],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        assert_eq!(
            engine.index_schema("users_email").await.unwrap(),
            IndexSchema {
                name: "users_email".into(),
                table_name: "users".into(),
                column_indices: vec![1],
                unique: true,
            }
        );

        let result = engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(2), Value::from("ada@example.com")]),
                returning: None,
            }))])
            .await;
        assert!(result.is_err());

        let rows = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "*".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(rows[0].rows.len(), 1);
    });
}

#[test]
fn indexes_defaults_for_rows_created_before_add_column() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1)]),
                returning: None,
            }))])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::AlterTable {
                    table_name: "users".into(),
                    operations: vec![AlterTableOperation::AddColumn(ColumnSchema {
                        name: "role".into(),
                        r#type: ValueType::Text,
                        default: Value::from("member"),
                        primary_key: false,
                    })],
                    if_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "users_role".into(),
                        table_name: "users".into(),
                        column_indices: vec![1],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        assert!(
            engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "users".into(),
                    row: Row::new(vec![Value::Integer(2), Value::from("member")]),
                    returning: None,
                }))])
                .await
                .is_err()
        );
    });
}

#[test]
fn primary_key_update_releases_old_index_records() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "users_id".into(),
                        table_name: "users".into(),
                        column_indices: vec![0],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1)]),
                returning: None,
            }))])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Update(QueryUpdate {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                assignments: vec![QueryUpdateAssignment {
                    column: QueryColumn::new("users".into(), "id".into()),
                    value: QueryExprValue::Value(Value::Integer(2)),
                }],
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        "users".into(),
                        "id".into(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(Value::Integer(1)))),
                )),
                returning: None,
            }))])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![Value::Integer(1)]),
                returning: None,
            }))])
            .await
            .unwrap();
    });
}

#[test]
fn applies_received_changes_once_and_exposes_them() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();

        let key = Row::new(vec![Value::Integer(1)]);
        let row = Row::new(vec![Value::Integer(1)]);
        let change = Change::row(
            Uuid::now_v7(),
            "users".into(),
            key.clone(),
            Some(postcard::to_allocvec(&row).unwrap()),
        );
        engine.apply_changes(vec![change.clone()]).await.unwrap();
        engine.apply_changes(vec![change]).await.unwrap();

        let results = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "id".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(results[0].rows, vec![row]);

        let (_, changes) = engine.changes_since(None).await.unwrap();
        assert_eq!(changes.len(), 3);
    });
}

#[test]
fn delivers_late_lower_uuid_changes_after_the_cursor() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Integer,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        let (cursor, _) = engine.changes_since(None).await.unwrap();
        let first = Change::row(
            Uuid::from_u128(2),
            "users".into(),
            Row::new(vec![Value::Integer(1)]),
            Some(postcard::to_allocvec(&Row::new(vec![Value::Integer(1)])).unwrap()),
        );
        engine.apply_changes(vec![first]).await.unwrap();
        let (cursor, _) = engine.changes_since(Some(&cursor)).await.unwrap();

        let late = Change::row(
            Uuid::nil(),
            "users".into(),
            Row::new(vec![Value::Integer(2)]),
            Some(postcard::to_allocvec(&Row::new(vec![Value::Integer(2)])).unwrap()),
        );
        engine.apply_changes(vec![late.clone()]).await.unwrap();

        let (_, changes) = engine.changes_since(Some(&cursor)).await.unwrap();
        assert_eq!(changes, vec![late]);
    });
}

#[test]
fn rolls_back_the_full_statement_batch() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Integer,
                default: Value::Null,
                primary_key: true,
            }],
        };

        let result = engine
            .execute(vec![
                Statement::DataDefinition(DataDefinition::CreateTable {
                    schema,
                    if_not_exists: false,
                }),
                Statement::Query(Query::Insert(QueryInsert {
                    table: "users".into(),
                    row: Row::new(vec![]),
                    returning: None,
                })),
            ])
            .await;
        assert!(result.is_err());

        let result = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("users".into(), "id".into())],
                ..Default::default()
            }))])
            .await;
        assert!(result.is_err());
    });
}
