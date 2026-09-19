#![cfg(feature = "in-memory")]

use engine::{
    Change, ChangeKey, DirectRowCodec, Engine, EnvelopeOutcome, Frontier, InMemoryKernel,
    SchemaChange, TableGenerationId, TransactionEnvelope,
};
use futures::executor::block_on;
use query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryFrom, QueryInsert, QuerySelect, QueryUpdate, QueryUpdateAssignment,
    Statement,
};
use schema::{ColumnSchema, IndexSchema, TableSchema};
use uuid::Uuid;
use value::{Row, Value, ValueType};

fn row_uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn uuid_value(value: u128) -> Value {
    Value::Uuid(row_uuid(value))
}

#[test]
fn creates_inserts_and_selects_rows() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let schema = TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
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
                row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
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
fn catalog_generations_are_immutable() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
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

        let table = engine.table_generation_id("users").await.unwrap();
        let column = engine.column_generation_id("users", "id").await.unwrap();
        let index = engine.index_generation_id("users_id").await.unwrap();

        assert_eq!(table, engine.table_generation_id("users").await.unwrap());
        assert_eq!(
            column,
            engine.column_generation_id("users", "id").await.unwrap()
        );
        assert_eq!(index, engine.index_generation_id("users_id").await.unwrap());
    });
}

#[test]
fn primary_key_updates_are_rejected() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![uuid_value(1)]),
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
                    value: QueryExprValue::Value(uuid_value(2)),
                }],
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        "users".into(),
                        "id".into(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(1)))),
                )),
                returning: None,
            }))])
            .await
            .unwrap_err();
    });
}

#[test]
fn deleted_uuid_cannot_be_reused() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        let insert = || {
            Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![uuid_value(1)]),
                returning: None,
            }))
        };
        engine.execute(vec![insert()]).await.unwrap();
        engine
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        "users".into(),
                        "id".into(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(1)))),
                )),
                returning: None,
            }))])
            .await
            .unwrap();
        assert!(engine.execute(vec![insert()]).await.is_err());
    });
}

#[test]
fn records_one_envelope_for_a_committed_statement_batch() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .execute(vec![
                Statement::DataDefinition(DataDefinition::CreateTable {
                    schema: TableSchema {
                        name: "users".into(),
                        columns: vec![ColumnSchema {
                            name: "id".into(),
                            r#type: ValueType::Uuid,
                            default: Value::Null,
                            primary_key: true,
                        }],
                    },
                    if_not_exists: false,
                }),
                Statement::Query(Query::Insert(QueryInsert {
                    table: "users".into(),
                    row: Row::new(vec![uuid_value(1)]),
                    returning: None,
                })),
            ])
            .await
            .unwrap();

        assert_eq!(
            engine
                .missing_envelopes(&Frontier::default())
                .await
                .unwrap()
                .len(),
            1
        );
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
                        r#type: ValueType::Uuid,
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
                row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
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
                Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(value)))),
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
            vec![Row::new(vec![uuid_value(1), Value::from("Grace")])]
        );

        engine
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
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
        assert!(results[0].rows.is_empty());

        assert_eq!(
            engine
                .missing_envelopes(&Frontier::default())
                .await
                .unwrap()
                .len(),
            4
        );
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
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![uuid_value(0)]),
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
                row: Row::new(vec![uuid_value(1), Value::from("member")]),
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
fn rejects_local_unique_index_duplicates() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        r#type: ValueType::Uuid,
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
                row: Row::new(vec![uuid_value(1), Value::from("ada@example.com")]),
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

        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![uuid_value(2), Value::from("ada@example.com")]),
                returning: None,
            }))])
            .await
            .unwrap_err();

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
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![uuid_value(1)]),
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
                        unique: false,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        engine
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "users".into(),
                row: Row::new(vec![uuid_value(2), Value::from("member")]),
                returning: None,
            }))])
            .await
            .unwrap();
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
        assert_eq!(rows[0].rows.len(), 2);
    });
}

#[test]
fn primary_key_update_is_rejected() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
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
                row: Row::new(vec![uuid_value(1)]),
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
                    value: QueryExprValue::Value(uuid_value(2)),
                }],
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        "users".into(),
                        "id".into(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(1)))),
                )),
                returning: None,
            }))])
            .await
            .unwrap_err();
    });
}

#[test]
fn concurrent_same_label_tables_choose_the_lowest_generation() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let schema = || TableSchema {
            name: "users".into(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Uuid,
                default: Value::Null,
                primary_key: true,
            }],
        };
        left.create_table(schema()).await.unwrap();
        right.create_table(schema()).await.unwrap();
        let left_id = left.table_generation_id("users").await.unwrap();
        let right_id = right.table_generation_id("users").await.unwrap();
        for envelope in left.missing_envelopes(&Frontier::default()).await.unwrap() {
            right.import_envelope(envelope).await.unwrap();
        }
        for envelope in right.missing_envelopes(&Frontier::default()).await.unwrap() {
            left.import_envelope(envelope).await.unwrap();
        }
        let winner = TableGenerationId(left_id.0.min(right_id.0));
        assert_eq!(left.table_generation_id("users").await.unwrap(), winner);
        assert_eq!(right.table_generation_id("users").await.unwrap(), winner);
    });
}

#[test]
fn imports_envelopes_idempotently_and_quarantines_corrupt_bytes() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let envelope = TransactionEnvelope::new(Vec::new(), Vec::new()).unwrap();

        assert_eq!(
            engine.import_envelope(envelope.clone()).await.unwrap(),
            EnvelopeOutcome::Applied
        );
        assert_eq!(
            engine.import_envelope(envelope.clone()).await.unwrap(),
            EnvelopeOutcome::Applied
        );
        assert_eq!(
            engine.envelope_outcome(envelope.id).await.unwrap(),
            Some(EnvelopeOutcome::Applied)
        );
        assert!(matches!(
            engine.import_envelope_bytes(vec![0]).await.unwrap(),
            EnvelopeOutcome::Quarantined { .. }
        ));
        assert_eq!(engine.envelope_outcomes().await.unwrap().len(), 2);
    });
}

#[test]
fn checkpoints_merge_envelopes_without_replacing_destination_state() {
    block_on(async {
        let source = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let destination = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let table_envelope = |name: &str| {
            TransactionEnvelope::new(
                Vec::new(),
                vec![Change::schema(
                    Uuid::now_v7(),
                    SchemaChange::CreateTable {
                        table: TableGenerationId(row_uuid(if name == "source" {
                            100
                        } else {
                            101
                        })),
                        label: name.into(),
                    },
                )],
            )
            .unwrap()
        };
        let source_envelope = table_envelope("source");
        let destination_envelope = table_envelope("destination");

        source.import_envelope(source_envelope).await.unwrap();
        destination
            .import_envelope(destination_envelope)
            .await
            .unwrap();
        destination
            .import_checkpoint(source.export_checkpoint().await.unwrap())
            .await
            .unwrap();

        assert!(destination.table_schema("source").await.is_ok());
        assert!(destination.table_schema("destination").await.is_ok());
    });
}

#[test]
fn checkpoints_bootstrap_state_idempotently_and_retain_causal_headers() {
    block_on(async {
        let source = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let destination = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        source
            .create_table(TableSchema {
                name: "people".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1)]),
                returning: None,
            }))])
            .await
            .unwrap();

        let checkpoint = source.export_checkpoint().await.unwrap();
        assert!(checkpoint.headers.len() >= 2);
        assert_eq!(checkpoint.rows.len(), 1);
        destination
            .import_checkpoint(checkpoint.clone())
            .await
            .unwrap();
        destination.import_checkpoint(checkpoint).await.unwrap();
        let rows = destination
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("people".into(), "id".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(rows[0].rows, vec![Row::new(vec![uuid_value(1)])]);

        let parent = TransactionEnvelope::new(Vec::new(), Vec::new()).unwrap();
        let compacted = TransactionEnvelope::new(vec![parent.id], Vec::new()).unwrap();
        let child = TransactionEnvelope::new(
            vec![parent.id],
            vec![Change::schema(
                Uuid::now_v7(),
                SchemaChange::CreateTable {
                    table: TableGenerationId(row_uuid(101)),
                    label: "delayed".into(),
                },
            )],
        )
        .unwrap();
        let header_source = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let header_destination = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        header_source.import_envelope(parent).await.unwrap();
        header_source.import_envelope(compacted).await.unwrap();
        header_destination
            .import_checkpoint(header_source.export_checkpoint().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            header_destination.import_envelope(child).await.unwrap(),
            EnvelopeOutcome::Applied
        );
    });
}

#[test]
fn retries_pending_envelopes_when_parents_arrive() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let parent = TransactionEnvelope::new(Vec::new(), Vec::new()).unwrap();
        let child = TransactionEnvelope::new(vec![parent.id], Vec::new()).unwrap();

        assert_eq!(
            engine.import_envelope(child.clone()).await.unwrap(),
            EnvelopeOutcome::Pending
        );
        assert_eq!(
            engine.envelope_outcome(child.id).await.unwrap(),
            Some(EnvelopeOutcome::Pending)
        );
        assert_eq!(
            engine.import_envelope(parent).await.unwrap(),
            EnvelopeOutcome::Applied
        );
        assert_eq!(
            engine.envelope_outcome(child.id).await.unwrap(),
            Some(EnvelopeOutcome::Applied)
        );
    });
}

#[test]
fn exports_only_envelopes_outside_the_causal_frontier() {
    block_on(async {
        let source = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let destination = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let parent = TransactionEnvelope::new(Vec::new(), Vec::new()).unwrap();
        let child = TransactionEnvelope::new(vec![parent.id], Vec::new()).unwrap();

        source.import_envelope(parent).await.unwrap();
        source.import_envelope(child).await.unwrap();
        for envelope in source
            .missing_envelopes(&destination.frontier().await.unwrap())
            .await
            .unwrap()
        {
            destination.import_envelope(envelope).await.unwrap();
        }
        assert!(
            source
                .missing_envelopes(&destination.frontier().await.unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn concurrent_same_label_columns_and_indexes_choose_lowest_generations() {
    block_on(async {
        let left = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        let right = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        left.create_table(TableSchema {
            name: "users".into(),
            columns: vec![ColumnSchema {
                name: "id".into(),
                r#type: ValueType::Uuid,
                default: Value::Null,
                primary_key: true,
            }],
        })
        .await
        .unwrap();
        for envelope in left.missing_envelopes(&Frontier::default()).await.unwrap() {
            right.import_envelope(envelope).await.unwrap();
        }

        let add_name = || {
            Statement::DataDefinition(DataDefinition::AlterTable {
                table_name: "users".into(),
                operations: vec![AlterTableOperation::AddColumn(ColumnSchema {
                    name: "name".into(),
                    r#type: ValueType::Text,
                    default: Value::Null,
                    primary_key: false,
                })],
                if_exists: false,
            })
        };
        left.execute(vec![add_name()]).await.unwrap();
        right.execute(vec![add_name()]).await.unwrap();
        let left_column = left.column_generation_id("users", "name").await.unwrap();
        let right_column = right.column_generation_id("users", "name").await.unwrap();
        for envelope in left
            .missing_envelopes(&right.frontier().await.unwrap())
            .await
            .unwrap()
        {
            right.import_envelope(envelope).await.unwrap();
        }
        for envelope in right
            .missing_envelopes(&left.frontier().await.unwrap())
            .await
            .unwrap()
        {
            left.import_envelope(envelope).await.unwrap();
        }
        assert_eq!(
            left.column_generation_id("users", "name").await.unwrap().0,
            left_column.0.min(right_column.0)
        );
        assert_eq!(
            right.column_generation_id("users", "name").await.unwrap(),
            left.column_generation_id("users", "name").await.unwrap()
        );

        let create_index = || {
            Statement::DataDefinition(DataDefinition::CreateIndex {
                schema: IndexSchema {
                    name: "users_name".into(),
                    table_name: "users".into(),
                    column_indices: vec![1],
                    unique: false,
                },
                if_not_exists: false,
            })
        };
        left.execute(vec![create_index()]).await.unwrap();
        right.execute(vec![create_index()]).await.unwrap();
        let left_index = left.index_generation_id("users_name").await.unwrap();
        let right_index = right.index_generation_id("users_name").await.unwrap();
        for envelope in left
            .missing_envelopes(&right.frontier().await.unwrap())
            .await
            .unwrap()
        {
            right.import_envelope(envelope).await.unwrap();
        }
        for envelope in right
            .missing_envelopes(&left.frontier().await.unwrap())
            .await
            .unwrap()
        {
            left.import_envelope(envelope).await.unwrap();
        }
        assert_eq!(
            left.index_generation_id("users_name").await.unwrap().0,
            left_index.0.min(right_index.0)
        );
        assert_eq!(
            right.index_generation_id("users_name").await.unwrap(),
            left.index_generation_id("users_name").await.unwrap()
        );
    });
}

#[test]
fn rejects_an_invalid_envelope_atomically() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![ColumnSchema {
                    name: "id".into(),
                    r#type: ValueType::Uuid,
                    default: Value::Null,
                    primary_key: true,
                }],
            })
            .await
            .unwrap();
        let valid = Change::row(
            Uuid::now_v7(),
            engine.table_generation_id("users").await.unwrap(),
            Uuid::now_v7(),
            Some(postcard::to_allocvec(&Row::new(vec![uuid_value(1)])).unwrap()),
        );
        let invalid = Change {
            id: Uuid::now_v7(),
            key: ChangeKey::Schema(SchemaChange::CreateTable {
                table: TableGenerationId(row_uuid(102)),
                label: "invalid".into(),
            }),
            value: Some(vec![0]),
        };
        let envelope = TransactionEnvelope::new(Vec::new(), vec![valid, invalid]).unwrap();

        assert!(engine.import_envelope(envelope).await.is_err());
        let rows = engine
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
        assert!(rows[0].rows.is_empty());
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
                r#type: ValueType::Uuid,
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
                    row: Row::new(vec![Value::Null]),
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

#[test]
fn index_lookup_selects_and_promotes_unique_key_contenders() {
    block_on(async {
        let engine = Engine::new(InMemoryKernel::new(), DirectRowCodec);
        engine
            .create_table(TableSchema {
                name: "users".into(),
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        r#type: ValueType::Uuid,
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
        for id in [1, 2] {
            engine
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "users".into(),
                    row: Row::new(vec![uuid_value(id), Value::from("ada@example.com")]),
                    returning: None,
                }))])
                .await
                .unwrap();
        }
        engine
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "users_email".into(),
                        table_name: "users".into(),
                        column_indices: vec![1],
                        unique: false,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();

        let key = Row::new(vec![Value::from("ada@example.com")]);
        assert_eq!(
            engine.index_lookup("users_email", &key).await.unwrap(),
            Some(Row::new(vec![
                uuid_value(1),
                Value::from("ada@example.com")
            ]))
        );
        engine
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "users".into(),
                    joins: vec![],
                },
                predicate: Some(QueryExpr::Equals(
                    Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
                        "users".into(),
                        "id".into(),
                    )))),
                    Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(1)))),
                )),
                returning: None,
            }))])
            .await
            .unwrap();
        assert_eq!(
            engine.index_lookup("users_email", &key).await.unwrap(),
            Some(Row::new(vec![
                uuid_value(2),
                Value::from("ada@example.com")
            ]))
        );
    });
}
