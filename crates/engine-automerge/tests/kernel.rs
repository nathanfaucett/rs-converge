use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use engine::{
    DirectRowCodec, Engine, Kernel, KernelTransaction, RowCodec, RowTable, TableGenerationId,
};
use engine_automerge::AutomergeRowCodec;
use engine_redb::RedbKernel;
use futures::executor::block_on;
use query::{
    AlterTableOperation, DataDefinition, Query, QueryColumn, QueryDelete, QueryExpr,
    QueryExprValue, QueryFrom, QueryInsert, QuerySelect, QueryUpdate, QueryUpdateAssignment,
    Statement,
};
use schema::{ColumnSchema, IndexSchema, TableSchema};
use uuid::Uuid;
use value::{Row, Value, ValueType};

fn uuid_value(value: u128) -> Value {
    Value::Uuid(Uuid::from_u128(value))
}

static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

fn database_path() -> PathBuf {
    let id = NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("engine-automerge-{}-{id}.redb", std::process::id()))
}

fn people_schema() -> TableSchema {
    TableSchema {
        name: "people".into(),
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
            ColumnSchema {
                name: "city".into(),
                r#type: ValueType::Text,
                default: Value::Null,
                primary_key: false,
            },
        ],
    }
}

fn replica(path: &PathBuf) -> Engine<RedbKernel, AutomergeRowCodec> {
    Engine::new(
        RedbKernel::new(Arc::new(redb::Database::create(path).unwrap())),
        AutomergeRowCodec::new(),
    )
}

fn id(value: u128) -> Option<QueryExpr> {
    Some(QueryExpr::Equals(
        Box::new(QueryExpr::Value(QueryExprValue::Column(QueryColumn::new(
            "people".into(),
            "id".into(),
        )))),
        Box::new(QueryExpr::Value(QueryExprValue::Value(uuid_value(value)))),
    ))
}

async fn people_rows(engine: &Engine<RedbKernel, AutomergeRowCodec>, columns: &[&str]) -> Vec<Row> {
    engine
        .execute(vec![Statement::Query(Query::Select(QuerySelect {
            from: QueryFrom {
                table: "people".into(),
                joins: vec![],
            },
            projection: columns
                .iter()
                .map(|column| QueryColumn::new("people".into(), (*column).into()))
                .collect(),
            ..Default::default()
        }))])
        .await
        .unwrap()[0]
        .rows
        .clone()
}

async fn sync(
    source: &Engine<RedbKernel, AutomergeRowCodec>,
    destination: &Engine<RedbKernel, AutomergeRowCodec>,
) -> engine::EngineResult<()> {
    let frontier = destination.frontier().await?;
    for envelope in source.missing_envelopes(&frontier).await? {
        destination.import_envelope(envelope).await?;
    }
    Ok(())
}

async fn update(engine: &Engine<RedbKernel, AutomergeRowCodec>, column: &str, value: Value) {
    engine
        .execute(vec![Statement::Query(Query::Update(QueryUpdate {
            from: QueryFrom {
                table: "people".into(),
                joins: vec![],
            },
            assignments: vec![QueryUpdateAssignment {
                column: QueryColumn::new("people".into(), column.into()),
                value: QueryExprValue::Value(value),
            }],
            predicate: id(1),
            returning: None,
        }))])
        .await
        .unwrap();
}

async fn add_column(engine: &Engine<RedbKernel, AutomergeRowCodec>, name: &str, default: Value) {
    engine
        .execute(vec![Statement::DataDefinition(
            DataDefinition::AlterTable {
                table_name: "people".into(),
                operations: vec![AlterTableOperation::AddColumn(ColumnSchema {
                    name: name.into(),
                    r#type: ValueType::Text,
                    default,
                    primary_key: false,
                })],
                if_exists: false,
            },
        )])
        .await
        .unwrap();
}

#[test]
fn logical_rows_persist_in_one_kernel_transaction() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = TableGenerationId(Uuid::from_u128(100));
        let row_id = Uuid::now_v7();
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                row_id,
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, &table, &row_id)
                .await
                .unwrap(),
            Some(Row::new(vec![uuid_value(1), Value::from("Ada")]))
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn redb_kernel_pairs_with_a_non_automerge_reconciler() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = DirectRowCodec;
        let table = TableGenerationId(Uuid::from_u128(101));
        let row_id = Uuid::now_v7();
        let row = Row::new(vec![uuid_value(1), Value::from("Ada")]);
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(&mut transaction, table, row_id, row.clone())
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert_eq!(
            reconciler
                .get_row(&transaction, &table, &row_id)
                .await
                .unwrap(),
            Some(row)
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn removing_a_logical_row_writes_a_tombstone() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = TableGenerationId(Uuid::from_u128(102));
        let row_id = Uuid::now_v7();
        let mut transaction = kernel.transaction().await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                row_id,
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        assert!(
            reconciler
                .remove_row(&mut transaction, &table, &row_id)
                .await
                .unwrap()
                .is_some()
        );
        transaction.commit().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            reconciler
                .get_row(&transaction, &table, &row_id)
                .await
                .unwrap()
                .is_none()
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn engine_persists_a_logical_row_through_the_public_transaction_seam() {
    let path = database_path();
    let database = Arc::new(redb::Database::create(&path).unwrap());
    let schema = TableSchema {
        name: "people".into(),
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
    block_on(async {
        let engine = Engine::new(RedbKernel::new(database.clone()), AutomergeRowCodec::new());
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
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        let engine = Engine::new(RedbKernel::new(database), AutomergeRowCodec::new());
        let result = engine
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("people".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(result[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn received_automerge_incremental_change_materializes_a_row() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let schema = TableSchema {
            name: "people".into(),
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
        let source = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::create(&source_path).unwrap())),
            AutomergeRowCodec::new(),
        );
        let destination = Engine::new(
            RedbKernel::new(Arc::new(redb::Database::create(&destination_path).unwrap())),
            AutomergeRowCodec::new(),
        );
        for engine in [&source, &destination] {
            engine.create_table(schema.clone()).await.unwrap();
        }
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada")]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&source, &destination).await.unwrap();
        let results = destination
            .execute(vec![Statement::Query(Query::Select(QuerySelect {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                projection: vec![QueryColumn::new("people".into(), "name".into())],
                ..Default::default()
            }))])
            .await
            .unwrap();
        assert_eq!(results[0].rows, vec![Row::new(vec![Value::from("Ada")])]);
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn rollback_discards_catalog_and_logical_row_changes() {
    let path = database_path();
    let kernel = RedbKernel::new(Arc::new(redb::Database::create(&path).unwrap()));
    block_on(async {
        let reconciler = AutomergeRowCodec::new();
        let table = TableGenerationId(Uuid::from_u128(103));
        let row_id = Uuid::now_v7();
        let mut transaction = kernel.transaction().await.unwrap();
        transaction.ensure_table("tables").await.unwrap();
        reconciler
            .ensure_table(&mut transaction, table)
            .await
            .unwrap();
        transaction
            .put_entry(
                "tables",
                Row::new(vec![Value::from("people")]),
                Row::new(vec![Value::from("people")]),
            )
            .await
            .unwrap();
        reconciler
            .put_row(
                &mut transaction,
                table,
                row_id,
                Row::new(vec![uuid_value(1), Value::from("Ada")]),
            )
            .await
            .unwrap();
        transaction.rollback().await.unwrap();

        let transaction = kernel.transaction().await.unwrap();
        assert!(
            transaction
                .get_entry("tables", &Row::new(vec![Value::from("people")]))
                .await
                .unwrap()
                .is_none()
        );
        transaction.rollback().await.unwrap();
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn bootstrap_destination_exclusively_from_source_canonical_changes() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();

        sync(&source, &destination).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London"),
            ])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn checkpoint_bootstraps_complete_automerge_documents() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        destination
            .import_checkpoint(source.export_checkpoint().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            people_rows(&destination, &["id", "name", "city"]).await,
            vec![Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London"),
            ])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn copied_files_reopen_with_distinct_actors_and_converge() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        {
            let source = replica(&source_path);
            source.create_table(people_schema()).await.unwrap();
            source
                .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                    table: "people".into(),
                    row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                    returning: None,
                }))])
                .await
                .unwrap();
        }
        std::fs::copy(&source_path, &destination_path).unwrap();

        let source = replica(&source_path);
        let destination = replica(&destination_path);
        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "name", Value::from("Linus")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        assert_eq!(
            people_rows(&destination, &["name"]).await,
            people_rows(&source, &["name"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_different_column_updates_converge() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "city", Value::from("Paris")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        let expected = vec![Row::new(vec![Value::from("Grace"), Value::from("Paris")])];
        assert_eq!(people_rows(&source, &["name", "city"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["name", "city"]).await,
            people_rows(&source, &["name", "city"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_same_column_updates_converge_to_the_automerge_winner() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        update(&destination, "name", Value::from("Linus")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["name"]).await,
            people_rows(&source, &["name"]).await
        );
        assert!(
            source
                .execute(vec![Statement::Query(Query::Update(QueryUpdate {
                    from: QueryFrom {
                        table: "people".into(),
                        joins: vec![],
                    },
                    assignments: vec![QueryUpdateAssignment {
                        column: QueryColumn::new("people".into(), "name".into()),
                        value: QueryExprValue::Value(Value::from("Margaret")),
                    }],
                    predicate: id(1),
                    returning: None,
                }))])
                .await
                .is_err()
        );
        assert_eq!(
            source
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap(),
            vec!["name"]
        );
        source
            .resolve_row(
                "people",
                &Row::new(vec![uuid_value(1)]),
                vec![("name".into(), Value::from("Margaret"))],
            )
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        assert!(
            source
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            people_rows(&destination, &["name"]).await,
            vec![Row::new(vec![Value::from("Margaret")])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn resolving_an_indexed_conflict_promotes_the_next_unique_contender() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::DataDefinition(
                DataDefinition::CreateIndex {
                    schema: IndexSchema {
                        name: "people_city".into(),
                        table_name: "people".into(),
                        column_indices: vec![2],
                        unique: true,
                    },
                    if_not_exists: false,
                },
            )])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(1),
                    Value::from("Ada"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        destination
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![
                    uuid_value(2),
                    Value::from("Grace"),
                    Value::from("London"),
                ]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let london = Row::new(vec![Value::from("London")]);
        assert_eq!(
            source.index_lookup("people_city", &london).await.unwrap(),
            Some(Row::new(vec![
                uuid_value(1),
                Value::from("Ada"),
                Value::from("London")
            ]))
        );

        update(&source, "city", Value::from("Paris")).await;
        update(&destination, "city", Value::from("Berlin")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        assert_eq!(
            source
                .row_conflicts("people", &Row::new(vec![uuid_value(1)]))
                .await
                .unwrap(),
            vec!["city"]
        );

        source
            .resolve_row(
                "people",
                &Row::new(vec![uuid_value(1)]),
                vec![("city".into(), Value::from("Paris"))],
            )
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();

        let expected = Some(Row::new(vec![
            uuid_value(2),
            Value::from("Grace"),
            Value::from("London"),
        ]));
        assert_eq!(
            source.index_lookup("people_city", &london).await.unwrap(),
            expected
        );
        assert_eq!(
            destination
                .index_lookup("people_city", &london)
                .await
                .unwrap(),
            source.index_lookup("people_city", &london).await.unwrap()
        );
        assert_eq!(
            people_rows(&source, &["id", "name", "city"]).await,
            people_rows(&destination, &["id", "name", "city"]).await
        );
        assert_eq!(
            source.frontier().await.unwrap(),
            destination.frontier().await.unwrap()
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn tombstoned_incoming_updates_are_superseded() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&source, "name", Value::from("Grace")).await;
        let envelope = source
            .missing_envelopes(&destination.frontier().await.unwrap())
            .await
            .unwrap()
            .pop()
            .unwrap();
        destination
            .execute(vec![Statement::Query(Query::Delete(QueryDelete {
                from: QueryFrom {
                    table: "people".into(),
                    joins: vec![],
                },
                predicate: id(1),
                returning: None,
            }))])
            .await
            .unwrap();
        assert_eq!(
            destination.import_envelope(envelope.clone()).await.unwrap(),
            engine::EnvelopeOutcome::Superseded
        );
        assert_eq!(
            destination.envelope_outcome(envelope.id).await.unwrap(),
            Some(engine::EnvelopeOutcome::Superseded)
        );
        sync(&destination, &source).await.unwrap();
        assert!(people_rows(&destination, &["id"]).await.is_empty());
        assert!(people_rows(&source, &["id"]).await.is_empty());
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn schema_add_column_then_row_mutation_replicates() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        add_column(&source, "role", Value::from("member")).await;
        update(&source, "role", Value::from("admin")).await;
        sync(&source, &destination).await.unwrap();
        assert_eq!(
            people_rows(&destination, &["role"]).await,
            vec![Row::new(vec![Value::from("admin")])]
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn concurrent_add_column_converges_with_defaults_and_later_updates() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        add_column(&source, "role", Value::from("member")).await;
        add_column(&destination, "team", Value::from("core")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let expected = vec![Row::new(vec![Value::from("member"), Value::from("core")])];
        assert_eq!(people_rows(&source, &["role", "team"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["role", "team"]).await,
            people_rows(&source, &["role", "team"]).await
        );

        update(&source, "role", Value::from("admin")).await;
        update(&destination, "team", Value::from("storage")).await;
        sync(&source, &destination).await.unwrap();
        sync(&destination, &source).await.unwrap();
        let expected = vec![Row::new(vec![Value::from("admin"), Value::from("storage")])];
        assert_eq!(people_rows(&source, &["role", "team"]).await, expected);
        assert_eq!(
            people_rows(&destination, &["role", "team"]).await,
            people_rows(&source, &["role", "team"]).await
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn reversed_delivery_retries_dependencies_and_relays_to_a_third_replica() {
    let source_path = database_path();
    let middle_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let middle = replica(&middle_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();

        let mut envelopes = source
            .missing_envelopes(&middle.frontier().await.unwrap())
            .await
            .unwrap();
        envelopes.reverse();
        for envelope in envelopes.clone() {
            middle.import_envelope(envelope).await.unwrap();
        }
        for envelope in envelopes {
            middle.import_envelope(envelope).await.unwrap();
        }
        sync(&middle, &destination).await.unwrap();

        let expected = vec![Row::new(vec![
            uuid_value(1),
            Value::from("Ada"),
            Value::Null,
        ])];
        assert_eq!(
            people_rows(&middle, &["id", "name", "city"]).await,
            expected
        );
        assert_eq!(
            people_rows(&destination, &["id", "name", "city"]).await,
            people_rows(&middle, &["id", "name", "city"]).await
        );
        assert_eq!(
            source.frontier().await.unwrap(),
            middle.frontier().await.unwrap()
        );
        assert_eq!(
            middle.frontier().await.unwrap(),
            destination.frontier().await.unwrap()
        );
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(middle_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}

#[test]
fn table_tombstone_supersedes_a_late_row_update() {
    let source_path = database_path();
    let destination_path = database_path();
    block_on(async {
        let source = replica(&source_path);
        let destination = replica(&destination_path);
        source.create_table(people_schema()).await.unwrap();
        source
            .execute(vec![Statement::Query(Query::Insert(QueryInsert {
                table: "people".into(),
                row: Row::new(vec![uuid_value(1), Value::from("Ada"), Value::Null]),
                returning: None,
            }))])
            .await
            .unwrap();
        sync(&source, &destination).await.unwrap();

        update(&destination, "name", Value::from("Grace")).await;
        let update = destination
            .missing_envelopes(&source.frontier().await.unwrap())
            .await
            .unwrap()
            .into_iter()
            .find(|envelope| envelope.changes.iter().any(|change| change.value.is_some()))
            .unwrap();
        source.drop_table("people").await.unwrap();

        assert_eq!(
            source.import_envelope(update.clone()).await.unwrap(),
            engine::EnvelopeOutcome::Superseded
        );
        assert_eq!(
            source.envelope_outcome(update.id).await.unwrap(),
            Some(engine::EnvelopeOutcome::Superseded)
        );
        sync(&source, &destination).await.unwrap();
        assert!(destination.table_schema("people").await.is_err());
        assert!(source.table_schema("people").await.is_err());
    });
    std::fs::remove_file(source_path).unwrap();
    std::fs::remove_file(destination_path).unwrap();
}
